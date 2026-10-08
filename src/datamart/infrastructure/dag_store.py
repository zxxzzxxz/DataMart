"""Versioned YAML repository. One OS file lock covers every per-DAG operation."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from filelock import FileLock

from datamart.workflows import convert
from datamart.workflows.schema import DesignerDag

log = logging.getLogger(__name__)
_ID_RE = re.compile(r"[a-z][a-z0-9_]*")
_VERSION_RE = re.compile(r"(\d{8}T\d{6}Z)(?:-(\d+))?")
_VERSION_TS_FORMAT = "%Y%m%dT%H%M%SZ"
_HISTORY_KEEP = 20


class MtimeConflictError(Exception):
    """The requested version no longer matches the current document."""


@dataclass(frozen=True)
class DagDocument:
    dag: DesignerDag
    mtime: float
    revision: str

    def metadata(self) -> dict:
        return {"mtime": self.mtime, "revision": self.revision}


class DagStore:
    def __init__(self, dags_dir: str, history_dir=None, *, lock_timeout=10):
        self.dir = Path(dags_dir) / "designer"
        self.history_dir = Path(history_dir or os.environ.get("DESIGNER_HISTORY_DIR", "/data/history"))
        self.lock_dir = self.dir.parent / ".designer-locks"
        self.lock_timeout = lock_timeout

    def _path(self, dag_id: str) -> Path:
        if not _ID_RE.fullmatch(dag_id):
            raise ValueError(f"非法 dag_id：{dag_id}")
        return self.dir / f"{dag_id}.yaml"

    def _lock(self, dag_id):
        self._path(dag_id)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        # Never unlink lock files: replacing an inode would split concurrent owners.
        return FileLock(self.lock_dir / f"{dag_id}.lock", timeout=self.lock_timeout)

    def _history_path(self, dag_id, version_id):
        self._path(dag_id)
        if not _VERSION_RE.fullmatch(version_id):
            raise ValueError(f"非法 version_id：{version_id}")
        return self.history_dir / dag_id / f"{version_id}.yaml"

    @staticmethod
    def _revision(raw: bytes):
        return hashlib.sha256(raw).hexdigest()

    def _read(self, path):
        with path.open("rb") as handle:
            raw = handle.read()
            modified = os.fstat(handle.fileno()).st_mtime
        return DagDocument(convert.from_yaml_text(raw.decode("utf-8")), modified, self._revision(raw))

    def load_document(self, dag_id):
        with self._lock(dag_id):
            return self._read(self._path(dag_id))

    def load(self, dag_id):
        document = self.load_document(dag_id)
        return document.dag, document.mtime

    def exists(self, dag_id):
        with self._lock(dag_id):
            return self._path(dag_id).exists()

    def list(self):
        documents = []
        for path in sorted(self.dir.glob("*.yaml")):
            try:
                document = self.load_document(path.stem)
            except FileNotFoundError:
                continue
            except ValueError:
                log.warning("跳过损坏的设计器 YAML：%s", path, exc_info=True)
                continue
            documents.append(
                {
                    "dag_id": document.dag.dag_id,
                    "description": document.dag.description,
                    "schedule": document.dag.schedule,
                    "node_count": len(document.dag.nodes),
                    **document.metadata(),
                }
            )
        return documents

    def create_document(self, dag):
        with self._lock(dag.dag_id):
            path = self._path(dag.dag_id)
            if path.exists():
                raise FileExistsError(dag.dag_id)
            return self._write(path, convert.to_yaml_text(dag))

    def create(self, dag):
        return self.create_document(dag).mtime

    @staticmethod
    def _check_version(document, client_revision, client_mtime):
        if client_revision is None and client_mtime is None:
            raise ValueError("更新必须携带 client_revision 或 client_mtime")
        if client_revision is not None:
            matches = client_revision == document.revision
        else:
            matches = abs(document.mtime - client_mtime) <= 1e-6
        if not matches:
            raise MtimeConflictError("文件已被他人修改，请刷新后重试（版本冲突）")

    def update_document(self, dag, *, client_revision=None, client_mtime=None):
        with self._lock(dag.dag_id):
            path = self._path(dag.dag_id)
            current = self._read(path)
            self._check_version(current, client_revision, client_mtime)
            new_text = convert.to_yaml_text(dag)
            if self._revision(new_text.encode()) == current.revision:
                return current
            self._snapshot(dag.dag_id, path.read_text(encoding="utf-8"))
            document = self._write(path, new_text)
            self._prune(dag.dag_id)
            return document

    def update(self, dag, client_mtime=None, *, client_revision=None):
        return self.update_document(dag, client_revision=client_revision, client_mtime=client_mtime).mtime

    def delete(self, dag_id):
        with self._lock(dag_id):
            self._path(dag_id).unlink()

    @staticmethod
    def _version_sort_key(path):
        match = _VERSION_RE.fullmatch(path.stem)
        return match.group(1), int(match.group(2) or 0)

    def _version_files(self, dag_id):
        directory = self.history_dir / dag_id
        if not directory.is_dir():
            return []
        return sorted(
            (p for p in directory.glob("*.yaml") if _VERSION_RE.fullmatch(p.stem)), key=self._version_sort_key
        )

    def _snapshot(self, dag_id, text):
        directory = self.history_dir / dag_id
        directory.mkdir(parents=True, exist_ok=True)
        stem = datetime.now(UTC).strftime(_VERSION_TS_FORMAT)
        index = 0
        while True:
            version_id = stem if index == 0 else f"{stem}-{index}"
            try:
                with (directory / f"{version_id}.yaml").open("x", encoding="utf-8") as handle:
                    handle.write(text)
                    handle.flush()
                    os.fsync(handle.fileno())
                return version_id
            except FileExistsError:
                index += 1

    def _prune(self, dag_id):
        for path in self._version_files(dag_id)[:-_HISTORY_KEEP]:
            path.unlink()

    def list_versions(self, dag_id):
        with self._lock(dag_id):
            return [
                {
                    "version_id": p.stem,
                    "saved_at": datetime.strptime(self._version_sort_key(p)[0], _VERSION_TS_FORMAT)
                    .replace(tzinfo=UTC)
                    .isoformat(),
                    "size_bytes": p.stat().st_size,
                }
                for p in reversed(self._version_files(dag_id))
            ]

    def get_version(self, dag_id, version_id):
        with self._lock(dag_id):
            path = self._history_path(dag_id, version_id)
            text = path.read_text(encoding="utf-8")
            return {
                "version_id": version_id,
                "content": text,
                "size_bytes": path.stat().st_size,
                "saved_at": datetime.strptime(self._version_sort_key(path)[0], _VERSION_TS_FORMAT)
                .replace(tzinfo=UTC)
                .isoformat(),
            }

    def restore_document(self, dag_id, version_id, *, client_revision=None, client_mtime=None):
        with self._lock(dag_id):
            path = self._path(dag_id)
            current = self._read(path)
            self._check_version(current, client_revision, client_mtime)
            # Read and validate the target before any history retention can remove it.
            text = self._history_path(dag_id, version_id).read_text(encoding="utf-8")
            convert.from_yaml_text(text)
            snapshot_id = self._snapshot(dag_id, path.read_text(encoding="utf-8"))
            document = self._write(path, text)
            self._prune(dag_id)
            return document, snapshot_id, version_id

    def restore(self, dag_id, version_id, *, client_revision=None, client_mtime=None):
        _document, snapshot_id, restored_from = self.restore_document(
            dag_id,
            version_id,
            client_revision=client_revision,
            client_mtime=client_mtime,
        )
        return snapshot_id, restored_from

    def _write(self, path, text):
        old_ns = path.stat().st_mtime_ns if path.exists() else 0
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o644)
            # Make legacy mtime tokens advance as well; new clients use content revisions.
            timestamp = max(os.stat(temporary).st_mtime_ns, old_ns + 1_000_000)
            os.utime(temporary, ns=(timestamp, timestamp))
            os.replace(temporary, path)
            return self._read(path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
