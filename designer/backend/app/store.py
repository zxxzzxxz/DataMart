"""设计器 YAML 文件存储：dags/designer/*.yaml 的 CRUD（YAML 是唯一事实源）。"""

import logging
import os
import re
import tempfile
from pathlib import Path

from dag_designer import convert
from dag_designer.schema import DesignerDag

log = logging.getLogger(__name__)

_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
# 乐观锁 mtime 比较容忍度（float 经 JSON 往返理论上精确，留一点余量防截断）
_MTIME_EPS = 1e-6


class MtimeConflictError(Exception):
    """文件 mtime 与客户端持有的不一致（并发编辑冲突）。"""


class DagStore:
    def __init__(self, dags_dir: str) -> None:
        self.dir = Path(dags_dir) / "designer"
        self.dir.mkdir(parents=True, exist_ok=True)

    def _path(self, dag_id: str) -> Path:
        # dag_id 已经过 schema 校验（纯小写标识符），这里再兜底防路径穿越
        if not _ID_RE.match(dag_id):
            raise ValueError(f"非法 dag_id：{dag_id}")
        return self.dir / f"{dag_id}.yaml"

    def list(self) -> list[dict]:
        """列出全部设计器 DAG 摘要；损坏的 YAML 跳过并记日志（不拖垮列表）。"""
        out = []
        for path in sorted(self.dir.glob("*.yaml")):
            try:
                dag = convert.load_yaml_file(path)
            except Exception:
                log.warning("跳过损坏的设计器 YAML：%s", path, exc_info=True)
                continue
            out.append(
                {
                    "dag_id": dag.dag_id,
                    "description": dag.description,
                    "schedule": dag.schedule,
                    "node_count": len(dag.nodes),
                    "mtime": path.stat().st_mtime,
                }
            )
        return out

    def load(self, dag_id: str) -> tuple[DesignerDag, float]:
        path = self._path(dag_id)
        if not path.exists():
            raise FileNotFoundError(dag_id)
        return convert.load_yaml_file(path), path.stat().st_mtime

    def create(self, dag: DesignerDag) -> float:
        path = self._path(dag.dag_id)
        if path.exists():
            raise FileExistsError(dag.dag_id)
        return self._write(path, dag)

    def update(self, dag: DesignerDag, client_mtime: float | None) -> float:
        path = self._path(dag.dag_id)
        if not path.exists():
            raise FileNotFoundError(dag.dag_id)
        current = path.stat().st_mtime
        if client_mtime is not None and abs(current - client_mtime) > _MTIME_EPS:
            raise MtimeConflictError(dag.dag_id)
        return self._write(path, dag)

    def delete(self, dag_id: str) -> None:
        path = self._path(dag_id)
        if not path.exists():
            raise FileNotFoundError(dag_id)
        path.unlink()

    @staticmethod
    def _write(path: Path, dag: DesignerDag) -> float:
        """先写临时文件再原子替换，避免 dag-processor 读到写了一半的 YAML。

        mkstemp 默认权限 0600，而 dag-processor 容器以 uid 50000 运行（读不到 root 的
        0600 文件），replace 前必须放宽为 0644。
        """
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(convert.to_yaml_text(dag))
            os.chmod(tmp, 0o644)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return path.stat().st_mtime
