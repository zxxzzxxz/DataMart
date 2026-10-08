import { useEffect, useState } from 'react';
import { api, errMessage } from '../api';
import type { DagVersion, DagVersionDetail } from '../types';

interface Props {
  dagId: string;
  onClose: () => void;
  onRolledBack: (versionId: string) => void;
}

function fmtTime(v: unknown): string {
  if (v === null || v === undefined) return '-';
  if (typeof v === 'number') return new Date(v > 1e12 ? v : v * 1000).toLocaleString();
  const s = String(v);
  if (/^\d+(\.\d+)?$/.test(s)) return new Date(Number(s) > 1e12 ? Number(s) : Number(s) * 1000).toLocaleString();
  const d = new Date(s);
  return isNaN(d.getTime()) ? s : d.toLocaleString();
}

function fmtSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
}

// 版本历史弹窗：列表 + 只读预览 + 二次确认回滚
export default function HistoryModal({ dagId, onClose, onRolledBack }: Props) {
  const [versions, setVersions] = useState<DagVersion[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [preview, setPreview] = useState<DagVersionDetail | null>(null);
  const [previewFor, setPreviewFor] = useState('');
  const [confirmId, setConfirmId] = useState('');
  const [rolling, setRolling] = useState(false);
  const [revision, setRevision] = useState('');

  useEffect(() => {
    let cancelled = false;
    Promise.all([api.listVersions(dagId), api.getDag(dagId)])
      .then(([res, current]) => {
        if (!cancelled) {
          setVersions(res.versions ?? []);
          setRevision(current.revision);
        }
      })
      .catch(e => {
        if (!cancelled) setError(errMessage(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dagId]);

  const loadPreview = async (v: DagVersion) => {
    setError('');
    setPreviewFor(v.version_id);
    try {
      const detail = await api.getVersion(dagId, v.version_id);
      setPreview(detail);
    } catch (e) {
      setError(errMessage(e));
    } finally {
      setPreviewFor('');
    }
  };

  const doRollback = async (versionId: string) => {
    setRolling(true);
    setError('');
    try {
      await api.rollbackDag(dagId, versionId, revision);
      onRolledBack(versionId);
    } catch (e) {
      setError(errMessage(e));
    } finally {
      setRolling(false);
    }
  };

  return (
    <div className="modal-mask">
      <div className="modal modal-wide">
        <h3>版本历史（{dagId}）</h3>
        {loading ? (
          <div className="muted">加载中…</div>
        ) : versions.length === 0 ? (
          <div className="muted">暂无历史版本</div>
        ) : (
          <div className="version-list">
            {versions.map(v => (
              <div className="version-row" key={v.version_id}>
                <div className="version-meta">
                  <span className="version-id">{v.version_id}</span>
                  <span>{fmtTime(v.saved_at)}</span>
                  <span className="muted">{fmtSize(v.size_bytes)}</span>
                </div>
                <div className="version-actions">
                  <button className="btn-link" onClick={() => loadPreview(v)} disabled={previewFor === v.version_id}>
                    {previewFor === v.version_id ? '加载中…' : '预览'}
                  </button>
                  {confirmId === v.version_id ? (
                    <>
                      <button className="btn-link danger" onClick={() => doRollback(v.version_id)} disabled={rolling}>
                        {rolling ? '回滚中…' : '确认回滚'}
                      </button>
                      <button className="btn-link" onClick={() => setConfirmId('')} disabled={rolling}>
                        取消
                      </button>
                    </>
                  ) : (
                    <button className="btn-link danger" onClick={() => setConfirmId(v.version_id)}>
                      回滚
                    </button>
                  )}
                </div>
                {confirmId === v.version_id && (
                  <div className="version-confirm">将用该版本内容覆盖当前画布（未保存的改动会丢失），确认回滚？</div>
                )}
              </div>
            ))}
          </div>
        )}
        {preview && (
          <div className="preview-box">
            <div className="preview-head">
              <span>
                版本 {preview.version_id} 内容（只读） · {fmtTime(preview.saved_at)}
              </span>
              <button className="btn-link" onClick={() => setPreview(null)}>
                收起
              </button>
            </div>
            <pre className="code-preview">{preview.content}</pre>
          </div>
        )}
        {error && <div className="error-text">{error}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}
