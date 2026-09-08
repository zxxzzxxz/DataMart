import { useCallback, useEffect, useState } from 'react';
import { api, errBodyErrors, errMessage, errStatus, isMock } from '../api';
import type { DagSummary } from '../types';

const DAG_ID_RE = /^[a-z][a-z0-9_]*$/;

interface Props {
  onOpen: (dagId: string) => void;
}

// DAG 列表视图（首页）
export default function DagListView({ onOpen }: Props) {
  const [dags, setDags] = useState<DagSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [health, setHealth] = useState<boolean | null>(null);
  const [showCreate, setShowCreate] = useState(false);

  const reload = useCallback(() => {
    setLoading(true);
    setError('');
    api
      .listDags()
      .then(setDags)
      .catch(e => setError(errMessage(e)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    reload();
    api
      .health()
      .then(h => setHealth(!!h?.ok))
      .catch(() => setHealth(false));
  }, [reload]);

  const remove = async (dagId: string) => {
    if (!window.confirm(`确定删除 DAG「${dagId}」吗？此操作不可恢复。`)) return;
    try {
      await api.deleteDag(dagId);
      reload();
    } catch (e) {
      alert('删除失败：' + errMessage(e));
    }
  };

  const openInAirflow = (dagId: string) => {
    window.open(`http://${window.location.hostname}:8080/dags/${encodeURIComponent(dagId)}`, '_blank');
  };

  return (
    <div className="page">
      <div className="page-header">
        <h1>
          DAG 可视化设计器
          {isMock() && <span className="badge-mock" style={{ marginLeft: 10 }}>mock 数据</span>}
        </h1>
        <div className="header-right">
          {health !== null && (
            <span className="health">
              <span className={`health-dot ${health ? 'ok' : 'bad'}`} />
              {health ? '后端正常' : '后端不可用'}
            </span>
          )}
          <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
            + 新建 DAG
          </button>
        </div>
      </div>

      {loading ? (
        <div className="empty-box">加载中…</div>
      ) : error ? (
        <div className="empty-box">
          <p>加载失败：{error}</p>
          <button className="btn" onClick={reload}>
            重试
          </button>
        </div>
      ) : dags.length === 0 ? (
        <div className="empty-box">暂无 DAG，点击右上角「新建 DAG」开始</div>
      ) : (
        <table className="tbl">
          <thead>
            <tr>
              <th>DAG ID</th>
              <th>描述</th>
              <th>调度</th>
              <th>节点数</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {dags.map(d => (
              <tr key={d.dag_id}>
                <td className="dag-id">{d.dag_id}</td>
                <td>{d.description}</td>
                <td>{d.schedule ? d.schedule : <span className="tag-manual">手动触发</span>}</td>
                <td>{d.node_count}</td>
                <td>
                  <button className="btn-link" onClick={() => onOpen(d.dag_id)}>
                    打开
                  </button>
                  <button className="btn-link" onClick={() => openInAirflow(d.dag_id)}>
                    在 Airflow 打开
                  </button>
                  <button className="btn-link danger" onClick={() => remove(d.dag_id)}>
                    删除
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {showCreate && (
        <CreateDagModal onClose={() => setShowCreate(false)} onCreated={onOpen} />
      )}
    </div>
  );
}

// 新建 DAG 弹窗
function CreateDagModal({ onClose, onCreated }: { onClose: () => void; onCreated: (dagId: string) => void }) {
  const [dagId, setDagId] = useState('');
  const [description, setDescription] = useState('');
  const [schedule, setSchedule] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setErr('');
    if (!DAG_ID_RE.test(dagId)) {
      setErr('DAG ID 需以小写字母开头，只能包含小写字母、数字和下划线');
      return;
    }
    setBusy(true);
    try {
      await api.createDag({
        dag_id: dagId,
        description,
        schedule: schedule.trim() === '' ? null : schedule.trim(),
        retries: 1,
        retry_delay_seconds: 300,
        alert_on_failure: true,
        nodes: [],
        edges: [],
      });
      onCreated(dagId);
    } catch (e) {
      const st = errStatus(e);
      const bodyErrs = errBodyErrors(e);
      if (st === 409) {
        setErr(`DAG ID「${dagId}」已存在，请换一个`);
      } else if (st === 422 && bodyErrs) {
        setErr(bodyErrs.map(x => x.message).join('；'));
      } else {
        setErr(errMessage(e));
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-mask">
      <div className="modal">
        <h3>新建 DAG</h3>
        <div className="field">
          <label>
            DAG ID<span className="required-star">*</span>
          </label>
          <input
            type="text"
            value={dagId}
            autoFocus
            placeholder="如 orders_daily"
            onChange={e => setDagId(e.target.value)}
          />
        </div>
        <div className="field">
          <label>描述</label>
          <input type="text" value={description} onChange={e => setDescription(e.target.value)} />
        </div>
        <div className="field">
          <label>调度（cron 表达式）</label>
          <input
            type="text"
            value={schedule}
            placeholder="留空 = 手动触发，如 0 2 * * *"
            onChange={e => setSchedule(e.target.value)}
          />
        </div>
        {err && <div className="error-text">{err}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>
            取消
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={busy}>
            {busy ? '创建中…' : '创建'}
          </button>
        </div>
      </div>
    </div>
  );
}
