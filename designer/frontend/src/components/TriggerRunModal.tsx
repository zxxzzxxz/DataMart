import { useState } from 'react';
import { api, errMessage } from '../api';

interface Props {
  dagId: string;
  onClose: () => void;
  onTriggered: (runId: string) => void;
}

// 触发运行弹窗：conf JSON 可留空，格式错误前端拦截
export default function TriggerRunModal({ dagId, onClose, onTriggered }: Props) {
  const [conf, setConf] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setError('');
    const trimmed = conf.trim();
    let parsed: Record<string, unknown> | undefined;
    if (trimmed) {
      try {
        parsed = JSON.parse(trimmed) as Record<string, unknown>;
      } catch {
        setError('conf 不是合法的 JSON，请检查格式（如 {"date": "2024-01-01"}）');
        return;
      }
    }
    setBusy(true);
    try {
      const res = await api.triggerDag(dagId, parsed);
      onTriggered(res.run_id);
    } catch (e) {
      setError(errMessage(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-mask">
      <div className="modal">
        <h3>触发运行（{dagId}）</h3>
        <div className="field">
          <label>运行参数（conf，JSON，可留空）</label>
          <textarea
            rows={6}
            value={conf}
            placeholder='{"date": "2024-01-01"}'
            onChange={e => setConf(e.target.value)}
          />
        </div>
        {error && <div className="error-text">{error}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>
            取消
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={busy}>
            {busy ? '触发中…' : '确认触发'}
          </button>
        </div>
      </div>
    </div>
  );
}
