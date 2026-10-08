import { useEffect, useState } from 'react';
import { api, errMessage } from '../api';

type CheckResult = { ok: true; durationMs: number } | { ok: false; error: string };

interface Props {
  sql: string;
  connId: string;
}

// SQL 语法校验：结果内联展示，错误摘要可展开看全文
export default function SqlCheckButton({ sql, connId }: Props) {
  const [result, setResult] = useState<CheckResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState(false);

  // SQL 或连接变化时重置结果
  useEffect(() => {
    setResult(null);
    setExpanded(false);
  }, [sql, connId]);

  const run = async () => {
    setBusy(true);
    try {
      const res = await api.checkSql(sql, connId);
      setResult(res.ok ? { ok: true, durationMs: res.duration_ms ?? 0 } : { ok: false, error: res.error ?? '未知错误' });
    } catch (e) {
      setResult({ ok: false, error: errMessage(e) });
    } finally {
      setBusy(false);
    }
  };

  const summary = result && !result.ok ? result.error.split('\n')[0].slice(0, 120) : '';
  const hasMore = result && !result.ok ? result.error.length > summary.length : false;

  return (
    <div className="sql-check">
      <button className="btn" onClick={run} disabled={busy}>
        {busy ? '校验中…' : '校验 SQL'}
      </button>
      {result?.ok && <div className="sql-check-ok">✓ 语法有效（{result.durationMs} ms）</div>}
      {result && !result.ok && (
        <div className="sql-check-err">
          <div>✗ {summary || '语法错误'}</div>
          {hasMore && (
            <button className="btn-link" onClick={() => setExpanded(v => !v)}>
              {expanded ? '收起全文' : '查看全文'}
            </button>
          )}
          {expanded && <pre>{result.error}</pre>}
        </div>
      )}
    </div>
  );
}
