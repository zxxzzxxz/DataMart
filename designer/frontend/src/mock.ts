// 后端未就绪时的开发用 mock 实现。
// 开启方式：URL 加 ?mock=1，或构建/开发环境变量 VITE_USE_MOCK=true。
// mock 在内存中维护 DAG 存储，行为遵循 API 契约（409 冲突 / 422 校验失败 / mtime 乐观锁）。

import type { DesignerDag, NodeType, ValidationError } from './types';

export function isMockEnabled(): boolean {
  if (import.meta.env.VITE_USE_MOCK === 'true') return true;
  try {
    return new URLSearchParams(window.location.search).get('mock') === '1';
  } catch {
    return false;
  }
}

const NODE_TYPES: NodeType[] = [
  {
    type: 'sql',
    label: 'SQL 转换',
    fields: [
      { name: 'sql', label: 'SQL 语句', kind: 'textarea', required: true },
      { name: 'conn_id', label: '连接', kind: 'select', options: ['warehouse'], default: 'warehouse', required: false },
    ],
  },
  {
    type: 'sql_check',
    label: '数据校验',
    fields: [
      { name: 'sql', label: '校验 SQL', kind: 'textarea', required: true },
      { name: 'conn_id', label: '连接', kind: 'select', options: ['warehouse'], default: 'warehouse', required: false },
      { name: 'min_rows', label: '最小行数', kind: 'number', default: 1, required: false },
    ],
  },
  {
    type: 'notify',
    label: '通知',
    fields: [
      { name: 'message', label: '消息内容', kind: 'textarea', required: true },
      { name: 'channel', label: '通知渠道', kind: 'select', options: ['email', 'dingtalk'], default: 'email', required: false },
    ],
  },
];

interface StoredDag extends DesignerDag {
  mtime: number;
}

const seedDag: StoredDag = {
  dag_id: 'demo_visual_etl',
  description: '可视化设计器示例 DAG（mock 数据）',
  schedule: '0 2 * * *',
  retries: 2,
  retry_delay_seconds: 300,
  alert_on_failure: true,
  nodes: [
    {
      id: 'extract_orders',
      type: 'sql',
      params: { sql: 'INSERT INTO dm_orders_daily SELECT ...', conn_id: 'warehouse' },
      position: { x: 80, y: 120 },
    },
    {
      id: 'check_orders',
      type: 'sql_check',
      params: { sql: 'SELECT COUNT(*) FROM dm_orders_daily', conn_id: 'warehouse', min_rows: 1 },
      position: { x: 420, y: 120 },
    },
    {
      id: 'notify_done',
      type: 'notify',
      params: { message: '订单日汇总完成', channel: 'email' },
      position: { x: 760, y: 120 },
    },
  ],
  edges: [
    { upstream: 'extract_orders', downstream: 'check_orders' },
    { upstream: 'check_orders', downstream: 'notify_done' },
  ],
  mtime: Date.now() / 1000,
};

let dags: StoredDag[] = [seedDag];

const NODE_ID_RE = /^[a-z][a-z0-9_]*$/;

function mockError(status: number, message: string, body?: unknown): Error {
  // 与 api.ts 的 ApiError 同形（status/body 字段），避免模块间循环依赖
  const err = new Error(message) as Error & { status: number; body?: unknown };
  err.status = status;
  err.body = body;
  return err;
}

function mockValidate(dag: DesignerDag): ValidationError[] {
  const errors: ValidationError[] = [];
  const ids = new Set<string>();

  for (const n of dag.nodes || []) {
    if (!NODE_ID_RE.test(n.id)) {
      errors.push({ node_id: n.id, field: null, message: `节点 id「${n.id}」不合法（需匹配 ^[a-z][a-z0-9_]*$）` });
    }
    if (ids.has(n.id)) {
      errors.push({ node_id: n.id, field: null, message: `节点 id「${n.id}」重复` });
    }
    ids.add(n.id);
    const nt = NODE_TYPES.find(t => t.type === n.type);
    if (!nt) {
      errors.push({ node_id: n.id, field: null, message: `未知节点类型「${n.type}」` });
      continue;
    }
    for (const f of nt.fields) {
      if (!f.required) continue;
      const v = n.params?.[f.name];
      if (v === undefined || v === null || String(v).trim() === '') {
        errors.push({ node_id: n.id, field: f.name, message: `${nt.label}节点「${n.id}」缺少必填参数「${f.label}」` });
      }
    }
  }

  for (const e of dag.edges || []) {
    if (!ids.has(e.upstream)) {
      errors.push({ node_id: null, field: null, message: `连线的上游节点「${e.upstream}」不存在` });
    }
    if (!ids.has(e.downstream)) {
      errors.push({ node_id: null, field: null, message: `连线的下游节点「${e.downstream}」不存在` });
    }
  }

  // 环检测（Kahn 拓扑排序）
  const indeg = new Map<string, number>();
  for (const id of ids) indeg.set(id, 0);
  for (const e of dag.edges || []) {
    if (ids.has(e.upstream) && ids.has(e.downstream)) {
      indeg.set(e.downstream, (indeg.get(e.downstream) || 0) + 1);
    }
  }
  const queue = [...ids].filter(id => (indeg.get(id) || 0) === 0);
  let visited = 0;
  while (queue.length > 0) {
    const cur = queue.pop() as string;
    visited += 1;
    for (const e of dag.edges || []) {
      if (e.upstream !== cur || !ids.has(e.downstream)) continue;
      const d = (indeg.get(e.downstream) || 0) - 1;
      indeg.set(e.downstream, d);
      if (d === 0) queue.push(e.downstream);
    }
  }
  if (visited < ids.size) {
    errors.push({ node_id: null, field: null, message: '依赖关系存在环，DAG 不允许成环' });
  }

  return errors;
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export async function mockRequest<T>(method: string, path: string, body?: unknown): Promise<T> {
  await sleep(120);
  const parts = path.split('/').filter(Boolean);

  if (method === 'GET' && path === '/health') return { ok: true } as T;
  if (method === 'GET' && path === '/node-types') return NODE_TYPES as T;

  if (method === 'GET' && path === '/dags') {
    return dags.map(d => ({
      dag_id: d.dag_id,
      description: d.description,
      schedule: d.schedule,
      node_count: d.nodes.length,
      mtime: d.mtime,
    })) as T;
  }

  if (method === 'POST' && path === '/dags') {
    const payload = body as DesignerDag;
    if (dags.some(d => d.dag_id === payload.dag_id)) {
      throw mockError(409, `dag_id「${payload.dag_id}」已存在`);
    }
    dags.push({ ...JSON.parse(JSON.stringify(payload)), mtime: Date.now() / 1000 });
    return { ok: true } as T;
  }

  if (parts[0] === 'dags' && parts.length === 2) {
    const id = decodeURIComponent(parts[1]);
    const dag = dags.find(d => d.dag_id === id);

    if (method === 'GET') {
      if (!dag) throw mockError(404, `DAG「${id}」不存在`);
      return JSON.parse(JSON.stringify(dag)) as T;
    }

    if (method === 'PUT') {
      if (!dag) throw mockError(404, `DAG「${id}」不存在`);
      const payload = body as DesignerDag & { client_mtime?: number };
      if (payload.client_mtime !== undefined && Math.abs(payload.client_mtime - dag.mtime) > 1e-6) {
        throw mockError(409, '文件已被他人修改');
      }
      const errors = mockValidate(payload);
      if (errors.length > 0) {
        throw mockError(422, '校验失败', { errors });
      }
      const { client_mtime: _ignored, ...rest } = payload;
      Object.assign(dag, JSON.parse(JSON.stringify(rest)), { mtime: Date.now() / 1000 });
      return { ok: true } as T;
    }

    if (method === 'DELETE') {
      dags = dags.filter(d => d.dag_id !== id);
      return { ok: true } as T;
    }
  }

  if (method === 'POST' && path === '/validate') {
    const errors = mockValidate(body as DesignerDag);
    return { ok: errors.length === 0, errors } as T;
  }

  throw mockError(404, `mock 未实现：${method} ${path}`);
}
