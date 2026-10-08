// 后端未就绪时的开发用 mock 实现。
// 开启方式：URL 加 ?mock=1，或构建/开发环境变量 VITE_USE_MOCK=true。
// mock 在内存中维护 DAG 存储，行为遵循 API 契约（409 冲突 / 422 校验失败 / mtime 乐观锁）。

import type { DesignerDag, NodeType, ValidationError } from './types';
import catalog from './generated/node-catalog.json';
import Ajv from 'ajv/dist/2020';
import type { AnySchema } from 'ajv';

export function isMockEnabled(): boolean {
  if (import.meta.env.VITE_USE_MOCK === 'true') return true;
  try {
    return new URLSearchParams(window.location.search).get('mock') === '1';
  } catch {
    return false;
  }
}

const NODE_TYPES = catalog as NodeType[];
const ajv = new Ajv({ allErrors: true, strict: false });
const parameterValidators = new Map(catalog.map(node => [node.type, ajv.compile(node.parameter_schema as AnySchema)]));
let revisionCounter = 0;
function nextRevision(): string { return (++revisionCounter).toString(16).padStart(64, '0'); }

interface StoredDag extends DesignerDag {
  mtime: number;
  revision: string;
}

const seedDag: StoredDag = {
  revision: nextRevision(),
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
      params: { sql: 'SELECT COUNT(*) FROM dm_orders_daily', conn_id: 'warehouse', pass_value: '1' },
      position: { x: 420, y: 120 },
    },
    {
      id: 'notify_done',
      type: 'notify',
      params: { message: '订单日汇总完成' },
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
    const params = { ...n.params };
    for (const field of nt.fields) {
      if (!field.required && (params[field.name] == null || params[field.name] === '')) delete params[field.name];
    }
    if (n.type === 'http' && typeof params.headers === 'string') {
      try { params.headers = JSON.parse(params.headers); } catch { /* schema reports invalid headers */ }
    }
    if (n.type === 'sensor_sql' && params.mode === 'normal') params.mode = 'poke';
    n.params = params;
    const validateParams = parameterValidators.get(n.type);
    if (validateParams && !validateParams(params)) {
      for (const issue of validateParams.errors ?? []) errors.push({ node_id: n.id,
        field: issue.instancePath.slice(1) || String(issue.params.missingProperty ?? issue.params.additionalProperty ?? 'params'),
        message: issue.message ?? '参数不合法' });
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

// 简易 YAML 序列化，用于版本预览展示
function dagToYaml(dag: DesignerDag): string {
  const lines: string[] = [];
  lines.push(`dag_id: ${dag.dag_id}`);
  lines.push(`description: ${dag.description ?? ''}`);
  lines.push(`schedule: ${dag.schedule ?? ''}`);
  lines.push(`retries: ${dag.retries ?? 0}`);
  lines.push('nodes:');
  for (const n of dag.nodes || []) {
    lines.push(`  - id: ${n.id}`);
    lines.push(`    type: ${n.type}`);
    for (const [k, v] of Object.entries(n.params ?? {})) {
      lines.push(`    ${k}: ${JSON.stringify(v)}`);
    }
  }
  lines.push('edges:');
  for (const e of dag.edges || []) {
    lines.push(`  - ${e.upstream} -> ${e.downstream}`);
  }
  return lines.join('\n');
}

interface MockVersion {
  version_id: string;
  saved_at: string;
  size_bytes: number;
  content: string;
  dag: StoredDag;
}

interface MockLiveRun {
  run: {
    run_id: string;
    state: string;
    logical_date: string;
    start_date: string;
    end_date: string | null;
    conf: unknown;
  };
  polled: boolean;
}

const versions: Record<string, MockVersion[]> = {};
const liveRuns: Record<string, MockLiveRun> = {};

function snapshotVersion(dag: StoredDag) {
  const content = dagToYaml(dag);
  const list = (versions[dag.dag_id] ??= []);
  list.unshift({
    version_id: `v${list.length + 1}`,
    saved_at: new Date().toISOString(),
    size_bytes: content.length,
    content,
    dag: JSON.parse(JSON.stringify(dag)) as StoredDag,
  });
}

snapshotVersion(seedDag);

function successTasks(dag: StoredDag) {
  return dag.nodes.map(n => ({ task_id: n.id, state: 'success', try_number: 1, duration: 1.2 }));
}

function seededRun() {
  const now = Date.now();
  return {
    run_id: `scheduled__${new Date(now - 86400e3).toISOString()}`,
    state: 'success',
    logical_date: new Date(now - 86400e3).toISOString(),
    start_date: new Date(now - 86400e3 + 60e3).toISOString(),
    end_date: new Date(now - 86400e3 + 180e3).toISOString(),
    conf: {},
  };
}

export async function mockRequest<T>(method: string, path: string, body?: unknown): Promise<T> {
  await sleep(120);
  // 拆出 query string（如 ?limit=10 / ?force=true）
  const qIndex = path.indexOf('?');
  const query = qIndex >= 0 ? new URLSearchParams(path.slice(qIndex + 1)) : new URLSearchParams();
  const pathOnly = qIndex >= 0 ? path.slice(0, qIndex) : path;
  const parts = pathOnly.split('/').filter(Boolean);

  if (method === 'GET' && pathOnly === '/health') return { ok: true } as T;
  if (method === 'GET' && pathOnly === '/node-types') return NODE_TYPES as T;

  if (method === 'GET' && pathOnly === '/dags') {
    return dags.map(d => ({
      dag_id: d.dag_id,
      description: d.description,
      schedule: d.schedule,
      node_count: d.nodes.length,
      mtime: d.mtime,
      revision: d.revision,
    })) as T;
  }

  if (method === 'POST' && pathOnly === '/dags') {
    const payload = body as DesignerDag;
    if (dags.some(d => d.dag_id === payload.dag_id)) {
      throw mockError(409, `dag_id「${payload.dag_id}」已存在`);
    }
    const stored = { ...JSON.parse(JSON.stringify(payload)), mtime: Date.now() / 1000, revision: nextRevision() } as StoredDag;
    dags.push(stored);
    return { ok: true, mtime: stored.mtime, revision: stored.revision } as T;
  }

  if (method === 'POST' && pathOnly === '/validate') {
    const errors = mockValidate(body as DesignerDag);
    return { ok: errors.length === 0, errors } as T;
  }

  if (method === 'POST' && pathOnly === '/check-sql') {
    const payload = body as { sql?: string; conn_id?: string };
    if (payload.conn_id !== 'warehouse') {
      throw mockError(400, `未知连接「${payload.conn_id}」`);
    }
    if (!payload.sql || String(payload.sql).trim() === '') {
      throw mockError(422, 'SQL 不能为空');
    }
    return { ok: true, duration_ms: 8 + Math.round(Math.random() * 20) } as T;
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
      const payload = body as DesignerDag & { client_mtime?: number; client_revision?: string };
      if (!payload.client_revision && payload.client_mtime === undefined) throw mockError(422, '缺少版本');
      if (payload.client_revision ? payload.client_revision !== dag.revision : Math.abs(payload.client_mtime! - dag.mtime) > 1e-6) {
        throw mockError(409, '文件已被他人修改');
      }
      const errors = mockValidate(payload);
      if (errors.length > 0) {
        throw mockError(422, '校验失败', { errors });
      }
      const { client_mtime: _ignored, client_revision: _revision, ...rest } = payload;
      snapshotVersion(dag);
      Object.assign(dag, JSON.parse(JSON.stringify(rest)), { mtime: Date.now() / 1000, revision: nextRevision() });
      return { ok: true, mtime: dag.mtime, revision: dag.revision } as T;
    }

    if (method === 'DELETE') {
      if (!dag) throw mockError(404, `DAG「${id}」不存在`);
      // 模拟进行中的运行：id 以 _busy 结尾时返回 409，force=true 可强制删除
      if (id.endsWith('_busy') && query.get('force') !== 'true') {
        throw mockError(409, `DAG「${id}」存在进行中的运行，无法删除`, {
          detail: `DAG「${id}」存在进行中的运行，无法删除`,
          active_runs: 2,
        });
      }
      dags = dags.filter(d => d.dag_id !== id);
      delete versions[id];
      delete liveRuns[id];
      return { ok: true } as T;
    }
  }

  // ---------- 运行态 ----------
  if (parts[0] === 'dags' && parts.length === 3 && parts[2] === 'runs' && method === 'GET') {
    const id = decodeURIComponent(parts[1]);
    const dag = dags.find(d => d.dag_id === id);
    if (!dag) throw mockError(404, `DAG「${id}」不存在`);
    const live = liveRuns[id];
    if (live) {
      return { runs: [live.run] } as T;
    }
    return { runs: [seededRun()] } as T;
  }

  if (parts[0] === 'dags' && parts.length === 3 && parts[2] === 'trigger' && method === 'POST') {
    const id = decodeURIComponent(parts[1]);
    const dag = dags.find(d => d.dag_id === id);
    if (!dag) throw mockError(404, `DAG「${id}」不存在`);
    const now = new Date().toISOString();
    const run = {
      run_id: `manual__${now}`,
      state: 'running',
      logical_date: now,
      start_date: now,
      end_date: null,
      conf: (body as { conf?: unknown })?.conf ?? {},
    };
    liveRuns[id] = { run, polled: false };
    return { run_id: run.run_id, state: run.state } as T;
  }

  if (parts[0] === 'dags' && parts.length === 5 && parts[2] === 'runs' && parts[4] === 'tasks' && method === 'GET') {
    const id = decodeURIComponent(parts[1]);
    const runId = decodeURIComponent(parts[3]);
    const dag = dags.find(d => d.dag_id === id);
    if (!dag) throw mockError(404, `DAG「${id}」不存在`);
    const live = liveRuns[id];
    if (live && live.run.run_id === runId) {
      if (!live.polled) {
        // 第一次拉取：第一个任务 running，其余 queued（演示轮询）
        live.polled = true;
        return {
          tasks: dag.nodes.map((n, i) => ({
            task_id: n.id,
            state: i === 0 ? 'running' : 'queued',
            try_number: 1,
            duration: null,
          })),
        } as T;
      }
      live.run.state = 'success';
      live.run.end_date = new Date().toISOString();
      return { tasks: successTasks(dag) } as T;
    }
    return { tasks: successTasks(dag) } as T;
  }

  // ---------- 版本历史 ----------
  if (parts[0] === 'dags' && parts.length === 3 && parts[2] === 'versions' && method === 'GET') {
    const id = decodeURIComponent(parts[1]);
    if (!dags.some(d => d.dag_id === id)) throw mockError(404, `DAG「${id}」不存在`);
    return { versions: (versions[id] ?? []).map(({ dag: _d, ...rest }) => rest) } as T;
  }

  if (parts[0] === 'dags' && parts.length === 4 && parts[2] === 'versions' && method === 'GET') {
    const id = decodeURIComponent(parts[1]);
    const vid = decodeURIComponent(parts[3]);
    const v = (versions[id] ?? []).find(x => x.version_id === vid);
    if (!v) throw mockError(404, `版本「${vid}」不存在`);
    const { dag: _d, ...rest } = v;
    return rest as T;
  }

  if (parts[0] === 'dags' && parts.length === 3 && parts[2] === 'rollback' && method === 'POST') {
    const id = decodeURIComponent(parts[1]);
    const dag = dags.find(d => d.dag_id === id);
    if (!dag) throw mockError(404, `DAG「${id}」不存在`);
    const vid = (body as { version_id?: string })?.version_id ?? '';
    const v = (versions[id] ?? []).find(x => x.version_id === vid);
    if (!v) throw mockError(404, `版本「${vid}」不存在`);
    const request = body as { client_revision?: string };
    if (!request.client_revision) throw mockError(422, '缺少版本');
    if (request.client_revision !== dag.revision) throw mockError(409, '文件已被他人修改');
    const restored = JSON.parse(JSON.stringify(v.dag));
    snapshotVersion(dag);
    Object.assign(dag, restored, { mtime: Date.now() / 1000, revision: nextRevision() });
    return { ok: true, version_id: vid, restored_from: vid, mtime: dag.mtime, revision: dag.revision } as T;
  }

  throw mockError(404, `mock 未实现：${method} ${path}`);
}
