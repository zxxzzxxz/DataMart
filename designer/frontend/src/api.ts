import type {
  CheckSqlResult,
  DagMutationResult,
  DagRun,
  DagVersion,
  DagVersionDetail,
  DesignerDag,
  DagSummary,
  NodeType,
  TaskStatus,
  ValidationError,
} from './types';
import { isMockEnabled, mockRequest } from './mock';

const TOKEN_KEY = 'dag_designer_token';

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

export { isMockEnabled as isMock } from './mock';

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

// 401 时由 App 注册的回调弹出 token 输入框
let unauthorizedHandler: (() => void) | null = null;
export function setUnauthorizedHandler(fn: () => void): void {
  unauthorizedHandler = fn;
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  if (isMockEnabled()) {
    return mockRequest<T>(method, path, body);
  }
  const headers: Record<string, string> = {};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const token = getToken();
  if (token) headers['Authorization'] = `Bearer ${token}`;

  let resp: Response;
  try {
    resp = await fetch(`/api${path}`, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  } catch {
    throw new ApiError(0, '无法连接后端服务');
  }

  if (resp.status === 401) {
    unauthorizedHandler?.();
    throw new ApiError(401, '未授权，请设置访问令牌');
  }
  if (!resp.ok) {
    let parsed: unknown;
    try {
      parsed = await resp.json();
    } catch {
      parsed = undefined;
    }
    const msg =
      (parsed as { detail?: string; message?: string })?.detail ||
      (parsed as { message?: string })?.message ||
      `请求失败（HTTP ${resp.status}）`;
    throw new ApiError(resp.status, msg, parsed);
  }
  if (resp.status === 204) return undefined as T;
  const text = await resp.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

export function errStatus(e: unknown): number {
  const s = (e as { status?: unknown })?.status;
  return typeof s === 'number' ? s : -1;
}

export function errMessage(e: unknown): string {
  return (e as { message?: string })?.message || String(e);
}

// 422 响应体里的 errors 数组（契约：body.errors）
export function errBodyErrors(e: unknown): ValidationError[] | null {
  const errs = (e as { body?: { errors?: unknown } })?.body?.errors;
  return Array.isArray(errs) ? (errs as ValidationError[]) : null;
}

// 错误响应体本身（如 409 删除冲突的 {detail, active_runs}）
export function errBody(e: unknown): Record<string, unknown> | null {
  const b = (e as { body?: unknown })?.body;
  return b && typeof b === 'object' ? (b as Record<string, unknown>) : null;
}

export const api = {
  health: () => request<{ ok: boolean }>('GET', '/health'),
  nodeTypes: () => request<NodeType[]>('GET', '/node-types'),
  listDags: () => request<DagSummary[]>('GET', '/dags'),
  getDag: (dagId: string) =>
    request<DesignerDag & { mtime: number; revision: string }>('GET', `/dags/${encodeURIComponent(dagId)}`),
  createDag: (dag: DesignerDag) => request<unknown>('POST', '/dags', dag),
  updateDag: (dagId: string, dag: DesignerDag, clientRevision: string) =>
    request<DagMutationResult>('PUT', `/dags/${encodeURIComponent(dagId)}`, { ...dag, client_revision: clientRevision }),
  deleteDag: (dagId: string, force = false) =>
    request<unknown>('DELETE', `/dags/${encodeURIComponent(dagId)}${force ? '?force=true' : ''}`),
  validate: (dag: DesignerDag) => request<{ ok: boolean; errors: ValidationError[] }>('POST', '/validate', dag),

  // ---------- 运行态 ----------
  listRuns: (dagId: string, limit = 10) =>
    request<{ runs: DagRun[] }>('GET', `/dags/${encodeURIComponent(dagId)}/runs?limit=${limit}`),
  triggerDag: (dagId: string, conf?: Record<string, unknown>) =>
    request<{ run_id: string; state: string }>(
      'POST',
      `/dags/${encodeURIComponent(dagId)}/trigger`,
      conf ? { conf } : {},
    ),
  listTasks: (dagId: string, runId: string) =>
    request<{ tasks: TaskStatus[] }>(
      'GET',
      `/dags/${encodeURIComponent(dagId)}/runs/${encodeURIComponent(runId)}/tasks`,
    ),

  // ---------- 版本历史 ----------
  listVersions: (dagId: string) =>
    request<{ versions: DagVersion[] }>('GET', `/dags/${encodeURIComponent(dagId)}/versions`),
  getVersion: (dagId: string, versionId: string) =>
    request<DagVersionDetail>(
      'GET',
      `/dags/${encodeURIComponent(dagId)}/versions/${encodeURIComponent(versionId)}`,
    ),
  rollbackDag: (dagId: string, versionId: string, clientRevision: string) =>
    request<{ ok: boolean; version_id: string; restored_from: string }>(
      'POST',
      `/dags/${encodeURIComponent(dagId)}/rollback`,
      { version_id: versionId, client_revision: clientRevision },
    ),

  // ---------- SQL 校验 ----------
  checkSql: (sql: string, connId: string) =>
    request<CheckSqlResult>('POST', '/check-sql', { sql, conn_id: connId }),
};
