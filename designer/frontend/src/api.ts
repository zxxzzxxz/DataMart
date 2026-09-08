import type { DesignerDag, DagSummary, NodeType, ValidationError } from './types';
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

export const api = {
  health: () => request<{ ok: boolean }>('GET', '/health'),
  nodeTypes: () => request<NodeType[]>('GET', '/node-types'),
  listDags: () => request<DagSummary[]>('GET', '/dags'),
  getDag: (dagId: string) =>
    request<DesignerDag & { mtime: number }>('GET', `/dags/${encodeURIComponent(dagId)}`),
  createDag: (dag: DesignerDag) => request<unknown>('POST', '/dags', dag),
  updateDag: (dagId: string, dag: DesignerDag, clientMtime: number) =>
    request<unknown>('PUT', `/dags/${encodeURIComponent(dagId)}`, { ...dag, client_mtime: clientMtime }),
  deleteDag: (dagId: string) => request<unknown>('DELETE', `/dags/${encodeURIComponent(dagId)}`),
  validate: (dag: DesignerDag) => request<{ ok: boolean; errors: ValidationError[] }>('POST', '/validate', dag),
};
