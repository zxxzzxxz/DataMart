// 与 designer-api 对接的数据结构（见 API 契约）

export type FieldKind = 'text' | 'textarea' | 'select' | 'number' | 'checkbox';

export interface NodeField {
  name: string;
  label: string;
  kind: FieldKind;
  options?: string[];
  default?: unknown;
  required?: boolean;
}

export interface NodeType {
  type: string;
  label: string;
  fields: NodeField[];
}

export interface DesignerNodeData {
  id: string;
  type: string;
  params: Record<string, unknown>;
  position: { x: number; y: number };
}

export interface DesignerEdgeData {
  upstream: string;
  downstream: string;
}

export interface DesignerDag {
  dag_id: string;
  description: string;
  schedule: string | null;
  retries: number;
  retry_delay_seconds: number;
  alert_on_failure: boolean;
  nodes: DesignerNodeData[];
  edges: DesignerEdgeData[];
}

export interface DagSummary {
  dag_id: string;
  description: string;
  schedule: string | null;
  node_count: number;
  mtime: number;
  revision: string;
}

export interface ValidationError {
  node_id: string | null;
  field: string | null;
  message: string;
}

// ---------- 运行状态 ----------

export interface DagRun {
  run_id: string;
  state: string;
  logical_date?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  conf?: unknown;
}

export interface TaskStatus {
  task_id: string;
  state: string;
  try_number?: number;
  duration?: number | null;
}

// ---------- 版本历史 ----------

export interface DagVersion {
  version_id: string;
  saved_at: string;
  size_bytes: number;
}

export interface DagVersionDetail extends DagVersion {
  content: string;
}

// ---------- SQL 校验 ----------

export type CheckSqlResult =
  | { ok: true; duration_ms: number }
  | { ok: false; error: string };

export interface DagMeta {
  description: string;
  schedule: string;
  retries: number;
  retryDelay: number;
  alertOnFailure: boolean;
}

export interface DagMutationResult {
  ok: true;
  mtime: number;
  revision: string;
}
