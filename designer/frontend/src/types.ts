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
}

export interface ValidationError {
  node_id: string | null;
  field: string | null;
  message: string;
}
