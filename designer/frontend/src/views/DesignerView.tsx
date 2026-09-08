import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Background,
  Controls,
  MarkerType,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

import { api, errBodyErrors, errMessage, errStatus, isMock } from '../api';
import type { DesignerDag, NodeType, ValidationError } from '../types';
import DesignerNode from '../components/DesignerNode';
import NodePanel, { DND_MIME } from '../components/NodePanel';
import PropertiesPanel from '../components/PropertiesPanel';

const rfNodeTypes = { designer: DesignerNode };

const NODE_ID_RE = /^[a-z][a-z0-9_]*$/;

// DAG 级可编辑属性（schedule 用字符串编辑，保存时空串转 null）
export interface DagMeta {
  description: string;
  schedule: string;
  retries: number;
  retryDelay: number;
  alertOnFailure: boolean;
}

interface Props {
  dagId: string;
  tokenVersion: number;
  onBack: () => void;
}

export default function DesignerView(props: Props) {
  return (
    <ReactFlowProvider>
      <CanvasInner {...props} />
    </ReactFlowProvider>
  );
}

// DesignerDag JSON → React Flow 节点
function toRfNodes(dag: DesignerDag, catalog: NodeType[]): Node[] {
  return (dag.nodes || []).map(n => ({
    id: n.id,
    type: 'designer',
    position: { x: n.position?.x ?? 0, y: n.position?.y ?? 0 },
    data: {
      nodeType: n.type,
      typeLabel: catalog.find(t => t.type === n.type)?.label ?? n.type,
      params: n.params ?? {},
    },
  }));
}

// DesignerDag JSON → React Flow 边（upstream → downstream，带箭头）
function toRfEdges(dag: DesignerDag): Edge[] {
  return (dag.edges || []).map(e => ({
    id: `${e.upstream}->${e.downstream}`,
    source: e.upstream,
    target: e.downstream,
    markerEnd: { type: MarkerType.ArrowClosed, color: '#64748b' },
  }));
}

// 分配 task_1、task_2… 形式的合法且不重复的新节点 id
function nextTaskId(nodes: Node[]): string {
  const used = new Set(nodes.map(n => n.id));
  let i = 1;
  while (used.has(`task_${i}`)) i += 1;
  return `task_${i}`;
}

function CanvasInner({ dagId, tokenVersion, onBack }: Props) {
  const { screenToFlowPosition, fitView } = useReactFlow();

  const [catalog, setCatalog] = useState<NodeType[]>([]);
  const [meta, setMeta] = useState<DagMeta>({
    description: '',
    schedule: '',
    retries: 1,
    retryDelay: 300,
    alertOnFailure: true,
  });
  const [mtime, setMtime] = useState(0);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [errors, setErrors] = useState<ValidationError[]>([]);
  const [notice, setNotice] = useState('');
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState('');
  const [saving, setSaving] = useState(false);
  const [validating, setValidating] = useState(false);

  // 有未保存改动时，token 更新触发的 effect 重跑不得覆盖画布
  const dirtyRef = useRef(false);
  useEffect(() => {
    dirtyRef.current = dirty;
  }, [dirty]);

  // 加载 DAG + 节点类型目录
  useEffect(() => {
    if (dirtyRef.current) return;
    let cancelled = false;
    setLoading(true);
    setLoadError('');
    Promise.all([api.getDag(dagId), api.nodeTypes()])
      .then(([dag, types]) => {
        if (cancelled) return;
        setCatalog(types);
        setMeta({
          description: dag.description ?? '',
          schedule: dag.schedule ?? '',
          retries: dag.retries ?? 1,
          retryDelay: dag.retry_delay_seconds ?? 300,
          alertOnFailure: dag.alert_on_failure ?? true,
        });
        setMtime(dag.mtime ?? 0);
        setNodes(toRfNodes(dag, types));
        setEdges(toRfEdges(dag));
        setSelectedId(null);
        setErrors([]);
        setNotice('');
        setDirty(false);
        // 等节点渲染完成后适配视野
        setTimeout(() => fitView({ padding: 0.2 }), 60);
      })
      .catch(e => {
        if (!cancelled) setLoadError(errMessage(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dagId, tokenVersion]);

  // 画布 graph → DesignerDag JSON
  const buildPayload = (): DesignerDag => ({
    dag_id: dagId,
    description: meta.description,
    schedule: meta.schedule.trim() === '' ? null : meta.schedule.trim(),
    retries: Number(meta.retries) || 0,
    retry_delay_seconds: Number(meta.retryDelay) || 0,
    alert_on_failure: meta.alertOnFailure,
    nodes: nodes.map(n => {
      const d = n.data as { nodeType: string; params: Record<string, unknown> };
      return {
        id: n.id,
        type: d.nodeType,
        params: d.params ?? {},
        position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
      };
    }),
    edges: edges.map(e => ({ upstream: e.source, downstream: e.target })),
  });

  // ---------- 画布事件 ----------

  const handleNodesChange = (changes: NodeChange[]) => {
    onNodesChange(changes);
    if (changes.some(c => c.type === 'remove' || c.type === 'position')) setDirty(true);
  };

  const handleEdgesChange = (changes: EdgeChange[]) => {
    onEdgesChange(changes);
    if (changes.some(c => c.type === 'remove')) setDirty(true);
  };

  // 连线 = 依赖（上游 source → 下游 target）
  const onConnect = (conn: Connection) => {
    if (!conn.source || !conn.target || conn.source === conn.target) return;
    if (edges.some(e => e.source === conn.source && e.target === conn.target)) return;
    setEdges(es =>
      addEdge(
        { ...conn, id: `${conn.source}->${conn.target}`, markerEnd: { type: MarkerType.ArrowClosed, color: '#64748b' } },
        es,
      ),
    );
    setDirty(true);
  };

  // 从节点面板拖入创建节点
  const onDrop = (ev: React.DragEvent) => {
    ev.preventDefault();
    const type = ev.dataTransfer.getData(DND_MIME);
    const nt = catalog.find(t => t.type === type);
    if (!nt) return;
    const position = screenToFlowPosition({ x: ev.clientX, y: ev.clientY });
    const id = nextTaskId(nodes);
    const params: Record<string, unknown> = {};
    for (const f of nt.fields) {
      params[f.name] = f.default ?? (f.kind === 'checkbox' ? false : f.kind === 'select' && f.options?.length ? f.options[0] : '');
    }
    setNodes(ns => [
      ...ns.map(n => ({ ...n, selected: false })),
      { id, type: 'designer', position, selected: true, data: { nodeType: type, typeLabel: nt.label, params } },
    ]);
    setSelectedId(id);
    setDirty(true);
  };

  // 节点改名：合法性 + 唯一性校验，同步更新连线引用
  const renameNode = (oldId: string, newId: string): string | null => {
    const id = newId.trim();
    if (id === oldId) return null;
    if (!NODE_ID_RE.test(id)) return '节点 id 需以小写字母开头，仅含小写字母、数字、下划线';
    if (nodes.some(n => n.id === id)) return `节点 id「${id}」已存在`;
    setNodes(ns => ns.map(n => (n.id === oldId ? { ...n, id } : n)));
    setEdges(es =>
      es.map(e => {
        const source = e.source === oldId ? id : e.source;
        const target = e.target === oldId ? id : e.target;
        return { ...e, source, target, id: `${source}->${target}` };
      }),
    );
    setSelectedId(id);
    setDirty(true);
    return null;
  };

  const updateNodeParams = (id: string, params: Record<string, unknown>) => {
    setNodes(ns => ns.map(n => (n.id === id ? { ...n, data: { ...n.data, params } } : n)));
    setDirty(true);
  };

  const deleteNode = (id: string) => {
    setNodes(ns => ns.filter(n => n.id !== id));
    setEdges(es => es.filter(e => e.source !== id && e.target !== id));
    setSelectedId(cur => (cur === id ? null : cur));
    setDirty(true);
  };

  // ---------- 校验 / 保存 ----------

  const applyValidation = (errs: ValidationError[]) => {
    setErrors(errs);
    setNotice(errs.length === 0 ? '校验通过' : '');
  };

  const onValidate = async () => {
    setValidating(true);
    setNotice('');
    try {
      const res = await api.validate(buildPayload());
      applyValidation(res.errors ?? []);
    } catch (e) {
      alert('校验请求失败：' + errMessage(e));
    } finally {
      setValidating(false);
    }
  };

  const onSave = async () => {
    setSaving(true);
    setNotice('');
    try {
      // 先 dry-run 校验，有错标红节点并在右侧面板列出
      const res = await api.validate(buildPayload());
      applyValidation(res.errors ?? []);
      if (!res.ok) return;

      await api.updateDag(dagId, buildPayload(), mtime);
      // 重新拉一次只为刷新 mtime（乐观锁基准）
      const fresh = await api.getDag(dagId);
      setMtime(fresh.mtime ?? mtime);
      setDirty(false);
      setNotice(`已保存（${new Date().toLocaleTimeString()}）`);
    } catch (e) {
      const st = errStatus(e);
      const bodyErrs = errBodyErrors(e);
      if (st === 409) {
        alert('保存失败：文件已被他人修改，请刷新后重试');
      } else if (st === 422 && bodyErrs) {
        applyValidation(bodyErrs);
      } else {
        alert('保存失败：' + errMessage(e));
      }
    } finally {
      setSaving(false);
    }
  };

  const handleBack = () => {
    if (dirty && !window.confirm('有未保存的改动，确定返回列表吗？')) return;
    onBack();
  };

  // ---------- 渲染 ----------

  const errorNodeIds = useMemo(
    () => new Set(errors.map(e => e.node_id).filter((x): x is string => !!x)),
    [errors],
  );

  // 校验失败的节点加红色边框 class
  const nodesForRender = useMemo(
    () => nodes.map(n => (errorNodeIds.has(n.id) ? { ...n, className: 'node-has-error' } : n)),
    [nodes, errorNodeIds],
  );

  const selectedNode = nodes.find(n => n.id === selectedId) ?? null;

  if (loading) {
    return <div className="center-msg">加载中…</div>;
  }
  if (loadError) {
    return (
      <div className="center-msg">
        <p>加载失败:{loadError}</p>
        <button className="btn" onClick={onBack}>
          返回列表
        </button>
      </div>
    );
  }

  return (
    <div className="designer">
      <header className="topbar">
        <button className="btn" onClick={handleBack}>
          ← 返回列表
        </button>
        <div className="topbar-title">
          {dagId}
          {dirty && (
            <span className="dirty-dot" title="有未保存的改动">
              ●
            </span>
          )}
          {isMock() && <span className="badge-mock">mock 数据</span>}
        </div>
        <div className="topbar-actions">
          <button className="btn" onClick={onValidate} disabled={validating}>
            {validating ? '校验中…' : '校验'}
          </button>
          <button className="btn btn-primary" onClick={onSave} disabled={saving}>
            {saving ? '保存中…' : '保存'}
          </button>
        </div>
      </header>
      <div className="designer-body">
        <NodePanel catalog={catalog} />
        <div
          className="canvas-wrap"
          onDrop={onDrop}
          onDragOver={ev => {
            ev.preventDefault();
            ev.dataTransfer.dropEffect = 'move';
          }}
        >
          <ReactFlow
            nodes={nodesForRender}
            edges={edges}
            nodeTypes={rfNodeTypes}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            onSelectionChange={({ nodes: sel }) => setSelectedId(sel.length > 0 ? sel[0].id : null)}
            deleteKeyCode={['Delete', 'Backspace']}
            colorMode="dark"
            fitView
          >
            <Background gap={18} />
            <Controls />
          </ReactFlow>
        </div>
        <PropertiesPanel
          dagId={dagId}
          meta={meta}
          onMetaChange={m => {
            setMeta(m);
            setDirty(true);
          }}
          selectedNode={selectedNode}
          catalog={catalog}
          errors={errors}
          notice={notice}
          onRenameNode={renameNode}
          onParamsChange={updateNodeParams}
          onDeleteNode={deleteNode}
        />
      </div>
    </div>
  );
}
