import { useEffect, useRef, useState } from 'react';
import { MarkerType, addEdge, useEdgesState, useNodesState, type Connection, type Edge,
  type EdgeChange, type Node, type NodeChange } from '@xyflow/react';
import { api, errBodyErrors, errMessage, errStatus } from '../api';
import type { DagMeta, DesignerDag, NodeType, ValidationError } from '../types';
const NODE_ID_RE = /^[a-z][a-z0-9_]*$/;

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


export function useDagEditor(dagId: string, tokenVersion: number) {
  const [catalog, setCatalog] = useState<NodeType[]>([]);
  const [meta, setMeta] = useState<DagMeta>({
    description: '',
    schedule: '',
    retries: 1,
    retryDelay: 300,
    alertOnFailure: true,
  });
  const [revision, setRevision] = useState('');
  const editGeneration = useRef(0);
  const markDirty = () => { editGeneration.current += 1; setDirty(true); };
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

  const [reloadTick, setReloadTick] = useState(0);

  // 有未保存改动时，token 更新触发的 effect 重跑不得覆盖画布
  const dirtyRef = useRef(false);
  useEffect(() => {
    dirtyRef.current = dirty;
  }, [dirty]);

  // 重载完成后要展示的提示（如回滚成功），避免被加载 effect 清空
  const pendingNotice = useRef('');

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
        setRevision(dag.revision);
        setNodes(toRfNodes(dag, types));
        setEdges(toRfEdges(dag));
        setSelectedId(null);
        setErrors([]);
        setNotice(pendingNotice.current);
        pendingNotice.current = '';
        setDirty(false);
        // 等节点渲染完成后适配视野

        // 拉取最近一次运行状态（进行中会自动轮询）

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
  }, [dagId, tokenVersion, reloadTick, setNodes, setEdges]);

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
    if (changes.some(c => c.type === 'remove' || c.type === 'position')) markDirty();
  };

  const handleEdgesChange = (changes: EdgeChange[]) => {
    onEdgesChange(changes);
    if (changes.some(c => c.type === 'remove')) markDirty();
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
    markDirty();
  };

  // 从节点面板拖入创建节点
  const addNode = (type: string, position: { x: number; y: number }) => {
    const nt = catalog.find(t => t.type === type);
    if (!nt) return;
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
    markDirty();
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
    markDirty();
    return null;
  };

  const updateNodeParams = (id: string, params: Record<string, unknown>) => {
    setNodes(ns => ns.map(n => (n.id === id ? { ...n, data: { ...n.data, params } } : n)));
    markDirty();
  };

  const deleteNode = (id: string) => {
    setNodes(ns => ns.filter(n => n.id !== id));
    setEdges(es => es.filter(e => e.source !== id && e.target !== id));
    setSelectedId(cur => (cur === id ? null : cur));
    markDirty();
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
    const payload = buildPayload();
    const generation = editGeneration.current;
    setSaving(true);
    setNotice('');
    try {
      // 先 dry-run 校验，有错标红节点并在右侧面板列出
      const res = await api.validate(payload);
      applyValidation(res.errors ?? []);
      if (!res.ok) return;

      const saved = await api.updateDag(dagId, payload, revision);
      setRevision(saved.revision);
      if (generation === editGeneration.current) setDirty(false);
      setNotice(`已保存（${new Date().toLocaleTimeString()}）`);
    } catch (e) {
      const st = errStatus(e);
      const bodyErrs = errBodyErrors(e);
      if (st === 409) {
        alert('保存失败：文件已被他人修改。点击工具栏「重新加载」查看最新版本后再编辑（当前未保存改动将丢失）');
      } else if (st === 422 && bodyErrs) {
        applyValidation(bodyErrs);
      } else {
        alert('保存失败：' + errMessage(e));
      }
    } finally {
      setSaving(false);
    }
  };

  const reload = (versionId: string) => {
    pendingNotice.current = `已回滚到版本 ${versionId}，画布已重新加载`;
    dirtyRef.current = false;
    setDirty(false);
    setReloadTick(t => t + 1);
  };
  // 乐观锁冲突后的恢复入口：丢弃本地未保存改动，从服务端重新加载当前版本
  const reloadCurrent = () => {
    dirtyRef.current = false;
    setDirty(false);
    setReloadTick(t => t + 1);
  };
  const updateMeta = (value: DagMeta) => { setMeta(value); markDirty(); };
  return { catalog, meta, nodes, edges, selectedId, setSelectedId, dirty, errors, notice,
    loading, loadError, saving, validating, setNotice, updateMeta, handleNodesChange, handleEdgesChange,
    onConnect, addNode, renameNode, updateNodeParams, deleteNode, onValidate, onSave, reload, reloadCurrent };
}
