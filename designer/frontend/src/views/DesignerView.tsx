import { useEffect, useMemo, useState } from 'react';
import { Background, Controls, ReactFlow, ReactFlowProvider, useReactFlow } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { isMock } from '../api';
import DesignerNode from '../components/DesignerNode';
import NodePanel, { DND_MIME } from '../components/NodePanel';
import PropertiesPanel from '../components/PropertiesPanel';
import TriggerRunModal from '../components/TriggerRunModal';
import HistoryModal from '../components/HistoryModal';
import { RunLegend, RunStatusPanel } from '../components/RunStatusPanel';
import { useDagEditor } from '../hooks/useDagEditor';
import { useDagRunStatus } from '../hooks/useDagRunStatus';
const rfNodeTypes = { designer: DesignerNode };
interface Props { dagId: string; tokenVersion: number; onBack: () => void; }
export default function DesignerView(props: Props) {
  return <ReactFlowProvider><CanvasInner {...props} /></ReactFlowProvider>;
}
function CanvasInner({ dagId, tokenVersion, onBack }: Props) {
  const { screenToFlowPosition, fitView } = useReactFlow();
  const { catalog, meta, nodes, edges, selectedId, setSelectedId, dirty, errors, notice,
    loading, loadError, saving, validating, setNotice, updateMeta, handleNodesChange, handleEdgesChange,
    onConnect, addNode, renameNode, updateNodeParams, deleteNode, onValidate, onSave, reload } = useDagEditor(dagId, tokenVersion);
  const { runInfo, polling, runUnavailable, refreshRunStatus } = useDagRunStatus(dagId, tokenVersion);
  const [showTrigger, setShowTrigger] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  useEffect(() => {
    if (loading) return;
    const timer = setTimeout(() => fitView({ padding: 0.2 }), 60);
    return () => clearTimeout(timer);
  }, [loading, fitView]);
  const onDrop = (event: React.DragEvent) => {
    event.preventDefault();
    addNode(event.dataTransfer.getData(DND_MIME), screenToFlowPosition({ x: event.clientX, y: event.clientY }));
  };
  const handleBack = () => { if (!dirty || window.confirm('有未保存的改动，确定返回列表吗？')) onBack(); };
  const handleTriggered = (runId: string) => { setShowTrigger(false); setNotice(`已触发，run_id=${runId}`); void refreshRunStatus(); };
  const handleRolledBack = (versionId: string) => { setShowHistory(false); reload(versionId); void refreshRunStatus(); };
  // ---------- 渲染 ----------

  const errorNodeIds = useMemo(
    () => new Set(errors.map(e => e.node_id).filter((x): x is string => !!x)),
    [errors],
  );

  // 校验失败的节点加红色边框 class；有任务状态的节点注入 taskState 供角标着色
  const nodesForRender = useMemo(
    () =>
      nodes.map(n => {
        const taskState = runInfo?.tasks.find(t => t.task_id === n.id)?.state ?? '';
        let out = errorNodeIds.has(n.id) ? { ...n, className: 'node-has-error' } : n;
        if (taskState) out = { ...out, data: { ...out.data, taskState } };
        return out;
      }),
    [nodes, errorNodeIds, runInfo],
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
          <button className="btn" onClick={() => setShowTrigger(true)}>
            运行
          </button>
          <button className="btn" onClick={() => setShowHistory(true)}>
            历史
          </button>
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
            colorMode="light"
            fitView
          >
            <Background gap={18} />
            <Controls />
            <RunStatusPanel
              runInfo={runInfo}
              polling={polling}
              unavailable={runUnavailable}
              onRefresh={() => void refreshRunStatus()}
            />
            <RunLegend />
          </ReactFlow>
        </div>
        <PropertiesPanel
          dagId={dagId}
          meta={meta}
          onMetaChange={m => {
            updateMeta(m);
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
      {showTrigger && (
        <TriggerRunModal dagId={dagId} onClose={() => setShowTrigger(false)} onTriggered={handleTriggered} />
      )}
      {showHistory && (
        <HistoryModal dagId={dagId} onClose={() => setShowHistory(false)} onRolledBack={handleRolledBack} />
      )}
    </div>
  );
}
