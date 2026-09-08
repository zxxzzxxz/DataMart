import type { NodeType } from '../types';
import { colorForType } from './typeColor';

export const DND_MIME = 'application/x-dag-node-type';

interface Props {
  catalog: NodeType[];
}

// 左侧节点面板：从 /api/node-types 渲染，可拖拽入画布
export default function NodePanel({ catalog }: Props) {
  return (
    <aside className="node-panel">
      <div className="panel-title">节点类型</div>
      <div className="panel-hint">拖拽到画布创建节点</div>
      {catalog.map(t => (
        <div
          key={t.type}
          className="palette-item"
          draggable
          onDragStart={ev => {
            ev.dataTransfer.setData(DND_MIME, t.type);
            ev.dataTransfer.effectAllowed = 'move';
          }}
        >
          <span className="palette-dot" style={{ background: colorForType(t.type) }} />
          <div>
            <div className="palette-label">{t.label}</div>
            <div className="palette-type">{t.type}</div>
          </div>
        </div>
      ))}
    </aside>
  );
}
