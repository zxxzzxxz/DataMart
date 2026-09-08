import { Handle, Position, type NodeProps } from '@xyflow/react';
import { colorForType } from './typeColor';

// 自定义节点：显示节点 id + 类型 label，顶部色条按类型区分
export default function DesignerNode({ id, data }: NodeProps) {
  const d = data as { nodeType?: string; typeLabel?: string };
  const t = d?.nodeType ?? 'unknown';
  const color = colorForType(t);
  return (
    <div className="dnode" style={{ borderTopColor: color }}>
      <Handle type="target" position={Position.Left} />
      <div className="dnode-id">{id}</div>
      <div className="dnode-type" style={{ color }}>
        {d?.typeLabel ?? t}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
