import { Handle, Position, type NodeProps } from '@xyflow/react';
import { colorForType } from './typeColor';
import { colorForTaskState, labelForTaskState } from './taskStateColor';

// 自定义节点：显示节点 id + 类型 label，顶部色条按类型区分；
// 有运行状态时在右上角显示彩色状态角标
export default function DesignerNode({ id, data }: NodeProps) {
  const d = data as { nodeType?: string; typeLabel?: string; taskState?: string };
  const t = d?.nodeType ?? 'unknown';
  const color = colorForType(t);
  const taskState = d?.taskState ?? '';
  return (
    <div className="dnode" style={{ borderTopColor: color }}>
      {taskState && (
        <span
          className="dnode-state"
          style={{ background: colorForTaskState(taskState) }}
          title={`任务状态：${labelForTaskState(taskState)}`}
        >
          {labelForTaskState(taskState)}
        </span>
      )}
      <Handle type="target" position={Position.Left} />
      <div className="dnode-id">{id}</div>
      <div className="dnode-type" style={{ color }}>
        {d?.typeLabel ?? t}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
