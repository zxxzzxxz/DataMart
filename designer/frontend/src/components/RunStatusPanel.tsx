import { Panel } from '@xyflow/react';
import type { DagRun, TaskStatus } from '../types';
import {
  TASK_STATE_LABELS,
  TASK_STATE_LEGEND,
  colorForTaskState,
  labelForTaskState,
} from './taskStateColor';

export interface RunInfo {
  run: DagRun;
  tasks: TaskStatus[];
}

interface Props {
  runInfo: RunInfo | null;
  polling: boolean;
  unavailable: boolean;
  onRefresh: () => void;
}

// 任务状态汇总：成功 2 · 失败 1 …
function summarizeTasks(tasks: TaskStatus[]): string {
  if (tasks.length === 0) return '';
  const counts = new Map<string, number>();
  for (const t of tasks) counts.set(t.state, (counts.get(t.state) ?? 0) + 1);
  const ordered = ['success', 'failed', 'running', 'queued', 'scheduled', 'upstream_failed', 'skipped', 'no_status'];
  const parts: string[] = [];
  for (const s of ordered) {
    const c = counts.get(s);
    if (c) parts.push(`${TASK_STATE_LABELS[s] ?? s} ${c}`);
    counts.delete(s);
  }
  for (const [s, c] of counts) parts.push(`${labelForTaskState(s)} ${c}`);
  return parts.join(' · ');
}

// 画布右上角：最近一次运行状态 + 手动刷新按钮；不可用时降级为提示条
export function RunStatusPanel({ runInfo, polling, unavailable, onRefresh }: Props) {
  if (unavailable) {
    return (
      <Panel position="top-right">
        <div className="run-unavailable">运行状态不可用（Airflow 未同步/不可达）</div>
      </Panel>
    );
  }
  const run = runInfo?.run;
  return (
    <Panel position="top-right">
      <div className="run-status-box">
        {run ? (
          <>
            <span className="run-id" title={run.run_id}>
              {run.run_id}
            </span>
            <span className="state-chip" style={{ background: colorForTaskState(run.state) }}>
              {labelForTaskState(run.state)}
            </span>
            {runInfo && runInfo.tasks.length > 0 && (
              <span className="muted run-summary">{summarizeTasks(runInfo.tasks)}</span>
            )}
          </>
        ) : (
          <span className="muted">暂无运行记录</span>
        )}
        <button className="btn btn-mini" onClick={onRefresh} disabled={polling}>
          {polling ? '轮询中…' : '刷新状态'}
        </button>
      </div>
    </Panel>
  );
}

// 画布右下角：任务状态颜色图例
export function RunLegend() {
  return (
    <Panel position="bottom-right">
      <div className="run-legend">
        {TASK_STATE_LEGEND.map(it => (
          <span className="legend-item" key={it.label}>
            <span className="legend-dot" style={{ background: it.color }} />
            {it.label}
          </span>
        ))}
      </div>
    </Panel>
  );
}
