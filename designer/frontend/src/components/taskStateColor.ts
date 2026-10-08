// Airflow 任务/运行状态 → 颜色与中文标签（用于画布节点角标、状态芯片与图例）

export const TASK_STATE_COLORS: Record<string, string> = {
  success: '#34c759',
  failed: '#ff3b30',
  running: '#007aff',
  queued: '#8e8e93',
  scheduled: '#8e8e93',
  upstream_failed: '#ff9500',
  skipped: '#d9a400',
  no_status: '#8e8e93',
};

export const TASK_STATE_LABELS: Record<string, string> = {
  success: '成功',
  failed: '失败',
  running: '运行中',
  queued: '排队中',
  scheduled: '已调度',
  upstream_failed: '上游失败',
  skipped: '跳过',
  no_status: '无状态',
};

// 未识别状态的兜底色（如 deferred / restarting 等）
const FALLBACK_COLOR = '#8e8e93';

export function colorForTaskState(state: string): string {
  return TASK_STATE_COLORS[state] ?? FALLBACK_COLOR;
}

export function labelForTaskState(state: string): string {
  return TASK_STATE_LABELS[state] ?? state;
}

// run 级进行中状态（其余视为终态，停止轮询）
export function isActiveRunState(state: string): boolean {
  return state === 'running' || state === 'queued';
}

// 画布图例条目
export const TASK_STATE_LEGEND = [
  { label: '成功', color: TASK_STATE_COLORS.success },
  { label: '失败', color: TASK_STATE_COLORS.failed },
  { label: '运行中', color: TASK_STATE_COLORS.running },
  { label: '排队/调度', color: TASK_STATE_COLORS.queued },
  { label: '上游失败', color: TASK_STATE_COLORS.upstream_failed },
  { label: '跳过', color: TASK_STATE_COLORS.skipped },
];
