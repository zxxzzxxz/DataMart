// 节点类型 → 颜色的映射；未知类型按名称哈希取色

const KNOWN: Record<string, string> = {
  sql: '#3b82f6',
  sql_check: '#f59e0b',
  notify: '#10b981',
};

const PALETTE = ['#8b5cf6', '#ef4444', '#06b6d4', '#ec4899', '#84cc16', '#f97316'];

export function colorForType(type: string): string {
  if (KNOWN[type]) return KNOWN[type];
  let h = 0;
  for (const ch of type) {
    h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  }
  return PALETTE[h % PALETTE.length];
}
