// 节点类型 → 颜色的映射（macOS 系统色板）；未知类型按名称哈希取色

const KNOWN: Record<string, string> = {
  sql: '#007aff',        // system blue
  sql_check: '#ff9500',  // system orange
  notify: '#34c759',     // system green
  bash: '#af52de',       // system purple
  http: '#5ac8fa',       // system teal
  sensor_sql: '#ff2d55', // system pink
  empty: '#8e8e93',      // system gray
};

const PALETTE = ['#af52de', '#ff3b30', '#5ac8fa', '#ff2d55', '#64d2ff', '#ff9500'];

export function colorForType(type: string): string {
  if (KNOWN[type]) return KNOWN[type];
  let h = 0;
  for (const ch of type) {
    h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  }
  return PALETTE[h % PALETTE.length];
}
