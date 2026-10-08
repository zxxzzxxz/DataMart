import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { api, ApiError } from '../api';
import { useDagEditor } from '../hooks/useDagEditor';
import { useDagRunStatus } from '../hooks/useDagRunStatus';
import type { DesignerDag, NodeType } from '../types';

let renderer: ReactTestRenderer | undefined;
afterEach(() => {
  if (renderer) act(() => renderer?.unmount());
  renderer = undefined;
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const initial: DesignerDag & { revision: string; mtime: number } = {
  dag_id: 'editor', description: 'original', schedule: null, retries: 2, retry_delay_seconds: 300,
  alert_on_failure: true, nodes: [{ id: 'start', type: 'empty', params: {}, position: { x: 0, y: 0 } }],
  edges: [], revision: 'a'.repeat(64), mtime: 1,
};

describe('editor request lifecycle', () => {
  it('saves the validated snapshot and preserves edits made during validation', async () => {
    vi.spyOn(api, 'getDag').mockResolvedValue(initial);
    vi.spyOn(api, 'nodeTypes').mockResolvedValue([{ type: 'empty', label: 'empty', fields: [] }] as NodeType[]);
    let finishValidation!: (value: { ok: boolean; errors: [] }) => void;
    vi.spyOn(api, 'validate').mockImplementation(() => new Promise(resolve => { finishValidation = resolve; }));
    const write = vi.spyOn(api, 'updateDag').mockResolvedValue({ ok: true, revision: 'b'.repeat(64), mtime: 2 });
    let editor!: ReturnType<typeof useDagEditor>;
    function Harness() { editor = useDagEditor('editor', 0); return null; }
    await act(async () => { renderer = create(<Harness />); });
    let saving!: Promise<void>;
    act(() => { saving = editor.onSave(); });
    act(() => { editor.updateMeta({ ...editor.meta, description: 'edited while waiting' }); });
    await act(async () => { finishValidation({ ok: true, errors: [] }); await saving; });
    expect(write.mock.calls[0][1].description).toBe('original');
    expect(editor.meta.description).toBe('edited while waiting');
    expect(editor.dirty).toBe(true);
  });

  it('retries transient polling failures and stops timers on unmount', async () => {
    vi.useFakeTimers();
    const runs = vi.spyOn(api, 'listRuns').mockRejectedValueOnce(new ApiError(0, 'network error'))
      .mockResolvedValue({ runs: [{ run_id: 'manual__2026-10-02T00:00:00+00:00', state: 'running' }] });
    vi.spyOn(api, 'listTasks').mockResolvedValue({ tasks: [] });
    let status!: ReturnType<typeof useDagRunStatus>;
    function Harness() { status = useDagRunStatus('editor', 0); return null; }
    await act(async () => { renderer = create(<Harness />); });
    expect(status.polling).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    expect(runs).toHaveBeenCalledTimes(2);
    expect(status.polling).toBe(true);
    act(() => renderer?.unmount());
    renderer = undefined;
    expect(vi.getTimerCount()).toBe(0);
  });
});
