import { useCallback, useEffect, useRef, useState } from 'react';
import { api, errStatus } from '../api';
import type { TaskStatus } from '../types';
import type { RunInfo } from '../components/RunStatusPanel';
import { isActiveRunState } from '../components/taskStateColor';

export function useDagRunStatus(dagId: string, tokenVersion: number) {
  const [runInfo, setRunInfo] = useState<RunInfo | null>(null);
  const [polling, setPolling] = useState(false);
  const [runUnavailable, setRunUnavailable] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const sequence = useRef(0);
  const active = useRef(false);
  const failures = useRef(0);
  const refreshRunStatus = useCallback(async function refresh() {
    const current = ++sequence.current;
    if (timer.current) clearTimeout(timer.current);
    const isCurrent = () => active.current && current === sequence.current;
    const schedule = (delay: number) => {
      if (isCurrent()) timer.current = setTimeout(() => void refresh(), delay);
    };
    try {
      const { runs } = await api.listRuns(dagId, 1);
      if (!isCurrent()) return;
      failures.current = 0;
      setRunUnavailable(false);
      const run = runs[0];
      if (!run) { setRunInfo(null); setPolling(false); return; }
      let tasks: TaskStatus[] = [];
      try {
        tasks = (await api.listTasks(dagId, run.run_id)).tasks;
      } catch { /* run status remains useful while task lookup recovers */ }
      if (!isCurrent()) return;
      setRunInfo({ run, tasks });
      const running = isActiveRunState(run.state);
      setPolling(running);
      if (running) schedule(5000);
    } catch (error) {
      if (!isCurrent()) return;
      const status = errStatus(error);
      setPolling(false);
      if (status === 404 || status === 503) setRunUnavailable(true);
      if (status !== 401 && status !== 403 && status !== 400) {
        failures.current += 1;
        schedule(Math.min(30_000, 5000 * 2 ** Math.min(failures.current - 1, 3)));
      }
    }
  }, [dagId]);

  useEffect(() => {
    active.current = true;
    void refreshRunStatus();
    return () => {
      active.current = false;
      sequence.current += 1;
      if (timer.current) clearTimeout(timer.current);
    };
  }, [refreshRunStatus, tokenVersion]);
  return { runInfo, polling, runUnavailable, refreshRunStatus };
}
