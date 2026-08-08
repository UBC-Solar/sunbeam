import {useEffect, useRef, useState} from "react";
import {
  ACTIVE_STATUSES,
  Event,
  WorkerMetrics,
  WorkerRun,
  getWorkerLogs,
  getWorkerMetrics,
  workerLogStreamUrl,
} from "../api";
import {formatClock, formatDuration, formatRelative} from "../time";
import {ConfirmButton} from "./ConfirmButton";
import {StatusBadge} from "./StatusBadge";
import {BusyIdleMeter, PipelineLatencyBars} from "./TimingCharts";

type WorkerDetailProps = {
  worker: WorkerRun;
  events: Event[];
  onClose: () => void;
  onStop: (workerId: string) => void;
};

export function WorkerDetail({worker, events, onClose, onStop}: WorkerDetailProps) {
  const isActive = ACTIVE_STATUSES.includes(worker.status);
  const eventName =
    events.find((e) => String(e.id) === String(worker.event_id))?.name ?? worker.event_id;

  const [metrics, setMetrics] = useState<WorkerMetrics | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);

  const [logs, setLogs] = useState<string[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [liveTail, setLiveTail] = useState(false);

  const logViewerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isActive) {
      setMetrics(null);
      return;
    }

    let cancelled = false;

    async function poll() {
      try {
        const snapshot = await getWorkerMetrics(worker.id);
        if (!cancelled) {
          setMetrics(snapshot);
          setMetricsError(null);
        }
      } catch (err) {
        if (!cancelled) setMetricsError((err as Error).message);
      }
    }

    poll();
    const interval = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [worker.id, isActive]);

  async function refreshLogs() {
    setLogsLoading(true);
    setLogsError(null);
    try {
      const result = await getWorkerLogs(worker.id, 500);
      setLogs(result?.lines ?? []);
    } catch (err) {
      setLogsError((err as Error).message);
    } finally {
      setLogsLoading(false);
    }
  }

  useEffect(() => {
    setLogs([]);
    setLiveTail(false);
    refreshLogs();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [worker.id]);

  useEffect(() => {
    if (!isActive && liveTail) setLiveTail(false);
  }, [isActive, liveTail]);

  useEffect(() => {
    if (!liveTail) return;

    const source = new EventSource(workerLogStreamUrl(worker.id, 50));
    source.onmessage = (event) => {
      setLogs((prev) => [...prev, event.data]);
    };
    source.onerror = () => {
      setLogsError("Live log stream disconnected.");
    };

    return () => source.close();
  }, [liveTail, worker.id]);

  useEffect(() => {
    const el = logViewerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [logs]);

  return (
    <div className="overlay" onClick={onClose}>
      <div className="drawer" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-header">
          <div>
            <h2>{eventName}</h2>
            <div style={{marginBottom: "0.4rem"}}>
              <StatusBadge status={worker.status} />
            </div>
            <span className="text-muted mono">{worker.id}</span>
          </div>

          <div style={{display: "flex", gap: "0.5rem", flex: "none"}}>
            {isActive && (
              <ConfirmButton
                label={worker.stop_requested ? "Stopping…" : "Stop worker"}
                confirmLabel="Confirm stop"
                disabled={worker.stop_requested}
                onConfirm={() => onStop(worker.id)}
              />
            )}
            <button onClick={onClose}>Close</button>
          </div>
        </div>

        <dl className="drawer-meta">
          <dt>Pipeline edition</dt>
          <dd>{worker.pipeline_edition}</dd>

          <dt>Host</dt>
          <dd>{worker.host ?? "—"}</dd>

          <dt>Container</dt>
          <dd className="mono">{worker.container_name ?? "—"}</dd>

          <dt>Created</dt>
          <dd>{formatClock(worker.created_at)}</dd>

          <dt>Duration</dt>
          <dd>{formatDuration(worker.started_at, worker.stopped_at)}</dd>

          {isActive && (
            <>
              <dt>Stage</dt>
              <dd>{worker.current_stage ?? "—"}</dd>
            </>
          )}

          <dt>Message</dt>
          <dd>{worker.status_message ?? "—"}</dd>

          {worker.failure_reason && (
            <>
              <dt>Failure reason</dt>
              <dd style={{color: "var(--status-critical)"}}>{worker.failure_reason}</dd>
            </>
          )}
        </dl>

        <div className="card" style={{marginTop: "1.25rem"}}>
          <h3>Live timing</h3>

          {!isActive && (
            <p className="text-muted">
              Worker is no longer active — live metrics aren't retained after a run ends.
            </p>
          )}

          {isActive && metricsError && (
            <div className="error-banner">Failed to load metrics: {metricsError}</div>
          )}

          {isActive && !metricsError && metrics === null && (
            <p className="text-muted">Waiting for the worker's first metrics report…</p>
          )}

          {isActive && metrics && (
            <>
              <BusyIdleMeter idlePct={metrics.idle_pct} busyPct={metrics.busy_pct} />
              <p className="text-muted" style={{fontSize: "0.75rem", margin: "0.6rem 0 1rem"}}>
                Reported {formatRelative(metrics.reported_at)} · writer {metrics.writer_ms.toFixed(2)} ms
              </p>
              <PipelineLatencyBars pipelines={metrics.pipelines} />
            </>
          )}
        </div>

        <div className="card">
          <div className="log-toolbar">
            <h3 style={{margin: 0}}>Logs</h3>
            <button onClick={refreshLogs} disabled={logsLoading}>
              {logsLoading ? "Refreshing…" : "Refresh"}
            </button>
            {isActive && (
              <button
                className={`pill-toggle ${liveTail ? "on" : ""}`}
                onClick={() => setLiveTail((v) => !v)}
              >
                {liveTail ? "Live tail: on" : "Live tail: off"}
              </button>
            )}
          </div>

          {logsError && <div className="error-banner">{logsError}</div>}

          <div className="log-viewer" ref={logViewerRef}>
            {logs.length === 0 && !logsLoading && (
              <span className="text-muted">No logs available.</span>
            )}
            {logs.map((line, i) => (
              <div className="log-line" key={i}>
                {line}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}