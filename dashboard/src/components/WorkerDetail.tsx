import {useEffect, useState} from "react";
import {
  ACTIVE_STATUSES,
  Event,
  WorkerMetrics,
  WorkerRun,
  getWorkerMetrics,
  shortRef,
} from "../api";
import {formatClock, formatDuration} from "../time";
import {ConfirmButton} from "./ConfirmButton";
import {StatusBadge} from "./StatusBadge";
import {LogTerminal} from "./LogTerminal";
import {TimingConsole} from "./TimingConsole";

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


  useEffect(() => {
    if (!isActive) {
      setMetrics(null);
      return;
    }

    let cancelled = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;

    async function poll() {
      // Replays report their progress twice a second and can be over in seconds, so keep up with them
      let delayMs = 2000;

      try {
        const snapshot = await getWorkerMetrics(worker.id);
        if (!cancelled) {
          setMetrics(snapshot);
          setMetricsError(null);
        }
        if (snapshot?.replay) delayMs = 500;
      } catch (err) {
        if (!cancelled) setMetricsError((err as Error).message);
      }

      if (!cancelled) timeout = setTimeout(poll, delayMs);
    }

    poll();
    return () => {
      cancelled = true;
      clearTimeout(timeout);
    };
  }, [worker.id, isActive]);

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

          <dt>Source</dt>
          <dd>
            {worker.source_ref ? (
              <>
                {shortRef(worker.source_ref)}{" "}
                <span className="mono text-secondary">{worker.source_sha?.slice(0, 7)}</span>
              </>
            ) : (
              "Server's working copy"
            )}
          </dd>

          <dt>InfluxDB</dt>
          <dd>{worker.influx_connection_name ?? "—"}</dd>

          <dt>Reprocess</dt>
          <dd>{worker.reprocess ? "Yes" : "No"}</dd>

          <dt>Host</dt>
          <dd>{worker.host ?? "—"}</dd>

          <dt>Container</dt>
          <dd className="mono">{worker.container_name ?? "—"}</dd>

          <dt>Created</dt>
          <dd>{formatClock(worker.created_at)}</dd>

          <dt>Duration</dt>
          <dd>{formatDuration(worker.started_at, worker.stopped_at)}</dd>

          <dt className="wide">Message</dt>
          <dd className="wide">{worker.status_message ?? "—"}</dd>

          {worker.failure_reason && (
            <>
              <dt className="wide">Failure reason</dt>
              <dd className="wide" style={{color: "var(--status-critical)"}}>
                {worker.failure_reason}
              </dd>
            </>
          )}
        </dl>

        <section className="drawer-section">
          <h3>Live timing</h3>

          {!isActive && (
            <p className="text-muted">Worker is no longer active — live metrics aren't retained after a run ends.</p>
          )}

          {isActive && metricsError && <div className="error-banner">Failed to load metrics: {metricsError}</div>}

          {isActive && !metricsError && metrics === null && (
            <p className="text-muted">Waiting for the worker's first metrics report…</p>
          )}

          {isActive && metrics && <TimingConsole metrics={metrics} />}
        </section>

        <section className="drawer-section">
          <h3>Logs</h3>
          <LogTerminal workerId={worker.id} containerName={worker.container_name} isActive={isActive} />
        </section>
      </div>
    </div>
  );
}