import {useMemo, useState} from "react";
import {ACTIVE_STATUSES, Event, WorkerRun} from "../api";
import {formatDuration, formatRelative} from "../time";
import {ConfirmButton} from "./ConfirmButton";
import {StatusBadge} from "./StatusBadge";

type WorkersTableProps = {
  workers: WorkerRun[];
  events: Event[];
  onSelect: (worker: WorkerRun) => void;
  onStop: (workerId: string) => void;
};

export function WorkersTable({workers, events, onSelect, onStop}: WorkersTableProps) {
  const [tab, setTab] = useState<"active" | "history">("active");

  const eventNameById = useMemo(() => {
    const map = new Map<string, string>();
    events.forEach((event) => map.set(String(event.id), event.name));
    return map;
  }, [events]);

  const active = workers.filter((w) => ACTIVE_STATUSES.includes(w.status));
  const history = workers.filter((w) => !ACTIVE_STATUSES.includes(w.status));

  const rows = tab === "active" ? active : history;

  return (
    <div className="card">
      <div className="card-header">
        <h2>Workers</h2>
      </div>

      <div className="tabs">
        <button
          className={`tab ${tab === "active" ? "active" : ""}`}
          onClick={() => setTab("active")}
        >
          Active ({active.length})
        </button>
        <button
          className={`tab ${tab === "history" ? "active" : ""}`}
          onClick={() => setTab("history")}
        >
          History ({history.length})
        </button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Status</th>
            <th>Event</th>
            <th>Edition</th>
            {tab === "active" ? (
              <>
                <th>Stage</th>
                <th>Message</th>
                <th>Heartbeat</th>
              </>
            ) : (
              <>
                <th>Duration</th>
                <th>Outcome</th>
              </>
            )}
            <th />
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 && (
            <tr className="empty-row">
              <td colSpan={7}>
                {tab === "active" ? "No active workers." : "No past runs yet."}
              </td>
            </tr>
          )}

          {rows.map((worker) => (
            <tr key={worker.id} className="clickable" onClick={() => onSelect(worker)}>
              <td>
                <StatusBadge status={worker.status} />
              </td>
              <td>{eventNameById.get(String(worker.event_id)) ?? worker.event_id}</td>
              <td className="text-secondary">{worker.pipeline_edition}</td>
              {tab === "active" ? (
                <>
                  <td className="text-secondary">{worker.current_stage ?? "—"}</td>
                  <td className="text-secondary">{worker.status_message ?? "—"}</td>
                  <td className="text-muted">{formatRelative(worker.last_heartbeat_at)}</td>
                </>
              ) : (
                <>
                  <td className="text-secondary">
                    {formatDuration(worker.started_at, worker.stopped_at)}
                  </td>
                  <td className="text-secondary" title={worker.failure_reason ?? undefined}>
                    {worker.status_message ?? worker.failure_reason ?? "—"}
                  </td>
                </>
              )}
              <td onClick={(e) => e.stopPropagation()}>
                {tab === "active" && (
                  <ConfirmButton
                    label={worker.stop_requested ? "Stopping…" : "Stop"}
                    confirmLabel="Confirm stop"
                    disabled={worker.stop_requested}
                    onConfirm={() => onStop(worker.id)}
                  />
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}