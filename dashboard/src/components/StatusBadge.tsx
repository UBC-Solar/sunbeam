import {WorkerStatus} from "../api";

const STATUS_META: Record<WorkerStatus, {label: string; color: string}> = {
  requested: {label: "Requested", color: "var(--status-warning)"},
  starting: {label: "Starting", color: "var(--status-warning)"},
  running: {label: "Running", color: "var(--status-good)"},
  stop_requested: {label: "Stop requested", color: "var(--status-warning)"},
  stopping: {label: "Stopping", color: "var(--status-warning)"},
  completed: {label: "Completed", color: "var(--status-good)"},
  failed: {label: "Failed", color: "var(--status-critical)"},
  lost: {label: "Lost", color: "var(--status-serious)"},
  cancelled: {label: "Cancelled", color: "var(--status-neutral)"},
};

export function StatusBadge({status}: {status: WorkerStatus}) {
  const meta = STATUS_META[status] ?? {label: status, color: "var(--status-neutral)"};

  return (
    <span className="badge">
      <span className="badge-dot" style={{background: meta.color}} />
      {meta.label}
    </span>
  );
}