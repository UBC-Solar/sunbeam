import {ReplayProgress} from "../api";

// "17:41:06", or "2024-07-17 17:41:06" when the replay spans more than one day
function utcClock(iso: string, withDate: boolean): string {
  const text = new Date(iso).toISOString();
  return withDate ? `${text.slice(0, 10)} ${text.slice(11, 19)}` : text.slice(11, 19);
}

function fmtSeconds(seconds: number | null): string {
  if (seconds === null) return "—";

  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;

  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  if (minutes > 0) return `${minutes}m ${String(secs).padStart(2, "0")}s`;
  return `${secs}s`;
}

export function ReplayProgressBar({replay}: {replay: ReplayProgress}) {
  const pct = Math.max(0, Math.min(100, replay.fraction * 100));
  const withDate = replay.start.slice(0, 10) !== replay.end.slice(0, 10);

  return (
    <div>
      <div className="progress-header">
        <span className="progress-pct">{pct.toFixed(1)}%</span>
        <span className="text-secondary mono">
          {utcClock(replay.current, withDate)} / {utcClock(replay.end, withDate)} UTC
        </span>
      </div>

      <div
        className="progress-track"
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(pct)}
      >
        <div className="progress-fill" style={{width: `${pct}%`}} />
      </div>

      <div className="stat-row" style={{marginTop: "1rem", marginBottom: 0}}>
        <div className="stat-tile">
          <div className="stat-label">Speed</div>
          <div className="stat-value">
            {replay.speed === null ? "—" : `×${Math.round(replay.speed).toLocaleString()}`}
          </div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Elapsed</div>
          <div className="stat-value">{fmtSeconds(replay.elapsed_s)}</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Remaining</div>
          <div className="stat-value">{fmtSeconds(replay.eta_s)}</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Window</div>
          <div className="stat-value">{fmtSeconds(replay.total_s)}</div>
        </div>
      </div>
    </div>
  );
}
