import {PipelineMetric, ReplayProgress, WorkerMetrics} from "../api";
import {formatRelative} from "../time";

// A running worker's metrics as a dense, monospaced readout: replay progress, scheduler load, and where the time goes,
// pipeline by pipeline. Rows keep the worker's order (fastest pipeline first, stages in the order they run), so the
// readout holds still between reports.

// Stages often take microseconds, so those are shown as such rather than as 0.00ms
function fmtMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms >= 1) return `${ms.toFixed(ms < 10 ? 2 : 1)}ms`;
  return `${Math.round(ms * 1000)}µs`;
}

function fmtSeconds(seconds: number | null): string {
  if (seconds === null) return "—";

  const total = Math.round(seconds);
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;

  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  if (minutes > 0) return `${minutes}m ${String(secs).padStart(2, "0")}s`;
  return `${secs}s`;
}

// "17:41:06", or "2024-07-17 17:41:06" when the replay spans more than one day
function utcClock(iso: string, withDate: boolean): string {
  const text = new Date(iso).toISOString();
  return withDate ? `${text.slice(0, 10)} ${text.slice(11, 19)}` : text.slice(11, 19);
}

const BAR_SEGMENTS = 24;

// A bar of whole segments, like a terminal's block characters
function SegmentBar({fraction, tone = "accent"}: {fraction: number; tone?: "accent" | "dim"}) {
  const filled = Math.round(Math.max(0, Math.min(1, fraction)) * BAR_SEGMENTS);
  return (
    <span className="console-bar" aria-hidden>
      <span className={`console-bar-fill ${tone}`} style={{width: `calc(${filled} * var(--segment))`}} />
    </span>
  );
}

function Stat({label, value}: {label: string; value: string}) {
  return (
    <span className="console-stat">
      <span className="console-dim">{label}</span> {value}
    </span>
  );
}

function ReplaySection({replay}: {replay: ReplayProgress}) {
  const withDate = replay.start.slice(0, 10) !== replay.end.slice(0, 10);

  return (
    <div className="console-section">
      <div className="console-line">
        <span className="console-label">replay</span>
        <SegmentBar fraction={replay.fraction} />
        <span className="console-strong">{(replay.fraction * 100).toFixed(1)}%</span>
      </div>
      <div className="console-line console-stats">
        <Stat label="at" value={`${utcClock(replay.current, withDate)} of ${utcClock(replay.end, withDate)} UTC`} />
      </div>
      <div className="console-line console-stats">
        <Stat label="speed" value={replay.speed === null ? "—" : `×${Math.round(replay.speed).toLocaleString()}`} />
        <Stat label="elapsed" value={fmtSeconds(replay.elapsed_s)} />
        <Stat label="remaining" value={fmtSeconds(replay.eta_s)} />
        <Stat label="window" value={fmtSeconds(replay.total_s)} />
      </div>
    </div>
  );
}

function PipelineRows({pipeline, totalMs}: {pipeline: PipelineMetric; totalMs: number}) {
  const late = pipeline.late_now_ms > 0.5;
  const isIngress = pipeline.name.startsWith("Ingress");
  const share = (ms: number) => (totalMs > 0 ? ms / totalMs : 0);

  return (
    <>
      <div className="console-row pipeline">
        <span className="console-name">
          {pipeline.name}
          {late && <span className="console-warn"> late {fmtMs(pipeline.late_now_ms)}</span>}
        </span>
        <span className="console-num">{fmtMs(pipeline.avg_ms)}</span>
        <span className="console-num">{pipeline.max_ms === null ? "—" : fmtMs(pipeline.max_ms)}</span>
        <SegmentBar fraction={share(pipeline.total_ms)} />
        <span className="console-num">{Math.round(share(pipeline.total_ms) * 100)}%</span>
      </div>
      {/* An ingress pipeline is its one ingress stage; listing it again would only repeat the row above */}
      {!isIngress && pipeline.stages.map((stage, i) => (
        <div className="console-row stage" key={stage.name}>
          <span className="console-name">
            <span className="console-tree">{i === pipeline.stages.length - 1 ? "└" : "├"}</span>
            {stage.name}
          </span>
          <span className="console-num">{fmtMs(stage.avg_ms)}</span>
          <span className="console-num">{fmtMs(stage.max_ms)}</span>
          <SegmentBar fraction={share(stage.total_ms)} tone="dim" />
          <span className="console-num">{Math.round(share(stage.total_ms) * 100)}%</span>
        </div>
      ))}
    </>
  );
}

export function TimingConsole({metrics}: {metrics: WorkerMetrics}) {
  const queue = metrics.writer_queue;
  // Every bar is a share of the same whole, so a stage's bar can be read against its pipeline's
  const totalMs = metrics.pipelines.reduce((sum, p) => sum + p.total_ms, 0);

  return (
    <div className="console">
      {metrics.replay && <ReplaySection replay={metrics.replay} />}

      <div className="console-section">
        <div className="console-line">
          <span className="console-label">busy</span>
          <SegmentBar fraction={metrics.busy_pct / 100} />
          <span className="console-strong">{metrics.busy_pct.toFixed(1)}%</span>
        </div>
        <div className="console-line console-stats">
          <Stat label="writer" value={fmtMs(metrics.writer_ms)} />
          {queue && (
            <Stat
              label="queue"
              value={`${queue.queue_depth.toLocaleString()}/${queue.queue_capacity.toLocaleString()}`}
            />
          )}
          {queue && <Stat label="flush" value={fmtMs(queue.avg_flush_ms)} />}
          <Stat label="reported" value={formatRelative(metrics.reported_at)} />
        </div>
      </div>

      <div className="console-section">
        <div className="console-row header">
          <span>pipeline</span>
          <span className="console-num">avg</span>
          <span className="console-num">max</span>
          <span>share</span>
          <span />
        </div>
        {metrics.pipelines.length === 0 && <div className="console-dim">No pipeline has run yet.</div>}
        {metrics.pipelines.map((pipeline) => (
          <PipelineRows key={pipeline.name} pipeline={pipeline} totalMs={totalMs} />
        ))}
      </div>
    </div>
  );
}
