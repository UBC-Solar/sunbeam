import {PipelineMetric} from "../api";

function fmtMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  return `${ms.toFixed(ms < 10 ? 2 : 1)} ms`;
}

export function BusyIdleMeter({idlePct, busyPct}: {idlePct: number; busyPct: number}) {
  const busy = Math.max(0, Math.min(100, busyPct));
  const idle = Math.max(0, 100 - busy);

  return (
    <div>
      <div className="split-bar">
        <div
          className="split-bar-segment"
          style={{width: `${busy}%`, background: "var(--accent)"}}
        />
        <div
          className="split-bar-segment"
          style={{width: `${idle}%`, background: "var(--gridline)"}}
        />
      </div>
      <div className="legend">
        <span className="legend-item">
          <span className="legend-swatch" style={{background: "var(--accent)"}} />
          Busy {busy.toFixed(1)}%
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{background: "var(--gridline)"}} />
          Idle {idle.toFixed(1)}%
        </span>
      </div>
    </div>
  );
}

function BarRow({
  label,
  valueMs,
  maxMs,
  variant,
  lateMs,
}: {
  label: string;
  valueMs: number;
  maxMs: number;
  variant: "pipeline" | "stage";
  lateMs?: number;
}) {
  const pct = maxMs > 0 ? Math.min(100, (valueMs / maxMs) * 100) : 0;
  const isLate = (lateMs ?? 0) > 0.5;

  return (
    <div className="bar-row">
      <span className="text-secondary" title={label}>
        {label}
      </span>
      <div className="bar-track">
        <div
          className={`bar-fill ${variant === "stage" ? "stage" : ""}`}
          style={{width: `${pct}%`}}
        />
      </div>
      <span className="bar-value">
        {fmtMs(valueMs)}
        {isLate && (
          <span style={{color: "var(--status-warning)", marginLeft: "0.4rem"}}>
            late {fmtMs(lateMs!)}
          </span>
        )}
      </span>
    </div>
  );
}

export function PipelineLatencyBars({pipelines}: {pipelines: PipelineMetric[]}) {
  if (pipelines.length === 0) {
    return <p className="text-muted">No pipeline activity reported yet.</p>;
  }

  const maxAvg = Math.max(...pipelines.map((p) => p.avg_ms), 0.001);

  return (
    <div>
      <div className="legend" style={{marginBottom: "0.5rem", marginTop: 0}}>
        <span className="legend-item">
          <span className="legend-swatch" style={{background: "var(--accent)"}} />
          Pipeline avg / tick
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{background: "var(--text-muted)"}} />
          Stage avg / call
        </span>
      </div>

      {pipelines.map((pipeline) => {
        const maxStageAvg = Math.max(
          ...pipeline.stages.map((s) => s.avg_ms),
          0.001,
        );

        return (
          <div key={pipeline.name} style={{marginBottom: "0.85rem"}}>
            <BarRow
              label={pipeline.name}
              valueMs={pipeline.avg_ms}
              maxMs={maxAvg}
              variant="pipeline"
              lateMs={pipeline.late_now_ms}
            />
            {pipeline.stages.length > 0 && (
              <div className="stage-list">
                {pipeline.stages.map((stage) => (
                  <BarRow
                    key={stage.name}
                    label={`↳ ${stage.name}`}
                    valueMs={stage.avg_ms}
                    maxMs={maxStageAvg}
                    variant="stage"
                  />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}