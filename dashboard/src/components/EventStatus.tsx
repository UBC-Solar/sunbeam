import {Event} from "../api";

const compact = new Intl.NumberFormat(undefined, {notation: "compact", maximumFractionDigits: 1});

// Whether the event has been run, is running, or never has; plus how many samples it has stored
export function EventStatus({event}: {event: Event}) {
  const [label, color] = event.running
    ? ["Running", "var(--status-good)"]
    : event.status === "unprocessed"
      ? ["Not run", null]
      : ["Ran", "var(--status-neutral)"];

  return (
    <span className="badge">
      <span
        className="badge-dot"
        style={color ? {background: color} : {border: "1px solid var(--text-muted)", boxSizing: "border-box"}}
      />
      {label}
      {event.sample_count > 0 && (
        <span className="text-muted" title={`${event.sample_count.toLocaleString()} samples stored`}>
          · {compact.format(event.sample_count)} samples
        </span>
      )}
    </span>
  );
}
