import {FormEvent, useEffect, useState} from "react";
import {Connection, Event, LaunchOptions} from "../api";

type LaunchDialogProps = {
  event: Event;
  connections: Connection[];
  onCancel: () => void;
  onLaunch: (options: LaunchOptions) => Promise<void>;
};

function lastTestLabel(connection: Connection): string {
  if (connection.last_test_ok === null) return "";
  return connection.last_test_ok ? " · last test passed" : " · last test failed";
}

export function LaunchDialog({event, connections, onCancel, onLaunch}: LaunchDialogProps) {
  const [connectionId, setConnectionId] = useState<number | null>(
    (connections.find((c) => c.is_default) ?? connections[0])?.id ?? null,
  );
  const [source, setSource] = useState<"local" | "git">("local");
  const [gitRef, setGitRef] = useState("");
  const [reprocess, setReprocess] = useState(event.is_replay);
  const [launching, setLaunching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onCancel();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onCancel]);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setLaunching(true);
    setError(null);
    try {
      await onLaunch({
        influxConnectionId: connectionId,
        sourceRef: source === "git" ? gitRef.trim() : null,
        // The server forces reprocess for replays too; sending it keeps the request honest
        reprocess: reprocess || event.is_replay,
      });
    } catch (err) {
      setError((err as Error).message);
      setLaunching(false);
    }
  }

  const missingRef = source === "git" && gitRef.trim() === "";

  return (
    <div className="modal-overlay" onClick={onCancel}>
      <form
        className="modal"
        role="dialog"
        aria-labelledby="launch-title"
        onClick={(e) => e.stopPropagation()}
        onSubmit={submit}
      >
        <h2 id="launch-title">Launch worker</h2>
        <p className="text-secondary modal-subtitle">
          {event.name} · pipeline {event.pipeline_edition} ·{" "}
          {event.is_replay ? "Replay of a past event" : "Realtime: runs until stopped"}
        </p>

        {error && <div className="error-banner">Couldn't launch: {error}</div>}

        <label className="field">
          <span className="field-label">InfluxDB connection</span>
          <select
            value={connectionId ?? ""}
            onChange={(e) => setConnectionId(Number(e.target.value))}
            disabled={connections.length === 0}
          >
            {connections.length === 0 && <option value="">No connections: add one first</option>}
            {connections.map((connection) => (
              <option key={connection.id} value={connection.id}>
                {connection.name}
                {connection.is_default ? " (default)" : ""}
                {lastTestLabel(connection)}
              </option>
            ))}
          </select>
          <span className="field-hint">
            Tested against this event before launching. The worker won't start if it can't see the event's data.
          </span>
        </label>

        <div className="field">
          <span className="field-label">Source</span>
          <div className="segmented">
            <label>
              <input
                type="radio"
                name="source"
                checked={source === "local"}
                onChange={() => setSource("local")}
              />
              Server's working copy
            </label>
            <label>
              <input type="radio" name="source" checked={source === "git"} onChange={() => setSource("git")} />
              Git ref
            </label>
          </div>
          {source === "git" && (
            <input
              type="text"
              className="mono"
              placeholder="branch, tag, or full commit SHA"
              value={gitRef}
              onChange={(e) => setGitRef(e.target.value)}
              autoFocus
            />
          )}
          <span className="field-hint">
            {source === "git"
              ? "Cloned from GitHub and built on first use (this can take a few minutes); later launches of the same commit reuse the image."
              : "The code the server was started with."}
          </span>
        </div>

        <label className="field field-checkbox">
          <input
            type="checkbox"
            checked={reprocess || event.is_replay}
            disabled={event.is_replay}
            onChange={(e) => setReprocess(e.target.checked)}
          />
          <span>
            <span className="field-label">Reprocess</span>
            <span className="field-hint">
              {event.is_replay
                ? "Always on for replays: they rewrite the whole event."
                : "Delete the event's earlier samples before writing."}
            </span>
          </span>
        </label>

        <div className="modal-actions">
          <button type="button" onClick={onCancel} disabled={launching}>
            Cancel
          </button>
          <button
            type="submit"
            className="primary"
            disabled={launching || connectionId === null || missingRef}
          >
            {launching ? "Testing connection…" : "Launch"}
          </button>
        </div>
      </form>
    </div>
  );
}
