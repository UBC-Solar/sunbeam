import {FormEvent, useEffect, useState} from "react";
import {ConfigurationProfiles, Event, LaunchOptions} from "../api";

type LaunchDialogProps = {
  event: Event;
  editions: string[];
  profiles: ConfigurationProfiles;
  onCancel: () => void;
  onLaunch: (options: LaunchOptions) => Promise<void>;
};

export function LaunchDialog({event, editions, profiles, onCancel, onLaunch}: LaunchDialogProps) {
  const [pipelineEdition, setPipelineEdition] = useState(
    editions.includes(event.pipeline_edition) ? event.pipeline_edition : (editions[0] ?? ""),
  );
  const [configurationProfile, setConfigurationProfile] = useState(profiles.default);
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
      // The server forces reprocess for replays too; sending it keeps the request honest
      await onLaunch({pipelineEdition, configurationProfile, reprocess: reprocess || event.is_replay});
    } catch (err) {
      setError((err as Error).message);
      setLaunching(false);
    }
  }

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
          {event.name} ·{" "}
          {event.is_replay ? "Replay of a past event" : "Realtime: runs until stopped"}
        </p>

        {error && <div className="error-banner">Couldn't launch: {error}</div>}

        <label className="field">
          <span className="field-label">Pipeline edition</span>
          <select value={pipelineEdition} onChange={(e) => setPipelineEdition(e.target.value)}>
            {editions.map((edition) => (
              <option key={edition} value={edition}>
                {edition}
                {edition === event.pipeline_edition ? " (event's edition)" : ""}
              </option>
            ))}
          </select>
        </label>

        <label className="field">
          <span className="field-label">Configuration profile</span>
          <select value={configurationProfile} onChange={(e) => setConfigurationProfile(e.target.value)}>
            {profiles.profiles.map((profile) => (
              <option key={profile} value={profile}>
                {profile}
                {profile === profiles.default ? " (server default)" : ""}
              </option>
            ))}
          </select>
          <span className="field-hint">Which context.toml profile the worker loads.</span>
        </label>

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
          <button type="submit" className="primary" disabled={launching || !pipelineEdition}>
            {launching ? "Launching…" : "Launch"}
          </button>
        </div>
      </form>
    </div>
  );
}
