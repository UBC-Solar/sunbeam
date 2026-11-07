import {FormEvent, useEffect, useState} from "react";
import {
  Event,
  EventFields,
  PipelineEdition,
  Vehicle,
  createEvent,
  deleteEvent,
  getPipelineEditions,
  getVehicles,
  updateEvent,
} from "../api";
import {ConfirmButton} from "./ConfirmButton";
import {EventStatus} from "./EventStatus";

type EventsPanelProps = {
  events: Event[];
  canLaunch: boolean;
  onLaunch: (event: Event) => void;
  onChanged: () => Promise<void>;
};

export function EventsPanel({events, canLaunch, onLaunch, onChanged}: EventsPanelProps) {
  // null: closed; "new": creating; otherwise the event being edited
  const [editing, setEditing] = useState<Event | "new" | null>(null);

  return (
    <div className="card">
      <div className="card-header">
        <h2>Events</h2>
        <button onClick={() => setEditing("new")}>Add event…</button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Event</th>
            <th>Mode</th>
            <th>Status</th>
            <th>Description</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {events.length === 0 && (
            <tr className="empty-row">
              <td colSpan={5}>No events yet. Add one to launch a worker for it.</td>
            </tr>
          )}
          {events.map((event) => (
            <tr key={event.id} className="clickable" onClick={() => setEditing(event)}>
              <td>{event.name}</td>
              <td className="text-secondary">{event.is_replay ? "Replay" : "Realtime"}</td>
              <td>
                <EventStatus event={event} />
              </td>
              <td className="text-secondary">{event.description ?? "—"}</td>
              <td>
                <button
                  className="primary"
                  onClick={(e) => {
                    // The row itself opens the editor
                    e.stopPropagation();
                    onLaunch(event);
                  }}
                  disabled={!canLaunch}
                  title={canLaunch ? undefined : "Add an InfluxDB connection first"}
                >
                  Launch worker…
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {editing && (
        <EventDialog
          event={editing === "new" ? null : editing}
          onClose={() => setEditing(null)}
          onChanged={onChanged}
        />
      )}
    </div>
  );
}

// Events are in UTC, and so are these inputs, whatever the browser's time zone: "2024-07-16T15:00"
function toUtcInput(iso: string | null): string {
  return iso ? new Date(iso).toISOString().slice(0, 16) : "";
}

function fromUtcInput(value: string): string | null {
  return value ? `${value}:00Z` : null;
}

type Form = {
  name: string;
  vehicle_id: number | null;
  description: string;
  starts_at: string;
  ends_at: string;
  pipeline_edition: string;
  stages: string[];
};

function formFor(event: Event | null): Form {
  return {
    name: event?.name ?? "",
    vehicle_id: event?.vehicle_id ?? null,
    description: event?.description ?? "",
    starts_at: toUtcInput(event?.starts_at ?? null),
    ends_at: toUtcInput(event?.ends_at ?? null),
    pipeline_edition: event?.pipeline_edition ?? "",
    stages: event?.stages ?? [],
  };
}

type EventDialogProps = {
  event: Event | null;
  onClose: () => void;
  onChanged: () => Promise<void>;
};

function EventDialog({event, onClose, onChanged}: EventDialogProps) {
  const [form, setForm] = useState<Form>(() => formFor(event));
  const [newStage, setNewStage] = useState("");
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [editions, setEditions] = useState<PipelineEdition[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([getVehicles(), getPipelineEditions()])
      .then(([vehiclesData, editionsData]) => {
        setVehicles(vehiclesData);
        setEditions(editionsData);
        // A new event starts on the first of each, so the form is never blank
        setForm((current) => ({
          ...current,
          vehicle_id: current.vehicle_id ?? vehiclesData[0]?.id ?? null,
          pipeline_edition: current.pipeline_edition || editionsData[editionsData.length - 1]?.id || "",
        }));
      })
      .catch((err) => setError((err as Error).message));
  }, []);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const edition = editions.find((e) => e.id === form.pipeline_edition);
  const complete =
    form.name.trim() !== "" && form.vehicle_id !== null && form.starts_at !== "" && form.stages.length > 0;

  function set<K extends keyof Form>(key: K, value: Form[K]) {
    setForm((current) => ({...current, [key]: value}));
  }

  const registered = edition?.stages ?? [];
  // Named by hand: stages the server's copy of the code doesn't have, to come from the git ref the event is launched from
  const unregistered = form.stages.filter((stage) => !registered.includes(stage));

  function chooseEdition(id: string) {
    // Stages are per edition: drop the old edition's that the new one lacks, but keep any named by hand
    const available = editions.find((e) => e.id === id)?.stages ?? [];
    setForm((current) => ({
      ...current,
      pipeline_edition: id,
      stages: current.stages.filter((stage) => available.includes(stage) || !registered.includes(stage)),
    }));
  }

  function toggleStage(stage: string, on: boolean) {
    set("stages", on ? [...form.stages, stage] : form.stages.filter((s) => s !== stage));
  }

  function addStage() {
    const stage = newStage.trim();
    if (stage && !form.stages.includes(stage)) set("stages", [...form.stages, stage]);
    setNewStage("");
  }

  async function run(action: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await action();
      await onChanged();
      onClose();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  function save(e: FormEvent) {
    e.preventDefault();
    const fields: EventFields = {
      name: form.name.trim(),
      vehicle_id: form.vehicle_id!,
      description: form.description.trim() || null,
      starts_at: fromUtcInput(form.starts_at)!,
      ends_at: fromUtcInput(form.ends_at),
      pipeline_edition: form.pipeline_edition,
      stages: form.stages,
    };
    return run(async () => {
      if (event) {
        await updateEvent(event.id, fields);
      } else {
        await createEvent(fields);
      }
    });
  }

  const locked = event?.running ?? false;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <form
        className="modal modal-wide"
        role="dialog"
        aria-labelledby="event-title"
        onClick={(e) => e.stopPropagation()}
        onSubmit={save}
      >
        <h2 id="event-title">{event ? "Edit event" : "New event"}</h2>
        <p className="text-secondary modal-subtitle">
          {!event
            ? "Workers read the event from here when they launch."
            : locked
              ? "A worker is running this event; stop it to make changes."
              : "Changes apply to the next worker launched for it."}
        </p>

        {error && <div className="error-banner">{error}</div>}

        <fieldset className="plain-fieldset" disabled={locked || busy}>
          <label className="field">
            <span className="field-label">Name</span>
            <input type="text" value={form.name} onChange={(e) => set("name", e.target.value)} autoFocus />
          </label>
          <label className="field">
            <span className="field-label">Description</span>
            <input type="text" value={form.description} onChange={(e) => set("description", e.target.value)} />
          </label>

          <div className="field-row">
            <label className="field">
              <span className="field-label">Starts (UTC)</span>
              <input
                type="datetime-local"
                value={form.starts_at}
                onChange={(e) => set("starts_at", e.target.value)}
              />
            </label>
            <label className="field">
              <span className="field-label">Ends (UTC)</span>
              <input type="datetime-local" value={form.ends_at} onChange={(e) => set("ends_at", e.target.value)} />
            </label>
          </div>
          <p className="field-hint" style={{marginTop: "-0.6rem", marginBottom: "1rem"}}>
            Leave the end empty for an ongoing event, which workers run live. One that has ended is replayed.
          </p>

          <div className="field-row">
            <label className="field">
              <span className="field-label">Vehicle</span>
              <select value={form.vehicle_id ?? ""} onChange={(e) => set("vehicle_id", Number(e.target.value))}>
                {vehicles.map((vehicle) => (
                  <option key={vehicle.id} value={vehicle.id}>
                    {vehicle.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span className="field-label">Pipeline edition</span>
              <select value={form.pipeline_edition} onChange={(e) => chooseEdition(e.target.value)}>
                {editions.map((e) => (
                  <option key={e.id} value={e.id}>
                    {e.id} · {e.name}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="field">
            <span className="field-label">Stages</span>
            <div className="stage-picker">
              {[...registered, ...unregistered].map((stage) => (
                <label key={stage} title={registered.includes(stage) ? undefined : "Not in the server's copy of the code"}>
                  <input
                    type="checkbox"
                    checked={form.stages.includes(stage)}
                    onChange={(e) => toggleStage(stage, e.target.checked)}
                  />
                  {stage}
                  {!registered.includes(stage) && <span className="text-secondary">(git ref only)</span>}
                </label>
              ))}
            </div>
            <div className="add-stage">
              <input
                type="text"
                className="mono"
                placeholder="Another stage, by name"
                value={newStage}
                onChange={(e) => setNewStage(e.target.value)}
                onKeyDown={(e) => {
                  // Enter adds the stage rather than submitting the dialog
                  if (e.key === "Enter") {
                    e.preventDefault();
                    addStage();
                  }
                }}
              />
              <button type="button" onClick={addStage} disabled={newStage.trim() === ""}>
                Add stage
              </button>
            </div>
            <span className="field-hint">
              {unregistered.length > 0
                ? "Stages the server's copy of the code doesn't have can only be launched from a git ref that has them; a worker without one fails, naming it."
                : "To try a stage the server doesn't have yet, add it by name and launch from a git ref that has it."}
            </span>
          </div>
        </fieldset>

        <div className="modal-actions">
          {event && (
            <ConfirmButton
              label="Delete"
              confirmLabel={event.sample_count > 0 ? "Delete it and its data?" : "Delete?"}
              disabled={locked || busy}
              onConfirm={() => run(() => deleteEvent(event.id))}
            />
          )}
          <span className="spacer" />
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary" disabled={locked || busy || !complete}>
            {busy ? "Saving…" : "Save"}
          </button>
        </div>
      </form>
    </div>
  );
}
