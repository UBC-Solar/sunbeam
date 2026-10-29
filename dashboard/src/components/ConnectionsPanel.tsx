import {FormEvent, useEffect, useState} from "react";
import {
  Connection,
  ConnectionFields,
  ConnectionTestResult,
  Event,
  createConnection,
  deleteConnection,
  makeDefaultConnection,
  testConnection,
  testConnectionFields,
  updateConnection,
} from "../api";
import {formatRelative} from "../time";
import {ConfirmButton} from "./ConfirmButton";

type ConnectionsPanelProps = {
  connections: Connection[];
  events: Event[];
  onChanged: () => Promise<void>;
};

function TestBadge({connection}: {connection: Connection}) {
  if (connection.last_test_ok === null) {
    return <span className="text-muted">Not tested</span>;
  }

  const color = connection.last_test_ok ? "var(--status-good)" : "var(--status-critical)";
  return (
    <span className="badge" title={connection.last_test_detail ?? undefined}>
      <span className="badge-dot" style={{background: color}} />
      {connection.last_test_ok ? "Passed" : "Failed"}
      {connection.last_tested_at && (
        <span className="text-muted">&nbsp;· {formatRelative(connection.last_tested_at)}</span>
      )}
    </span>
  );
}

export function ConnectionsPanel({connections, events, onChanged}: ConnectionsPanelProps) {
  // null: closed; "new": creating; otherwise the connection being edited
  const [editing, setEditing] = useState<Connection | "new" | null>(null);

  return (
    <div className="card">
      <div className="card-header">
        <h2>InfluxDB connections</h2>
        <button onClick={() => setEditing("new")}>Add connection…</button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>URL</th>
            <th>Bucket</th>
            <th>Last test</th>
          </tr>
        </thead>
        <tbody>
          {connections.length === 0 && (
            <tr className="empty-row">
              <td colSpan={4}>No connections. Workers need one to read telemetry.</td>
            </tr>
          )}
          {connections.map((connection) => (
            <tr key={connection.id} className="clickable" onClick={() => setEditing(connection)}>
              <td>
                {connection.name}
                {connection.is_default && (
                  <span className="default-star" title="Default for new workers">
                    {" "}★
                  </span>
                )}
              </td>
              <td className="text-secondary mono">{connection.url}</td>
              <td className="text-secondary">{connection.bucket}</td>
              <td>
                <TestBadge connection={connection} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {editing && (
        <ConnectionDialog
          connection={editing === "new" ? null : editing}
          events={events}
          onClose={() => setEditing(null)}
          onChanged={onChanged}
        />
      )}
    </div>
  );
}

type ConnectionDialogProps = {
  connection: Connection | null;
  events: Event[];
  onClose: () => void;
  onChanged: () => Promise<void>;
};

const EMPTY_FIELDS: ConnectionFields & {name: string} = {name: "", url: "", organization: "", bucket: "", token: ""};

function ConnectionDialog({connection, events, onClose, onChanged}: ConnectionDialogProps) {
  const [fields, setFields] = useState(connection ? {...connection} : EMPTY_FIELDS);
  const [testEventId, setTestEventId] = useState<number | null>(null);
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null);
  const [busy, setBusy] = useState<"saving" | "testing" | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  function set(key: keyof typeof EMPTY_FIELDS, value: string) {
    setFields((current) => ({...current, [key]: value}));
    // A result for other settings would be misleading
    setTestResult(null);
  }

  const settings: ConnectionFields = {
    url: fields.url.trim(),
    organization: fields.organization.trim(),
    bucket: fields.bucket.trim(),
    token: fields.token.trim(),
  };
  const unchanged =
    connection !== null &&
    (Object.keys(settings) as (keyof ConnectionFields)[]).every((key) => settings[key] === connection[key]);
  const complete = fields.name.trim() !== "" && Object.values(settings).every((value) => value !== "");

  async function run(kind: "saving" | "testing", action: () => Promise<void>) {
    setBusy(kind);
    setError(null);
    try {
      await action();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(null);
    }
  }

  function test() {
    return run("testing", async () => {
      // Saved settings are tested through the connection itself, so the result is recorded on it
      const result = unchanged
        ? await testConnection(connection!.id, testEventId)
        : await testConnectionFields(settings, testEventId);
      setTestResult(result);
      if (unchanged) await onChanged();
    });
  }

  function save(e: FormEvent) {
    e.preventDefault();
    return run("saving", async () => {
      const body = {...settings, name: fields.name.trim()};
      if (connection) {
        await updateConnection(connection.id, body);
      } else {
        await createConnection(body);
      }
      await onChanged();
      onClose();
    });
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <form
        className="modal"
        role="dialog"
        aria-labelledby="connection-title"
        onClick={(e) => e.stopPropagation()}
        onSubmit={save}
      >
        <h2 id="connection-title">{connection ? "Edit connection" : "New InfluxDB connection"}</h2>
        <p className="text-secondary modal-subtitle">
          Workers get these settings when they launch; changes apply to the next launch.
        </p>

        {error && <div className="error-banner">{error}</div>}

        <label className="field">
          <span className="field-label">Name</span>
          <input type="text" value={fields.name} onChange={(e) => set("name", e.target.value)} autoFocus />
        </label>
        <label className="field">
          <span className="field-label">URL</span>
          <input
            type="text"
            className="mono"
            placeholder="http://influxdb.telemetry.ubcsolar.com"
            value={fields.url}
            onChange={(e) => set("url", e.target.value)}
          />
        </label>
        <label className="field">
          <span className="field-label">Organization</span>
          <input
            type="text"
            className="mono"
            value={fields.organization}
            onChange={(e) => set("organization", e.target.value)}
          />
        </label>
        <label className="field">
          <span className="field-label">Bucket</span>
          <input type="text" value={fields.bucket} onChange={(e) => set("bucket", e.target.value)} />
        </label>
        <label className="field">
          <span className="field-label">Token</span>
          <input type="text" className="mono" value={fields.token} onChange={(e) => set("token", e.target.value)} />
        </label>

        <label className="field">
          <span className="field-label">Test against</span>
          <select
            value={testEventId ?? ""}
            onChange={(e) => {
              setTestEventId(e.target.value === "" ? null : Number(e.target.value));
              setTestResult(null);
            }}
          >
            <option value="">Connection and bucket only</option>
            {events.map((event) => (
              <option key={event.id} value={event.id}>
                Data in {event.name}
              </option>
            ))}
          </select>
        </label>

        {testResult && (
          <ul className="test-steps" aria-live="polite">
            {testResult.steps.map((step) => (
              <li key={step.name}>
                <span style={{color: step.ok ? "var(--status-good)" : "var(--status-critical)"}}>
                  {step.ok ? "✓" : "✗"}
                </span>
                <span className="step-name">{step.name}</span>
                <span>{step.detail}</span>
              </li>
            ))}
            {testResult.ok && testEventId !== null && testResult.window_start === null && (
              <li className="text-muted">The event hasn't started, so there was no data to look for.</li>
            )}
          </ul>
        )}

        <div className="modal-actions">
          {connection && (
            <ConfirmButton
              label="Delete"
              confirmLabel="Delete?"
              disabled={busy !== null}
              onConfirm={() =>
                run("saving", async () => {
                  await deleteConnection(connection.id);
                  await onChanged();
                  onClose();
                })
              }
            />
          )}
          {connection && !connection.is_default && (
            <button
              type="button"
              disabled={busy !== null}
              onClick={() =>
                run("saving", async () => {
                  await makeDefaultConnection(connection.id);
                  await onChanged();
                  onClose();
                })
              }
            >
              Make default
            </button>
          )}
          <span className="spacer" />
          <button type="button" onClick={test} disabled={busy !== null || !complete}>
            {busy === "testing" ? "Testing…" : "Test"}
          </button>
          <button type="submit" className="primary" disabled={busy !== null || !complete}>
            {busy === "saving" ? "Saving…" : "Save"}
          </button>
        </div>
      </form>
    </div>
  );
}
