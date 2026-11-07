import {FormEvent, useEffect, useState} from "react";
import {Vehicle, createVehicle, deleteVehicle, updateVehicle} from "../api";
import {ConfirmButton} from "./ConfirmButton";

export function VehiclesTable({vehicles, onEdit}: {vehicles: Vehicle[]; onEdit: (vehicle: Vehicle) => void}) {
  return (
    <table>
      <thead>
        <tr>
          <th>Vehicle</th>
          <th>Events</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {vehicles.length === 0 && (
          <tr className="empty-row">
            <td colSpan={3}>No vehicles yet. Add one before creating an event.</td>
          </tr>
        )}
        {vehicles.map((vehicle) => (
          <tr key={vehicle.id} className="clickable" onClick={() => onEdit(vehicle)}>
            <td>{vehicle.name}</td>
            <td className="text-secondary">{vehicle.event_count}</td>
            <td className="text-secondary">{vehicle.description ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

type VehicleDialogProps = {
  // null to create one
  vehicle: Vehicle | null;
  onClose: () => void;
  onChanged: () => Promise<void>;
};

export function VehicleDialog({vehicle, onClose, onChanged}: VehicleDialogProps) {
  const [name, setName] = useState(vehicle?.name ?? "");
  const [description, setDescription] = useState(vehicle?.description ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  async function run(action: () => Promise<unknown>) {
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
    const fields = {name: name.trim(), description: description.trim() || null};
    return run(() => (vehicle ? updateVehicle(vehicle.id, fields) : createVehicle(fields)));
  }

  const inUse = (vehicle?.event_count ?? 0) > 0;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <form
        className="modal"
        role="dialog"
        aria-labelledby="vehicle-title"
        onClick={(e) => e.stopPropagation()}
        onSubmit={save}
      >
        <h2 id="vehicle-title">{vehicle ? "Edit vehicle" : "New vehicle"}</h2>
        {inUse && (
          <p className="text-secondary modal-subtitle">
            It has {vehicle!.event_count} event{vehicle!.event_count === 1 ? "" : "s"}, so it can't be deleted until
            they're deleted.
          </p>
        )}

        {error && <div className="error-banner">{error}</div>}

        <fieldset className="plain-fieldset" disabled={busy}>
          <label className="field">
            <span className="field-label">Name</span>
            <input type="text" value={name} onChange={(e) => setName(e.target.value)} autoFocus />
          </label>
          <label className="field">
            <span className="field-label">Description</span>
            <input type="text" value={description} onChange={(e) => setDescription(e.target.value)} />
          </label>
        </fieldset>

        <div className="modal-actions">
          {vehicle && (
            <ConfirmButton
              label="Delete"
              confirmLabel="Delete?"
              disabled={busy || inUse}
              onConfirm={() => run(() => deleteVehicle(vehicle.id))}
            />
          )}
          <span className="spacer" />
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary" disabled={busy || name.trim() === ""}>
            {busy ? "Saving…" : "Save"}
          </button>
        </div>
      </form>
    </div>
  );
}
