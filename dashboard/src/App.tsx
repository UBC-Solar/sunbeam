import {useEffect, useMemo, useState} from "react";
import {
  Event,
  WorkerRun,
  getEvents,
  getPipelineEditions,
  getWorkers,
  launchWorker,
  stopWorker,
} from "./api";
import {WorkerDetail} from "./components/WorkerDetail";
import {WorkersTable} from "./components/WorkersTable";

function App() {
  const [events, setEvents] = useState<Event[]>([]);
  const [workers, setWorkers] = useState<WorkerRun[]>([]);
  const [editions, setEditions] = useState<string[]>([]);
  const [selectedEdition, setSelectedEdition] = useState<string>("");
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null);

  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    try {
      const [eventsData, workersData, editionsData] = await Promise.all([
        getEvents(),
        getWorkers(),
        getPipelineEditions(),
      ]);

      setEvents(eventsData);
      setWorkers(workersData);
      setEditions(editionsData);
      setError(null);
      setLoaded(true);

      setSelectedEdition((current) => {
        if (!current && editionsData.length > 0) return editionsData[0];
        return current;
      });
    } catch (err) {
      setError((err as Error).message);
    }
  }

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, []);

  const selectedWorker = useMemo(
    () => workers.find((w) => w.id === selectedWorkerId) ?? null,
    [workers, selectedWorkerId],
  );

  async function onLaunch(eventId: number) {
    await launchWorker(eventId, selectedEdition);
    await refresh();
  }

  async function onStop(workerId: string) {
    await stopWorker(workerId);
    await refresh();
  }

  return (
    <main>
      <h1>Sunbeam Orchestrator</h1>
      <p className="subtitle">Launch and monitor pipeline workers.</p>

      {error && <div className="error-banner">Couldn't reach the server: {error}</div>}
      {!loaded && !error && <p className="text-muted">Loading…</p>}

      {loaded && (
        <>
          <div className="card">
            <div className="card-header">
              <h2>Events</h2>
              <label className="text-secondary">
                Pipeline edition:{" "}
                <select
                  value={selectedEdition}
                  onChange={(e) => setSelectedEdition(e.target.value)}
                >
                  {editions.map((edition) => (
                    <option key={edition} value={edition}>
                      {edition}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Description</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {events.length === 0 && (
                  <tr className="empty-row">
                    <td colSpan={3}>No events found.</td>
                  </tr>
                )}
                {events.map((event) => (
                  <tr key={event.id}>
                    <td>{event.name}</td>
                    <td className="text-secondary">{event.description ?? "—"}</td>
                    <td>
                      <button className="primary" onClick={() => onLaunch(event.id)}>
                        Launch worker
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <WorkersTable
            workers={workers}
            events={events}
            onSelect={(worker) => setSelectedWorkerId(worker.id)}
            onStop={onStop}
          />
        </>
      )}

      {selectedWorker && (
        <WorkerDetail
          worker={selectedWorker}
          events={events}
          onClose={() => setSelectedWorkerId(null)}
          onStop={onStop}
        />
      )}
    </main>
  );
}

export default App;