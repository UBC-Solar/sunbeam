import {useEffect, useMemo, useState} from "react";
import {
  Connection,
  Event,
  LaunchOptions,
  WorkerRun,
  getConnections,
  getEvents,
  getPipelineEditions,
  getWorkers,
  launchWorker,
  stopWorker,
} from "./api";
import {ConnectionsPanel} from "./components/ConnectionsPanel";
import {LaunchDialog} from "./components/LaunchDialog";
import {WorkerDetail} from "./components/WorkerDetail";
import {WorkersTable} from "./components/WorkersTable";

function App() {
  const [events, setEvents] = useState<Event[]>([]);
  const [workers, setWorkers] = useState<WorkerRun[]>([]);
  const [editions, setEditions] = useState<string[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [launchingEvent, setLaunchingEvent] = useState<Event | null>(null);
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null);

  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    try {
      const [eventsData, workersData, editionsData, connectionsData] = await Promise.all([
        getEvents(),
        getWorkers(),
        getPipelineEditions(),
        getConnections(),
      ]);

      setEvents(eventsData);
      setWorkers(workersData);
      setEditions(editionsData);
      setConnections(connectionsData);
      setError(null);
      setLoaded(true);
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

  async function onLaunch(event: Event, options: LaunchOptions) {
    await launchWorker(event.id, options);
    setLaunchingEvent(null);
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
            </div>

            <table>
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Mode</th>
                  <th>Description</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {events.length === 0 && (
                  <tr className="empty-row">
                    <td colSpan={4}>No events found.</td>
                  </tr>
                )}
                {events.map((event) => (
                  <tr key={event.id}>
                    <td>{event.name}</td>
                    <td className="text-secondary">{event.is_replay ? "Replay" : "Realtime"}</td>
                    <td className="text-secondary">{event.description ?? "—"}</td>
                    <td>
                      <button
                        className="primary"
                        onClick={() => setLaunchingEvent(event)}
                        disabled={connections.length === 0}
                        title={connections.length === 0 ? "Add an InfluxDB connection first" : undefined}
                      >
                        Launch worker…
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

          <ConnectionsPanel connections={connections} events={events} onChanged={refresh} />
        </>
      )}

      {launchingEvent && (
        <LaunchDialog
          event={launchingEvent}
          editions={editions}
          connections={connections}
          onCancel={() => setLaunchingEvent(null)}
          onLaunch={(options) => onLaunch(launchingEvent, options)}
        />
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