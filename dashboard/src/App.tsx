import {useEffect, useMemo, useState} from "react";
import {
  ConfigurationProfiles,
  Event,
  LaunchOptions,
  WorkerRun,
  getConfigurationProfiles,
  getEvents,
  getPipelineEditions,
  getWorkers,
  launchWorker,
  stopWorker,
} from "./api";
import {LaunchDialog} from "./components/LaunchDialog";
import {WorkerDetail} from "./components/WorkerDetail";
import {WorkersTable} from "./components/WorkersTable";

function App() {
  const [events, setEvents] = useState<Event[]>([]);
  const [workers, setWorkers] = useState<WorkerRun[]>([]);
  const [editions, setEditions] = useState<string[]>([]);
  const [profiles, setProfiles] = useState<ConfigurationProfiles | null>(null);
  const [launchingEvent, setLaunchingEvent] = useState<Event | null>(null);
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null);

  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    try {
      const [eventsData, workersData, editionsData, profilesData] = await Promise.all([
        getEvents(),
        getWorkers(),
        getPipelineEditions(),
        getConfigurationProfiles(),
      ]);

      setEvents(eventsData);
      setWorkers(workersData);
      setEditions(editionsData);
      setProfiles(profilesData);
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
                        disabled={profiles === null}
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
        </>
      )}

      {launchingEvent && profiles && (
        <LaunchDialog
          event={launchingEvent}
          editions={editions}
          profiles={profiles}
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