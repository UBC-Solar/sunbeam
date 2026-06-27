import {useEffect, useState} from "react";
import {
  Event,
  WorkerRun,
  getEvents,
  getPipelineEditions,
  getWorkers,
  launchWorker,
  stopWorker,
} from "./api";

function App() {
  const [events, setEvents] = useState<Event[]>([]);
  const [workers, setWorkers] = useState<WorkerRun[]>([]);
  const [editions, setEditions] = useState<string[]>([]);
  const [selectedEdition, setSelectedEdition] = useState<string>("");

  async function refresh() {
    const eventsPromise = getEvents();
    const workersPromise = getWorkers(true);
    const editionsPromise = getPipelineEditions();

    const eventsData = await eventsPromise;
    const workersData = await workersPromise;
    const editionsData = await editionsPromise;

    setEvents(eventsData);
    setWorkers(workersData);
    setEditions(editionsData);

    setSelectedEdition((current) => {
      if (!current && editionsData.length > 0) {
        return editionsData[0];
      }

      return current;
    });
  }

  useEffect(() => {
    refresh();

    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, []);

  async function onLaunch(eventId: string) {
    await launchWorker(eventId, selectedEdition);
    await refresh();
  }

  async function onStop(workerId: string) {
    await stopWorker(workerId);
    await refresh();
  }

  return (
    <main style={{padding: "2rem", fontFamily: "system-ui"}}>
      <h1>Sunbeam Orchestrator</h1>

      <section>
        <h2>Events</h2>

        <label>
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

        <table>
          <thead>
            <tr>
              <th>Event</th>
              <th>Description</th>
              <th>Launch</th>
            </tr>
          </thead>
          <tbody>
            {events.map((event) => (
              <tr key={event.id}>
                <td>{event.name}</td>
                <td>{event.description}</td>
                <td>
                  <button onClick={() => onLaunch(event.id)}>
                    Launch worker
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <h2>Active Workers</h2>

        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Edition</th>
              <th>Stage</th>
              <th>Message</th>
              <th>Host</th>
              <th>Last heartbeat</th>
              <th>Stop</th>
            </tr>
          </thead>
          <tbody>
            {workers.map((worker) => (
              <tr key={worker.id}>
                <td>{worker.status}</td>
                <td>{worker.pipeline_edition}</td>
                <td>{worker.current_stage ?? "-"}</td>
                <td>{worker.status_message ?? "-"}</td>
                <td>{worker.host ?? "-"}</td>
                <td>{worker.last_heartbeat_at ?? "-"}</td>
                <td>
                  <button
                    disabled={worker.stop_requested}
                    onClick={() => onStop(worker.id)}
                  >
                    {worker.stop_requested ? "Stopping..." : "Stop"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

export default App;