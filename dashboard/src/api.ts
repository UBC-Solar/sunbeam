// @ts-ignore
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export type WorkerRun = {
  id: string;
  event_id: string;
  pipeline_edition: string;
  image_tag: string;
  status: string;
  host: string | null;
  container_id: string | null;
  container_name: string | null;
  current_stage: string | null;
  status_message: string | null;
  stop_requested: boolean;
  created_at: string;
  started_at: string | null;
  last_heartbeat_at: string | null;
  stopped_at: string | null;
  failure_reason: string | null;
};

export type Event = {
  id: string;
  name: string;
  description: string | null;
  created_at: string;
};

export async function getEvents(): Promise<Event[]> {
  const response = await fetch(`${API_BASE_URL}/events`);
  if (!response.ok) throw new Error("Failed to fetch events");
  return response.json();
}

export async function getPipelineEditions(): Promise<string[]> {
  const response = await fetch(`${API_BASE_URL}/pipeline-editions`);
  if (!response.ok) throw new Error("Failed to fetch pipeline editions");
  return response.json();
}

export async function getWorkers(activeOnly = false): Promise<WorkerRun[]> {
  const response = await fetch(
    `${API_BASE_URL}/workers?active_only=${activeOnly}`,
  );
  if (!response.ok) throw new Error("Failed to fetch workers");
  return response.json();
}

export async function launchWorker(
  eventId: string,
  pipelineEdition: string,
): Promise<WorkerRun> {
  const response = await fetch(`${API_BASE_URL}/workers/launch`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      event_id: eventId,
      pipeline_edition: pipelineEdition,
    }),
  });

  if (!response.ok) throw new Error("Failed to launch worker");
  return response.json();
}

export async function stopWorker(workerId: string): Promise<WorkerRun> {
  const response = await fetch(`${API_BASE_URL}/workers/${workerId}/stop`, {
    method: "POST",
  });

  if (!response.ok) throw new Error("Failed to stop worker");
  return response.json();
}