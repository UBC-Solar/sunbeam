// @ts-ignore
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "/api";

export type WorkerStatus =
  | "requested"
  | "building"
  | "starting"
  | "running"
  | "stop_requested"
  | "stopping"
  | "completed"
  | "failed"
  | "lost"
  | "cancelled";

export const ACTIVE_STATUSES: WorkerStatus[] = [
  "requested",
  "building",
  "starting",
  "running",
  "stop_requested",
  "stopping",
];

export type WorkerRun = {
  id: string;
  event_id: number;
  pipeline_edition: string;
  image_tag: string | null;
  status: WorkerStatus;
  host: string | null;
  container_id: string | null;
  container_name: string | null;
  current_stage: string | null;
  status_message: string | null;
  stop_requested: boolean;
  influx_connection_id: number | null;
  // A snapshot of the connection's name at launch; external workers report their URL and bucket instead
  influx_connection_name: string | null;
  // The git ref the image was built from (null: the server's own copy of the code), and its commit
  source_ref: string | null;
  source_sha: string | null;
  reprocess: boolean;
  created_at: string;
  started_at: string | null;
  last_heartbeat_at: string | null;
  stopped_at: string | null;
  failure_reason: string | null;
};

export type Event = {
  id: number;
  name: string;
  description: string | null;
  starts_at: string;
  ends_at: string | null;
  pipeline_edition: string;
  // A past event: its worker replays it, always wiping earlier samples first
  is_replay: boolean;
};

export type ConnectionFields = {
  url: string;
  organization: string;
  bucket: string;
  token: string;
};

export type Connection = ConnectionFields & {
  id: number;
  name: string;
  kind: "influxdb";
  is_default: boolean;
  created_at: string;
  updated_at: string;
  last_tested_at: string | null;
  last_test_ok: boolean | null;
  last_test_detail: string | null;
};

export type ConnectionTestResult = {
  ok: boolean;
  detail: string;
  steps: {name: string; ok: boolean; detail: string}[];
  window_start: string | null;
  window_end: string | null;
};

export type LaunchOptions = {
  pipelineEdition: string;
  influxConnectionId: number | null;
  // A git branch, tag, or full commit SHA; null builds from the server's own copy of the code
  sourceRef: string | null;
  reprocess: boolean;
};

export type StageMetric = {
  name: string;
  total_ms: number;
  avg_ms: number;
  max_ms: number;
  calls: number;
};

export type PipelineMetric = {
  name: string;
  total_ms: number;
  avg_ms: number;
  ticks: number;
  late_now_ms: number;
  late_max_ms: number;
  stages: StageMetric[];
};

export type ReplayProgress = {
  start: string;
  end: string;
  current: string;
  fraction: number;
  processed_s: number;
  total_s: number;
  elapsed_s: number;
  speed: number | null;
  eta_s: number | null;
};

export type WorkerMetrics = {
  idle_pct: number;
  busy_pct: number;
  writer_ms: number;
  pipelines: PipelineMetric[];
  replay: ReplayProgress | null;
  reported_at: string;
};

export type WorkerLogs = {
  lines: string[];
};

async function asJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    // FastAPI puts the reason in "detail"; fall back to the status line when there isn't one
    let detail: unknown = null;
    try {
      detail = (await response.json()).detail;
    } catch {
      // Not JSON
    }
    throw new Error(typeof detail === "string" ? detail : `${response.status} ${response.statusText}`);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json();
}

function jsonRequest(method: string, body?: unknown): RequestInit {
  return {
    method,
    headers: {"Content-Type": "application/json"},
    body: body === undefined ? undefined : JSON.stringify(body),
  };
}

export async function getEvents(): Promise<Event[]> {
  return asJson(await fetch(`${API_BASE_URL}/events`));
}

export async function getPipelineEditions(): Promise<string[]> {
  return asJson(await fetch(`${API_BASE_URL}/pipeline-editions`));
}

export async function getWorkers(): Promise<WorkerRun[]> {
  return asJson(await fetch(`${API_BASE_URL}/workers?active_only=false`));
}

export async function getConnections(): Promise<Connection[]> {
  return asJson(await fetch(`${API_BASE_URL}/connections`));
}

export async function createConnection(
  fields: ConnectionFields & {name: string; is_default?: boolean},
): Promise<Connection> {
  return asJson(await fetch(`${API_BASE_URL}/connections`, jsonRequest("POST", fields)));
}

export async function updateConnection(
  id: number,
  fields: Partial<ConnectionFields & {name: string}>,
): Promise<Connection> {
  return asJson(await fetch(`${API_BASE_URL}/connections/${id}`, jsonRequest("PATCH", fields)));
}

export async function deleteConnection(id: number): Promise<void> {
  return asJson(await fetch(`${API_BASE_URL}/connections/${id}`, {method: "DELETE"}));
}

export async function makeDefaultConnection(id: number): Promise<Connection> {
  return asJson(await fetch(`${API_BASE_URL}/connections/${id}/default`, {method: "POST"}));
}

// Test settings before they're saved; with an event, also require data over its window
export async function testConnectionFields(
  fields: ConnectionFields,
  eventId: number | null,
): Promise<ConnectionTestResult> {
  return asJson(
    await fetch(`${API_BASE_URL}/connections/test`, jsonRequest("POST", {...fields, event_id: eventId})),
  );
}

// Test a saved connection; the result is recorded on it
export async function testConnection(id: number, eventId: number | null): Promise<ConnectionTestResult> {
  const query = eventId === null ? "" : `?event_id=${eventId}`;
  return asJson(await fetch(`${API_BASE_URL}/connections/${id}/test${query}`, {method: "POST"}));
}

export async function launchWorker(eventId: number, options: LaunchOptions): Promise<WorkerRun> {
  const response = await fetch(
    `${API_BASE_URL}/workers/launch`,
    jsonRequest("POST", {
      event_id: eventId,
      pipeline_edition: options.pipelineEdition,
      influx_connection_id: options.influxConnectionId,
      source_ref: options.sourceRef,
      reprocess: options.reprocess,
    }),
  );

  return asJson(response);
}

export async function stopWorker(workerId: string): Promise<WorkerRun> {
  const response = await fetch(`${API_BASE_URL}/workers/${workerId}/stop`, {
    method: "POST",
  });

  return asJson(response);
}

export async function getWorkerMetrics(
  workerId: string,
): Promise<WorkerMetrics | null> {
  const response = await fetch(`${API_BASE_URL}/workers/${workerId}/metrics`);

  if (response.status === 404) {
    return null;
  }

  return asJson(response);
}

export async function getWorkerLogs(
  workerId: string,
  tail = 500,
): Promise<WorkerLogs | null> {
  const response = await fetch(
    `${API_BASE_URL}/workers/${workerId}/logs?tail=${tail}`,
  );

  if (response.status === 404) {
    return null;
  }

  return asJson(response);
}

export function workerLogStreamUrl(workerId: string, tail = 200): string {
  return `${API_BASE_URL}/workers/${workerId}/logs/stream?tail=${tail}`;
}