// @ts-ignore
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "/api";

export type WorkerStatus =
  | "requested"
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
  "starting",
  "running",
  "stop_requested",
  "stopping",
];

export type WorkerRun = {
  id: string;
  event_id: number;
  pipeline_edition: string;
  image_tag: string;
  status: WorkerStatus;
  host: string | null;
  container_id: string | null;
  container_name: string | null;
  current_stage: string | null;
  status_message: string | null;
  stop_requested: boolean;
  configuration_profile: string | null;
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

export type ConfigurationProfiles = {
  profiles: string[];
  default: string;
};

export type LaunchOptions = {
  pipelineEdition: string;
  configurationProfile: string;
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
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
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

export async function getConfigurationProfiles(): Promise<ConfigurationProfiles> {
  return asJson(await fetch(`${API_BASE_URL}/configuration-profiles`));
}

export async function launchWorker(eventId: number, options: LaunchOptions): Promise<WorkerRun> {
  const response = await fetch(`${API_BASE_URL}/workers/launch`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      event_id: eventId,
      pipeline_edition: options.pipelineEdition,
      configuration_profile: options.configurationProfile,
      reprocess: options.reprocess,
    }),
  });

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