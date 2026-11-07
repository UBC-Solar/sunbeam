import {ReactNode, useEffect, useLayoutEffect, useRef, useState} from "react";
import {getWorkerLogs, workerLogStreamUrl} from "../api";

// A worker's container logs as a read-only terminal: the time, a coloured level and the logger before each message.
//
// Docker prefixes every line with its own timestamp, and the worker's logging adds the usual Python fields:
//   2026-11-07T22:55:10.0226Z 2026-11-07 22:55:10,022 INFO sunbeam.worker.preflight: Preflight server: OK (reachable)
// Lines without the Python fields (a traceback, a replay's fetch progress) keep the level of the line before them, so
// a traceback is shown as part of its error.

type Level = "DEBUG" | "INFO" | "WARNING" | "ERROR" | "CRITICAL";

type LogLine = {
  time: string | null;
  level: Level | null;
  logger: string | null;
  message: string;
  // The level of the latest line that had one; colours lines that don't
  context: Level | null;
};

const PATTERN =
  /^(?:(\d{4}-\d\d-\d\dT[\d:.]+Z?)\s)?(?:\d{4}-\d\d-\d\d (\d\d:\d\d:\d\d),(\d{3})\s+(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+([\w.]+):\s?)?(.*)$/;

// Colour codes Rich or Python might print; shown as plain text
const ANSI = /\x1b\[[0-9;]*m/g;

function parse(lines: string[]): LogLine[] {
  let context: Level | null = null;
  return lines.map((raw) => {
    const [, dockerTime, time, millis, level, logger, message] = raw.replace(ANSI, "").match(PATTERN) ?? [];
    if (level) context = level as Level;
    return {
      time: time ? `${time}.${millis}` : dockerTime ? dockerTime.slice(11, 23) : null,
      level: (level as Level) ?? null,
      logger: logger ? logger.replace(/^sunbeam\./, "") : null,
      message: message ?? raw,
      context,
    };
  });
}

const LEVEL_LABEL: Record<Level, string> = {DEBUG: "DBG", INFO: "INF", WARNING: "WRN", ERROR: "ERR", CRITICAL: "CRT"};

// Within a message: `key=` dimmed (the worker's timing lines are all key=value), OK and FAILED coloured
const HIGHLIGHT = /(\b[\w.]+=)|\b(OK)\b|\b(FAILED|Traceback)\b/g;

function Message({text}: {text: string}) {
  const parts: ReactNode[] = [];
  let last = 0;
  for (const match of text.matchAll(HIGHLIGHT)) {
    const index = match.index ?? 0;
    parts.push(text.slice(last, index));
    const [token, key, ok, failed] = match;
    const className = key ? "term-key" : ok ? "term-ok" : failed ? "term-bad" : "";
    parts.push(
      <span key={index} className={className}>
        {token}
      </span>,
    );
    last = index + token.length;
  }
  parts.push(text.slice(last));
  return <>{parts}</>;
}

function Line({line}: {line: LogLine}) {
  const level = line.level ?? line.context;
  // A line without its own level that follows an error is part of it, e.g. a traceback
  const continuation = !line.level && (level === "ERROR" || level === "CRITICAL");

  return (
    <div className={`term-line ${continuation ? "term-error-text" : ""} ${line.level ? `level-${line.level}` : ""}`}>
      <span className="term-time">{line.time ?? ""}</span>
      <span className={`term-level ${line.level ? `level-${line.level}` : ""}`}>
        {line.level ? LEVEL_LABEL[line.level] : ""}
      </span>
      <span className="term-text">
        {line.logger && <span className="term-logger">{line.logger} </span>}
        <Message text={line.message} />
      </span>
    </div>
  );
}

type LogTerminalProps = {
  workerId: string;
  // The container's name, for the title bar; null when there's no container to read
  containerName: string | null;
  isActive: boolean;
};

export function LogTerminal({workerId, containerName, isActive}: LogTerminalProps) {
  const [lines, setLines] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // A running worker's logs follow along, as a terminal's would
  const [liveTail, setLiveTail] = useState(isActive);

  const bodyRef = useRef<HTMLDivElement>(null);
  // Follow new lines only while scrolled to the bottom, as a terminal does
  const followRef = useRef(true);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const result = await getWorkerLogs(workerId, 500);
      setLines(result?.lines ?? []);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setLines([]);
    setLiveTail(isActive);
    followRef.current = true;
    if (!isActive) refresh();
    // Only when the worker changes
  }, [workerId]);

  useEffect(() => {
    if (!isActive && liveTail) setLiveTail(false);
  }, [isActive, liveTail]);

  useEffect(() => {
    if (!liveTail) return;

    // The stream starts with the latest lines itself, so it replaces what was loaded rather than repeating it
    setLines([]);
    followRef.current = true;
    const source = new EventSource(workerLogStreamUrl(workerId, 500));
    source.onmessage = (event) => setLines((previous) => [...previous, event.data]);
    source.onerror = () => setError("Live log stream disconnected.");
    return () => source.close();
  }, [liveTail, workerId]);

  useLayoutEffect(() => {
    const body = bodyRef.current;
    if (body && followRef.current) body.scrollTop = body.scrollHeight;
  }, [lines]);

  function onScroll() {
    const body = bodyRef.current;
    if (body) followRef.current = body.scrollHeight - body.scrollTop - body.clientHeight < 24;
  }

  const parsed = parse(lines);

  return (
    <div className="terminal">
      <div className="terminal-bar">
        <span className="terminal-title">{containerName ?? "logs"}</span>
        <span className="spacer" />
        {isActive && (
          <button className={`terminal-button ${liveTail ? "on" : ""}`} onClick={() => setLiveTail((on) => !on)}>
            {liveTail ? "● live" : "○ live"}
          </button>
        )}
        <button className="terminal-button" onClick={refresh} disabled={loading}>
          {loading ? "loading…" : "refresh"}
        </button>
      </div>

      <div className="terminal-body" ref={bodyRef} onScroll={onScroll}>
        {error && <div className="term-line term-error-text">{error}</div>}
        {parsed.length === 0 && !loading && !error && <div className="term-dim">No logs available.</div>}
        {parsed.map((line, i) => (
          <Line key={i} line={line} />
        ))}
      </div>
    </div>
  );
}
