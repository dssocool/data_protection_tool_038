import { isDemoMode } from "./mockData";

export function getAgentPath(): string | null {
  const segments = window.location.pathname.split("/");
  const agentsIdx = segments.indexOf("agents");
  if (agentsIdx === -1 || agentsIdx + 1 >= segments.length) {
    return isDemoMode() ? "demo" : null;
  }
  return segments[agentsIdx + 1];
}

export async function agentFetch<T = unknown>(
  endpoint: string,
  options?: RequestInit,
): Promise<T> {
  const agentPath = getAgentPath();
  if (!agentPath) throw new Error("No agent path available");
  const url = `/api/agents/${agentPath}/${endpoint}`;
  const res = await fetch(url, options);
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

export async function agentPost<T = unknown>(
  endpoint: string,
  body: unknown,
): Promise<T> {
  return agentFetch<T>(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
