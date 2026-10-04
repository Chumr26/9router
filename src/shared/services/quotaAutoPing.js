// Quota auto-ping scheduler: warms 5h windows by sending tiny opt-in requests right after reset.
import "open-sse/index.js";

import { getSettings, getProviderConnections, updateProviderConnection } from "@/lib/localDb";
import { getClaudeUsage } from "open-sse/services/usage/claude.js";
import { getCodexUsage } from "open-sse/services/usage/codex.js";
import { getExecutor } from "open-sse/executors/index.js";
import { CLAUDE_CLI_SPOOF_HEADERS } from "open-sse/providers/shared.js";
import { proxyAwareFetch } from "open-sse/utils/proxyFetch.js";
import { resolveConnectionProxyConfig } from "@/lib/network/connectionProxy";
import { refreshAndUpdateCredentials } from "@/app/api/usage/[connectionId]/route.js";
import { QUOTA_AUTOPING_CONFIG } from "@/shared/constants/config";

const C = QUOTA_AUTOPING_CONFIG;
const CLAUDE_PING_URL = "https://api.anthropic.com/v1/messages?beta=true";

const providerHandlers = {
  claude: {
    getUsage: getClaudeUsage,
    sendPing: sendClaudePing,
  },
  codex: {
    getUsage: getCodexUsage,
    sendPing: sendCodexPing,
  },
};

// Survive Next.js hot reload and keep one scheduler per server process.
const g = (global.__quotaAutoPing ??= {
  interval: null,
  running: false,
  resetCache: {},
  failureCache: {},
});

function cacheKey(provider, connectionId) {
  return `${provider}:${connectionId}`;
}

function normalizeResetKey(resetAt) {
  const ms = new Date(resetAt).getTime();
  if (!Number.isFinite(ms)) return resetAt;
  return new Date(Math.floor(ms / 60000) * 60000).toISOString();
}

function getResetDriftMs(previousResetAt, nextResetAt) {
  const previousMs = new Date(previousResetAt).getTime();
  const nextMs = new Date(nextResetAt).getTime();
  if (!Number.isFinite(previousMs) || !Number.isFinite(nextMs)) return 0;
  return nextMs - previousMs;
}

function toFiniteNumber(value, fallback = null) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return fallback;
}

function isQuotaExhausted(quota) {
  if (!quota || quota.unlimited === true) return false;
  const remaining = toFiniteNumber(quota.remaining);
  if (remaining !== null) return remaining <= 0;

  const used = toFiniteNumber(quota.used);
  const total = toFiniteNumber(quota.total);
  return total !== null && total > 0 && used !== null && used >= total;
}

function wasPingedRecently(connection, intervalMs, nowMs = Date.now()) {
  if (!intervalMs) return false;
  const lastPingAtMs = new Date(connection.lastPingAt).getTime();
  return Number.isFinite(lastPingAtMs) && nowMs - lastPingAtMs < intervalMs;
}

function isBlockingQuotaName(name, sessionKey) {
  if (name === sessionKey) return false;
  return !String(name).toLowerCase().includes("session");
}

function hasExhaustedBlockingQuota(quotas, sessionKey) {
  return Object.entries(quotas || {}).some(([name, quota]) => isBlockingQuotaName(name, sessionKey) && isQuotaExhausted(quota));
}

function shouldPingForReset(providerConfig, cachedReset, resetAt, now) {
  if (providerConfig.pingWhenResetAtSlides) {
    return Boolean(cachedReset) && getResetDriftMs(cachedReset, resetAt) >= (providerConfig.resetAtDriftMs || 0);
  }

  const resetMs = new Date(resetAt).getTime();
  return Number.isFinite(resetMs) && now >= resetMs - C.pingLeadMs;
}

function buildProxyOptions(cfg) {
  return {
    connectionProxyEnabled: cfg.connectionProxyEnabled === true,
    connectionProxyUrl: cfg.connectionProxyUrl || "",
    connectionNoProxy: cfg.connectionNoProxy || "",
    vercelRelayUrl: cfg.vercelRelayUrl || "",
    strictProxy: false,
  };
}

async function sendClaudePing(connection, providerConfig, proxyOptions, deps) {
  const res = await deps.proxyAwareFetch(CLAUDE_PING_URL, {
    method: "POST",
    signal: providerConfig.signal,
    headers: {
      ...CLAUDE_CLI_SPOOF_HEADERS,
      "Authorization": `Bearer ${connection.accessToken}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: providerConfig.pingModel,
      max_tokens: providerConfig.pingMaxTokens,
      messages: [{ role: "user", content: providerConfig.pingText }],
    }),
  }, proxyOptions);
  if (!res.ok) return false;
  if (providerConfig.verifyCompletion) {
    const body = await res.json();
    return body.type === "message" && !body.error;
  }
  return true;
}

function buildCodexPingInput(text) {
  return [{
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }],
  }];
}

async function drainResponseBody(response) {
  if (typeof response?.text === "function") {
    return await response.text();
  }

  const reader = response?.body?.getReader?.();
  if (!reader) return "";

  const decoder = new TextDecoder();
  let text = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return text + decoder.decode();
      text += decoder.decode(value, { stream: true });
    }
  } finally {
    reader.releaseLock?.();
  }
}

async function sendCodexPing(connection, providerConfig, proxyOptions, deps) {
  const executor = deps.getExecutor("codex");
  const { response } = await executor.execute({
    model: providerConfig.pingModel,
    stream: true,
    signal: providerConfig.signal,
    credentials: {
      accessToken: connection.accessToken,
      connectionId: connection.id,
      providerSpecificData: connection.providerSpecificData,
    },
    proxyOptions,
    log: console,
    body: {
      model: providerConfig.pingModel,
      input: buildCodexPingInput(providerConfig.pingText),
      instructions: providerConfig.pingInstructions,
      reasoning: providerConfig.pingReasoningEffort
        ? { effort: providerConfig.pingReasoningEffort, summary: "auto" }
        : undefined,
      store: false,
      stream: true,
    },
  });
  if (!response.ok) {
    try { await response.body?.cancel?.(); } catch { /* noop */ }
    return false;
  }

  // Codex only starts the 5h window after the streaming response completes.
  const text = await drainResponseBody(response);
  if (providerConfig.verifyCompletion) {
    const events = text.split(/\r?\n/).filter((line) => line.startsWith("data: ")).flatMap((line) => {
      try { return [JSON.parse(line.slice(6))]; } catch { return []; }
    });
    return !events.some((event) => ["error", "response.failed", "response.incomplete"].includes(event.type)) &&
      events.some((event) => event.type === "response.completed" && event.response?.status === "completed");
  }
  return true;
}

async function withAccountLock(connectionId, deps, state, callback) {
  state.inFlight ??= new Set();
  if (state.inFlight.has(connectionId)) return { status: "skipped", message: "Account already has a ping running." };
  state.inFlight.add(connectionId);
  let lease;
  const signal = AbortSignal.timeout(60000);
  let abort;
  try {
    if (deps.acquirePingLease) {
      lease = await deps.acquirePingLease(connectionId);
      if (!lease) return { status: "skipped", message: "Account already has a ping running." };
    }
    return await Promise.race([
      callback(signal),
      new Promise((resolve) => {
        abort = () => resolve({ status: "failed", message: "Ping timed out; delivery may have occurred. Check quota before retrying." });
        signal.addEventListener("abort", abort, { once: true });
      }),
    ]);
  } finally {
    if (abort) signal.removeEventListener("abort", abort);
    state.inFlight.delete(connectionId);
    if (lease) await deps.releasePingLease(connectionId, lease);
  }
}

export async function pingAccountNow(connectionId, options = {}, deps = createDefaultDeps(), state = g) {
  const connections = await deps.getProviderConnections({ isActive: true });
  const connection = connections.find((item) => item.id === connectionId && item.isActive !== false && item.authType === "oauth");
  const providerConfig = connection && C.providers[connection.provider];
  if (!providerConfig) return { status: "skipped", message: "Active Claude or Codex OAuth account required." };
  return withAccountLock(connectionId, deps, state, async (signal) => {
    const work = async () => {
      const proxyOptions = buildProxyOptions(await deps.resolveConnectionProxyConfig(connection.providerSpecificData));
      const refreshed = await deps.refreshAndUpdateCredentials(connection, false, proxyOptions);
      if (!refreshed.connection.accessToken) return { status: "skipped", message: "Account has no access token. Reconnect before pinging." };
      const handler = providerHandlers[connection.provider];
      const usage = await handler.getUsage(refreshed.connection.accessToken, proxyOptions, { force: true });
      const quotas = usage?.quotas || {};
      const quota = quotas[providerConfig.quotaKey];
      if (!quota) return { status: "skipped", message: "Quota unavailable; no request sent." };
      if (isQuotaExhausted(quota) || hasExhaustedBlockingQuota(quotas, providerConfig.quotaKey)) {
        return { status: "skipped", message: "Quota exhausted; no request sent." };
      }
      signal.throwIfAborted();
      const config = {
        ...providerConfig,
        pingModel: options.model || providerConfig.pingModel,
        pingText: options.prompt || providerConfig.pingText,
        pingReasoningEffort: options.reasoning || providerConfig.pingReasoningEffort,
        signal,
        verifyCompletion: true,
      };
      const succeeded = await handler.sendPing(refreshed.connection, config, proxyOptions, deps);
      signal.throwIfAborted();
      if (!succeeded) return { status: "failed", message: "Provider did not complete the ping." };
      await deps.updateProviderConnection(connectionId, {
        lastPingAt: new Date().toISOString(),
        ...(quota.resetAt ? { lastPingedResetAt: quota.resetAt, lastPingedResetKey: normalizeResetKey(quota.resetAt) } : {}),
      });
      return { status: "succeeded", message: "Ping completed." };
    };
    try {
      return await work();
    } catch {
      return { status: "failed", message: "Ping failed. Check account authentication and provider availability." };
    }
  });
}

function shouldSkipAfterFailure(state, key, nowMs = Date.now()) {
  const failedAt = state.failureCache[key];
  return failedAt && nowMs - failedAt < C.failureCooldownMs;
}

async function pingConnection(conn, provider, providerConfig, handler, deps, state = g) {
  const key = cacheKey(provider, conn.id);

  // resetAt is stable for time-based windows; Codex polls every tick because inactive windows slide forward.
  const cachedReset = state.resetCache[key];
  if (!providerConfig.pingWhenResetAtSlides && cachedReset && Date.now() < new Date(cachedReset).getTime() - C.refreshAheadMs) return;

  // Avoid hammering provider auth/quota endpoints if a ping failed recently.
  if (shouldSkipAfterFailure(state, key)) return;

  const proxyCfg = await deps.resolveConnectionProxyConfig(conn.providerSpecificData);
  const proxyOptions = buildProxyOptions(proxyCfg);

  let connection = conn;
  try {
    const r = await deps.refreshAndUpdateCredentials(connection, false, proxyOptions);
    connection = r.connection;
  } catch (e) {
    state.failureCache[key] = Date.now();
    console.warn(`[AutoPing] ${provider}:${conn.id}: refresh failed: ${e.message}`);
    return;
  }

  const usage = await handler.getUsage(connection.accessToken, proxyOptions);
  providerConfig.signal?.throwIfAborted();
  const quotas = usage?.quotas || {};
  const quota = quotas?.[providerConfig.quotaKey];
  const resetAt = quota?.resetAt;
  if (!resetAt) return;

  state.resetCache[key] = resetAt;

  if (providerConfig.skipWhenBlockingQuotaExhausted && hasExhaustedBlockingQuota(quotas, providerConfig.quotaKey)) return;
  if (isQuotaExhausted(quota)) return;

  const now = Date.now();
  const resetKey = normalizeResetKey(resetAt);
  const lastPingedResetKey = connection.lastPingedResetKey || normalizeResetKey(connection.lastPingedResetAt);

  // Claude waits for reset. Codex pings only when resetAt slides, which means the 5h window is inactive.
  if (!shouldPingForReset(providerConfig, cachedReset, resetAt, now)) return;
  if (wasPingedRecently(connection, providerConfig.minPingIntervalMs, now)) return;
  if (lastPingedResetKey === resetKey) return;

  const ok = await handler.sendPing(connection, providerConfig, proxyOptions, deps);
  providerConfig.signal?.throwIfAborted();
  if (!ok) {
    // Do not mark reset as pinged unless upstream accepted the tiny request.
    state.failureCache[key] = Date.now();
    console.warn(`[AutoPing] ${provider}:${connection.id}: ping failed (reset ${resetAt})`);
    return;
  }

  delete state.failureCache[key];
  await deps.updateProviderConnection(connection.id, {
    lastPingedResetAt: resetAt,
    lastPingedResetKey: resetKey,
    lastPingAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
  console.log(`[AutoPing] ${provider}:${connection.id}: ping sent (reset ${resetAt})`);
}

function createDefaultDeps() {
  return {
    getSettings,
    getProviderConnections,
    updateProviderConnection,
    resolveConnectionProxyConfig,
    refreshAndUpdateCredentials,
    proxyAwareFetch,
    getExecutor,
    acquirePingLease: async (id) => (await import("@/lib/db/repos/wakeupRepo.js")).acquirePingLease(id),
    releasePingLease: async (id, lease) => (await import("@/lib/db/repos/wakeupRepo.js")).releasePingLease(id, lease),
  };
}

export async function runQuotaAutoPingTick(deps = createDefaultDeps(), state = g) {
  if (state.running) return;
  state.running = true;
  try {
    const settings = await deps.getSettings();

    for (const [provider, providerConfig] of Object.entries(C.providers)) {
      const handler = providerHandlers[provider];
      if (!handler) continue;

      const enabledMap = settings?.[providerConfig.settingsKey]?.connections || {};
      if (Object.keys(enabledMap).length === 0) continue;

      const conns = await deps.getProviderConnections({ provider, isActive: true });
      const targets = conns.filter((conn) => conn.authType === "oauth" && enabledMap[conn.id] === true);
      for (const conn of targets) {
        try {
          await withAccountLock(conn.id, deps, state, (signal) => pingConnection(conn, provider, { ...providerConfig, signal }, handler, deps, state));
        } catch (e) {
          state.failureCache[cacheKey(provider, conn.id)] = Date.now();
          console.warn(`[AutoPing] ${provider}:${conn.id}: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.warn("[AutoPing] tick error:", e.message);
  } finally {
    state.running = false;
  }
}

export function startQuotaAutoPing() {
  if (g.interval) return;
  console.log("[AutoPing] scheduler started");
  runQuotaAutoPingTick().catch(() => {});
  g.interval = setInterval(() => { runQuotaAutoPingTick().catch(() => {}); }, C.tickIntervalMs);
  if (g.interval.unref) g.interval.unref();
}

export function stopQuotaAutoPing() {
  if (!g.interval) return;
  clearInterval(g.interval);
  g.interval = null;
  console.log("[AutoPing] scheduler stopped");
}

export function configureQuotaAutoPing(settings) {
  const enabled = Object.values(C.providers).some((providerConfig) =>
    Object.values(settings?.[providerConfig.settingsKey]?.connections || {}).some(Boolean)
  );
  if (enabled) startQuotaAutoPing();
  else stopQuotaAutoPing();
}
