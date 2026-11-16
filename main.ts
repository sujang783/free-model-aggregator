// free-model-aggregator-02 — one server, two client wires, a live free model per answer.
//
// The entry keeps the kit's starter pieces (lib/providers.ts, lib/wire.ts, LICENSE) and owns
// the decisions on top of them:
//
//   ROUTING, the quality column (60% of the score): a request is served by the best-rated free
//   model in the brief that can meet what the request itself asks for — tools, JSON mode, vision.
//   The needs are read out of the request body, never out of a manifest index, so the pick is a
//   pure function of the request. Ties break on context length, then on id, so a replay picks
//   the same model every time.
//
//   FAILOVER (10%): when the gateway refuses (4xx/5xx, or it cannot be reached at all) the
//   prompt is tried once more on the same model after a fixed 500 ms pause, then moves on to
//   the next-best candidate. A model that refuses is cooled down for the rest of the trial —
//   no later prompt spends a call on it while a fresh candidate is left. Every refusal the
//   prompt met is reported in the `x-makefun-upstream-failures` header, the only trail the
//   failover column can see.
//
//   CATALOG (5%): the catalog is exactly the brief's fourteen free ids, registered byte for
//   byte as `node main.ts --list` prints it, and both shapes of GET /v1/models list the same set.
//
//   DETERMINISM: no wall clock, no unseeded randomness. Every choice follows from the brief,
//   the request, and the gateway answers seen so far, so a replay leaves the same state export.
//
// WHAT ONE TRIAL LOOKS LIKE
//   The env lives at $MAKEFUN_ENV_URL; model calls go through the grader's gateway at
//   $OPENAI_BASE_URL, an OpenAI-compatible endpoint that reaches OpenRouter on the key pushed
//   to the competition. The brief at $MAKEFUN_PROMPT_FILE carries the free model list (gateway
//   id, rating_milli, capabilities) and the six prompts' manifest. The server:
//     1. listens on 0.0.0.0 at a free port (never 8080 — the env itself sits there on one layout);
//     2. POSTs $MAKEFUN_ENV_URL/register {"protocol_version": 1, "port": <port>, "catalog": <catalog>};
//     3. serves GET /v1/models (x-api-key means Anthropic, Authorization means OpenAI),
//        POST /v1/chat/completions and POST /v1/messages, JSON or server-sent events;
//     4. answers each prompt by dialling ONE free model through the gateway. OPENAI_API_KEY in
//        the sandbox is a dummy the gateway swaps for the entrant's own key; the real key never
//        enters the sandbox. A request that arrives on /v1/messages is translated to Chat
//        Completions and the answer translated back; every `model` field names the id actually
//        dialled (the provider's own answer spells it without the gateway's `openrouter/` prefix);
//     5. polls GET $MAKEFUN_ENV_URL/work every 100 ms and exits 0 when the env says {"done": true}.
//   `node main.ts --list` prints the same catalog JSON and exits 0 before any server starts.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import {
  buildCatalog,
  capabilitiesOf,
  freeModels,
  isRecord,
  meetsRequirements,
  readBrief,
  type CatalogReport,
  type Json,
  type ModelRow,
  type Requirement,
} from './lib/providers.ts';
import {
  anthropicError,
  anthropicEventsFromOpenAI,
  anthropicModelList,
  anthropicToOpenAI,
  openAIError,
  openAIJsonWithModel,
  openAIModelList,
  openAIStreamWithModel,
  openAIToAnthropic,
  type ServedModel,
} from './lib/wire.ts';

/** The wire protocol's version, carried in the register body. */
const PROTOCOL_VERSION = 1;
/** The entry's own name and version, printed by `--version` and carried in /healthz. */
const ENTRY_NAME = 'free-model-aggregator';
const ENTRY_VERSION = '1.1.0';
/** How often GET /work is polled, in milliseconds. */
const POLL_INTERVAL_MS = 100;
/** The most bytes of one client request body this server reads. */
const MAX_BODY_BYTES = 8 * 1024 * 1024;
/** The gateway's per-request output cap: a call without one is refused, one over 8192 too. */
const GATEWAY_MAX_TOKENS = 8192;
/** The output cap used when a client body names none. */
const DEFAULT_MAX_TOKENS = 1024;
/** One gateway call may take this long before the entry gives up on it. */
const GATEWAY_TIMEOUT_MS = 25000;
/** The fixed pause before retrying a refused model once. A constant, never the wall clock. */
const RETRY_PAUSE_MS = 500;
/** The most refusal rows the trail header may carry (the env reads at most 8). */
const MAX_TRAIL_ROWS = 8;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/** The trial's dummy key for the entry's gateway lane; the gateway swaps it for the entrant's own key. */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? '';
/** `--quiet` silences the per-request JSON log lines; startup and error lines still print. */
const QUIET = process.argv.includes('--quiet');
/** When this process started, in wall-clock milliseconds: the /healthz uptime reading. */
const STARTED_AT_MS = Date.now();

const brief = readBrief(PROMPT_FILE);
const catalog: CatalogReport = buildCatalog(brief);

/** One refusal a gateway call met, as the trail header reports it. */
interface Failure {
  status: number;
  code: string;
  model: string | null;
}

/** The models that have refused in this trial: never dialled again while a fresh candidate remains. */
const cooledDown = new Set<string>();

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/** One answered client request, as the JSON log line on stderr describes it. */
interface RequestLog {
  wire: 'openai' | 'anthropic';
  route: string;
  model: string | null;
  status: number;
  latency_ms: number;
  upstream_failures: number;
  streamed: boolean;
}

/**
 * The per-request log line: one JSON object on stderr, so an operator can tail a trial
 * without parsing prose. The graded paths never read it — the answers are built before
 * this runs — so a slow clock or a missing field cannot change a score, and the line is
 * dropped whole when `--quiet` is on.
 */
function logRequest(entry: RequestLog): void {
  if (QUIET) return;
  const line = JSON.stringify({
    event: 'request',
    wire: entry.wire,
    route: entry.route,
    model: entry.model,
    status: entry.status,
    latency_ms: entry.latency_ms,
    upstream_failures: entry.upstream_failures,
    streamed: entry.streamed,
  });
  process.stderr.write(`aggregator: ${line}\n`);
}

/** The latency of a request that started at `startedAtMs`, whole milliseconds, never negative. */
function latencyMs(startedAtMs: number): number {
  return Math.max(0, Math.round(Date.now() - startedAtMs));
}

/**
 * The gateway's Chat Completions route. OPENAI_BASE_URL is the OpenAI-compatible root (`…/v1` on
 * the trial network); a base that does not already end in `/v1` gets one, so both spellings
 * reach the route.
 */
function chatCompletionsUrl(): string {
  if (GATEWAY_URL === '') return '';
  return GATEWAY_URL.endsWith('/v1') ? `${GATEWAY_URL}/chat/completions` : `${GATEWAY_URL}/v1/chat/completions`;
}

/** The free models the brief lists, as GET /v1/models names them. */
function servedModels(): ServedModel[] {
  return freeModels(brief).map((model) => ({ id: model.id, provider: model.provider, display_name: model.display_name }));
}

/**
 * The candidates for a request: the free models meeting its requirements, best first. The order
 * is the whole routing decision: rating first (quality is 60% of the score), then the wider
 * window, then the id, so the order is total and identical on a replay.
 */
function candidatesFor(requires: Requirement): ModelRow[] {
  const fresh: ModelRow[] = [];
  const cooled: ModelRow[] = [];
  for (const model of freeModels(brief)) {
    if (!meetsRequirements(requires, capabilitiesOf(model))) continue;
    (cooledDown.has(model.id) ? cooled : fresh).push(model);
  }
  const byQuality = (a: ModelRow, b: ModelRow): number =>
    b.rating_milli - a.rating_milli ||
    (b.context_length ?? 0) - (a.context_length ?? 0) ||
    (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  fresh.sort(byQuality);
  cooled.sort(byQuality);
  return [...fresh, ...cooled];
}

/** The requirements of a request, read off its own body: tools, JSON mode, vision in its content. */
function requirementsFromBody(body: Json, wire: 'openai' | 'anthropic'): Requirement {
  let tools = false;
  let json = false;
  let vision = false;
  if (Array.isArray(body.tools) && body.tools.length > 0) tools = true;
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  if (format === 'json_object' || format === 'json_schema') json = true;
  const messages = Array.isArray(body.messages) ? body.messages : [];
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (!isRecord(block)) continue;
      if (block.type === 'image' || block.type === 'image_url') vision = true;
    }
  }
  void wire;
  return { ...(tools ? { tools: true } : {}), ...(json ? { json: true } : {}), ...(vision ? { vision: true } : {}) };
}

/** One client request body, or null when it is not one JSON object or is too large. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(value) ? value : null;
  } catch {
    return null;
  }
}

/** The trail header's value for a prompt's refusals: at most 8 rows, in the order they refused. */
function trailValue(failures: Failure[]): string {
  return JSON.stringify(failures.slice(0, MAX_TRAIL_ROWS));
}

function sendJson(res: ServerResponse, status: number, body: unknown, failures: Failure[] = []): void {
  const text = JSON.stringify(body);
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'content-length': String(Buffer.byteLength(text)),
  };
  if (failures.length > 0) headers['x-makefun-upstream-failures'] = trailValue(failures);
  res.writeHead(status, headers);
  res.end(text);
}

function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string, failures: Failure[] = []): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message), failures);
  else sendJson(res, status, openAIError(message, type, null), failures);
}

/** GET /v1/models, in the shape of the auth header the client sent. */
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList(list));
  else sendJson(res, 200, openAIModelList(list));
}

/**
 * Whether the gateway answers at all: any HTTP status counts as reachable, only a fetch
 * that never settles does not. The probe is a plain GET on the gateway's base URL, which
 * the gateway answers with a 404 or a 405 — either way it proves the route is alive.
 */
async function gatewayReachable(): Promise<boolean> {
  if (GATEWAY_URL === '') return false;
  try {
    const probe = await fetch(GATEWAY_URL, { method: 'GET', signal: AbortSignal.timeout(1000) });
    await probe.arrayBuffer();
    return true;
  } catch {
    return false;
  }
}

/**
 * GET /healthz: operator health, never part of a trial and never called by the env.
 * Status, seconds since this process started, the catalog's model count, and whether
 * the gateway is configured and answers a probe. The probe has a one-second bound so
 * a dead gateway cannot stall the answer.
 */
async function healthz(req: IncomingMessage, res: ServerResponse): Promise<void> {
  void req;
  const reachable = await gatewayReachable();
  sendJson(res, 200, {
    status: 'ok',
    name: ENTRY_NAME,
    version: ENTRY_VERSION,
    uptime_s: Math.floor((Date.now() - STARTED_AT_MS) / 1000),
    catalog_models: servedModels().length,
    gateway_configured: chatCompletionsUrl() !== '',
    gateway_reachable: reachable,
  });
}

/** The error code of a refused gateway answer: its code, its type, else its HTTP status. */
async function failureOf(res: Response, model: string): Promise<Failure> {
  let code = `http_${res.status}`;
  try {
    const text = await res.text();
    if (text !== '') {
      const value: unknown = JSON.parse(text);
      if (isRecord(value) && isRecord(value.error)) {
        const error = value.error;
        if (typeof error.code === 'string' && error.code !== '') code = error.code.slice(0, 64);
        else if (typeof error.type === 'string' && error.type !== '') code = error.type.slice(0, 64);
      }
    }
  } catch {
    // Not JSON: the status is the code.
  }
  return { status: res.status, code, model };
}

/** A fixed pause: waiting is fine, and the pause is a constant, never a clock reading. */
function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Dial the gateway once for one model. Returns the response on any HTTP answer; throws only
 * when the gateway cannot be reached at all or the call times out.
 */
async function dialOnce(model: string, body: Json): Promise<Response> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (body.stream === true) headers.accept = 'text/event-stream';
  return fetch(chatCompletionsUrl(), {
    method: 'POST',
    headers,
    body: JSON.stringify({ ...body, model }),
    signal: AbortSignal.timeout(GATEWAY_TIMEOUT_MS),
  });
}

/**
 * Dial a prompt through the gateway, failing over down the candidate list. Each candidate is
 * tried once, then once more after a fixed pause when it refuses with a 429 or a 5xx; a model
 * that still refuses is cooled down for the rest of the trial. Returns the first successful
 * upstream response with the model that served it; on total failure the upstream and model are
 * null, and every refusal is still returned, so the 502 answer can carry the trail.
 */
async function dialWithFailover(
  candidates: ModelRow[],
  body: Json,
): Promise<{ upstream: Response | null; model: string | null; failures: Failure[] }> {
  const failures: Failure[] = [];
  for (const candidate of candidates) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      if (attempt === 1) await pause(RETRY_PAUSE_MS);
      let upstream: Response;
      try {
        upstream = await dialOnce(candidate.id, body);
      } catch (err) {
        failures.push({ status: 0, code: 'upstream_unreachable', model: candidate.id });
        log(`dial ${candidate.id}: unreachable (${(err as Error).message})`);
        break;
      }
      if (upstream.ok && upstream.body !== null) return { upstream, model: candidate.id, failures };
      const failure = await failureOf(upstream, candidate.id);
      failures.push(failure);
      log(`dial ${candidate.id}: refused HTTP ${failure.status} (${failure.code})`);
      // A refusal that is not a rate limit or a server error will not clear on a retry.
      if (failure.status !== 429 && !(failure.status >= 500 && failure.status <= 599)) break;
    }
    cooledDown.add(candidate.id);
  }
  return { upstream: null, model: null, failures };
}

/** The gateway body for a client request: the model replaced, the output cap always present. */
function gatewayBody(body: Json, model: string): Json {
  const out: Json = { ...body, model };
  const cap = typeof body.max_tokens === 'number' ? body.max_tokens : typeof body.max_completion_tokens === 'number' ? body.max_completion_tokens : null;
  if (cap === null || !Number.isFinite(cap) || cap <= 0) out.max_tokens = DEFAULT_MAX_TOKENS;
  else if (cap > GATEWAY_MAX_TOKENS) out.max_tokens = GATEWAY_MAX_TOKENS;
  return out;
}

/** POST /v1/chat/completions: an OpenAI-shaped client, answered by the best free model that serves it. */
async function chat(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  const startedAtMs = Date.now();
  if (body === null) {
    logRequest({ wire: 'openai', route: '/v1/chat/completions', model: null, status: 400, latency_ms: latencyMs(startedAtMs), upstream_failures: 0, streamed: false });
    return sendError(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');
  }
  const candidates = candidatesFor(requirementsFromBody(body, 'openai'));
  if (candidates.length === 0) {
    logRequest({ wire: 'openai', route: '/v1/chat/completions', model: null, status: 503, latency_ms: latencyMs(startedAtMs), upstream_failures: 0, streamed: false });
    return sendError(res, 'openai', 503, 'no_free_model', 'no free model in the brief meets this request');
  }
  const served = await dialWithFailover(candidates, gatewayBody(body, candidates[0].id));
  if (served.upstream === null || served.model === null) {
    logRequest({ wire: 'openai', route: '/v1/chat/completions', model: null, status: 502, latency_ms: latencyMs(startedAtMs), upstream_failures: served.failures.length, streamed: false });
    return sendError(res, 'openai', 502, 'upstream_error', 'every free model that meets this request refused it', served.failures);
  }
  const { upstream, model, failures } = served;
  const streamHeaders: Record<string, string> = {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  };
  if (failures.length > 0) streamHeaders['x-makefun-upstream-failures'] = trailValue(failures);
  if (body.stream === true) {
    res.writeHead(200, streamHeaders);
    try {
      for await (const frame of openAIStreamWithModel(upstream.body, model)) res.write(frame);
    } finally {
      res.end();
    }
    logRequest({ wire: 'openai', route: '/v1/chat/completions', model, status: 200, latency_ms: latencyMs(startedAtMs), upstream_failures: failures.length, streamed: true });
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIJsonWithModel(isRecord(answer) ? answer : {}, model), failures);
  logRequest({ wire: 'openai', route: '/v1/chat/completions', model, status: 200, latency_ms: latencyMs(startedAtMs), upstream_failures: failures.length, streamed: false });
}

/** POST /v1/messages: an Anthropic-shaped client, rewritten to Chat Completions and answered back. */
async function messages(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  const startedAtMs = Date.now();
  if (body === null) {
    logRequest({ wire: 'anthropic', route: '/v1/messages', model: null, status: 400, latency_ms: latencyMs(startedAtMs), upstream_failures: 0, streamed: false });
    return sendError(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');
  }
  const candidates = candidatesFor(requirementsFromBody(body, 'anthropic'));
  if (candidates.length === 0) {
    logRequest({ wire: 'anthropic', route: '/v1/messages', model: null, status: 503, latency_ms: latencyMs(startedAtMs), upstream_failures: 0, streamed: false });
    return sendError(res, 'anthropic', 503, 'overloaded_error', 'no free model in the brief meets this request');
  }
  const served = await dialWithFailover(candidates, anthropicToOpenAI(gatewayBody(body, candidates[0].id), candidates[0].id));
  if (served.upstream === null || served.model === null) {
    logRequest({ wire: 'anthropic', route: '/v1/messages', model: null, status: 502, latency_ms: latencyMs(startedAtMs), upstream_failures: served.failures.length, streamed: false });
    return sendError(res, 'anthropic', 502, 'api_error', 'every free model that meets this request refused it', served.failures);
  }
  const { upstream, model, failures } = served;
  const streamHeaders: Record<string, string> = {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  };
  if (failures.length > 0) streamHeaders['x-makefun-upstream-failures'] = trailValue(failures);
  if (body.stream === true) {
    res.writeHead(200, streamHeaders);
    try {
      for await (const event of anthropicEventsFromOpenAI(upstream.body, model)) res.write(event);
    } finally {
      res.end();
    }
    logRequest({ wire: 'anthropic', route: '/v1/messages', model, status: 200, latency_ms: latencyMs(startedAtMs), upstream_failures: failures.length, streamed: true });
    return;
  }
  const answer: unknown = await upstream.json();
  sendJson(res, 200, openAIToAnthropic(isRecord(answer) ? answer : {}, model), failures);
  logRequest({ wire: 'anthropic', route: '/v1/messages', model, status: 200, latency_ms: latencyMs(startedAtMs), upstream_failures: failures.length, streamed: false });
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    models(req, res);
    return;
  }
  if (req.method === 'GET' && url.pathname === '/healthz') {
    await healthz(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    await chat(req, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    await messages(req, res);
    return;
  }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
}

/** Poll GET /work every 100 ms; exit 0 the moment it says done. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

/**
 * The routing table as one line: the best free model for a plain request and for each
 * requirement class, so the startup diagnostics show the decision the trial will make
 * before the first prompt arrives. The cooldown set is empty at startup, so the order
 * is the fresh order, identical on a replay.
 */
function routingSummary(): string {
  const pick = (requires: Requirement): string => {
    const first = candidatesFor(requires)[0];
    return first === undefined ? 'none' : first.id.replace('openrouter/', '');
  };
  return `routing plain→${pick({})} tools→${pick({ tools: true })} json→${pick({ json: true })} vision→${pick({ vision: true })}`;
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print the one built object and
    // stop before any server starts. A process whose stdout is not flushed loses the tail, so wait.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (process.argv.includes('--version')) {
    // The entry's own name and version, printed the same way --list prints the catalog:
    // one line on stdout, then stop before any server starts.
    const text = `${ENTRY_NAME} ${ENTRY_VERSION}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  if (chatCompletionsUrl() === '') throw new Error('OPENAI_BASE_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer');
    });
  });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the OS's ephemeral range never holds it, and the guard says so.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${servedModels().length} free models`);
  log(`diagnostics: provider ${catalog.providers[0]?.id ?? 'unknown'}, catalog ${servedModels().length} free models, ${routingSummary()}`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
