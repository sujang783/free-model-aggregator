[![make.fun · free-model-aggregator-02](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![rank](https://make.fun/badge/free-model-aggregator-02/sujang783/free-model-aggregator/rank.svg)](https://make.fun/bounty/free-model-aggregator-02/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![reward](https://make.fun/badge/free-model-aggregator-02/sujang783/free-model-aggregator/reward.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)

# Free Model Aggregator — one server, two wires, a live free model behind every answer

An entry for make.fun's **free-model-aggregator-02** bounty ("The best live free-model
aggregator", $500 pot).

One server that serves the OpenAI Chat Completions API and the Anthropic Messages API — in
JSON and in server-sent events — and answers every prompt with a real completion from a live
free OpenRouter model, dialled through the grader's gateway.

## What it does

`main.ts` (Node 22, standard library only) reads the brief at `$MAKEFUN_PROMPT_FILE`, listens
on a free port, registers with the env, and serves:

- `POST /v1/chat/completions` — OpenAI wire, JSON or SSE
- `POST /v1/messages` — Anthropic wire, JSON or SSE (translated to Chat Completions upstream
  and back)
- `GET /v1/models` — the OpenAI list for `Authorization: Bearer`, the Anthropic list for
  `x-api-key`

Routing picks the **highest-rated** free model in the brief that meets the request's own
requirements (tools, JSON mode, vision) — quality is 60% of the score. A gateway refusal is
retried once on the same model after a fixed 500 ms pause, then the prompt fails over to the
next-best candidate; a model that refuses is cooled down for the rest of the trial. Every
refusal is reported in the `x-makefun-upstream-failures` header. The catalog is exactly the
brief's fourteen free ids, registered byte for byte as `node main.ts --list` prints it.

Run it:

```sh
node main.ts --list   # prints the catalog document and exits 0
node main.ts          # serves; needs MAKEFUN_ENV_URL, MAKEFUN_PROMPT_FILE, OPENAI_BASE_URL
```

## Endpoints

- `GET /v1/models` — the catalog in the shape of the auth header sent: the OpenAI list
  for `Authorization: Bearer`, the Anthropic list for `x-api-key`.
- `POST /v1/chat/completions` — OpenAI wire, JSON or SSE.
- `POST /v1/messages` — Anthropic wire, JSON or SSE, translated to Chat Completions
  upstream and back.
- `GET /healthz` — operator health, never part of a trial: status, the entry's name and
  version, seconds since the process started, the catalog's model count, and whether the
  gateway is configured and answers a probe.

## Routing table

Routing is a pure function of the request: the requirements are read out of the body
(tools declared, a JSON `response_format`, image blocks in the content), and the pick is
the highest-rated free model meeting them. Ties break on context length, then on id, so
a replay picks the same model every time. At startup the server prints the table as one
line — the best model for a plain request and for each requirement class — so an operator
can see the decision before the first prompt arrives.

## Failover policy

A gateway refusal is retried once on the same model after a fixed 500 ms pause, then
the prompt moves to the next-best candidate. A model that still refuses is cooled down
for the rest of the trial: no later prompt spends a call on it while a fresh candidate
remains. A refusal that is not a rate limit or a server error is never retried — it will
not clear on a second try. Every refusal a prompt met is reported in the
`x-makefun-upstream-failures` header, at most 8 rows, in the order they refused.

## Operations

- `node main.ts --version` prints the entry name and version and exits 0.
- `node main.ts --quiet` silences the per-request JSON log lines; startup and error
  lines still print.
- Every answered request writes one JSON line to stderr: wire, route, serving model,
  HTTP status, latency in milliseconds, the count of upstream failures, and whether it
  streamed. The lines are diagnostics only — the answers are built before they run —
  so logging can never change a score.
- On startup the server prints the listening port, the provider id, the catalog size,
  and the routing table as one line.

## Layout

```
main.ts            entrypoint: the server, rating-based routing, failover, catalog
lib/providers.ts   the brief's types and parser, the free model list, the catalog
lib/wire.ts        Anthropic <-> Chat Completions translation, the model field, SSE writers
```

## License

MIT — see [LICENSE](LICENSE).
