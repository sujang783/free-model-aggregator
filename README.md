[![Free Model Aggregator](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/c/free-model-aggregator-02)

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

## Layout

```
main.ts            entrypoint: the server, rating-based routing, failover, catalog
lib/providers.ts   the brief's types and parser, the free model list, the catalog
lib/wire.ts        Anthropic <-> Chat Completions translation, the model field, SSE writers
```

## License

MIT — see [LICENSE](LICENSE).
