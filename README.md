# Free-Api-Now

A tiny, zero-dependency Node.js proxy that lets you point Anthropic **and** OpenAI
clients at a single local endpoint (`http://127.0.0.1:8787`) and route them to an
upstream OpenAI-compatible provider.

Originally built for [FreeTheAi](https://api.freetheai.xyz), but works with any
OpenAI-compatible upstream by changing `upstreamBase` / `upstreamPath` in the config.

## Features

- **Dual API surface** — forwards Anthropic `/v1/messages` (and `count_tokens`)
  and OpenAI `/v1/chat/completions` routes.
- **Model rewriting** — map client model names (e.g. `claude-opus-4-8`) to upstream
  aliases (e.g. `glm/glm-5.2`). Unmapped, non-`claude-*` model IDs pass through.
- **Keep-alive agent** — reuses one warm TLS socket so the per-request handshake
  cost amortizes to zero (low time-to-first-token after the first request).
- **Retry with exponential backoff** — retries transient errors (429/5xx, rate
  limits, timeouts) without changing the model.
- **Transparent SSE streaming** — relays `text/event-stream` byte-for-byte with no
  buffering, so streaming + thinking work.
- **Zero runtime dependencies.** Node >= 18. Single file (`proxy.js`).

## Setup

1. Copy the config template and edit it:

   ```sh
   cp config.example.json config.json
   ```

   Set `defaultModel`, `modelMapping`, and `retry` to taste.

2. Run the proxy:

   ```sh
   node proxy.js
   ```

   It listens on `http://127.0.0.1:8787`.

## Usage

### Anthropic clients (e.g. Claude Code)

```sh
set ANTHROPIC_BASE_URL=http://127.0.0.1:8787
set ANTHROPIC_AUTH_TOKEN=<your upstream key>   # or ANTHROPIC_API_KEY
claude
```

### OpenAI clients

```sh
set OPENAI_BASE_URL=http://127.0.0.1:8787/v1
set OPENAI_API_KEY=<your upstream key>
```

Point any OpenAI-compatible tool at `http://127.0.0.1:8787/v1` and call
`/chat/completions`.

## Endpoints

| Route                       | API      |
|-----------------------------|----------|
| `/v1/messages`              | Anthropic |
| `/v1/messages/count_tokens` | Anthropic |
| `/v1/chat/completions`      | OpenAI |
| `/v1/models`                | Lists configured models |
| `/health`                   | Health check |

## Notes

- `config.json` is git-ignored (it holds your live settings). Edit the template
  `config.example.json` or your own `config.json`.
- Model rewriting only applies to names that match the `modelMapping` table or
  start with `claude`. Real upstream model IDs are forwarded as-is.
