#!/usr/bin/env node
'use strict';

/**
 * Local Anthropic-compatible proxy for FreeTheAi (https://api.freetheai.xyz/v1).
 *
 * What it does:
 *   - Listens on http://127.0.0.1:8787 (configurable)
 *   - Forwards /v1/messages and /v1/messages/count_tokens to FreeTheAi
 *   - Relays SSE streaming byte-for-byte (no buffering) so streaming + thinking works
 *   - Passes through anthropic-version, anthropic-beta, x-api-key, Authorization
 *   - Rewrites Claude Code's claude-* model names to a FreeTheAi alias (configurable)
 *   - Returns Anthropic-shaped JSON errors on failure
 *
 * Usage with Claude Code:
 *   set ANTHROPIC_BASE_URL=http://127.0.0.1:8787
 *   set ANTHROPIC_AUTH_TOKEN=fta_your_key   (or ANTHROPIC_API_KEY)
 *   claude
 *
 * Zero runtime dependencies. Node >= 18.
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

const CONFIG_PATH = path.join(__dirname, 'config.json');

// Single keep-alive agent, reused for every upstream request. The first request
// pays one TCP+TLS handshake; every subsequent request reuses the warm socket,
// eliminating 1–2 extra round trips per request (the bulk of TTFT).
const upstreamAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 64,
  maxFreeSockets: 16,
  keepAliveMsecs: 1000,
  scheduling: 'lifo' // reuse most-recently-freed (warmest) socket first
});

function loadConfig() {
  const defaults = {
    listenHost: '127.0.0.1',
    listenPort: 8787,
    upstreamBase: 'https://api.freetheai.xyz',
    upstreamPath: '/v1',
    defaultModel: 'glm/glm-5.2',
    modelMapping: {
      'claude-opus-4-8': 'glm/glm-5.2',
      'claude-opus-4-7': 'glm/glm-5.2',
      'claude-opus-4-6': 'glm/glm-5.2',
      'claude-opus-4-5-20251101': 'glm/glm-5.2',
      'claude-opus-4-1-20250805': 'glm/glm-5.2',
      'claude-opus-4-20250514': 'glm/glm-5.2',
      'claude-sonnet-4-6': 'glm/glm-5.1',
      'claude-sonnet-4-5-20250929': 'glm/glm-5.1',
      'claude-sonnet-4-20250514': 'glm/glm-5.1',
      'claude-3-7-sonnet-20250219': 'glm/glm-5.1',
      'claude-3-5-sonnet-20241022': 'glm/glm-5.1',
      'claude-haiku-4-5-20251001': 'opc/deepseek-v4-flash-free',
      'claude-3-5-haiku-20241022': 'opc/deepseek-v4-flash-free',
      'claude-3-opus-20240229': 'olm/kimi-k2.7-code'
    },
    retry: {
      maxAttempts: 6,
      backoffMs: 1500,
      maxBackoffMs: 30000,
      jitterMs: 800,
      retryOnStatus: [408, 409, 425, 429, 500, 502, 503, 504],
      retryOnBodyPatterns: [
        'rate limit', 'rate_limit', 'ratelimit',
        'overloaded', 'overload',
        'temporarily unavailable', 'temporarily_unavailable',
        'service unavailable', 'service_unavailable',
        'try again', 'please try again',
        'capacity', 'busy', 'timeout', 'timed out',
        'upstream error', 'bad gateway', 'internal error',
        'provider error', 'connection reset', 'connection refused',
        'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EAI_AGAIN'
      ]
    },
    logLevel: 'info'
  };
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      const file = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
      const merged = Object.assign({}, defaults, file);
      merged.modelMapping = Object.assign({}, defaults.modelMapping, file.modelMapping || {});
      merged.retry = Object.assign({}, defaults.retry, file.retry || {});
      merged.models = file.models || defaults.models || {};
      merged.rewriteResponseModel = file.rewriteResponseModel !== undefined ? file.rewriteResponseModel : true;
      return merged;
    }
  } catch (e) {
    console.error('[proxy] Could not parse config.json:', e.message);
  }
  return defaults;
}

const CFG = loadConfig();

function log(...args) {
  if (CFG.logLevel !== 'silent') console.log('[proxy]', ...args);
}

// Rewrites body.model to an upstream alias in place.
// Returns true if the model was actually changed (caller should re-serialize).
function rewriteModel(body) {
  if (!body || typeof body !== 'object') return false;
  const requested = body.model;
  let mapped = requested;
  if (requested && CFG.modelMapping[requested]) {
    mapped = CFG.modelMapping[requested];
  } else if (requested && /^claude/i.test(requested)) {
    mapped = CFG.defaultModel;
  }
  if (mapped !== requested) {
    log(`model ${requested} -> ${mapped}`);
    body.model = mapped;
    return true;
  }
  return false;
}

function sendJsonError(res, status, type, message) {
  const payload = JSON.stringify({
    type,
    error: { type, message }
  });
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload)
  });
  res.end(payload);
}

function buildUpstreamHeaders(req) {
  const h = {};
  const passthrough = [
    'content-type', 'accept', 'accept-encoding', 'accept-language',
    'anthropic-version', 'anthropic-beta', 'x-api-key', 'authorization',
    'user-agent', 'x-stainless-arch', 'x-stainless-os', 'x-stainless-lang',
    'x-stainless-package-version', 'x-stainless-runtime', 'x-stainless-runtime-version',
    'x-app', 'x-request-id'
  ];
  for (const k of passthrough) {
    const v = req.headers[k];
    if (v !== undefined) h[k] = v;
  }
  if (!h['accept']) h['accept'] = 'application/json';
  return h;
}

function isRetryable(status, bodyBuf) {
  const R = CFG.retry;
  if (R.retryOnStatus.includes(status) || status >= 500) return true;
  const bodyStr = (bodyBuf && bodyBuf.toString('utf8') || '').toLowerCase();
  return R.retryOnBodyPatterns.some((p) => bodyStr.includes(p.toLowerCase()));
}

function shouldRetry(status, bodyBuf, attempt) {
  if (attempt >= CFG.retry.maxAttempts) return false;
  return isRetryable(status, bodyBuf);
}

function computeBackoff(n) {
  const base = CFG.retry.backoffMs * Math.pow(2, n - 1);
  const capped = Math.min(base, CFG.retry.maxBackoffMs);
  const jitter = Math.random() * CFG.retry.jitterMs;
  return Math.round(capped + jitter);
}

// Allowed upstream routes. Anthropic: /messages (+/messages/count_tokens).
// OpenAI: /chat/completions. Both / and /v1 prefixes are accepted.
function isAllowedRoute(pathname) {
  return pathname === '/messages' ||
         pathname === '/v1/messages' ||
         pathname === '/messages/count_tokens' ||
         pathname === '/v1/messages/count_tokens' ||
         pathname === '/chat/completions' ||
         pathname === '/v1/chat/completions';
}

function forward(req, res) {
  const start = Date.now();
  const url = new URL(req.url, 'http://placeholder');

  // Allowed routes. Anthropic: /messages(+count_tokens). OpenAI: /chat/completions.
  // Path normalization below strips a leading /v1 and re-prepends CFG.upstreamPath,
  // so both "/v1/chat/completions" and "/chat/completions" land on the upstream.
  const allowed = isAllowedRoute(url.pathname);

  if (!allowed) {
    log('reject (not allowed):', req.method, url.pathname);
    return sendJsonError(res, 404, 'not_found_error', 'This proxy only forwards /messages and /chat/completions routes.');
  }

  let normPath = url.pathname.replace(/^\/v1/, '');
  const upstreamUrl = CFG.upstreamBase + CFG.upstreamPath + normPath + url.search;

  let parsed;
  try {
    parsed = new URL(upstreamUrl);
  } catch (e) {
    return sendJsonError(res, 500, 'proxy_error', 'Bad upstream URL: ' + upstreamUrl);
  }

  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    let bodyBuf = Buffer.concat(chunks);
    let outBody = bodyBuf;
    let currentModel = null;

    // Parse the body once: capture currentModel and rewrite the model in one pass.
    // Re-serialize only when the model actually changed; otherwise forward bodyBuf
    // untouched (zero copy).
    if (req.method === 'POST' && bodyBuf.length > 0) {
      const ct = (req.headers['content-type'] || '').toLowerCase();
      if (ct.includes('application/json') || ct.includes('text/plain')) {
        try {
          const obj = JSON.parse(bodyBuf.toString('utf8'));
          currentModel = obj.model || null;
          if (rewriteModel(obj)) {
            outBody = Buffer.from(JSON.stringify(obj), 'utf8');
          }
        } catch (e) {
          log('body not JSON, forwarding as-is:', e.message);
        }
      }
    }

    const baseHeaders = buildUpstreamHeaders(req);
    baseHeaders['host'] = parsed.host;

    let attempt = 0;

    function sendRequest() {
      attempt += 1;
      const reqBody = outBody;
      const headers = Object.assign({}, baseHeaders);
      headers['content-length'] = String(reqBody.length);

      const upstreamReq = https.request(
        {
          protocol: parsed.protocol,
          hostname: parsed.hostname,
          port: parsed.port || 443,
          method: req.method,
          path: parsed.pathname + parsed.search,
          headers,
          agent: upstreamAgent
        },
        (upstreamRes) => {
          const elapsed = Date.now() - start;
          const ct = upstreamRes.headers['content-type'] || '';
          const streaming = ct.includes('text/event-stream');
          const status = upstreamRes.statusCode;

          // Retryable status? Buffer the (small) error body and retry.
          const statusRetryable = CFG.retry.retryOnStatus.includes(status);
          if (statusRetryable && attempt < CFG.retry.maxAttempts && !res.headersSent) {
            const collect = [];
            upstreamRes.on('data', (c) => collect.push(c));
            upstreamRes.on('end', () => {
              const buf = Buffer.concat(collect);
              const wait = computeBackoff(attempt);
              log(
                `retryable ${status} attempt ${attempt}/${CFG.retry.maxAttempts} ` +
                `model=${currentModel} waiting ${wait}ms body=${buf.toString('utf8').slice(0, 200)}`
              );
              setTimeout(sendRequest, wait);
            });
            return;
          }

          // Not retryable — commit and deliver. Stream or buffer based on content-type.
          const respHeaders = Object.assign({}, upstreamRes.headers);
          delete respHeaders['content-length'];
          delete respHeaders['transfer-encoding'];
          respHeaders['content-type'] = ct || 'application/json';

          if (status >= 400) {
            const collect = [];
            upstreamRes.on('data', (c) => collect.push(c));
            upstreamRes.on('end', () => {
              const buf = Buffer.concat(collect);
              log(`upstream ${status} body: ${buf.toString('utf8').slice(0, 1000)}`);
              if (!res.headersSent) {
                res.writeHead(status, respHeaders);
                res.end(buf);
              }
            });
            return;
          }

          // Success — stream through in real time (no buffering).
          log(`<- ${status} ${req.method} ${url.pathname} (${elapsed}ms${streaming ? ', stream' : ', attempt ' + attempt})`);
          if (streaming) {
            respHeaders['cache-control'] = 'no-cache';
            respHeaders['connection'] = 'keep-alive';
            respHeaders['x-accel-buffering'] = 'no';
          }
          res.writeHead(status, respHeaders);

          if (streaming) {
            upstreamRes.on('data', (chunk) => {
              if (!res.write(chunk)) {
                upstreamRes.pause();
                res.once('drain', () => upstreamRes.resume());
              }
            });
            upstreamRes.on('end', () => res.end());
            res.on('close', () => upstreamRes.destroy());
          } else {
            upstreamRes.pipe(res);
          }
        }
      );

      upstreamReq.on('error', (err) => {
        log('upstream error:', err.message);
        if (!res.headersSent && attempt < CFG.retry.maxAttempts) {
          const wait = computeBackoff(attempt);
          log(`retry attempt ${attempt}/${CFG.retry.maxAttempts} after error in ${wait}ms`);
          setTimeout(sendRequest, wait);
        } else if (!res.headersSent) {
          sendJsonError(res, 502, 'api_error', 'Upstream connection failed: ' + err.message);
        } else {
          try { res.end(); } catch (_) {}
        }
      });

      if (reqBody.length) upstreamReq.write(reqBody);
      upstreamReq.end();
    }

    sendRequest();
  });

  req.on('error', (err) => {
    log('client error:', err.message);
    if (!res.headersSent) sendJsonError(res, 400, 'invalid_request_error', err.message);
  });
}

function buildModelsList() {
  const models = CFG.models || {};
  return {
    data: Object.entries(models).map(([id, info]) => ({
      id,
      display_name: info.name || id,
      type: 'model',
      created_at: '2025-01-01T00:00:00Z',
      max_tokens: info.maxTokens || 8192
    }))
  };
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/health' || req.url === '/v1')) {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({
      service: 'claude-proxy',
      upstream: CFG.upstreamBase + CFG.upstreamPath,
      status: 'ok'
    }, null, 2));
  }
  if (req.method === 'GET' && (req.url === '/models' || req.url === '/v1/models')) {
    const body = JSON.stringify(buildModelsList());
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(body);
  }
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET,POST,OPTIONS',
      'access-control-allow-headers': '*'
    });
    return res.end();
  }
  return forward(req, res);
});

server.listen(CFG.listenPort, CFG.listenHost, () => {
  log(`listening on http://${CFG.listenHost}:${CFG.listenPort}`);
  log(`upstream: ${CFG.upstreamBase}${CFG.upstreamPath}/messages`);
  log(`default model: ${CFG.defaultModel}`);
});

process.on('SIGINT', () => { log('shutting down'); server.close(() => process.exit(0)); });
process.on('SIGTERM', () => { server.close(() => process.exit(0)); });
