#!/usr/bin/env node
// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { spawn, spawnSync } from 'node:child_process';
import { createServer } from 'node:http';
import { createReadStream, existsSync, mkdtempSync, rmSync, statSync } from 'node:fs';
import { once } from 'node:events';
import { createServer as createNetServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)));
const page = '/test-webgpu-shader-smoke.html';

const contentTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.mjs', 'text/javascript; charset=utf-8'],
  ['.wgsl', 'text/plain; charset=utf-8'],
]);

function findChromium() {
  const candidates = [
    process.env.CHROME_BIN,
    process.env.CHROMIUM_BIN,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    '/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary',
    '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }

  for (const command of ['google-chrome', 'chromium', 'chromium-browser', 'microsoft-edge']) {
    const found = spawnSync('which', [command], { encoding: 'utf8' });
    if (found.status === 0 && found.stdout.trim()) return found.stdout.trim();
  }

  throw new Error('Chromium/Chrome not found. Set CHROME_BIN to the browser executable.');
}

function send(res, status, body, headers = {}) {
  res.writeHead(status, {
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'cross-origin',
    'Cache-Control': 'no-store',
    ...headers,
  });
  res.end(body);
}

function createStaticServer() {
  return createServer((req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    const requested = url.pathname === '/' ? page : decodeURIComponent(url.pathname);
    const filePath = normalize(join(root, requested));

    if (!filePath.startsWith(root)) {
      send(res, 403, 'Forbidden\n', { 'Content-Type': 'text/plain; charset=utf-8' });
      return;
    }
    if (!existsSync(filePath)) {
      send(res, 404, 'Not found\n', { 'Content-Type': 'text/plain; charset=utf-8' });
      return;
    }

    const stats = statSync(filePath);
    if (!stats.isFile()) {
      send(res, 403, 'Directory listing disabled\n', { 'Content-Type': 'text/plain; charset=utf-8' });
      return;
    }

    res.writeHead(200, {
      'Content-Type': contentTypes.get(extname(filePath).toLowerCase()) || 'application/octet-stream',
      'Content-Length': stats.size,
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'cross-origin',
      'Cache-Control': 'no-store',
    });
    createReadStream(filePath).pipe(res);
  });
}

function listen(server) {
  return new Promise((resolveListen, rejectListen) => {
    server.once('error', rejectListen);
    server.listen(0, '127.0.0.1', () => resolveListen(server.address()));
  });
}

async function freePort() {
  const server = createNetServer();
  await new Promise((resolveListen, rejectListen) => {
    server.once('error', rejectListen);
    server.listen(0, '127.0.0.1', resolveListen);
  });
  const port = server.address().port;
  server.close();
  await once(server, 'close');
  return port;
}

function extraChromiumArgs() {
  const raw = process.env.ANTFLY_INFERENCE_WEBGPU_CHROMIUM_ARGS;
  if (!raw) return [];
  return raw.split(/\s+/).filter(Boolean);
}

async function waitForJson(url, timeoutMs) {
  const started = Date.now();
  let lastError = null;
  while (Date.now() - started < timeoutMs) {
    try {
      const resp = await fetch(url);
      if (resp.ok) return await resp.json();
      lastError = new Error(`HTTP ${resp.status}`);
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 100));
  }
  throw new Error(`Timed out waiting for ${url}: ${lastError?.message ?? lastError}`);
}

function connectCdp(wsUrl) {
  return new Promise((resolveConnect, rejectConnect) => {
    const ws = new WebSocket(wsUrl);
    const pending = new Map();
    let nextId = 1;

    ws.addEventListener('open', () => {
      resolveConnect({
        send(method, params = {}) {
          const id = nextId++;
          ws.send(JSON.stringify({ id, method, params }));
          return new Promise((resolveSend, rejectSend) => {
            pending.set(id, { resolveSend, rejectSend });
          });
        },
        close() {
          ws.close();
        },
      });
    });
    ws.addEventListener('error', rejectConnect, { once: true });
    ws.addEventListener('message', (event) => {
      const msg = JSON.parse(event.data);
      if (!msg.id) return;
      const entry = pending.get(msg.id);
      if (!entry) return;
      pending.delete(msg.id);
      if (msg.error) {
        entry.rejectSend(new Error(msg.error.message ?? JSON.stringify(msg.error)));
      } else {
        entry.resolveSend(msg.result);
      }
    });
  });
}

async function waitForResult(cdp, timeoutMs) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const evaluated = await cdp.send('Runtime.evaluate', {
      expression: `({
        result: document.body.dataset.result || '',
        log: document.getElementById('log')?.textContent || ''
      })`,
      returnByValue: true,
    });
    const value = evaluated.result?.value ?? {};
    if (value.result === 'PASS' || value.result === 'FAIL') return value;
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 250));
  }
  const evaluated = await cdp.send('Runtime.evaluate', {
    expression: `document.getElementById('log')?.textContent || document.documentElement.outerHTML`,
    returnByValue: true,
  });
  throw new Error(`Timed out waiting for WebGPU smoke result.\n${evaluated.result?.value ?? ''}`);
}

const server = createStaticServer();
const address = await listen(server);
const url = `http://127.0.0.1:${address.port}${page}`;
const userDataDir = mkdtempSync(join(tmpdir(), 'inference-webgpu-smoke-'));
let chromiumProcess = null;

try {
  const chromium = findChromium();
  const debugPort = await freePort();
  const args = [
    '--headless=new',
    '--enable-unsafe-webgpu',
    '--ignore-gpu-blocklist',
    '--disable-search-engine-choice-screen',
    '--no-first-run',
    '--no-default-browser-check',
    `--user-data-dir=${userDataDir}`,
    `--remote-debugging-port=${debugPort}`,
    ...extraChromiumArgs(),
    url,
  ];

  chromiumProcess = spawn(chromium, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  const stderrChunks = [];
  chromiumProcess.stderr.on('data', (chunk) => stderrChunks.push(chunk));

  const targets = await waitForJson(`http://127.0.0.1:${debugPort}/json/list`, 10000);
  const target = targets.find((entry) => entry.url === url) ?? targets[0];
  if (!target?.webSocketDebuggerUrl) {
    throw new Error(`No Chrome DevTools target for ${url}`);
  }

  const cdp = await connectCdp(target.webSocketDebuggerUrl);
  try {
    await cdp.send('Runtime.enable');
    const result = await waitForResult(cdp, 30000);
    if (result.result !== 'PASS') {
      throw new Error(`WebGPU shader smoke failed.\n${result.log}\n${Buffer.concat(stderrChunks).toString('utf8')}`);
    }
  } finally {
    cdp.close();
  }

  console.log(`PASS: Chromium WebGPU shader smoke completed at ${url}`);
} finally {
  if (chromiumProcess && chromiumProcess.exitCode == null) {
    chromiumProcess.kill('SIGTERM');
    await Promise.race([
      once(chromiumProcess, 'exit'),
      new Promise((resolveDelay) => setTimeout(resolveDelay, 2000)),
    ]);
    if (chromiumProcess.exitCode == null) chromiumProcess.kill('SIGKILL');
  }
  server.close();
  rmSync(userDataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
