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

import { app, BrowserWindow, dialog, ipcMain } from 'electron';
import { createReadStream, existsSync, statSync } from 'node:fs';
import { readdir, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { extname, join, normalize, relative, resolve, sep } from 'node:path';
import { homedir } from 'node:os';

const webRoot = resolve(new URL('../web/', import.meta.url).pathname);
const defaultModelsDir = resolve(join(homedir(), '.termite/models'));

const contentTypes = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.mjs', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
  ['.wgsl', 'text/plain; charset=utf-8'],
  ['.wasm', 'application/wasm'],
  ['.txt', 'text/plain; charset=utf-8'],
  ['.png', 'image/png'],
  ['.jpg', 'image/jpeg'],
  ['.jpeg', 'image/jpeg'],
  ['.gif', 'image/gif'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
  ['.mp3', 'audio/mpeg'],
  ['.wav', 'audio/wav'],
  ['.ogg', 'audio/ogg'],
]);

let serverOrigin = null;
let mainWindow = null;

function normalizeRelativePath(baseDir, absolutePath) {
  return relative(baseDir, absolutePath).split(sep).join('/');
}

function isInterestingModelFile(entryName) {
  const lowered = entryName.toLowerCase();
  return lowered.endsWith('.gguf') ||
    lowered.endsWith('.safetensors') ||
    lowered === 'config.json' ||
    lowered === 'gliner_config.json' ||
    lowered === 'rebel_config.json' ||
    lowered === 'tokenizer.json' ||
    lowered === 'tokenizer_config.json';
}

function toFileUrl(filePath) {
  const url = new URL('/__termite__/file', serverOrigin);
  url.searchParams.set('path', filePath);
  return url.href;
}

async function collectModelFiles(rootDir, currentDir = rootDir, out = []) {
  const entries = await readdir(currentDir, { withFileTypes: true });
  for (const entry of entries) {
    const absPath = join(currentDir, entry.name);
    if (entry.isDirectory()) {
      await collectModelFiles(rootDir, absPath, out);
      continue;
    }
    if (!entry.isFile() || !isInterestingModelFile(entry.name)) continue;
    const fileStat = await stat(absPath);
    out.push({
      path: normalizeRelativePath(rootDir, absPath),
      sizeBytes: fileStat.size,
      url: toFileUrl(absPath),
    });
  }
  return out;
}

function writeStandardHeaders(res, extra = {}) {
  res.writeHead(200, {
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'cross-origin',
    'Cache-Control': 'no-store',
    ...extra,
  });
}

function parseRangeHeader(rangeHeader, size) {
  if (typeof rangeHeader !== 'string' || !rangeHeader.startsWith('bytes=')) return null;
  const [spec] = rangeHeader.slice('bytes='.length).split(',');
  if (!spec) return null;
  const [startPart, endPart] = spec.split('-');

  if (startPart === '' && endPart === '') return null;

  let start;
  let end;
  if (startPart === '') {
    const suffixLength = Number.parseInt(endPart, 10);
    if (!Number.isFinite(suffixLength) || suffixLength <= 0) return null;
    start = Math.max(0, size - suffixLength);
    end = size - 1;
  } else {
    start = Number.parseInt(startPart, 10);
    end = endPart === '' ? size - 1 : Number.parseInt(endPart, 10);
  }

  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end < start || start >= size) {
    return null;
  }
  return {
    start,
    end: Math.min(end, size - 1),
  };
}

function streamFile(req, res, filePath) {
  const stats = statSync(filePath);
  const contentType = contentTypes.get(extname(filePath).toLowerCase()) || 'application/octet-stream';
  const range = parseRangeHeader(req.headers.range, stats.size);
  const commonHeaders = {
    'Accept-Ranges': 'bytes',
    'Content-Type': contentType,
  };

  if (range) {
    const contentLength = range.end - range.start + 1;
    res.writeHead(206, {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'cross-origin',
      'Cache-Control': 'no-store',
      ...commonHeaders,
      'Content-Length': contentLength,
      'Content-Range': `bytes ${range.start}-${range.end}/${stats.size}`,
    });
    createReadStream(filePath, { start: range.start, end: range.end }).pipe(res);
    return;
  }

  writeStandardHeaders(res, {
    ...commonHeaders,
    'Content-Length': stats.size,
  });
  createReadStream(filePath).pipe(res);
}

function createStaticServer() {
  return createServer((req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    if (url.pathname === '/__termite__/file') {
      const absPath = url.searchParams.get('path');
      if (!absPath) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Missing path\n');
        return;
      }
      const normalizedPath = resolve(absPath);
      if (!existsSync(normalizedPath) || !statSync(normalizedPath).isFile()) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Not found\n');
        return;
      }
      streamFile(req, res, normalizedPath);
      return;
    }

    const requested = url.pathname === '/' ? '/index.html' : url.pathname;
    const filePath = normalize(join(webRoot, requested));
    if (!filePath.startsWith(webRoot) || !existsSync(filePath)) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found\n');
      return;
    }
    const stats = statSync(filePath);
    if (!stats.isFile()) {
      res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Forbidden\n');
      return;
    }
    streamFile(req, res, filePath);
  });
}

async function startServer() {
  const server = createStaticServer();
  await new Promise((resolvePromise, rejectPromise) => {
    server.once('error', rejectPromise);
    server.listen(0, '127.0.0.1', () => resolvePromise());
  });
  const address = server.address();
  serverOrigin = `http://127.0.0.1:${address.port}`;
}

async function createWindow() {
  if (!serverOrigin) {
    await startServer();
  }

  mainWindow = new BrowserWindow({
    width: 1440,
    height: 980,
    webPreferences: {
      preload: join(app.getAppPath(), 'preload.mjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  await mainWindow.loadURL(`${serverOrigin}/index.html`);
}

ipcMain.handle('termite-electron:get-default-models-dir', async () => defaultModelsDir);

ipcMain.handle('termite-electron:choose-models-dir', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory'],
    defaultPath: defaultModelsDir,
  });
  if (result.canceled || result.filePaths.length === 0) return null;
  return resolve(result.filePaths[0]);
});

ipcMain.handle('termite-electron:scan-models-dir', async (_event, dirPath) => {
  const resolved = resolve(dirPath || defaultModelsDir);
  const files = await collectModelFiles(resolved);
  return {
    directoryPath: resolved,
    files,
  };
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});
