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

import { createReadStream, existsSync, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, isAbsolute, join, normalize, relative, resolve } from 'node:path';

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

export function pathIsInside(candidatePath, rootPath) {
  const candidate = resolve(candidatePath);
  const root = resolve(rootPath);
  const rel = relative(root, candidate);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sepForPlatform()}`) && !isAbsolute(rel));
}

function sepForPlatform() {
  return process.platform === 'win32' ? '\\' : '/';
}

export function resolveAllowedFilePath(rawPath, allowedRoots) {
  if (!rawPath || !isAbsolute(rawPath)) return null;
  const normalizedPath = resolve(rawPath);
  for (const root of allowedRoots) {
    if (pathIsInside(normalizedPath, root)) return normalizedPath;
  }
  return null;
}

function writeStandardHeaders(res, extra = {}) {
  res.writeHead(200, {
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'same-origin',
    'Cache-Control': 'no-store',
    ...extra,
  });
}

export function parseRangeHeader(rangeHeader, size) {
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
      'Cross-Origin-Resource-Policy': 'same-origin',
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

export function createStaticServer({ webRoot, getAllowedFileRoots }) {
  const normalizedWebRoot = resolve(webRoot);
  return createServer((req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    if (url.pathname === '/__termite__/file') {
      const rawPath = url.searchParams.get('path');
      if (!rawPath) {
        res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Missing path\n');
        return;
      }
      const normalizedPath = resolveAllowedFilePath(rawPath, getAllowedFileRoots().map((root) => resolve(root)));
      if (!normalizedPath) {
        res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Forbidden\n');
        return;
      }
      if (!existsSync(normalizedPath) || !statSync(normalizedPath).isFile()) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Not found\n');
        return;
      }
      streamFile(req, res, normalizedPath);
      return;
    }

    const requested = url.pathname === '/' ? '/index.html' : url.pathname;
    const filePath = normalize(join(normalizedWebRoot, requested));
    if (!pathIsInside(filePath, normalizedWebRoot) || !existsSync(filePath)) {
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
