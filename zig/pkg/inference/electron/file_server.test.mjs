// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");

import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

import { createStaticServer, pathIsInside, resolveAllowedFilePath } from './file_server.mjs';

async function withServer(server, fn) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  try {
    const { port } = server.address();
    return await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('pathIsInside requires true path containment', () => {
  assert.equal(pathIsInside('/tmp/models/file.gguf', '/tmp/models'), true);
  assert.equal(pathIsInside('/tmp/models2/file.gguf', '/tmp/models'), false);
  assert.equal(pathIsInside('/tmp/models/../secret.txt', '/tmp/models'), false);
});

test('resolveAllowedFilePath rejects paths outside allowed model roots', async () => {
  const root = await mkdtemp(join(tmpdir(), 'termite-models-'));
  const outside = await mkdtemp(join(tmpdir(), 'termite-outside-'));
  assert.equal(resolveAllowedFilePath(join(root, 'model.gguf'), [root]), join(root, 'model.gguf'));
  assert.equal(resolveAllowedFilePath(join(outside, 'secret.txt'), [root]), null);
  assert.equal(resolveAllowedFilePath('relative.gguf', [root]), null);
});

test('file endpoint streams only files under allowed model roots', async () => {
  const webRoot = await mkdtemp(join(tmpdir(), 'inference-web-'));
  await writeFile(join(webRoot, 'index.html'), '<!doctype html>');
  const modelRoot = await mkdtemp(join(tmpdir(), 'termite-models-'));
  const outsideRoot = await mkdtemp(join(tmpdir(), 'termite-outside-'));
  const modelPath = join(modelRoot, 'model.gguf');
  const secretPath = join(outsideRoot, 'secret.txt');
  await writeFile(modelPath, 'model-bytes');
  await writeFile(secretPath, 'secret-bytes');

  const server = createStaticServer({
    webRoot,
    getAllowedFileRoots: () => [modelRoot],
  });

  await withServer(server, async (origin) => {
    const allowed = await fetch(`${origin}/__termite__/file?path=${encodeURIComponent(modelPath)}`);
    assert.equal(allowed.status, 200);
    assert.equal(await allowed.text(), 'model-bytes');

    const forbidden = await fetch(`${origin}/__termite__/file?path=${encodeURIComponent(secretPath)}`);
    assert.equal(forbidden.status, 403);
    assert.equal(await forbidden.text(), 'Forbidden\n');
  });
});
