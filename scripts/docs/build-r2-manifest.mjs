#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { extname, join, relative, resolve, sep } from 'node:path';

const MIME_TYPES = new Map([
  ['.css', 'text/css'],
  ['.gif', 'image/gif'],
  ['.html', 'text/html'],
  ['.jpeg', 'image/jpeg'],
  ['.jpg', 'image/jpeg'],
  ['.js', 'text/javascript'],
  ['.json', 'application/json'],
  ['.md', 'text/markdown'],
  ['.mdx', 'text/markdown'],
  ['.pdf', 'application/pdf'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
  ['.yaml', 'application/yaml'],
  ['.yml', 'application/yaml'],
]);

function usage() {
  return 'usage: build-r2-manifest.mjs --source DIR --output DIR --repository OWNER/REPO --commit SHA';
}

function parseArgs(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    const value = argv[index + 1];
    if (!name?.startsWith('--') || !value) throw new Error(usage());
    values.set(name.slice(2), value);
  }

  for (const required of ['source', 'output', 'repository', 'commit']) {
    if (!values.get(required)) throw new Error(`missing --${required}\n${usage()}`);
  }
  if (!/^[0-9a-f]{40}$/i.test(values.get('commit'))) {
    throw new Error('--commit must be a 40-character Git commit SHA');
  }
  return values;
}

async function walk(root, directory = root) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(root, path)));
    else if (entry.isFile()) files.push(path);
  }
  return files;
}

function portablePath(root, path) {
  return relative(root, path).split(sep).join('/');
}

const args = parseArgs(process.argv.slice(2));
const source = resolve(args.get('source'));
const output = resolve(args.get('output'));
const repository = args.get('repository');
const commit = args.get('commit').toLowerCase();
const sourceInfo = await stat(source);
if (!sourceInfo.isDirectory()) throw new Error(`source is not a directory: ${source}`);

const paths = (await walk(source)).sort((left, right) =>
  portablePath(source, left).localeCompare(portablePath(source, right), 'en')
);
const files = [];
for (const path of paths) {
  const content = await readFile(path);
  const relativePath = portablePath(source, path);
  files.push({
    path: relativePath,
    key: `docs/releases/${commit}/${relativePath}`,
    content_type: MIME_TYPES.get(extname(relativePath).toLowerCase()) ?? 'application/octet-stream',
    size: content.byteLength,
    sha256: createHash('sha256').update(content).digest('hex'),
  });
}

const manifestKey = `docs/manifests/${commit}.json`;
const manifest = {
  schema_version: 1,
  source_repository: repository,
  source_commit: commit,
  source_url: `https://github.com/${repository}/tree/${commit}/docs`,
  object_prefix: `docs/releases/${commit}/`,
  file_count: files.length,
  files,
};
const current = {
  schema_version: 1,
  source_repository: repository,
  source_commit: commit,
  manifest_key: manifestKey,
  object_prefix: manifest.object_prefix,
};

await mkdir(output, { recursive: true });
await writeFile(join(output, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
await writeFile(join(output, 'current.json'), `${JSON.stringify(current, null, 2)}\n`);
