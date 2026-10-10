// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { readFile } from 'node:fs/promises';
// Node fixtures run from source; browsers fetch the prepared CPU asset URL.
const path = process.env.EXTRACTION_WASM ?? new URL('../../../zig-out/antfly-extraction-cpu.wasm', import.meta.url);
const original = globalThis.fetch;
globalThis.fetch = async (url, options) => String(url).endsWith('/antfly-extraction-cpu.wasm')
  ? new Response(await readFile(path))
  : original(url, options);
