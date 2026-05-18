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

import fs from 'fs';
import path from 'path';

export function resolveWasmPath(root, options = {}) {
  const memoryModel = options.memoryModel ?? process.env.TERMITE_WASM_MEMORY_MODEL ?? 'wasm32';
  const candidates = memoryModel === 'wasm64'
    ? [
        path.join(root, 'zig-out', 'bin', 'termite-wasm64.wasm'),
        path.join(root, 'web', 'termite-wasm64.wasm'),
        path.join(root, 'zig-out', 'bin', 'termite.wasm'),
        path.join(root, 'web', 'termite.wasm'),
      ]
    : [
        path.join(root, 'zig-out', 'bin', 'termite-wasm32.wasm'),
        path.join(root, 'web', 'termite-wasm32.wasm'),
        path.join(root, 'zig-out', 'bin', 'termite.wasm'),
        path.join(root, 'web', 'termite.wasm'),
      ];

  for (const wasmPath of candidates) {
    if (fs.existsSync(wasmPath)) {
      return { wasmPath, memoryModel };
    }
  }

  return { wasmPath: null, memoryModel };
}

export function wasmBuildHint(memoryModel) {
  const buildCmd = memoryModel === 'wasm64'
    ? 'zig build -Dwasm=true -Dwasm-memory-model=wasm64 wasm'
    : 'zig build -Dwasm=true wasm';
  return `Run: ${buildCmd}`;
}
