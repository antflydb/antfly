// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

// Module hooks that let Node import Worker modules for unit tests: compiled
// WASM imports become empty placeholders (kernels instantiate lazily) and
// `cloudflare:workers` gets a minimal DurableObject base. Register with
// `module.register(new URL("./support/workers-node-hooks.mjs", import.meta.url))`.
const workers = `
export class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }
export class WorkerEntrypoint { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }
`;
export async function resolve(specifier, context, next) {
  if (specifier === "cloudflare:workers") return { url: "stub:cloudflare-workers", shortCircuit: true };
  if (specifier.endsWith(".wasm")) return { url: "stub:wasm:" + specifier, shortCircuit: true };
  return next(specifier, context);
}
export async function load(url, context, next) {
  if (url === "stub:cloudflare-workers") return { format: "module", source: workers, shortCircuit: true };
  if (url.startsWith("stub:wasm:")) return { format: "module", source: "export default null;", shortCircuit: true };
  return next(url, context);
}
