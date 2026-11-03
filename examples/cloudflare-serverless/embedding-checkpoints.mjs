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

// One bounded sweep of disposable embedding computation checkpoints. Serving
// documents contain their vectors inline and never reference this namespace.
export async function pruneEmbeddingCheckpoints(env) {
  const namespace = env.ANTFLY_INCARNATION;
  const today = Math.floor(Date.now() / 86400000);
  const coordinator = env.COMMITS.getByName(namespace);
  const cursor = await coordinator.pruneEmbeddingCheckpointMetadata(namespace, today);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(namespace));
  const domain = Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
  const prefix = `antfly/embedding-cache/${domain}/`;
  const page = await env.DATABASE.list({prefix, limit: 128, cursor: cursor ?? undefined});
  const expired = page.objects.filter(object => {
    const day = object.key.slice(prefix.length).split("/")[0];
    return /^[a-f0-9]{8}$/.test(day) && parseInt(day, 16) + 1 < today;
  }).map(object => object.key);
  if (expired.length) await env.DATABASE.delete(expired);
  await coordinator.saveEmbeddingCheckpointCursor(namespace, cursor, page.truncated ? page.cursor : null);
  return {deleted: expired.length, has_more: page.truncated};
}
