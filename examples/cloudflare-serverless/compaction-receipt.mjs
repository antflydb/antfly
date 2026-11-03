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

// Receipts live in the Antfly metadata tree, with the same namespace lifetime
// as mutation receipts. HEAD, lease release and the outcome commit together.
export async function compactionIdentity(id, policy) {
  if (typeof id !== 'string' || !id.length || !id.isWellFormed() || new TextEncoder().encode(id).byteLength > 256) throw Error('InvalidCompactionOperationId');
  if (!Number.isSafeInteger(policy.max_documents) || policy.max_documents < 1 ||
      !Number.isSafeInteger(policy.max_artifact_bytes) || policy.max_artifact_bytes < 1) throw Error('InvalidCompactionPolicy');
  const hash = async value => [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value)))].map(byte => byte.toString(16).padStart(2, '0')).join('');
  return {key: '__antfly_compaction/receipts/' + await hash(id), fingerprint: await hash(JSON.stringify([1, policy.max_documents, policy.max_artifact_bytes]))};
}

export async function readCompactionReceipt(tx, identity) {
  const prior = await tx.get(identity.key);
  if (prior && prior.fingerprint !== identity.fingerprint) throw Error('CompactionOperationConflict');
  return prior;
}

export async function beginCompactionReceipt(tx, identity, acquire) {
  const prior = await readCompactionReceipt(tx, identity);
  if (prior?.state === 'complete') return {complete: true, result: prior.result};
  // Native fencing controls takeover; an expired JavaScript timer never grants
  // publication authority. No failed attempt may replace a completed receipt.
  const initial = await acquire();
  tx.put(identity.key, {fingerprint: identity.fingerprint, state: 'pending', lease: initial.lease});
  return initial;
}

export async function finishCompactionReceipt(tx, identity, batch, result, finalize) {
  const prior = await readCompactionReceipt(tx, identity);
  if (prior?.state === 'complete') return prior.result;
  if (prior?.state !== 'pending' || prior.lease.owner !== batch?.lease?.owner || prior.lease.token !== batch?.lease?.token) throw Error('WorkLeaseLost');
  if (typeof result?.published !== 'boolean' || typeof result.version !== 'string' ||
      !/^(0|[1-9][0-9]{0,19})$/.test(result.version) || BigInt(result.version) > 18446744073709551615n ||
      !Number.isSafeInteger(result.artifact_count) || result.artifact_count < 0) throw Error('InvalidCompactionResult');
  const receipt = {published: result.published, version: result.version, artifact_count: result.artifact_count};
  await finalize(receipt);
  tx.put(identity.key, {fingerprint: identity.fingerprint, state: 'complete', result: receipt});
  return receipt;
}
