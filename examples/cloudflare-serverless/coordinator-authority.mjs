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

// Deployment authority changes once during an explicitly frozen migration.
// It is not a per-transaction HEAD and never sits in the write hot path.
import{readLeasePolicy,conditionalReadLeasePolicy,validReadLeasePolicy}from"../../zig/lib/objectstore/wasm/read-view-lease.mjs";
import {callR2Binding} from "../../zig/lib/objectstore/wasm/r2.mjs";
export function coordinatorAuthorityMode(env){const backend=env.METADATA_ROOT_BACKEND??"journal";if(!["journal","conditional","bounded-journal"].includes(backend))throw Error("InvalidMetadataRootBackend");return backend==="conditional"?"r2-conditional":backend==="bounded-journal"?"r2-bounded":"r2";}
export const isObjectStoreAuthority=mode=>mode==="r2"||mode==="r2-conditional"||mode==="r2-bounded";
export async function openAuthority(env, mode, {allowRetired = false, allowExistingSqlite = false, allowCreate = true} = {}) {
  if(!["r2","r2-conditional","r2-bounded","sqlite"].includes(mode))throw Error("CoordinatorAuthorityInvalid");
  const namespace = env.ANTFLY_INCARNATION;
  if (!namespace || !env.DATABASE) throw new Error("CoordinatorNamespaceRequired");
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(namespace)));
  const id = Array.from(digest, b => b.toString(16).padStart(2,"0")).join("");
  const key = `antfly/authority-v1/${id}/CURRENT`;
  let object = await callR2Binding(()=>env.DATABASE.get(key)), descriptor;
  if (!object) {
    if (!allowCreate) throw new Error("CoordinatorAuthorityMissing");
    {
      const storageBytes = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(env.COORDINATOR_STORAGE_ID ?? namespace)));
      const storageId = Array.from(storageBytes,b=>b.toString(16).padStart(2,"0")).join("");
      const prefixes = [`antfly/coord-v1/${storageId}/`,`antfly/coord-v2/${storageId}/`];
      if (isObjectStoreAuthority(mode) || !allowExistingSqlite) prefixes.push(`antfly/wal-v1/${id}/`, `antfly/manifests/${namespace}/`);
      for (const prefix of prefixes) if ((await callR2Binding(()=>env.DATABASE.list({prefix,limit:1}))).objects.length) throw new Error("CoordinatorMigrationRequired");
    }
    descriptor = {format:1,namespace,mode,storage_id: isObjectStoreAuthority(mode) ? env.COORDINATOR_STORAGE_ID ?? namespace : null,...((mode==="r2-conditional"||mode==="r2-bounded")?{read_lease_policy:conditionalReadLeasePolicy}:mode==="r2"&&env.FENCED_READ_LEASES!=="false"?{read_lease_policy:readLeasePolicy}:{})};
    const written = await callR2Binding(()=>env.DATABASE.put(key, JSON.stringify(descriptor), {onlyIf:{etagDoesNotMatch:"*"},httpMetadata:{contentType:"application/json"}}));
    if (written) return {key, descriptor, etag:written.etag};
    object = await callR2Binding(()=>env.DATABASE.get(key));
    if (!object) throw new Error("CoordinatorAuthorityMissing");
  }
  if (object.size > 4096) throw new Error("CoordinatorAuthorityInvalid");
  descriptor = await callR2Binding(()=>object.json());
  if(descriptor.read_lease_policy!=null&&!validReadLeasePolicy(descriptor.read_lease_policy))throw Error("CoordinatorAuthorityInvalid");
  if((descriptor.mode==="r2-conditional"||descriptor.mode==="r2-bounded")&&descriptor.read_lease_policy?.format!==2)throw Error("CoordinatorAuthorityInvalid");
  if (descriptor.format !== 1 || descriptor.namespace !== namespace) throw new Error("CoordinatorAuthorityInvalid");
  if (descriptor.mode !== mode) {
    if (!(mode === "r2" && descriptor.mode === "sqlite" && env.COORDINATOR_MIGRATION_REQUIRED === "true" && env.LEGACY_COMMITS) && !(mode === "sqlite" && descriptor.mode === "r2" && allowRetired)) throw new Error("CoordinatorAuthorityMismatch");
  } else if (isObjectStoreAuthority(mode) && descriptor.storage_id !== (env.COORDINATOR_STORAGE_ID ?? namespace)) throw new Error("CoordinatorAuthorityMismatch");
  return {key, descriptor, etag:object.etag};
}
export async function activateR2(env, authority, token) {
  if (isObjectStoreAuthority(authority.descriptor.mode)) return authority;
  const descriptor = {...authority.descriptor,mode:"r2",storage_id:env.COORDINATOR_STORAGE_ID ?? env.ANTFLY_INCARNATION,source_token:token};
  const result = await callR2Binding(()=>env.DATABASE.put(authority.key, JSON.stringify(descriptor), {onlyIf:{etagMatches:authority.etag},httpMetadata:{contentType:"application/json"}}));
  if (!result) return openAuthority(env,"r2");
  return {key:authority.key,descriptor,etag:result.etag};
}
