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

// A Durable Object is a provider-serialized actor, including after eviction.
// Resume its owner identity within one deployment; a code version or physical
// actor change still creates a distinct native fence. Generic hosts retain
// random owners when provider identity/version metadata is unavailable.
export function actorOwner(actorId,version,namespace){
 if(typeof actorId!=="string"||!/^[a-f0-9]{64}$/i.test(actorId)||typeof version!=="string"||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(version)||typeof namespace!=="string"||!namespace.length)return;
 const input=JSON.stringify(["antfly.cloudflare.owner.v1",actorId.toLowerCase(),version.toLowerCase(),namespace]);
 return crypto.subtle.digest("SHA-256",new TextEncoder().encode(input)).then(bytes=>Array.from(new Uint8Array(bytes).subarray(0,16)));
}
