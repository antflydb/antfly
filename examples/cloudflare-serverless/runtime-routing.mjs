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

export function readerSlots(env){const n=Number(env.READERS_PER_TENANT??2);if(!Number.isInteger(n)||n<1||n>4)throw new Error("InvalidReaderPoolSize");return n;}
export function runtimeName(namespace,slot){return JSON.stringify(["runtime-v1",namespace,slot]);}
export const readOperations=new Set(["lookup","list","search","count","status","latest"]);
export const buildOperations=new Set(["publish","compact","enrich","reindex","gc_prepare","gc_trace","gc_wal","collect","collect_metadata","migrate_coordinator"]);
