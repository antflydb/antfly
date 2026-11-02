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

import { ObjectStoreError } from "./r2.mjs";
// The physical store binding must declare this placement permanently. Switching
// between R2-only and coordinated HEADs would create competing authorities.
export function withCommitCoordinator(transport, coordinator, collection) {
  return async (request, body) => {
    if (!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key ?? "") || /\/progress\//.test(request.key ?? ""))) return transport(request, body);
    const result = await coordinator.objectCall(collection ? { ...request, collection } : request, body);
    if (result.status !== 0) throw new ObjectStoreError(result.status, "commit coordinator rejected operation");
    return { json: result.json, body: result.body };
  };
}
