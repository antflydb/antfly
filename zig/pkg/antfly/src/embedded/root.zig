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

const support = @import("embedded_support");

pub const db = @import("embedded_db_surface");
pub const api = @import("embedded_api_surface");
pub const host_environment = support.host_environment;
pub const object_storage = support.object_storage;
pub const lsm_backend = support.lsm_backend;
pub const storage_backend = support.backend_types;
pub const db_types = support.db_types;

test "embedded package surfaces are reachable" {
    _ = db;
    _ = api;
    _ = host_environment;
    _ = object_storage;
    _ = lsm_backend;
    _ = storage_backend;
    _ = db_types;
}
