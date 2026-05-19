// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const support = @import("embedded_support");

pub const db = @import("embedded_db_surface");
pub const api = @import("embedded_api_surface");
pub const host_environment = support.host_environment;
pub const object_storage = support.object_storage;
pub const lsm_backend = support.lsm_backend;
pub const storage_backend = support.backend_types;
pub const db_types = support.db_types;

test {
    _ = db;
    _ = api;
    _ = host_environment;
    _ = object_storage;
    _ = lsm_backend;
    _ = storage_backend;
    _ = db_types;
}
