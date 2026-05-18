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

pub const backend_adapter = @import("storage/backend_adapter.zig");
pub const backend_types = @import("storage/backend_types.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const lsm_storage = lsm_backend.storage_io;
pub const host_environment = @import("storage/host_environment.zig");
pub const object_storage = @import("storage/object_storage.zig");
pub const db = @import("storage/db/db.zig");
pub const db_core = @import("storage/db/core.zig");
pub const db_types = @import("storage/db/types.zig");
pub const template_remote_host = @import("storage/db/template_remote_host.zig");
pub const enrichment_embedder = @import("storage/db/enrichment/embedder.zig");
pub const enrichment_runtime = @import("storage/db/enrichment/enrichment_runtime.zig");
pub const derived_executor = @import("storage/db/derived/derived_executor.zig");
pub const ttl_runtime = @import("storage/db/maintenance/ttl_runtime.zig");
pub const transaction_runtime = @import("storage/db/maintenance/transaction_runtime.zig");
pub const schema = @import("storage/schema.zig");
pub const batch = @import("api/batch.zig");
pub const query = @import("api/query.zig");
pub const query_contract = @import("api/query_contract.zig");

test {
    _ = backend_adapter;
    _ = backend_types;
    _ = lsm_backend;
    _ = host_environment;
    _ = object_storage;
    _ = db;
    _ = db_core;
    _ = db_types;
    _ = template_remote_host;
    _ = derived_executor;
    _ = ttl_runtime;
    _ = transaction_runtime;
    _ = schema;
    _ = batch;
    _ = query;
    _ = query_contract;
}
