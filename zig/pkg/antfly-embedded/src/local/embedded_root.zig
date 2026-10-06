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

pub const backend_adapter = @import("storage/backend_adapter.zig");
pub const backend_types = @import("storage/backend_types.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const lsm_storage = lsm_backend.storage_io;
pub const lite = @import("storage/lite/mod.zig");
pub const lite_backend = lite.backend;
pub const lite_native = lite.native;
pub const host_environment = @import("storage/host_environment.zig");
pub const object_storage = @import("storage/object_storage.zig");
pub const background_runtime = @import("storage/background_runtime.zig");
pub const db = @import("antfly_source_root").antfly_sources.physical_db;
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
pub const backup_codec = @import("storage/backup_codec.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const full_text_index_defaults = @import("common/full_text_index_defaults.zig");

test {
    _ = backend_adapter;
    _ = backend_types;
    _ = lsm_backend;
    _ = lite;
    _ = lite_backend;
    _ = lite_native;
    _ = host_environment;
    _ = object_storage;
    _ = background_runtime;
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
    _ = backup_codec;
    _ = portable_backup;
    _ = full_text_index_defaults;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = if (@import("builtin").os.tag == .freestanding)
    @import("source_owner_embedded_db.zig")
else
    @import("source_owner_storage.zig");

// Lake scans currently require native synchronous file/object-store I/O.
pub const lake = if (@import("builtin").os.tag == .freestanding) struct {} else @import("lake.zig");
