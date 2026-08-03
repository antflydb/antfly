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

const batch = @import("api/batch.zig");
const http_client = @import("api/http_client.zig");
const http_internal_group_write_routes = @import("api/http_internal_group_write_routes.zig");
const provisioned_storage = @import("api/provisioned_storage.zig");
const table_writes = @import("api/table_writes.zig");
const table_writes_bulk_ingest = @import("api/table_writes/bulk_ingest.zig");
const table_writes_cache = @import("api/table_writes/cache.zig");
const table_writes_core = @import("api/table_writes/core.zig");
const table_writes_index_config = @import("api/table_writes/index_config.zig");
const table_writes_integrity = @import("api/table_writes/integrity.zig");
const table_writes_integrity_types = @import("api/table_writes/integrity_types.zig");
const table_writes_managed_db = @import("api/table_writes/managed_db.zig");
const table_writes_schema_jobs = @import("api/table_writes/schema_jobs.zig");
const table_writes_backup_restore = @import("api/table_writes/backup_restore.zig");
const table_writes_relational_mutation = @import("api/table_writes/relational_mutation.zig");
const table_writes_remote_wire = @import("api/table_writes/remote_wire.zig");
const table_writes_sources = @import("api/table_writes/sources.zig");

test {
    _ = batch;
    _ = http_client;
    _ = http_internal_group_write_routes;
    _ = provisioned_storage;
    _ = table_writes;
    _ = table_writes_bulk_ingest;
    _ = table_writes_cache;
    _ = table_writes_core;
    _ = table_writes_index_config;
    _ = table_writes_integrity;
    _ = table_writes_integrity_types;
    _ = table_writes_managed_db;
    _ = table_writes_schema_jobs;
    _ = table_writes_backup_restore;
    _ = table_writes_relational_mutation;
    _ = table_writes_remote_wire;
    _ = table_writes_sources;
}
