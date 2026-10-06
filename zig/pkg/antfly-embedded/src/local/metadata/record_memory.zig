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

const std = @import("std");
const records = @import("../common/topology_records.zig");
pub fn freeTable(alloc: std.mem.Allocator, record: records.TableRecord) void {
    alloc.free(record.lake_index_catalog_json);
    if (record.storage_migration) |migration| alloc.free(migration.request.job_id);
    alloc.free(record.relational_retirement_json);
    alloc.free(record.name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
}

pub fn freeRange(alloc: std.mem.Allocator, record: records.RangeRecord) void {
    alloc.free(record.start_key);
    if (record.end_key) |key| alloc.free(key);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_artifact_backup_id);
    alloc.free(record.restore_location);
    alloc.free(record.restore_snapshot_path);
    alloc.free(record.restore_connection);
    alloc.free(record.restore_artifact_sha256);
    alloc.free(record.restore_native_manifest_sha256);
}
