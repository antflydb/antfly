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

const std = @import("std");
const records = @import("../common/topology_records.zig");
pub fn freeTable(alloc: std.mem.Allocator, record: records.TableRecord) void {
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
