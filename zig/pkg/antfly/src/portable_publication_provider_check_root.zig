// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Force actual directory/Lite pre-publication admission composition.
const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const sources = @import("antfly_local_sources");
const db_mod = sources.storage_db_db;
const restore = sources.storage_lite_portable_restore;
const Controls = sources.storage_docstore.DocStore.PublicationAdmission;
export fn antfly_check_directory_publication(db: *db_mod.DB, alloc: *const std.mem.Allocator, bytes: [*]const u8, len: usize, controls: *const Controls) callconv(.c) u8 {
    db.importPortableIntoEmptyWithAdmissionControls(alloc.*, bytes[0..len], .{ .table_id = 1, .shard_id = 1, .range_id = 1 }, controls.*) catch return 0;
    return 1;
}
export fn antfly_check_lite_publication(db: *db_mod.DB, backend: *sources.storage_lite_backend.Handle, alloc: *const std.mem.Allocator, bytes: [*]const u8, len: usize, controls: *const Controls) callconv(.c) u8 {
    restore.importPortableIntoLiteDbWithAdmissionControls(alloc.*, db, backend, bytes[0..len], controls.*) catch return 0;
    return 1;
}
