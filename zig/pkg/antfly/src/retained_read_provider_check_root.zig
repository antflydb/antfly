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

//! Analyze both sides of native retained-read creation and page callbacks.
const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const provider = capi_dependencies.relational_read_provider;
const types = @import("antfly_local_sources").storage_db_types;
export fn antfly_check_retained_read_provider(raw: *anyopaque, alloc: *const std.mem.Allocator, opts: *const types.ScanOptions, admission: *const provider.View.Admission) callconv(.c) u8 {
    var source: provider.Provider = undefined;
    const status = @import("capi/server_owner.zig").storageOwnerRelationalReadProvider(raw, &capi_dependencies.runtime_native_abi.TypeContract.of(provider.Provider), &source);
    if (!status.isOk()) return 0;
    const fence = (source.tryFenceWithAdmission(alloc.*, "rows", opts.*, admission.*) catch return 0) orelse return 0;
    defer fence.deinit();
    fence.validateWithAdmission(admission.*) catch return 0;
    const view = fence.openWithAdmission(alloc.*, "", "", opts.*, admission.*) catch return 0;
    defer view.deinit();
    var page = view.nextWithAdmission(alloc.*, 2, admission.*) catch return 0;
    defer page.deinit();
    const proofs = view.rangeProofsWithAdmission(alloc.*, admission.*) catch return 0;
    defer alloc.free(proofs);
    const normalized = view.normalizeWithAdmission(alloc.*, &.{}, admission.*) catch return 0;
    defer types.freeNormalizedWrites(alloc.*, normalized);
    const cut = fence.captureSnapshotWithAdmission(alloc.*, admission.*) catch return 0;
    defer cut.deinit();
    const delayed = cut.openWithAdmission(alloc.*, "", "", opts.*, admission.*) catch return 0;
    defer delayed.deinit();
    return 1;
}

// Exercise the real bound-source vtable too: its single-owner multi-view and
// dynamic-cut constructors retain the same DB fences as the storage provider.
export fn antfly_check_bound_statement_capture(raw: *anyopaque, alloc: *const std.mem.Allocator, admission: *const provider.View.Admission) callconv(.c) u8 {
    const reads = @import("api/table_reads.zig");
    const owner: *reads.BoundTableReadSource = @ptrCast(@alignCast(raw));
    const source = owner.source();
    const statement = source.openRelationalStatementWithAdmission(alloc.*, &.{.{ .table = "rows", .from = "", .to = "", .opts = .{} }}, .read_index, admission.*) catch return 0;
    defer statement.deinit();
    const cut = source.openRelationalStatementSnapshotWithAdmission(alloc.*, "rows", 1, .read_index, null, null, admission.*) catch return 0;
    defer cut.deinit();
    return 1;
}

export fn antfly_check_maintenance_original_lifetime(raw: ?*anyopaque, request: *const @import("kernel_owner_abi").MaintenanceRequest, result: *@import("kernel_owner_abi").MaintenanceResult) callconv(.c) @import("kernel_owner_abi").Status {
    return @import("capi/server_owner.zig").storageOwnerMaintenance(raw, request, result);
}
