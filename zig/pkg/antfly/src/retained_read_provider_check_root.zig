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
