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

//! Real physical WAL provider, separately linked from the control-only Primary.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
comptime {
    const owner = @import("storage/kernel_wal_owner.zig");
    @export(&owner.open, .{ .name = "antfly_storage_wal_open" });
    @export(&owner.openWithAllocator, .{ .name = "antfly_storage_wal_open_with_allocator" });
    @export(&owner.close, .{ .name = "antfly_storage_wal_close" });
    @export(&owner.append, .{ .name = "antfly_storage_wal_append" });
    @export(&owner.prepareAppend, .{ .name = "antfly_storage_wal_prepare_append" });
    @export(&owner.preparedPublish, .{ .name = "antfly_storage_wal_prepared_publish" });
    @export(&owner.preparedDestroy, .{ .name = "antfly_storage_wal_prepared_destroy" });
    @export(&owner.sync, .{ .name = "antfly_storage_wal_sync" });
    @export(&owner.truncatePrefix, .{ .name = "antfly_storage_wal_truncate_prefix" });
    @export(&owner.truncateSuffix, .{ .name = "antfly_storage_wal_truncate_suffix" });
    @export(&owner.iterate, .{ .name = "antfly_storage_wal_iterate" });
    @export(&owner.iterateWithAllocator, .{ .name = "antfly_storage_wal_iterate_with_allocator" });
    @export(&owner.read, .{ .name = "antfly_storage_wal_read" });
    @export(&owner.readWithAllocator, .{ .name = "antfly_storage_wal_read_with_allocator" });
    @export(&owner.readWithAdmission, .{ .name = "antfly_storage_wal_read_with_admission" });
    @export(&owner.recoveryPosition, .{ .name = "antfly_storage_wal_recovery_position" });
    @export(&owner.appendIfRecoveryPosition, .{ .name = "antfly_storage_wal_append_if_recovery_position" });
    @export(&owner.ownedReadBegin, .{ .name = "antfly_storage_wal_owned_read_begin" });
    @export(&owner.ownedReadStep, .{ .name = "antfly_storage_wal_owned_read_step" });
    @export(&owner.ownedReadChunk, .{ .name = "antfly_storage_wal_owned_read_chunk" });
    @export(&owner.ownedReadDestroy, .{ .name = "antfly_storage_wal_owned_read_destroy" });
    @export(&owner.lastLsn, .{ .name = "antfly_storage_wal_last_lsn" });
    @export(&owner.lastLsnWithAdmission, .{ .name = "antfly_storage_wal_last_lsn_with_admission" });
    @export(&owner.statsSnapshot, .{ .name = "antfly_storage_wal_stats_snapshot" });
    @export(&bufferDestroy, .{ .name = "antfly_storage_owner_buffer_destroy" });
}

fn bufferDestroy(bytes: *@import("kernel_owner_abi").OwnedBytes) callconv(.c) void {
    if (bytes.ptr) |ptr| @import("std").heap.c_allocator.free(ptr[0..@intCast(bytes.len)]);
    bytes.* = .{};
}
