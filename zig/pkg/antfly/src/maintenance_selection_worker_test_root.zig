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

const runtime = @import("data/runtime.zig");
const raft_batch = @import("data/raft_batch.zig");
const runtime_status = @import("antfly_local_sources").api_runtime_status;
const indexes = @import("api/indexes.zig");
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const private_provisioning = @import("data/private_provisioning.zig");

// The auth storage adapter deliberately receives storage through an injected
// module to avoid a production import cycle. Focused runtime tests expose the
// same narrow surface as root.zig.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = runtime;
    _ = raft_batch;
    _ = runtime_status;
    _ = indexes;
    _ = table_writes;
    _ = private_provisioning;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_control.zig");

pub const consumer_tests_only = true;

pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

// These cases use real helper runtime construction, worker reservations and
// shutdown, but open no table, apply store, storage context or WAL. Keep those
// unreachable cleanup branches fail-closed rather than linking every native
// inference/query/storage provider into this scheduling-only fixture.
const fixture = @import("maintenance_selection_fixture.zig");
pub const MaintenanceSelectionProbe = fixture.Probe;
const abi = @import("kernel_owner_abi");
fn denyClose(_: ?*anyopaque) callconv(.c) void {
    @panic("maintenance worker fixture must not close a storage owner or WAL");
}
fn denyContext(_: ?*anyopaque) callconv(.c) abi.Status {
    @panic("maintenance worker fixture must not construct a storage context");
}
fn denyDrain(_: ?*anyopaque, _: *const abi.CloseDrainRequest) callconv(.c) abi.Status {
    @panic("maintenance worker fixture must not drain a storage owner");
}
fn denyAppend(_: ?*anyopaque, _: *const abi.WalAppendRequest, _: *abi.WalAppendResult) callconv(.c) abi.Status {
    @panic("maintenance worker fixture must not append a WAL");
}
fn denySync(_: ?*anyopaque, _: *const abi.WalSyncRequest) callconv(.c) abi.Status {
    @panic("maintenance worker fixture must not sync a WAL");
}
fn denyTruncate(_: ?*anyopaque, _: *const abi.WalPositionRequest) callconv(.c) abi.Status {
    @panic("maintenance worker fixture must not truncate a WAL");
}
fn denyStatus(_: ?*anyopaque, _: *const abi.JsonOperationRequest, _: *abi.OwnedBytes) callconv(.c) abi.Status {
    @panic("maintenance selection fixture must not publish runtime status");
}
fn denyBuffer(_: *abi.OwnedBytes) callconv(.c) void {
    @panic("maintenance selection fixture must not own a provider buffer");
}
comptime {
    _ = @as(@TypeOf(&abi.antfly_storage_owner_runtime_status_json), &denyStatus);
    _ = @as(@TypeOf(&abi.antfly_storage_owner_buffer_destroy), &denyBuffer);
    @export(&denyStatus, .{ .name = "antfly_storage_owner_runtime_status_json" });
    @export(&denyBuffer, .{ .name = "antfly_storage_owner_buffer_destroy" });
    @export(&fixture.maintenance, .{ .name = "antfly_storage_owner_maintenance" });
    _ = @as(@TypeOf(&abi.antfly_data_apply_store_close), &denyClose);
    _ = @as(@TypeOf(&abi.antfly_metadata_apply_store_close), &denyClose);
    _ = @as(@TypeOf(&abi.antfly_storage_owner_close), &denyClose);
    _ = @as(@TypeOf(&abi.antfly_storage_wal_close), &denyClose);
    _ = @as(@TypeOf(&abi.antfly_storage_context_destroy), &denyContext);
    _ = @as(@TypeOf(&abi.antfly_storage_owner_drain_close), &denyDrain);
    _ = @as(@TypeOf(&abi.antfly_storage_wal_append), &denyAppend);
    _ = @as(@TypeOf(&abi.antfly_storage_wal_sync), &denySync);
    _ = @as(@TypeOf(&abi.antfly_storage_wal_truncate_prefix), &denyTruncate);
    @export(&denyClose, .{ .name = "antfly_data_apply_store_close" });
    @export(&denyClose, .{ .name = "antfly_metadata_apply_store_close" });
    @export(&denyClose, .{ .name = "antfly_storage_owner_close" });
    @export(&denyClose, .{ .name = "antfly_storage_wal_close" });
    @export(&denyContext, .{ .name = "antfly_storage_context_destroy" });
    @export(&denyDrain, .{ .name = "antfly_storage_owner_drain_close" });
    @export(&denyAppend, .{ .name = "antfly_storage_wal_append" });
    @export(&denySync, .{ .name = "antfly_storage_wal_sync" });
    @export(&denyTruncate, .{ .name = "antfly_storage_wal_truncate_prefix" });
}
