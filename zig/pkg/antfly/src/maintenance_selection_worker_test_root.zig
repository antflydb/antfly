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

test "data runtime background provider clients preserve shared bounded admission" {
    const std = @import("std");
    const Provider = @import("antfly_local_sources").inference_managed_embedder.ProviderRuntime;
    var owner = Provider.init(std.testing.allocator, std.testing.io);
    defer owner.deinit();
    const clients = .{ try owner.httpClient(), try owner.generationHttpClient(), try owner.assetHttpClient() };
    const repeated = .{ try owner.httpClient(), try owner.generationHttpClient(), try owner.assetHttpClient() };
    inline for (clients, repeated) |client, same| {
        try std.testing.expect(client == same);
        const Admission = @TypeOf(client.outbound);
        var held: [20]Admission.Lease = undefined;
        var acquired: usize = 0;
        defer for (held[0..acquired]) |*lease| lease.release();
        const hosts = [_][]const u8{ "a", "b", "c", "d", "e" };
        for (&held, 0..) |*lease, i| {
            lease.* = try client.outbound.acquire(hosts[i / 4], 443, .{});
            acquired += 1;
        }
        // A new endpoint still shares the service-wide bound. Oversized queue
        // metadata forces immediate refusal without timing or network traffic.
        if (client.outbound.acquire("extra", 443, .{ .retained_bytes = 65 << 20 })) |value| {
            var unexpected = value;
            unexpected.release();
            return error.TestExpectedError;
        } else |err| try std.testing.expectEqual(error.OutboundCapacityUnavailable, err);
        held[0].release();
        var next = try client.outbound.acquire("extra", 443, .{});
        next.release();
    }
}

test "data runtime maintenance bounded selection keeps original authority while registry ownership waits" {
    const std = @import("std");
    const local = @import("antfly_local_sources");
    const abi_runtime = @import("antfly_runtime_abi");
    const Source = @import("api/kernel_owner_source.zig").ProvisionedKernelOwnerSource;
    const Facade = table_writes.ProvisionedTableWriteSource;
    const Check = struct {
        reject: std.atomic.Value(bool) = .init(false),
        cause: anyerror,
        fn check(raw: *anyopaque) callconv(.c) abi_runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.reject.load(.acquire)) abi_runtime.error_abi.statusFromError(self.cause) else .ok;
        }
    };
    const Worker = struct {
        facade: *Facade,
        original: *const abi_runtime.runnable_abi.Reentry,
        io: std.Io,
        done: std.Io.Event = .unset,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            defer self.done.set(self.io);
            _ = self.facade.runLsmMaintenanceRoundDetailedWithLifetime(self.original) catch |cause| {
                self.failure = cause;
            };
        }
    };
    var backend = try local.storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer backend.deinit();
    var workers = try backend.ptr().acquireWorkers(.{ .capacity = 2 });
    defer workers.release();
    const io = workers.io();
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        const catalog = @import("api/table_catalog.zig").emptyCatalogSource();
        var source = Source.init(std.testing.allocator, "/unused", catalog, @import("raft/read_gate.zig").alreadyReadSafeBarrier());
        defer source.entries.deinit(std.testing.allocator);
        _ = source.withBackendRuntime(backend.ptr(), .{});
        var facade = Facade.init("/unused", catalog);
        defer facade.deinit();
        facade.storage_maintenance_source = source.maintenanceSource();
        try std.testing.expect(source.mutex.tryLock());
        var held = true;
        defer if (held) source.mutex.unlock();
        var check: Check = .{ .cause = cause };
        const original: abi_runtime.runnable_abi.Reentry = .{ .context = &check, .check = Check.check };
        var worker: Worker = .{ .facade = &facade, .original = &original, .io = io };
        var future = try io.concurrent(Worker.run, .{&worker});
        var joined = false;
        defer if (!joined) {
            check.reject.store(true, .release);
            if (held) source.mutex.unlock();
            held = false;
            future.await(io);
        };
        const until = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
        while (!source.lsm_selection_claim.load(.acquire)) {
            if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUnexpectedResult;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        check.reject.store(true, .release);
        try worker.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        future.await(io);
        joined = true;
        try std.testing.expectEqual(cause, worker.failure.?);
        try std.testing.expect(!source.mutex.tryLock());
        try std.testing.expect(!source.lsm_selection_claim.load(.acquire));
        try std.testing.expectError(cause, original.checkpoint());
        source.mutex.unlock();
        held = false;
        var retry: Check = .{ .cause = cause };
        const fresh: abi_runtime.runnable_abi.Reentry = .{ .context = &retry, .check = Check.check };
        const result = try facade.runLsmMaintenanceRoundDetailedWithLifetime(&fresh);
        try std.testing.expect(!result.progressed and !result.selection_pending);
        try std.testing.expectError(cause, original.checkpoint());
    }
}
