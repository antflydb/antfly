// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Consumer integration wiring. Only test roots import this fixture; the
//! production provider has no dependency on its source or test allocator.
const std = @import("std");
const table_writes = @import("table_writes.zig");
const owner_source = @import("kernel_owner_source.zig");
const client = @import("../storage/kernel_owner_client.zig");
const services = @import("../storage/kernel_runtime_services.zig");
const test_allocator = std.testing.allocator;
const read_gate = @import("../raft/read_gate.zig");

pub const Bound = struct {
    context: client.Context,
    source: *table_writes.ProvisionedTableWriteSource,
    owner: *owner_source.ProvisionedKernelOwnerSource,

    pub fn init(source: *table_writes.ProvisionedTableWriteSource) !Bound {
        const alloc = std.testing.allocator;
        var context = client.Context{};
        var allocator_bridge = services.memory.Allocator.fromStd(&test_allocator);
        var io_bridge: ?services.executor.Borrow = null;
        if (source.backend_runtime) |runtime| {
            if (runtime.usesBorrowedIo()) {
                const io = runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
                io_bridge = .init(&io);
            }
        }
        try context.ensureWithRuntime(.{
            .allocator = &allocator_bridge,
            .io = if (io_bridge) |*borrow| borrow else null,
        });
        errdefer context.deinit();
        const owner = try alloc.create(owner_source.ProvisionedKernelOwnerSource);
        owner.* = .init(alloc, source.replica_root_dir, source.catalog, read_gate.alreadyReadSafeBarrier());
        _ = owner.withStorageContextHandle(context.handle.?);
        _ = owner.withGroupVisibleRootGeneration(source.group_visible_root_generation);
        _ = owner.withTransactionRecoverySource(source.transactionRecoverySource());
        if (source.runtime_status_cache) |cache| _ = owner.withRuntimeStatusCache(cache);
        _ = source.withLocalWriteSource(owner.writeSource());
        _ = source.withStorageSnapshotSource(owner.snapshotSource());
        _ = source.withStorageMaintenanceSource(owner.maintenanceSource());
        return .{ .context = context, .source = source, .owner = owner };
    }

    pub fn deinit(self: *Bound) void {
        // Source workers retain callbacks into the owner. Drain them before
        // releasing the real compiled provider and its handles.
        self.source.deinit();
        self.deinitOwner();
    }

    pub fn deinitOwner(self: *Bound) void {
        self.owner.deinit();
        std.testing.allocator.destroy(self.owner);
        self.context.deinit();
        self.* = undefined;
    }
};
