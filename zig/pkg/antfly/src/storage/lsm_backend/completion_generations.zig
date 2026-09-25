// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Physically disjoint publication generations. Reader-held allocations keep
//! their original domain, while admission selects an entirely empty domain.
const std = @import("std");
const domains = @import("completion_allocator.zig");
const resources = @import("../resource_manager.zig");

pub const cell_generations = 3;
pub const metadata_generations = 4;
pub const metadata_bytes = 8 * 1024 * 1024;

pub const Generations = struct {
    cells: [cell_generations]*domains.RecyclingScratch,
    metadata: [metadata_generations]*domains.RecyclingScratch,
    capacity: usize,
    cell_bytes: usize,

    pub fn create(backing: std.mem.Allocator, manager: *resources.ResourceManager, capacity: usize, cell_bytes: usize) !Generations {
        if (capacity == 0 or capacity > 4) return error.UnsupportedCompletionProfile;
        const bytes = try std.math.mul(usize, capacity, try domains.PublicationReservation.backingFootprint(cell_bytes));
        var cells: [cell_generations]*domains.RecyclingScratch = undefined;
        var cell_count: usize = 0;
        errdefer for (cells[0..cell_count]) |domain| domain.retire();
        for (&cells) |*domain| {
            domain.* = try domains.RecyclingScratch.create(backing, manager, bytes);
            cell_count += 1;
        }
        var metadata: [metadata_generations]*domains.RecyclingScratch = undefined;
        var metadata_count: usize = 0;
        errdefer for (metadata[0..metadata_count]) |domain| domain.retire();
        for (&metadata) |*domain| {
            domain.* = try domains.RecyclingScratch.create(backing, manager, metadata_bytes);
            metadata_count += 1;
        }
        return .{ .cells = cells, .metadata = metadata, .capacity = capacity, .cell_bytes = cell_bytes };
    }

    pub fn retire(self: *Generations) void {
        for (self.cells) |domain| domain.retire();
        for (self.metadata) |domain| domain.retire();
        self.* = undefined;
    }

    pub const CellReservations = struct {
        items: [4]?*domains.PublicationReservation = @splat(null),

        pub fn deinit(self: *CellReservations) void {
            for (&self.items) |*item| if (item.*) |reservation| {
                reservation.finish();
                item.* = null;
            };
        }
    };

    pub fn reserveCells(self: *Generations) !CellReservations {
        const empty = for (self.cells) |domain| {
            if (domain.isEmpty()) break domain;
        } else return error.CompletionReservationBusy;
        var result: CellReservations = .{};
        errdefer result.deinit();
        for (result.items[0..self.capacity]) |*item|
            item.* = try domains.PublicationReservation.create(empty, self.cell_bytes);
        return result;
    }

    pub fn emptyMetadata(self: *Generations) !*domains.RecyclingScratch {
        for (self.metadata) |domain| if (domain.isEmpty()) return domain;
        return error.CompletionReservationBusy;
    }
};

test "workload admission completion generations retain readers without consuming another cohort span" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var generations = try Generations.create(failing.allocator(), &manager, 4, 4096);
    var retired = false;
    defer if (!retired) generations.retire();
    const charge = manager.snapshot().memory.used_bytes;
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    var readers: [3]struct { allocator: std.mem.Allocator, bytes: []u8 } = undefined;
    for (&readers, 0..) |*reader, i| {
        var reservations = try generations.reserveCells();
        const allocator = reservations.items[0].?.allocator();
        const bytes = try allocator.alloc(u8, 1024);
        @memset(bytes, @intCast(i));
        reader.* = .{ .allocator = allocator, .bytes = bytes };
        reservations.deinit();
    }
    try std.testing.expectError(error.CompletionReservationBusy, generations.reserveCells());
    readers[0].allocator.free(readers[0].bytes);
    var next = try generations.reserveCells();
    for (next.items) |item| try std.testing.expect(item.?.remainingBytes() > 3800);
    next.deinit();
    try std.testing.expectEqual(charge, manager.snapshot().memory.used_bytes);
    try std.testing.expect(!failing.has_induced_failure);
    generations.retire();
    retired = true;
    for (readers[1..], 1..) |reader, i| {
        try std.testing.expectEqual(@as(u8, @intCast(i)), reader.bytes[1023]);
        reader.allocator.free(reader.bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission completion generations reject resource fit before installation" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 16 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    try std.testing.expectError(error.ResourceBudgetExceeded, Generations.create(alloc, &manager, 4, 8 * 1024 * 1024));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission completion metadata bound covers actual binary-bound publication and reader retention" {
    const alloc = std.testing.allocator;
    const repository = @import("repository.zig");
    const Store = @import("run_store.zig").Store;
    const Directory = @import("run_directory.zig").Directory;
    const path = "/root/runs/18446744073709551615.tbl";
    const namespace = "ns\x00x";
    const count = 64;
    const key_bytes = 4096;
    const bound = try std.math.add(usize, try Store.freshAllocationBound(count, key_bytes + namespace.len, path.len), try Directory.freshAllocationBound(count, key_bytes + namespace.len, path.len));
    try std.testing.expect(bound <= metadata_bytes);
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    const domain = try domains.RecyclingScratch.create(failing.allocator(), &manager, try domains.PublicationReservation.backingFootprint(bound));
    var retired = false;
    defer if (!retired) domain.retire();
    const reservation = try domains.PublicationReservation.create(domain, bound);
    var finished = false;
    defer if (!finished) reservation.finish();
    const pub_alloc = reservation.allocator();
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    const View = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *repository.Run) !void {}
        pub fn releaseDirectoryRunSnapshotRef(_: *repository.Run) void {}
    };
    var view = View{ .allocator = pub_alloc };
    var writer: Store = .{};
    defer writer.deinit(alloc);
    var directory: ?*Directory = try Directory.create(pub_alloc);
    defer if (directory) |held| held.destroy(alloc);
    var key: [key_bytes]u8 = @splat(0x81);
    for (0..count) |i| {
        std.mem.writeInt(u64, key[0..8], i, .big);
        var run = try repository.cloneRunCompactionSnapshot(pub_alloc, .{
            .id = i + 1,
            .level = 1,
            .path = @constCast(path),
            .size_bytes = 100,
            .entry_count = 1,
            .smallest_namespace_name = @constCast(namespace),
            .smallest_key = &key,
            .largest_namespace_name = @constCast(namespace),
            .largest_key = &key,
            .bloom_filter = null,
            .state = null,
        });
        writer.append(pub_alloc, run) catch |err| {
            run.deinit(alloc);
            return err;
        };
        try directory.?.put(&view, writer.find(&run).?.*);
    }
    const durable = try directory.?.fork(pub_alloc);
    defer durable.destroy(alloc);
    const reader = try directory.?.fork(alloc);
    defer reader.destroy(alloc);
    try std.testing.expectEqual(@as(usize, count), reader.count());
    // Only two directory headers were included in the prepaid bound; this
    // external reader header is ordinary, while all shared nodes stay prepaid.
    try std.testing.expect(!failing.has_induced_failure);
    reservation.finish();
    finished = true;
    domain.retire();
    retired = true;
    directory.?.destroy(alloc);
    directory = null;
    var cursor = reader.readCursor();
    const first = cursor.next().?;
    try std.testing.expectEqualSlices(u8, namespace, first.run.smallest_namespace_name.?);
    try std.testing.expectEqual(@as(usize, key_bytes), first.run.smallest_key.len);
}
