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

//! Immutable path identities and deadlines. Forks retain a root; publication
//! diffs skip shared subtrees. Deadline summaries prune future-only subtrees.
const std = @import("std");
const Account = @import("memory_account.zig").Account;
const Path = @import("repository.zig").ObsoletePath;
const Payload = struct {
    refs: std.atomic.Value(usize) = .init(1),
    path: []u8,
    account: *Account,
};

const Entry = struct {
    path: []const u8,
    deadline: u64 = 0,
    payload: ?*Payload = null,

    pub const Summary = struct { deadline: u64 = std.math.maxInt(u64), wire_bytes: u64 = 0 };
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        return .{ .deadline = @min(entry.deadline, left.deadline, right.deadline), .wire_bytes = left.wire_bytes +| right.wire_bytes +| 12 +| entry.path.len };
    }
    pub fn retainShared(self: Entry) Entry {
        _ = self.payload.?.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        const payload = self.payload orelse return;
        if (payload.refs.fetchSub(1, .acq_rel) != 1) return;
        // Payload slots and adopted strings were charged separately; each
        // charge owns an accounting reference, including a zero-length path.
        payload.account.discharge(payload.path.len);
        payload.account.discharge(@sizeOf(Payload));
        allocator.free(payload.path);
        allocator.destroy(payload);
    }
    pub fn retainedBytes(_: Entry) usize {
        return 0;
    }
    pub fn eql(a: Entry, b: Entry) bool {
        return a.deadline == b.deadline and std.mem.eql(u8, a.path, b.path);
    }
    fn value(self: Entry) Path {
        return .{ .path = @constCast(self.path), .delete_after_ns = self.deadline };
    }
};

fn compare(a: Entry, b: Entry) std.math.Order {
    return std.mem.order(u8, a.path, b.path);
}

pub const Ledger = struct {
    const Tree = @import("ordered_index.zig").Index(Entry, compare);
    tree: Tree = .{},
    spare: std.ArrayListUnmanaged(*Payload) = .empty,
    allocator: ?std.mem.Allocator = null,

    pub const empty: Ledger = .{};
    pub fn count(self: *const Ledger) usize {
        return if (self.tree.root) |root| root.count else 0;
    }
    pub fn manifestBytes(self: *const Ledger) u64 {
        return if (self.tree.root) |root| root.summary.wire_bytes else 0;
    }
    pub fn fork(self: *const Ledger) Ledger {
        return .{ .tree = self.tree.fork(), .allocator = self.allocator };
    }
    pub fn deinit(self: *Ledger, allocator: std.mem.Allocator) void {
        for (self.spare.items) |payload| {
            self.tree.account.?.discharge(@sizeOf(Payload));
            allocator.destroy(payload);
        }
        self.spare.deinit(allocator);
        self.tree.deinit(allocator);
        self.* = .{};
    }
    pub fn memoryBytes(self: *const Ledger, pass: u64) u64 {
        return self.spare.capacity * @sizeOf(*Payload) + self.tree.spare.capacity * @sizeOf(*Tree.Node) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0);
    }
    pub fn ensureUnusedCapacity(self: *Ledger, allocator: std.mem.Allocator, edits: usize) !void {
        self.allocator = allocator;
        try self.tree.prepareEdits(allocator, edits);
        try self.spare.ensureTotalCapacity(allocator, edits);
        while (self.spare.items.len < edits) {
            const payload = try allocator.create(Payload);
            self.tree.account.?.charge(@sizeOf(Payload));
            self.spare.appendAssumeCapacity(payload);
        }
    }
    pub fn prepareMemoryBound(self: *const Ledger, edits: usize) !u64 {
        return std.math.add(u64, try self.tree.prepareMemoryBound(edits), try std.math.mul(u64, edits, @sizeOf(Payload) + 2 * @sizeOf(*Payload)));
    }
    /// Takes ownership of path; reserves all fallible work before publication.
    pub fn append(self: *Ledger, allocator: std.mem.Allocator, path: Path) !void {
        // Fallible edits may be nested inside a caller's already-reserved
        // atomic publication. Do not consume its remaining prepared slots.
        try self.ensureUnusedCapacity(allocator, self.spare.items.len + 1);
        self.appendAssumeCapacity(path);
    }
    pub fn appendAssumeCapacity(self: *Ledger, path: Path) void {
        const payload = self.spare.pop().?;
        payload.* = .{ .path = path.path, .account = self.tree.account.? };
        payload.account.charge(path.path.len);
        const entry = Entry{ .path = path.path, .deadline = path.delete_after_ns, .payload = payload };
        defer entry.deinit(self.allocator.?);
        self.tree.putPrepared(self.allocator.?, entry);
    }
    pub fn get(self: *const Ledger, path: []const u8) ?Path {
        return (Tree.find(self.tree.root, .{ .path = path }) orelse return null).entry.value();
    }
    pub fn contains(self: *const Ledger, path: []const u8) bool {
        return self.get(path) != null;
    }
    pub fn at(self: *const Ledger, rank: usize) Path {
        return self.tree.root.?.at(rank).value();
    }
    pub fn setDeadlinePrepared(self: *Ledger, path: []const u8, deadline: u64) void {
        var entry = (Tree.find(self.tree.root, .{ .path = path }) orelse unreachable).entry.retainShared();
        defer entry.deinit(self.allocator.?);
        entry.deadline = deadline;
        self.tree.putPrepared(self.allocator.?, entry);
    }
    pub fn removePrepared(self: *Ledger, path: []const u8) void {
        self.tree.removePrepared(self.allocator.?, .{ .path = path });
    }
    pub const Cursor = struct {
        root: ?*const Tree.Node,
        index: Tree.Cursor = .{},
        rank: usize = 0,
        pub fn next(self: *@This()) ?Path {
            const root = self.root orelse return null;
            if (self.rank == root.count) return null;
            const value = self.index.at(root, self.rank).value();
            self.rank += 1;
            return value;
        }
    };
    pub fn iterator(self: *const Ledger) Cursor {
        return .{ .root = self.tree.root };
    }
    pub fn earliestDeadline(self: *const Ledger) ?u64 {
        return if (self.tree.root) |root| root.summary.deadline else null;
    }

    /// Stable path bookmarks survive root replacement and removal of the last
    /// visited entry. Subtree deadlines avoid scanning future-only ranges.
    /// Unlike a borrowed traversal stack, this can resume on the live tree.
    pub fn nextDueAfter(self: *const Ledger, deadline: u64, after: ?[]const u8) ?Path {
        return dueAfter(self.tree.root, deadline, after);
    }

    fn dueAfter(root: ?*const Tree.Node, deadline: u64, after: ?[]const u8) ?Path {
        const node = root orelse return null;
        if (node.summary.deadline > deadline) return null;
        if (after) |path| if (std.mem.order(u8, node.entry.path, path) != .gt)
            return dueAfter(node.right, deadline, after);
        if (dueAfter(node.left, deadline, after)) |entry| return entry;
        if (node.entry.deadline <= deadline) return node.entry.value();
        return dueAfter(node.right, deadline, after);
    }
    pub const DueCursor = struct {
        stack: [2 * @bitSizeOf(usize)]*const Tree.Node = undefined,
        len: usize = 0,
        deadline: u64,
        pub fn next(self: *@This(), budget: *usize) ?Path {
            while (self.len != 0 and budget.* != 0) {
                budget.* -= 1;
                self.len -= 1;
                const node = self.stack[self.len];
                if (node.summary.deadline > self.deadline) continue;
                if (node.right) |right| {
                    self.stack[self.len] = right;
                    self.len += 1;
                }
                if (node.left) |left| {
                    self.stack[self.len] = left;
                    self.len += 1;
                }
                if (node.entry.deadline <= self.deadline) return node.entry.value();
            }
            return null;
        }
    };
    pub fn due(self: *const Ledger, deadline: u64) DueCursor {
        var cursor = DueCursor{ .deadline = deadline };
        if (self.tree.root) |root| {
            cursor.stack[0] = root;
            cursor.len = 1;
        }
        return cursor;
    }
    /// Explicit diagnostic/export projection, never used by publication.
    pub fn project(self: *const Ledger, allocator: std.mem.Allocator) ![]Path {
        const out = try allocator.alloc(Path, self.count());
        var cursor = self.iterator();
        for (out) |*path| path.* = cursor.next().?;
        return out;
    }
    pub fn changesSince(self: *const Ledger, previous: *const Ledger, visitor: anytype) !void {
        var adapter = struct {
            target: @TypeOf(visitor),
            pub fn put(adapter: *@This(), entry: Entry) !void {
                try adapter.target.put(entry.value());
            }
            pub fn remove(adapter: *@This(), entry: Entry) !void {
                try adapter.target.remove(entry.value());
            }
        }{ .target = visitor };
        try self.tree.changesSince(&previous.tree, &adapter);
    }
};

test "obsolete ledger snapshots retain deadlines and diff only changed paths" {
    const allocator = std.testing.allocator;
    var ledger: Ledger = .{};
    defer ledger.deinit(allocator);
    for (0..256) |i| {
        const path = try std.fmt.allocPrint(allocator, "runs/{d:0>5}.tbl", .{i});
        errdefer allocator.free(path);
        try ledger.append(allocator, .{ .path = path, .delete_after_ns = i });
    }
    var snapshot = ledger.fork();
    defer snapshot.deinit(allocator);
    try ledger.ensureUnusedCapacity(allocator, 2);
    ledger.removePrepared("runs/00001.tbl");
    ledger.setDeadlinePrepared("runs/00002.tbl", 1000);
    try std.testing.expectEqual(@as(usize, 256), snapshot.count());
    try std.testing.expectEqual(@as(u64, 2), snapshot.get("runs/00002.tbl").?.delete_after_ns);
    const Visitor = struct {
        puts: usize = 0,
        removes: usize = 0,
        pub fn put(self: *@This(), path: Path) !void {
            try std.testing.expectEqualStrings("runs/00002.tbl", path.path);
            self.puts += 1;
        }
        pub fn remove(self: *@This(), path: Path) !void {
            try std.testing.expectEqualStrings("runs/00001.tbl", path.path);
            self.removes += 1;
        }
    };
    var visitor: Visitor = .{};
    try ledger.changesSince(&snapshot, &visitor);
    try std.testing.expectEqual(@as(usize, 1), visitor.puts);
    try std.testing.expectEqual(@as(usize, 1), visitor.removes);
    // Resume from an entry removed from the live root; future-only subtrees
    // and deadline changes do not require retaining an old traversal stack.
    const after = try allocator.dupe(u8, ledger.nextDueAfter(10, null).?.path);
    defer allocator.free(after);
    try ledger.ensureUnusedCapacity(allocator, 1);
    ledger.removePrepared(after);
    try std.testing.expectEqualStrings("runs/00003.tbl", ledger.nextDueAfter(10, after).?.path);
    try std.testing.expect(ledger.nextDueAfter(10, "runs/00010.tbl") == null);
}

test "obsolete ledger churn publication scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const time = @import("antfly_platform").time;
    for ([_]usize{ 1000, 10000, 100000 }) |count| {
        var ledger: Ledger = .{};
        defer ledger.deinit(allocator);
        var baseline: std.StringHashMapUnmanaged(u64) = .empty;
        defer baseline.deinit(allocator);
        for (0..count) |i| {
            const path = try std.fmt.allocPrint(allocator, "runs/{d:0>8}.tbl", .{i});
            errdefer allocator.free(path);
            try baseline.put(allocator, path, 1000000);
            try ledger.append(allocator, .{ .path = path, .delete_after_ns = 1000000 });
        }
        var snapshot = ledger.fork();
        defer snapshot.deinit(allocator);
        try ledger.ensureUnusedCapacity(allocator, 1);
        ledger.setDeadlinePrepared(ledger.at(count / 2).path, 2000000);
        const Visitor = struct {
            changes: usize = 0,
            pub fn put(self: *@This(), _: Path) !void {
                self.changes += 1;
            }
            pub fn remove(self: *@This(), _: Path) !void {
                self.changes += 1;
            }
        };
        var delta_ns: [9]u64 = undefined;
        var scan_ns: [9]u64 = undefined;
        var pin_ns: [9]u64 = undefined;
        for (0..9) |sample| {
            var started = time.monotonicNs();
            for (0..100) |_| {
                var visitor: Visitor = .{};
                try ledger.changesSince(&snapshot, &visitor);
                try std.testing.expectEqual(@as(usize, 1), visitor.changes);
            }
            delta_ns[sample] = (time.monotonicNs() - started) / 100;
            started = time.monotonicNs();
            var cursor = ledger.iterator();
            var changes: usize = 0;
            while (cursor.next()) |path| changes += @intFromBool(baseline.get(path.path).? != path.delete_after_ns);
            try std.testing.expectEqual(@as(usize, 1), changes);
            scan_ns[sample] = time.monotonicNs() - started;
            started = time.monotonicNs();
            for (0..1000) |_| {
                var pin = ledger.fork();
                pin.deinit(allocator);
            }
            pin_ns[sample] = (time.monotonicNs() - started) / 1000;
            var due = ledger.due(0);
            var budget: usize = 1;
            try std.testing.expect(due.next(&budget) == null);
            try std.testing.expectEqual(@as(usize, 0), due.len);
        }
        std.mem.sort(u64, &delta_ns, {}, std.sort.asc(u64));
        std.mem.sort(u64, &scan_ns, {}, std.sort.asc(u64));
        std.mem.sort(u64, &pin_ns, {}, std.sort.asc(u64));
        std.debug.print("\nLSM obsolete ledger paths={d} delta_median_ns={d} full_scan_median_ns={d} pin_median_ns={d} retained_bytes={d}\n", .{ count, delta_ns[4], scan_ns[4], pin_ns[4], ledger.memoryBytes(@import("memory_account.zig").nextPass()) });
    }
}
