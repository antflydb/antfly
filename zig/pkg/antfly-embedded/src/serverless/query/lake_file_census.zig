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

//! Persistent immutable file proofs. Manifest deltas path-copy only affected
//! branches; subtree XOR/count summaries answer authenticated range censuses.
//! These are preparation caches, not authority over source freshness or deletes.
const std = @import("std");
const A = std.mem.Allocator;
pub const Summary = struct { first: [32]u8, last: [32]u8, digest: [32]u8, count: u64 };
pub const Range = struct { count: u64 = 0, digest: [32]u8 = @splat(0) };
pub const Fingerprint = struct { ptr: *anyopaque, get: *const fn (*anyopaque, []const u8) anyerror![32]u8 };
pub fn key(id: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(id, &out, .{});
    return out;
}
pub fn proofHash(id: [32]u8, digest: [32]u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-file-set-proof-v1");
    hash.update(&id);
    hash.update(&digest);
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}
const Entry = struct {
    refs: std.atomic.Value(usize) = .init(1),
    a: A,
    id: []u8,
    key: [32]u8,
    digest: [32]u8,
    proof: [32]u8,
    fn create(a: A, id: []const u8, digest: [32]u8) !*Entry {
        const self = try a.create(Entry);
        errdefer a.destroy(self);
        const k = key(id);
        self.* = .{ .a = a, .id = try a.dupe(u8, id), .key = k, .digest = digest, .proof = proofHash(k, digest) };
        return self;
    }
    fn retain(self: *Entry) *Entry {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *Entry) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        a.free(self.id);
        a.destroy(self);
    }
};
const Node = struct {
    refs: std.atomic.Value(usize) = .init(1),
    a: A,
    entry: *Entry,
    left: ?*Node,
    right: ?*Node,
    summary: Summary,
    priority: u64,
    fn retain(node: ?*Node) ?*Node {
        if (node) |n| _ = n.refs.fetchAdd(1, .monotonic);
        return node;
    }
    fn release(node: ?*Node) void {
        const n = node orelse return;
        if (n.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = n.a;
        release(n.left);
        release(n.right);
        n.entry.release();
        a.destroy(n);
    }
    fn create(a: A, entry: *Entry, left: ?*Node, right: ?*Node) !*Node {
        const self = try a.create(Node);
        var summary: Summary = .{ .first = entry.key, .last = entry.key, .digest = entry.proof, .count = 1 };
        if (left) |n| {
            summary.first = n.summary.first;
            summary.count += n.summary.count;
            xor(&summary.digest, n.summary.digest);
        }
        if (right) |n| {
            summary.last = n.summary.last;
            summary.count += n.summary.count;
            xor(&summary.digest, n.summary.digest);
        }
        self.* = .{ .a = a, .entry = entry.retain(), .left = retain(left), .right = retain(right), .summary = summary, .priority = std.mem.readInt(u64, entry.proof[0..8], .little) };
        return self;
    }
    const Split = struct { left: ?*Node, right: ?*Node };
    fn split(a: A, root: ?*Node, k: [32]u8, depth: usize) !Split {
        if (depth == 256) return error.ExternalSourceSnapshotTooLarge;
        const n = root orelse return .{ .left = null, .right = null };
        if (std.mem.lessThan(u8, &n.entry.key, &k)) {
            const parts = try split(a, n.right, k, depth + 1);
            defer release(parts.left);
            errdefer release(parts.right);
            return .{ .left = try create(a, n.entry, n.left, parts.left), .right = parts.right };
        }
        const parts = try split(a, n.left, k, depth + 1);
        defer release(parts.right);
        errdefer release(parts.left);
        return .{ .left = parts.left, .right = try create(a, n.entry, parts.right, n.right) };
    }
    fn before(priority: u64, k: [32]u8, other: *Node) bool {
        return priority < other.priority or (priority == other.priority and std.mem.lessThan(u8, &k, &other.entry.key));
    }
    fn put(a: A, root: ?*Node, entry: *Entry, depth: usize) !*Node {
        if (depth == 256) return error.ExternalSourceSnapshotTooLarge;
        const n = root orelse return create(a, entry, null, null);
        const order = std.mem.order(u8, &entry.key, &n.entry.key);
        if (order == .eq) {
            if (!std.mem.eql(u8, entry.id, n.entry.id)) return error.InvalidExternalSourceInventory;
            return create(a, entry, n.left, n.right);
        }
        const priority = std.mem.readInt(u64, entry.proof[0..8], .little);
        if (before(priority, entry.key, n)) {
            const parts = try split(a, root, entry.key, depth + 1);
            defer release(parts.left);
            defer release(parts.right);
            return create(a, entry, parts.left, parts.right);
        }
        const branch = try put(a, if (order == .lt) n.left else n.right, entry, depth + 1);
        defer release(branch);
        return create(a, n.entry, if (order == .lt) branch else n.left, if (order == .gt) branch else n.right);
    }
    fn merge(a: A, left: ?*Node, right: ?*Node, depth: usize) !?*Node {
        if (depth == 256) return error.ExternalSourceSnapshotTooLarge;
        const l = left orelse return retain(right);
        const r = right orelse return retain(left);
        if (before(l.priority, l.entry.key, r)) {
            const branch = try merge(a, l.right, right, depth + 1);
            defer release(branch);
            return try create(a, l.entry, l.left, branch);
        }
        const branch = try merge(a, left, r.left, depth + 1);
        defer release(branch);
        return try create(a, r.entry, branch, r.right);
    }
    fn remove(a: A, root: ?*Node, k: [32]u8, depth: usize) !?*Node {
        if (depth == 256) return error.ExternalSourceSnapshotTooLarge;
        const n = root orelse return error.InvalidExternalSourceInventory;
        const order = std.mem.order(u8, &k, &n.entry.key);
        if (order == .eq) return merge(a, n.left, n.right, depth + 1);
        const branch = try remove(a, if (order == .lt) n.left else n.right, k, depth + 1);
        defer release(branch);
        return try create(a, n.entry, if (order == .lt) branch else n.left, if (order == .gt) branch else n.right);
    }
};
fn xor(out: *[32]u8, other: [32]u8) void {
    for (out, other) |*v, r| v.* ^= r;
}
pub const Census = struct {
    refs: std.atomic.Value(usize) = .init(1),
    a: A,
    domain: [32]u8,
    root: ?*Node = null,
    rehashed: usize = 0,
    pub fn retain(self: *Census) *Census {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Census) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        Node.release(self.root);
        a.destroy(self);
    }
    pub fn summary(self: *const Census) ?Summary {
        return if (self.root) |n| n.summary else null;
    }
    pub fn find(self: *const Census, k: [32]u8) ?*const Entry {
        var next = self.root;
        while (next) |n| switch (std.mem.order(u8, &k, &n.entry.key)) {
            .eq => return n.entry,
            .lt => next = n.left,
            .gt => next = n.right,
        };
        return null;
    }
    pub fn range(self: *const Census, first: [32]u8, last: [32]u8) Range {
        var result: Range = .{};
        accumulate(self.root, first, last, &result.count, &result.digest);
        return result;
    }
    fn accumulate(root: ?*Node, first: [32]u8, last: [32]u8, count: *u64, digest: *[32]u8) void {
        const n = root orelse return;
        if (std.mem.order(u8, &n.summary.last, &first) == .lt or std.mem.order(u8, &n.summary.first, &last) == .gt) return;
        if (std.mem.order(u8, &n.summary.first, &first) != .lt and std.mem.order(u8, &n.summary.last, &last) != .gt) {
            count.* += n.summary.count;
            xor(digest, n.summary.digest);
            return;
        }
        accumulate(n.left, first, last, count, digest);
        if (std.mem.order(u8, &n.entry.key, &first) != .lt and std.mem.order(u8, &n.entry.key, &last) != .gt) {
            count.* += 1;
            xor(digest, n.entry.proof);
        }
        accumulate(n.right, first, last, count, digest);
    }
    pub const Iterator = struct {
        stack: [256]*Node = undefined,
        len: usize = 0,
        pending: ?*Node,
        lower: ?[32]u8 = null,
        pub fn next(self: *Iterator) !?*const Entry {
            while (self.pending) |n| {
                if (self.lower) |lower| if (std.mem.lessThan(u8, &n.entry.key, &lower)) {
                    self.pending = n.right;
                    continue;
                };
                if (self.len == self.stack.len) return error.ExternalSourceSnapshotTooLarge;
                self.stack[self.len] = n;
                self.len += 1;
                self.pending = n.left;
            }
            if (self.len == 0) return null;
            self.len -= 1;
            const n = self.stack[self.len];
            self.pending = n.right;
            return n.entry;
        }
    };
    pub fn iterator(self: *const Census) Iterator {
        return .{ .pending = self.root };
    }
    pub fn iteratorFrom(self: *const Census, lower: [32]u8) Iterator {
        return .{ .pending = self.root, .lower = lower };
    }
    pub fn build(a: A, domain: [32]u8, prior: ?*const Census, removed: []const []const u8, added: []const []const u8, all: anytype, fingerprint: Fingerprint, context: @import("lake_read_context.zig").Context) !*Census {
        const self = try a.create(Census);
        self.* = .{ .a = a, .domain = domain };
        errdefer self.release();
        const reuse = prior != null and std.mem.eql(u8, &prior.?.domain, &domain);
        if (reuse) {
            self.root = Node.retain(prior.?.root);
            for (removed) |id| {
                try context.ensureActive();
                const next = try Node.remove(a, self.root, key(id), 0);
                Node.release(self.root);
                self.root = next;
            }
            for (added) |id| try self.add(id, fingerprint, context);
        } else for (all) |file| try self.add(file.file_id, fingerprint, context);
        return self;
    }
    fn add(self: *Census, id: []const u8, fingerprint: Fingerprint, context: @import("lake_read_context.zig").Context) !void {
        try context.ensureActive();
        const entry = try Entry.create(self.a, id, try fingerprint.get(fingerprint.ptr, id));
        defer entry.release();
        const next = try Node.put(self.a, self.root, entry, 0);
        Node.release(self.root);
        self.root = next;
        self.rehashed += 1;
    }
};

test "external lake metadata navigation persistent file census hashes only changed manifest files" {
    const a = std.testing.allocator;
    const Input = struct { file_id: []const u8 };
    const Hasher = struct {
        changed: bool = false,
        calls: usize = 0,
        fn get(raw: *anyopaque, id: []const u8) ![32]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            var value = key(id);
            if (self.changed and std.mem.eql(u8, id, "f3")) value[0] ^= 1;
            return value;
        }
    };
    const files = [_]Input{ .{ .file_id = "f0" }, .{ .file_id = "f1" }, .{ .file_id = "f2" }, .{ .file_id = "f3" }, .{ .file_id = "f4" } };
    var hasher: Hasher = .{};
    const fingerprint: Fingerprint = .{ .ptr = &hasher, .get = Hasher.get };
    const first = try Census.build(a, @splat(1), null, &.{}, &.{}, &files, fingerprint, .{});
    defer first.release();
    const old = first.summary().?;
    try std.testing.expectEqual(@as(usize, 5), hasher.calls);
    hasher.changed = true;
    hasher.calls = 0;
    const next = try Census.build(a, @splat(1), first, &.{"f3"}, &.{"f3"}, &files, fingerprint, .{});
    defer next.release();
    try std.testing.expectEqual(@as(usize, 1), hasher.calls);
    try std.testing.expectEqual(@as(u64, 5), next.summary().?.count);
    try std.testing.expectEqualDeep(old, first.summary().?);
    hasher.calls = 0;
    const fresh = try Census.build(a, @splat(2), next, &.{}, &.{}, &files, fingerprint, .{});
    defer fresh.release();
    try std.testing.expectEqual(@as(usize, 5), hasher.calls);
    try std.testing.expectEqualDeep(next.summary(), fresh.summary());
    var it = next.iterator();
    var seen: usize = 0;
    while (try it.next()) |entry| {
        const range_ = next.range(entry.key, entry.key);
        try std.testing.expectEqual(@as(u64, 1), range_.count);
        try std.testing.expectEqual(entry.proof, range_.digest);
        var from = next.iteratorFrom(entry.key);
        try std.testing.expectEqualStrings(entry.id, (try from.next()).?.id);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 5), seen);
}

fn censusFailureScenario(a: A) !void {
    const Hash = struct {
        fn get(_: *anyopaque, id: []const u8) ![32]u8 {
            return key(id);
        }
    };
    const files = [_]struct { file_id: []const u8 }{ .{ .file_id = "a" }, .{ .file_id = "b" }, .{ .file_id = "c" } };
    var unused: u8 = 0;
    const fingerprint: Fingerprint = .{ .ptr = &unused, .get = Hash.get };
    const before = try Census.build(a, @splat(1), null, &.{}, &.{}, &files, fingerprint, .{});
    defer before.release();
    const after = try Census.build(a, @splat(1), before, &.{"b"}, &.{"d"}, &files, fingerprint, .{});
    defer after.release();
    try std.testing.expect(after.find(key("b")) == null);
    try std.testing.expect(before.find(key("b")) != null);
    try std.testing.expect(after.find(key("d")) != null);
}
test "external lake metadata navigation file census unwinds path copies under allocation failure" {
    try censusFailureScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, censusFailureScenario, .{});
}
