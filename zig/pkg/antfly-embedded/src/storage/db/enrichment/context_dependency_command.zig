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

//! Replicated context-dependency mutations for Raft-owned tables.
//!
//! Embedded tables plan dependency-store changes against a local reader and
//! commit them directly. A replicated table cannot: every replica must hold
//! byte-identical subscriptions, states, manifests, and queues, and only the
//! group leader may plan. So the leader plans exactly as before, through a
//! `Recorder` that captures the read set (every key read and every key range
//! scanned, as digests), and proposes a `Command` carrying the read set and
//! the exact puts and deletes. Every replica applies the command at the same
//! log position: if the read set still matches, the effects are written and
//! the command digest is recorded as the last applied command; otherwise the
//! command is a deterministic no-op and the leader re-plans. Replicas never
//! re-plan, so planning code may stay non-deterministic in allocation and
//! iteration details without risking divergence.
//!
//! The second form, `inbox`, appends change notices delivered by source
//! tables (proposed by any group's leader to every group of the dependent
//! table). The dependent leader later applies inbox notices through a plan
//! command that deletes them in the same commit.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const store = @import("context_dependency_store.zig");

pub const Digest = [Sha256.digest_length]u8;

/// Source-side notice outbox (see context_notices.zig).
const outbox_family: u8 = 0x04;
/// Inbox of delivered notices (dependent side), keyed by notice digest.
pub const inbox_family: u8 = 0x07;
/// Digest of the last applied plan command (identical on every replica).
pub const last_applied_key = store.familyPrefix(0x08);

pub const max_effects: usize = 4096;
pub const max_reads: usize = 4096;
pub const max_inbox_notices: usize = 256;

pub const Command = struct {
    /// Hex-encoded binary keys/values (JSON-safe on the batch wire).
    puts: []const Put = &.{},
    deletes: []const []const u8 = &.{},
    reads: []const Read = &.{},
    scans: []const Scan = &.{},
    /// Delivered notices to append to this group's inbox.
    inbox: []const []const u8 = &.{},
    /// Outbox entries delivered by the leader, as hex SHA-256 of the notice
    /// bytes. Outbox keys embed each replica's local sequence, so replicas
    /// delete matching entries by content, not by key.
    acks: []const []const u8 = &.{},
    /// Dependents this plan invalidates. Every replica journals their
    /// rebuilds only once the plan applies, so a rebuild never runs against
    /// dependency state the plan has not yet written.
    rebuilds: []const Rebuild = &.{},

    pub const Put = struct { key: []const u8, value: []const u8 };
    /// Hex-encoded artifact name and document key.
    pub const Rebuild = struct { artifact: []const u8, doc_key: []const u8 };
    /// `digest` is empty when the key was absent.
    pub const Read = struct { key: []const u8, digest: []const u8 = "" };
    /// Every entry in `[lower, upper)`, in key order.
    pub const Scan = struct { lower: []const u8, upper: []const u8, digest: []const u8 };

    pub fn isInbox(self: Command) bool {
        return self.inbox.len != 0;
    }

    pub fn validate(self: Command) !void {
        const plan = self.puts.len != 0 or self.deletes.len != 0 or self.reads.len != 0 or self.scans.len != 0 or self.acks.len != 0;
        if (plan == self.isInbox()) return error.InvalidContextDependencyCommand;
        if (self.puts.len + self.deletes.len > max_effects or self.reads.len + self.scans.len > max_reads or self.inbox.len > max_inbox_notices)
            return error.InvalidContextDependencyCommand;
        for (self.puts) |p| if (!isHex(p.key) or !isHex(p.value) or p.key.len == 0) return error.InvalidContextDependencyCommand;
        for (self.deletes) |k| if (!isHex(k) or k.len == 0) return error.InvalidContextDependencyCommand;
        for (self.reads) |r| if (!isHex(r.key) or (r.digest.len != 0 and r.digest.len != 64) or !isHex(r.digest)) return error.InvalidContextDependencyCommand;
        for (self.scans) |s| if (!isHex(s.lower) or !isHex(s.upper) or s.digest.len != 64 or !isHex(s.digest)) return error.InvalidContextDependencyCommand;
        if (self.acks.len > max_effects) return error.InvalidContextDependencyCommand;
        for (self.acks) |d| if (d.len != 64 or !isHex(d)) return error.InvalidContextDependencyCommand;
        if (self.rebuilds.len > max_effects or (self.rebuilds.len != 0 and self.isInbox())) return error.InvalidContextDependencyCommand;
        for (self.rebuilds) |r| if (!isHex(r.artifact) or !isHex(r.doc_key) or r.artifact.len == 0 or r.doc_key.len == 0) return error.InvalidContextDependencyCommand;
    }

    /// Identity recorded after a successful apply.
    pub fn digest(self: Command) Digest {
        var h = Sha256.init(.{});
        h.update("antfly.context_dependency.command.v1");
        for (self.puts) |p| {
            field(&h, p.key);
            field(&h, p.value);
        }
        field(&h, "|");
        for (self.deletes) |k| field(&h, k);
        field(&h, "|");
        for (self.reads) |r| {
            field(&h, r.key);
            field(&h, r.digest);
        }
        field(&h, "|");
        for (self.scans) |s| {
            field(&h, s.lower);
            field(&h, s.upper);
            field(&h, s.digest);
        }
        field(&h, "|");
        for (self.inbox) |n| field(&h, n);
        field(&h, "|");
        for (self.acks) |d| field(&h, d);
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }
};

fn field(h: *Sha256, bytes: []const u8) void {
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, bytes.len, .little);
    h.update(&len);
    h.update(bytes);
}

fn isHex(s: []const u8) bool {
    if (s.len % 2 != 0) return false;
    for (s) |c| if (!std.ascii.isHex(c)) return false;
    return true;
}

fn hexAlloc(a: Allocator, bytes: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{x}", .{bytes});
}

/// `refs` (any slice whose items have `artifact_name` and `doc_key`) as
/// command rebuilds, allocated in `a`.
pub fn rebuildsAlloc(a: Allocator, refs: anytype) ![]const Command.Rebuild {
    if (refs.len > max_effects) return error.ContextBudgetExceeded;
    const out = try a.alloc(Command.Rebuild, refs.len);
    for (refs, out) |r, *o| o.* = .{ .artifact = try hexAlloc(a, r.artifact_name), .doc_key = try hexAlloc(a, r.doc_key) };
    return out;
}

/// The decoded artifact name and document key of `rebuild`, in `a`.
pub fn decodeRebuildAlloc(a: Allocator, rebuild: Command.Rebuild) !struct { artifact: []u8, doc_key: []u8 } {
    return .{ .artifact = try unhexAlloc(a, rebuild.artifact), .doc_key = try unhexAlloc(a, rebuild.doc_key) };
}

fn unhexAlloc(a: Allocator, hex: []const u8) ![]u8 {
    const out = try a.alloc(u8, hex.len / 2);
    _ = std.fmt.hexToBytes(out, hex) catch return error.InvalidContextDependencyCommand;
    return out;
}

pub fn ackDigestHexAlloc(a: Allocator, notice: []const u8) ![]u8 {
    return hexAlloc(a, &valueDigest(notice));
}

fn valueDigest(value: []const u8) Digest {
    var out: Digest = undefined;
    Sha256.hash(value, &out, .{});
    return out;
}

/// Wraps a planning reader and records its read set.
pub const Recorder = struct {
    arena: std.heap.ArenaAllocator,
    inner: store.Reader,
    reads: std.ArrayListUnmanaged(Command.Read) = .empty,
    scans: std.ArrayListUnmanaged(Command.Scan) = .empty,

    pub fn init(alloc: Allocator, inner: store.Reader) Recorder {
        return .{ .arena = std.heap.ArenaAllocator.init(alloc), .inner = inner };
    }

    pub fn deinit(self: *Recorder) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn reader(self: *Recorder) store.Reader {
        return .{ .ptr = self, .get_fn = getImpl, .scan_fn = scanImpl };
    }

    fn getImpl(p: *anyopaque, alloc: Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *Recorder = @ptrCast(@alignCast(p));
        const value = try self.inner.get(alloc, key);
        const a = self.arena.allocator();
        try self.reads.append(a, .{
            .key = try hexAlloc(a, key),
            .digest = if (value) |v| try hexAlloc(a, &valueDigest(v)) else "",
        });
        return value;
    }

    const ScanState = struct {
        visitor: store.Reader.Visitor,
        hash: Sha256,
        last: ?[]u8 = null,
        stopped: bool = false,
        alloc: Allocator,

        fn visit(p: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
            const state: *ScanState = @ptrCast(@alignCast(p));
            field(&state.hash, key);
            field(&state.hash, value);
            if (state.last) |old| state.alloc.free(old);
            state.last = try state.alloc.dupe(u8, key);
            const more = try state.visitor.visit_fn(state.visitor.ptr, key, value);
            if (!more) state.stopped = true;
            return more;
        }
    };

    fn scanImpl(p: *anyopaque, lower: []const u8, upper: []const u8, visitor: store.Reader.Visitor) anyerror!void {
        const self: *Recorder = @ptrCast(@alignCast(p));
        const a = self.arena.allocator();
        var state = ScanState{ .visitor = visitor, .hash = Sha256.init(.{}), .alloc = a };
        try self.inner.scan(lower, upper, .{ .ptr = &state, .visit_fn = ScanState.visit });
        // A visitor that stopped early depends only on the entries it saw:
        // the validated range ends just after the last of them.
        const effective_upper = if (state.stopped) try std.mem.concat(a, u8, &.{ state.last.?, &.{0} }) else upper;
        var d: Digest = undefined;
        state.hash.final(&d);
        try self.scans.append(a, .{ .lower = try hexAlloc(a, lower), .upper = try hexAlloc(a, effective_upper), .digest = try hexAlloc(a, &d) });
    }

    /// The proposal for `plan` against the recorded read set (borrows the
    /// recorder's arena; encode before deinit).
    pub fn commandAlloc(self: *Recorder, plan: *const store.Plan) !Command {
        const a = self.arena.allocator();
        if (plan.puts.items.len + plan.deletes.items.len > max_effects) return error.ContextBudgetExceeded;
        const puts = try a.alloc(Command.Put, plan.puts.items.len);
        for (plan.puts.items, puts) |e, *o| o.* = .{ .key = try hexAlloc(a, e.key), .value = try hexAlloc(a, e.value) };
        const deletes = try a.alloc([]const u8, plan.deletes.items.len);
        for (plan.deletes.items, deletes) |k, *o| o.* = try hexAlloc(a, k);
        return .{ .puts = puts, .deletes = deletes, .reads = self.reads.items, .scans = self.scans.items };
    }
};

pub fn inboxKeyAlloc(alloc: Allocator, notice: []const u8) ![]u8 {
    const prefix = store.familyPrefix(inbox_family);
    const d = valueDigest(notice);
    return std.mem.concat(alloc, u8, &.{ &prefix, &d });
}

pub const Outcome = enum { applied, conflict };

/// Validate and apply a command inside a write transaction (`get`, `put`,
/// `delete`, `openCursor`). Deterministic: a conflicting read set writes
/// nothing.
pub fn applyTxn(alloc: Allocator, txn: anytype, command: Command) !Outcome {
    try command.validate();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (command.isInbox()) {
        for (command.inbox) |notice| {
            const key = try inboxKeyAlloc(a, notice);
            try txn.put(key, notice);
        }
        return .applied;
    }
    for (command.reads) |r| {
        const key = try unhexAlloc(a, r.key);
        const current: ?[]const u8 = txn.get(key) catch |err| blk: {
            if (@as(anyerror, err) != error.NotFound) return err;
            break :blk null;
        };
        const expected_absent = r.digest.len == 0;
        if (current == null) {
            if (!expected_absent) return .conflict;
            continue;
        }
        if (expected_absent) return .conflict;
        const hex = try hexAlloc(a, &valueDigest(current.?));
        if (!std.mem.eql(u8, hex, r.digest)) return .conflict;
    }
    for (command.scans) |s| {
        const lower = try unhexAlloc(a, s.lower);
        const upper = try unhexAlloc(a, s.upper);
        var h = Sha256.init(.{});
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(lower);
        while (entry) |e| {
            if (!std.mem.lessThan(u8, e.key, upper)) break;
            field(&h, e.key);
            field(&h, e.value);
            entry = try cursor.next();
        }
        var d: Digest = undefined;
        h.final(&d);
        if (!std.mem.eql(u8, try hexAlloc(a, &d), s.digest)) return .conflict;
    }
    for (command.deletes) |hex| {
        const key = try unhexAlloc(a, hex);
        txn.delete(key) catch |err| {
            if (@as(anyerror, err) != error.NotFound) return err;
        };
    }
    for (command.puts) |p| try txn.put(try unhexAlloc(a, p.key), try unhexAlloc(a, p.value));
    if (command.acks.len != 0) {
        const prefix = store.familyPrefix(outbox_family);
        var doomed = std.ArrayListUnmanaged([]u8).empty;
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(&prefix);
            while (entry) |e| {
                if (!std.mem.startsWith(u8, e.key, &prefix)) break;
                const hex = try hexAlloc(a, &valueDigest(e.value));
                for (command.acks) |ack| if (std.mem.eql(u8, ack, hex)) {
                    try doomed.append(a, try a.dupe(u8, e.key));
                    break;
                };
                entry = try cursor.next();
            }
        }
        for (doomed.items) |key| txn.delete(key) catch |err| {
            if (@as(anyerror, err) != error.NotFound) return err;
        };
    }
    const d = command.digest();
    try txn.put(&last_applied_key, &d);
    return .applied;
}

/// Whether `command` was the last plan applied (read after the proposal
/// committed on the leader).
pub fn wasApplied(alloc: Allocator, reader: store.Reader, command: Command) !bool {
    const value = (try reader.get(alloc, &last_applied_key)) orelse return false;
    defer alloc.free(value);
    return std.mem.eql(u8, value, &command.digest());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const MemTxn = struct {
    s: *store.MemoryStore,
    fn get(self: *MemTxn, key: []const u8) ![]const u8 {
        for (self.s.map.items) |e| if (std.mem.eql(u8, e.key, key)) return e.value;
        return error.NotFound;
    }
    fn put(self: *MemTxn, key: []const u8, value: []const u8) !void {
        var plan = store.Plan.init(self.s.alloc);
        defer plan.deinit();
        try plan.put(key, value);
        try self.s.apply(&plan);
    }
    fn delete(self: *MemTxn, key: []const u8) !void {
        var plan = store.Plan.init(self.s.alloc);
        defer plan.deinit();
        try plan.delete(key);
        try self.s.apply(&plan);
    }
    const Entry = struct { key: []const u8, value: []const u8 };
    const Cursor = struct {
        s: *store.MemoryStore,
        i: usize = 0,
        fn seekAtOrAfter(self: *Cursor, lower: []const u8) !?Entry {
            while (self.i < self.s.map.items.len and std.mem.lessThan(u8, self.s.map.items[self.i].key, lower)) self.i += 1;
            return self.current();
        }
        fn next(self: *Cursor) !?Entry {
            self.i += 1;
            return self.current();
        }
        fn current(self: *Cursor) ?Entry {
            if (self.i >= self.s.map.items.len) return null;
            const e = self.s.map.items[self.i];
            return .{ .key = e.key, .value = e.value };
        }
        fn close(_: *Cursor) void {}
    };
    fn openCursor(self: *MemTxn) !Cursor {
        return .{ .s = self.s };
    }
};

fn memGet(s: *store.MemoryStore, key: []const u8) ?[]const u8 {
    for (s.map.items) |e| if (std.mem.eql(u8, e.key, key)) return e.value;
    return null;
}

test "replicas apply leader plans identically and reject stale read sets" {
    const alloc = testing.allocator;
    var leader = store.MemoryStore{ .alloc = alloc };
    defer leader.deinit();
    var follower = store.MemoryStore{ .alloc = alloc };
    defer follower.deinit();
    for ([_]*store.MemoryStore{ &leader, &follower }) |s| {
        var seed = store.Plan.init(alloc);
        defer seed.deinit();
        try seed.put("a", "1");
        try seed.put("b1", "x");
        try seed.put("b2", "y");
        try s.apply(&seed);
    }
    // Plan on the leader: read `a`, scan `b*` (stopping after one entry).
    var recorder = Recorder.init(alloc, leader.reader());
    defer recorder.deinit();
    const r = recorder.reader();
    const a_value = (try r.get(alloc, "a")).?;
    alloc.free(a_value);
    const First = struct {
        fn visit(_: *anyopaque, _: []const u8, _: []const u8) anyerror!bool {
            return false;
        }
    };
    var dummy: u8 = 0;
    try r.scan("b", "c", .{ .ptr = &dummy, .visit_fn = First.visit });
    var plan = store.Plan.init(alloc);
    defer plan.deinit();
    try plan.put("z", "planned");
    try plan.delete("b1");
    const command = try recorder.commandAlloc(&plan);
    try command.validate();
    // A later change outside the read set (b2, after the early stop) does
    // not conflict; both replicas apply identically.
    for ([_]*store.MemoryStore{ &leader, &follower }) |s| {
        var change = store.Plan.init(alloc);
        defer change.deinit();
        try change.put("b2", "changed");
        try s.apply(&change);
        var txn = MemTxn{ .s = s };
        try testing.expectEqual(Outcome.applied, try applyTxn(alloc, &txn, command));
        try testing.expect(try wasApplied(alloc, s.reader(), command));
        try testing.expectEqualStrings("planned", memGet(s, "z").?);
        try testing.expect(memGet(s, "b1") == null);
    }
    // Same plan again: `b1` is gone, so the scanned range no longer matches.
    var again = MemTxn{ .s = &follower };
    try testing.expectEqual(Outcome.conflict, try applyTxn(alloc, &again, command));
    // Inbox appends are keyed by notice digest (redelivery is idempotent).
    const inbox = Command{ .inbox = &.{ "n1", "n1", "n2" } };
    try inbox.validate();
    try testing.expectEqual(Outcome.applied, try applyTxn(alloc, &again, inbox));
    try testing.expectEqual(@as(usize, 2), follower.countPrefix(&store.familyPrefix(inbox_family)));
    try testing.expectError(error.InvalidContextDependencyCommand, (Command{}).validate());
}

test "context dependency commands carry hex rebuilds on plans only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Ref = struct { artifact_name: []const u8, doc_key: []const u8 };
    const rebuilds = try rebuildsAlloc(a, @as([]const Ref, &.{.{ .artifact_name = "brief", .doc_key = "person:\x001" }}));
    const plan: Command = .{ .deletes = &.{"aa"}, .rebuilds = rebuilds };
    try plan.validate();
    const decoded = try decodeRebuildAlloc(a, plan.rebuilds[0]);
    try std.testing.expectEqualStrings("brief", decoded.artifact);
    try std.testing.expectEqualStrings("person:\x001", decoded.doc_key);
    // Survives the JSON wire the host proposes.
    const encoded = try std.json.Stringify.valueAlloc(a, plan, .{});
    const parsed = try std.json.parseFromSliceLeaky(Command, a, encoded, .{});
    try std.testing.expectEqualStrings(rebuilds[0].doc_key, parsed.rebuilds[0].doc_key);
    // Inbox appends never journal; rebuilds must be hex.
    try std.testing.expectError(error.InvalidContextDependencyCommand, (Command{ .inbox = &.{"n"}, .rebuilds = rebuilds }).validate());
    try std.testing.expectError(error.InvalidContextDependencyCommand, (Command{ .deletes = &.{"aa"}, .rebuilds = &.{.{ .artifact = "zz", .doc_key = "00" }} }).validate());
}
