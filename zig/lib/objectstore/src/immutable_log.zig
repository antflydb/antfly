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

//! Object-native serial commits, ownership fencing and bounded recovery.
//! Each slot is created once. Seals preserve the last root while moving to a
//! new epoch. Checkpoints are monotonic recovery floors, never per-write HEADs.
const std = @import("std");
const Client = @import("client.zig").Client;
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const State = struct {
    format: u8 = 1,
    epoch: [16]u8 = @splat(0),
    owner: [16]u8 = @splat(0),
    next: u64 = 0,
    revision: u64 = 0,
    root: []const u8 = "",
};
pub const Frame = struct {
    format: u8 = 1,
    kind: enum { commit, seal },
    epoch: [16]u8,
    slot: u64,
    parent_revision: u64,
    state: State,
};
pub const Checkpoint = struct { format: u8 = 1, written_ms: u64, state: State };
pub const Recovery = struct { state: State, has_more: bool = false };
pub const max_root_bytes = 12 * 1024;
// Keep space for epoch rotation, larger counters and checkpoint headers under
// the existing sixteen-KiB record limit. Optional read capsules may be evicted;
// authoritative tree roots and WAL records cannot be silently truncated.
pub fn rootFits(a: A, source: State, root: []const u8) !bool {
    if (root.len > max_root_bytes) return false;
    const next = State{ .epoch = source.epoch, .owner = source.owner, .next = source.next +| 1, .revision = source.revision +| 1, .root = root };
    const bytes = try std.json.Stringify.valueAlloc(a, Frame{ .kind = .commit, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next }, .{});
    defer a.free(bytes);
    return bytes.len <= 15360;
}
pub const Log = struct {
    client: Client,
    bucket: []const u8,
    prefix: []const u8,
    owner: [16]u8,
    io: std.Io,
    cancellation: ?types.CancellationToken = null,
    checkpoint_commits: u64 = epoch_commits,
    const max_steps = 64;
    pub const epoch_commits = 64;

    fn key(self: Log, a: A, epoch: [16]u8, slot: ?u64) ![]u8 {
        return if (slot) |n| std.fmt.allocPrint(a, "{s}/epochs/{s}/{d:0>20}.json", .{ self.prefix, std.fmt.bytesToHex(epoch, .lower), n }) else std.fmt.allocPrint(a, "{s}/epochs/{s}/SEALED", .{ self.prefix, std.fmt.bytesToHex(epoch, .lower) });
    }
    fn read(self: Log, a: A, k: []const u8) !?types.GetResult {
        var client = self.client;
        client.allocator = a;
        var result = client.getObject(self.bucket, k, .{ .skip_metadata_probe = true, .cancellation = self.cancellation }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        if (result.body.len > 16384) {
            result.deinit(a);
            return error.InvalidImmutableLog;
        }
        return result;
    }
    pub fn checkpoint(self: Log, a: A) !?Checkpoint {
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{self.prefix});
        defer a.free(k);
        var value = (try self.read(a, k)) orelse {
            if (try self.read(a, try std.fmt.allocPrint(a, "{s}/CHECKPOINT_REQUIRED", .{self.prefix}))) |_| return error.ImmutableLogCheckpointMissing;
            return null;
        };
        defer value.deinit(a);
        const cp = (try std.json.parseFromSlice(Checkpoint, a, value.body, .{ .allocate = .alloc_always })).value;
        try validate(cp.state);
        if (cp.format != 1) return error.InvalidImmutableLog;
        return cp;
    }
    fn validate(state: State) !void {
        if (state.revision > 9_007_199_254_740_991) return error.ImmutableLogRevisionExceeded;
        if (state.format != 1 or state.root.len > max_root_bytes or state.next > epoch_commits) return error.InvalidImmutableLog;
    }
    fn frame(a: A, bytes: []const u8, epoch: [16]u8, slot: ?u64) !Frame {
        const value = (try std.json.parseFromSlice(Frame, a, bytes, .{ .allocate = .alloc_always })).value;
        try validate(value.state);
        if (value.format != 1 or !std.mem.eql(u8, &value.epoch, &epoch) or (slot != null and value.slot != slot.?) or value.state.revision != try std.math.add(u64, value.parent_revision, 1)) return error.InvalidImmutableLog;
        if (value.kind == .commit) {
            if (!std.mem.eql(u8, &value.state.epoch, &epoch) or value.state.next != try std.math.add(u64, value.slot, 1)) return error.InvalidImmutableLog;
        } else if (std.mem.eql(u8, &value.state.epoch, &epoch) or value.state.next != 0) return error.InvalidImmutableLog;
        return value;
    }
    fn immutable(self: Log, a: A, k: []const u8, bytes: []const u8) !void {
        if (bytes.len > 16384) return error.InvalidImmutableLog;
        var client = self.client;
        client.allocator = a;
        var receipt = client.putObject(self.bucket, k, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed => {
                var existing = (try self.read(a, k)) orelse return error.PreconditionFailed;
                defer existing.deinit(a);
                if (!std.mem.eql(u8, existing.body, bytes)) return error.PreconditionFailed;
                return;
            },
            else => return err,
        };
        receipt.deinit(a);
    }
    fn marker(self: Log, a: A, value: Frame) !void {
        if (value.kind != .seal) return error.InvalidImmutableLog;
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        const k = try self.key(a, value.epoch, null);
        defer a.free(k);
        try self.immutable(a, k, bytes);
    }
    fn same(l: State, r: State) bool {
        return l.format == r.format and std.mem.eql(u8, &l.epoch, &r.epoch) and std.mem.eql(u8, &l.owner, &r.owner) and l.next == r.next and l.revision == r.revision and std.mem.eql(u8, l.root, r.root);
    }
    fn start(self: Log, a: A, state: State) !void {
        const k = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(state.epoch, .lower) });
        const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
        if (try self.read(a, k)) |stored| {
            if (!std.mem.eql(u8, stored.body, bytes)) return error.InvalidImmutableLog;
        } else try self.immutable(a, k, bytes);
    }
    fn authenticate(self: Log, a: A, state: State, cp: ?Checkpoint) !void {
        if (cp) |value| {
            if (value.state.revision > state.revision) return error.ImmutableLogFenced;
            if (value.state.revision == state.revision) {
                if (!same(value.state, state)) return error.InvalidImmutableLog;
                return;
            }
        }
        if (state.revision == 0) {
            if (!same(state, State{})) return error.InvalidImmutableLog;
            return;
        }
        if (state.next == 0) {
            const k = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(state.epoch, .lower) });
            const value = (try self.read(a, k)) orelse return error.InvalidImmutableLog;
            const recorded = (try std.json.parseFromSlice(State, a, value.body, .{ .allocate = .alloc_always })).value;
            if (!same(recorded, state)) return error.InvalidImmutableLog;
        } else {
            const value = (try self.read(a, try self.key(a, state.epoch, state.next - 1))) orelse return error.InvalidImmutableLog;
            const recorded = try frame(a, value.body, state.epoch, state.next - 1);
            if (recorded.kind != .commit or !same(recorded.state, state)) return error.InvalidImmutableLog;
        }
    }
    /// Metadata allocations belong to the caller's bounded request arena.
    pub fn recover(self: Log, a: A, cursor: ?State, takeover: bool) !Recovery {
        return self.recoverMode(a, cursor, takeover, false, false);
    }
    /// Observe an existing owner's committed prefix without repairing markers,
    /// rotating epochs, checkpointing or acquiring ownership.
    pub fn observe(self: Log, a: A, cursor: State) !Recovery {
        return self.recoverMode(a, cursor, false, true, false);
    }
    /// Authenticate the current committed prefix without acquiring an owner.
    /// Unlike an owned read session, this observer may follow valid seals to
    /// another owner. Every cached cursor remains an authenticated hint.
    pub fn observeCommitted(self: Log, a: A, cursor: ?State) !Recovery {
        return self.recoverMode(a, cursor, false, true, true);
    }
    fn recoverMode(self: Log, a: A, cursor: ?State, takeover: bool, read_only: bool, follow_owners: bool) !Recovery {
        var state = cursor orelse State{};
        try validate(state);
        const checkpoint_value = try self.checkpoint(a);
        if (checkpoint_value) |cp| if (cp.state.revision > state.revision) {
            state = cp.state;
        };
        try self.authenticate(a, state, checkpoint_value);
        for (0..max_steps) |_| {
            if (!follow_owners and !takeover and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
            if (self.cancellation) |token| try token.check();
            const sealed_key = try self.key(a, state.epoch, null);
            if (try self.read(a, sealed_key)) |stored| {
                const sealed = try frame(a, stored.body, state.epoch, null);
                if (sealed.kind != .seal or sealed.parent_revision < state.revision) return error.InvalidImmutableLog;
                if (!read_only) try self.start(a, sealed.state);
                state = sealed.state;
                continue;
            }
            const k = try self.key(a, state.epoch, state.next);
            if (try self.read(a, k)) |stored| {
                const item = try frame(a, stored.body, state.epoch, state.next);
                if (item.parent_revision != state.revision) return error.InvalidImmutableLog;
                if (item.kind == .seal) {
                    if (!std.mem.eql(u8, item.state.root, state.root)) return error.InvalidImmutableLog;
                    if (!read_only) {
                        try self.start(a, item.state);
                        try self.marker(a, item);
                    }
                } else if (!std.mem.eql(u8, &item.state.owner, &state.owner)) return error.InvalidImmutableLog;
                state = item.state;
                continue;
            }
            if (!follow_owners and !std.mem.eql(u8, &state.owner, &self.owner)) {
                if (!takeover) return error.ImmutableLogFenced;
                state = self.seal(a, state, self.owner) catch |err| switch (err) {
                    error.PreconditionFailed => continue,
                    else => return err,
                };
                _ = self.writeCheckpoint(a, state, true) catch false;
            }
            if (!read_only and state.next == epoch_commits) state = try self.seal(a, state, self.owner);
            return .{ .state = state };
        }
        if (!follow_owners and !takeover and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
        return .{ .state = state, .has_more = true };
    }
    fn seal(self: Log, a: A, source: State, owner: [16]u8) !State {
        var epoch: [16]u8 = undefined;
        self.io.random(&epoch);
        if (std.mem.eql(u8, &epoch, &source.epoch)) return error.InvalidImmutableLog;
        const next = State{ .epoch = epoch, .owner = owner, .revision = try std.math.add(u64, source.revision, 1), .root = source.root };
        try validate(next);
        const value = Frame{ .kind = .seal, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next };
        const k = try self.key(a, source.epoch, source.next);
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        try self.immutable(a, k, bytes);
        // The accepted seal is authoritative. These two independent immutable
        // recovery hints can upload together; a partial failure is repaired by
        // ordinary recovery from that same seal, never by publishing a new root.
        const start_key = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(next.epoch, .lower) });
        const marker_key = try self.key(a, source.epoch, null);
        const start_bytes = try std.json.Stringify.valueAlloc(a, next, .{});
        const requests = [_]types.PutObjectRequest{
            .{ .key = start_key, .body = start_bytes, .options = .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation } },
            .{ .key = marker_key, .body = bytes, .options = .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation } },
        };
        var client = self.client;
        client.allocator = a;
        const results = client.putObjects(self.bucket, &requests) catch |err| switch (err) {
            error.PreconditionFailed => {
                try self.start(a, next);
                try self.marker(a, value);
                return next;
            },
            else => return err,
        };
        defer {
            for (results) |*result| result.deinit(a);
            a.free(results);
        }
        return next;
    }
    fn unfenced(self: Log, a: A, state: State) !void {
        if (!std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
        // The checkpoint floor also fences deleted old epochs. A delayed PUT
        // may recreate an orphan slot, but can never receive a successful ack.
        try self.authenticate(a, state, try self.checkpoint(a));
        const k = try self.key(a, state.epoch, null);
        if (try self.read(a, k)) |_| return error.ImmutableLogFenced;
    }
    pub fn append(self: Log, a: A, source: State, root: []const u8) !State {
        try validate(source);
        if (self.checkpoint_commits == 0 or epoch_commits % self.checkpoint_commits != 0) return error.InvalidImmutableLog;
        if (source.next >= epoch_commits) return error.ImmutableLogRotationRequired;
        try self.unfenced(a, source);
        const next = State{ .epoch = source.epoch, .owner = source.owner, .next = source.next + 1, .revision = try std.math.add(u64, source.revision, 1), .root = root };
        try validate(next);
        const value = Frame{ .kind = .commit, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next };
        const k = try self.key(a, source.epoch, source.next);
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        try self.immutable(a, k, bytes);
        try self.unfenced(a, next);
        var result = next;
        if (next.next == epoch_commits) result = self.seal(a, next, self.owner) catch next;
        // A hint failure cannot replace a durable commit's receipt.
        // A shorter best-effort recovery floor avoids replaying an entire
        // 64-slot epoch after an idle restart. The one-second overwrite limit
        // still applies, and hint failure never replaces an accepted commit.
        if (result.next % self.checkpoint_commits == 0) _ = self.writeCheckpoint(a, result, false) catch false;
        return result;
    }
    pub fn retirable(self: Log, a: A, keys: []const []const u8, before: u64) ![]const []const u8 {
        if (keys.len > 32) return error.ImmutableLogBatchExceeded;
        const cp = (try self.checkpoint(a)) orelse return error.ImmutableLogCheckpointMissing;
        if (cp.state.revision < before) return error.InvalidImmutableLog;
        const prefix = try std.fmt.allocPrint(a, "{s}/epochs/", .{self.prefix});
        var result = std.ArrayList([]const u8).empty;
        for (keys) |k| {
            if (!std.mem.startsWith(u8, k, prefix)) continue;
            const suffix = k[prefix.len..];
            if (suffix.len < 34 or suffix[32] != '/') continue;
            var epoch: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&epoch, suffix[0..32]) catch continue;
            const name = suffix[33..];
            if (std.mem.eql(u8, name, "START")) {
                const stored = (try self.read(a, k)) orelse continue;
                const start_state = (try std.json.parseFromSlice(State, a, stored.body, .{})).value;
                try validate(start_state);
                if (!std.mem.eql(u8, &start_state.epoch, &epoch) or start_state.next != 0) return error.InvalidImmutableLog;
                if (start_state.revision < before) try result.append(a, k);
                continue;
            }
            const slot: ?u64 = if (std.mem.eql(u8, name, "SEALED")) null else blk: {
                if (name.len != 25 or !std.mem.endsWith(u8, name, ".json")) continue;
                break :blk std.fmt.parseInt(u64, name[0..20], 10) catch continue;
            };
            const stored = (try self.read(a, k)) orelse continue;
            const item = try frame(a, stored.body, epoch, slot);
            if (slot == null and item.kind != .seal) return error.InvalidImmutableLog;
            // The post-PUT checkpoint-floor check prevents old in-flight
            // writers acknowledging a recreated, pruned slot (the ABA case).
            if (item.state.revision < before) try result.append(a, k);
        }
        return result.toOwnedSlice(a);
    }
    fn requireCheckpoint(self: Log, a: A) !void {
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT_REQUIRED", .{self.prefix});
        if (try self.read(a, k)) |value| {
            if (!std.mem.eql(u8, value.body, "1")) return error.InvalidImmutableLog;
        } else try self.immutable(a, k, "1");
    }
    /// At most one successful checkpoint overwrite per second. A forced
    /// checkpoint reports false until its rate window opens; callers retry.
    pub fn writeCheckpoint(self: Log, a: A, state: State, force: bool) !bool {
        try self.unfenced(a, state);
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{self.prefix});
        const previous = try self.read(a, k);
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        if (previous) |value| {
            const old = (try std.json.parseFromSlice(Checkpoint, a, value.body, .{})).value;
            if (old.state.revision >= state.revision) {
                if (force) try self.requireCheckpoint(a);
                return true;
            }
            if (now < old.written_ms +| 1000) return false;
        } else if (!force and state.revision < self.checkpoint_commits) return false;
        const bytes = try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = now, .state = state }, .{});
        if (bytes.len > 16384) return error.InvalidImmutableLog;
        var client = self.client;
        client.allocator = a;
        var receipt = client.compareAndPut(self.bucket, k, bytes, if (previous) |value| value.metadata.writeRevision() orelse return error.ConditionalReadUnsupported else null, .{ .content_type = "application/json", .cancellation = self.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectStoreRateLimited => return false,
            else => return err,
        };
        receipt.deinit(a);
        if (force) try self.requireCheckpoint(a);
        return true;
    }
};

test "immutable log fences old owners and recovers rotated commits from a checkpoint" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("test");
    var log = Log{ .client = client, .bucket = "test", .prefix = "db", .owner = @splat(1), .io = testing.io };
    var current = (try log.recover(a, null, true)).state;
    const stale = current;
    for (0..130) |i| {
        current = try log.append(a, current, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    }
    var recovered = try log.recover(a, null, true);
    while (recovered.has_more) recovered = try log.recover(a, recovered.state, true);
    try testing.expectEqualStrings("root-129", recovered.state.root);
    log.owner = @splat(2);
    const winner = (try log.recover(a, recovered.state, true)).state;
    try testing.expect(std.mem.eql(u8, &winner.owner, &log.owner));
    log.owner = @splat(1);
    try testing.expectError(error.ImmutableLogFenced, log.append(a, stale, "stale"));
    try testing.expectError(error.ImmutableLogFenced, log.recover(a, current, false));
}

test "immutable log rejects forged cached cursors before acknowledging roots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const log = Log{ .client = client, .bucket = "fixture", .prefix = "db", .owner = @splat(1), .io = std.testing.io };
    var state = (try log.recover(a, null, true)).state;
    state = try log.append(a, state, "real-root");
    var forged = state;
    forged.root = "forged-root";
    try std.testing.expectError(error.InvalidImmutableLog, log.recover(a, forged, false));
    try std.testing.expectError(error.InvalidImmutableLog, log.append(a, forged, "unreachable"));
    forged = state;
    forged.epoch = @splat(7);
    forged.next = 0;
    try std.testing.expectError(error.InvalidImmutableLog, log.append(a, forged, "unreachable"));
}

test "immutable log checkpoints short recovery tails without making hint success part of commit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const log = Log{ .client = client, .bucket = "fixture", .prefix = "db", .owner = @splat(1), .io = std.testing.io, .checkpoint_commits = 8 };
    var state = (try log.recover(a, null, true)).state;
    // Open the rate window without sleeping or depending on the test clock.
    var receipt = try client.putObject("fixture", "db/CHECKPOINT", try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = 0, .state = state }, .{}), .{});
    receipt.deinit(std.testing.allocator);
    for (0..8) |i| state = try log.append(a, state, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    try std.testing.expectEqual(state.revision, (try log.checkpoint(a)).?.state.revision);
    state = try log.append(a, state, "root-8");
    memory.resetOperationCount();
    const recovered = try log.recover(a, null, false);
    try std.testing.expectEqualStrings("root-8", recovered.state.root);
    try std.testing.expect(memory.operationCount() <= 8);
    // A throttled hint still returns the immutable durable commit's result.
    const cp = (try log.checkpoint(a)).?;
    receipt = try client.putObject("fixture", "db/CHECKPOINT", try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = std.math.maxInt(u64), .state = cp.state }, .{}), .{});
    receipt.deinit(std.testing.allocator);
    for (9..17) |i| state = try log.append(a, state, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    try std.testing.expectEqual(cp.state.revision, (try log.checkpoint(a)).?.state.revision);
    try std.testing.expectEqualStrings("root-16", (try log.recover(a, null, false)).state.root);
}

test "immutable log cold observers authenticate sealed owners without writing or repairing hints" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const first = Log{ .client = client, .bucket = "db", .prefix = "cold", .owner = @splat(1), .io = std.testing.io };
    var state = (try first.recover(m, null, true)).state;
    state = try first.append(m, state, "committed");
    _ = try first.writeCheckpoint(m, state, true);
    const Hook = struct {
        fn noCheckpoint(raw: *anyopaque, alloc: A, bucket: []const u8, key_: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
            if (std.mem.endsWith(u8, key_, "/CHECKPOINT")) return error.InjectedHintFailure;
            const backend: *@import("memory.zig").MemoryClient = @ptrCast(@alignCast(raw));
            var base = backend.client();
            base.allocator = alloc;
            return base.putObject(bucket, key_, body, opts);
        }
        fn noWrites(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: types.PutOptions) !types.PutResult {
            return error.ReadOnlyObserverWrote;
        }
    };
    var owner_table = client.vtable.*;
    owner_table.put_object = Hook.noCheckpoint;
    var owner_client = client;
    owner_client.vtable = &owner_table;
    const second = Log{ .client = owner_client, .bucket = "db", .prefix = "cold", .owner = @splat(2), .io = std.testing.io };
    const taken = (try second.recover(m, state, true)).state;
    const marker = try std.fmt.allocPrint(m, "cold/epochs/{s}/SEALED", .{std.fmt.bytesToHex(state.epoch, .lower)});
    const start = try std.fmt.allocPrint(m, "cold/epochs/{s}/START", .{std.fmt.bytesToHex(taken.epoch, .lower)});
    try client.deleteObject("db", marker, .{});
    try client.deleteObject("db", start, .{});
    var read_table = client.vtable.*;
    read_table.put_object = Hook.noWrites;
    read_table.put_objects = null;
    var read_client = client;
    read_client.vtable = &read_table;
    const observer = Log{ .client = read_client, .bucket = "db", .prefix = "cold", .owner = @splat(9), .io = std.testing.io };
    const observed = try observer.observeCommitted(m, null);
    try std.testing.expect(!observed.has_more);
    try std.testing.expectEqualStrings("committed", observed.state.root);
    try std.testing.expectEqual(taken.owner, observed.state.owner);
    try std.testing.expectError(error.FileNotFound, client.statObject("db", marker));
    try std.testing.expectError(error.FileNotFound, client.statObject("db", start));
    var forged = state;
    forged.root = "forged";
    try std.testing.expectError(error.InvalidImmutableLog, observer.observeCommitted(m, forged));
    try std.testing.expectError(error.ImmutableLogFenced, first.observe(m, state));
    const repaired = (try second.recover(m, null, true)).state;
    try std.testing.expectEqual(taken.revision, repaired.revision);
    _ = try second.append(m, repaired, "writer-still-owned");
}

test "immutable log partial epoch announcement batches recover from the accepted seal" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const Hook = struct {
        fn partial(raw: *anyopaque, alloc: A, bucket: []const u8, items: []const types.PutObjectRequest) ![]types.PutResult {
            const backend: *@import("memory.zig").MemoryClient = @ptrCast(@alignCast(raw));
            var base = backend.client();
            base.allocator = alloc;
            var first = try base.putObject(bucket, items[0].key, items[0].body, items[0].options);
            first.deinit(alloc);
            return error.InjectedAnnouncementFailure;
        }
    };
    var table = client.vtable.*;
    table.put_objects = Hook.partial;
    client.vtable = &table;
    const log = Log{ .client = client, .bucket = "db", .prefix = "partial", .owner = @splat(1), .io = std.testing.io };
    try std.testing.expectError(error.InjectedAnnouncementFailure, log.recover(m, null, true));
    const observed = try log.observeCommitted(m, null);
    try std.testing.expectEqualStrings("", observed.state.root);
    try std.testing.expectEqual(log.owner, observed.state.owner);
    const repaired = (try log.recover(m, null, true)).state;
    try std.testing.expectEqual(observed.state.revision, repaired.revision);
    _ = try log.append(m, repaired, "recovered");
    try std.testing.expectEqualStrings("recovered", (try log.observeCommitted(m, null)).state.root);
}
