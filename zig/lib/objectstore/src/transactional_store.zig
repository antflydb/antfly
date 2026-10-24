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

//! Bounded transactional ordered metadata over native pages and immutable R2
//! commits. No filesystem emulation, SQL engine or Durable Object storage.
const std = @import("std");
const objects = @import("root.zig");
const journal = objects.immutable_log;
const tree = @import("ordered_tree.zig");
const Pages = @import("ordered_pages.zig").Pages;
const A = std.mem.Allocator;
pub const Command = struct {
    kind: enum { get, list, put, delete },
    key: []const u8,
    value: ?[]const u8 = null,
    after: []const u8 = "",
    limit: usize = 128,
};
pub const Request = struct {
    action: enum { begin, read, commit, checkpoint, trace, retire_logs },
    owner: [16]u8,
    state: ?journal.State = null,
    commands: []const Command = &.{},
    page: ?tree.Ref = null,
    keys: []const []const u8 = &.{},
    before: []const u8 = "0",
};
pub const Context = struct {
    alloc: A,
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    io: std.Io,
    cancellation: ?objects.CancellationToken = null,

    pub fn execute(self: Context, req: Request) ![]u8 {
        var metadata = std.heap.ArenaAllocator.init(self.alloc);
        defer metadata.deinit();
        const m = metadata.allocator();
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.namespace, &digest, .{});
        const base = try std.fmt.allocPrint(m, "{s}/coord-v1/{s}", .{ self.prefix, std.fmt.bytesToHex(digest, .lower) });
        const domain = Pages.domainForNamespace(base);
        const log = journal.Log{ .client = self.client, .bucket = self.bucket, .prefix = base, .owner = req.owner, .io = self.io, .cancellation = self.cancellation };
        if (req.action == .begin) {
            const recovered = try log.recover(m, req.state, req.state == null or !std.mem.eql(u8, &req.state.?.owner, &req.owner));
            return std.json.Stringify.valueAlloc(self.alloc, .{ .state = recovered.state, .has_more = recovered.has_more, .base = base, .page_prefix = try std.fmt.allocPrint(m, "{s}/artifacts/graph/{s}/", .{ base, std.fmt.bytesToHex(domain, .lower) }) }, .{});
        }
        const state = req.state orelse return error.CoordinatorSessionRequired;
        if (!std.mem.eql(u8, &state.owner, &req.owner)) return error.ImmutableLogFenced;
        if (req.action == .retire_logs) return std.json.Stringify.valueAlloc(self.alloc, .{ .keys = try log.retirable(m, req.keys, try std.fmt.parseInt(u64, req.before, 10)) }, .{});
        if (req.action == .checkpoint) return std.json.Stringify.valueAlloc(self.alloc, .{ .checkpointed = try log.writeCheckpoint(m, state, true) }, .{});
        if (req.commands.len > 512) return error.CoordinatorBatchExceeded;
        const page_prefix = try std.fmt.allocPrint(m, "{s}/artifacts/graph/{s}", .{ base, std.fmt.bytesToHex(domain, .lower) });
        var reads: u64 = 16 * 1024 * 1024;
        var writes: u64 = 16 * 1024 * 1024;
        var page_client = self.client;
        page_client.allocator = self.alloc;
        var pages = Pages{ .client = page_client, .bucket = self.bucket, .prefix = page_prefix, .domain = domain, .attempt = Pages.attemptForRevision(self.io, state.revision +| 1), .cancellation = self.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        var cache = tree.Cache{ .alloc = self.alloc, .underlying = pages.store() };
        defer cache.deinit();
        const root = if (state.root.len != 0) (try std.json.parseFromSlice(?tree.Ref, m, state.root, .{})).value else null;
        if (req.action == .trace) {
            const ref = req.page orelse return error.InvalidCollectionTask;
            var page = try tree.readPage(self.alloc, cache.store(), ref);
            defer page.deinit(self.alloc);
            var children = std.ArrayList(tree.Ref).empty;
            defer children.deinit(self.alloc);
            for (page.entries) |entry| if (entry.child) |child| {
                try children.append(self.alloc, child.*);
            };
            return std.json.Stringify.valueAlloc(self.alloc, .{ .children = children.items, .key = try pages.key(m, ref) }, .{});
        }
        if (req.action == .read) {
            const Row = struct { key: []const u8, value: []const u8 };
            var output = std.ArrayList([]const Row).empty;
            for (req.commands) |command| {
                if (command.kind == .get) {
                    const values = try tree.lookupMany(self.alloc, cache.store(), root, &.{command.key});
                    defer {
                        for (values) |value| if (value) |entry| entry.deinit(self.alloc);
                        self.alloc.free(values);
                    }
                    const rows = if (values[0]) |entry| try m.dupe(Row, &.{.{ .key = try m.dupe(u8, entry.key), .value = try m.dupe(u8, entry.value) }}) else &.{};
                    try output.append(m, rows);
                } else if (command.kind == .list) {
                    if (command.limit > 128 or command.limit == 0) return error.CoordinatorBatchExceeded;
                    const upper = try upperBound(m, command.key);
                    const lower = if (std.mem.order(u8, command.after, command.key) == .gt) command.after else command.key;
                    var cursor = try tree.Cursor.init(self.alloc, cache.store(), root, lower, upper);
                    defer cursor.deinit();
                    var rows = std.ArrayList(Row).empty;
                    while (rows.items.len < command.limit) {
                        const row = (try cursor.next()) orelse break;
                        if (command.after.len != 0 and std.mem.order(u8, row.key, command.after) != .gt) continue;
                        try rows.append(m, .{ .key = try m.dupe(u8, row.key), .value = try m.dupe(u8, row.value) });
                    }
                    try output.append(m, rows.items);
                } else return error.CoordinatorReadRequired;
            }
            return std.json.Stringify.valueAlloc(self.alloc, .{ .results = output.items }, .{});
        }
        // append authenticates this exact expected root, races the next-slot
        // conditional create, and checks the uncached floor before its receipt.
        // Recovering the tail again here duplicates remote reads without
        // strengthening that condition; stale attempts publish no visible root.
        const changes = try self.alloc.alloc(tree.Mutation, req.commands.len);
        defer self.alloc.free(changes);
        for (req.commands, changes) |command, *change| {
            if (!std.unicode.utf8ValidateSlice(command.key) or (command.value != null and !std.unicode.utf8ValidateSlice(command.value.?))) return error.CoordinatorValueEncodingInvalid;
            if (command.key.len == 0 or command.key.len > 16384 or (command.value != null and command.value.?.len > 32768)) return error.CoordinatorValueExceeded;
            change.* = .{ .key = command.key, .value = switch (command.kind) {
                .put => command.value orelse return error.CoordinatorValueRequired,
                .delete => null,
                else => return error.CoordinatorWriteRequired,
            } };
        }
        std.mem.sort(tree.Mutation, changes, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        for (changes, 0..) |change, i| if (i != 0 and std.mem.eql(u8, changes[i - 1].key, change.key)) return error.CoordinatorDuplicateMutation;
        const next_root = try tree.apply(self.alloc, cache.store(), root, changes);
        const encoded_root = try std.json.Stringify.valueAlloc(m, next_root, .{});
        const next = try log.append(m, state, encoded_root);
        return std.json.Stringify.valueAlloc(self.alloc, .{ .state = next, .base = base }, .{});
    }
};

fn upperBound(a: A, prefix: []const u8) !?[]u8 {
    var i = prefix.len;
    while (i > 0) {
        i -= 1;
        if (prefix[i] == 255) continue;
        const result = try a.dupe(u8, prefix[0 .. i + 1]);
        result[i] += 1;
        return result;
    }
    return null;
}

test "object-native store read allocation failures release all request ownership" {
    const testing = std.testing;
    const a = testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const context = Context{ .alloc = a, .client = client, .bucket = "fixture", .prefix = "native", .namespace = "allocation", .io = testing.io };
    const initial = try context.execute(.{ .action = .begin, .owner = @splat(1) });
    defer a.free(initial);
    const Header = struct { state: journal.State };
    const parsed = try std.json.parseFromSlice(Header, a, initial, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const committed = try context.execute(.{ .action = .commit, .owner = @splat(1), .state = parsed.value.state, .commands = &.{ .{ .kind = .put, .key = "a", .value = "alpha" }, .{ .kind = .put, .key = "b", .value = "beta" } } });
    defer a.free(committed);
    const snapshot = try std.json.parseFromSlice(Header, a, committed, .{ .ignore_unknown_fields = true });
    defer snapshot.deinit();
    const Check = struct {
        fn run(alloc: A, storage: objects.Client, state: journal.State) !void {
            const db = Context{ .alloc = alloc, .client = storage, .bucket = "fixture", .prefix = "native", .namespace = "allocation", .io = testing.io };
            const result = try db.execute(.{ .action = .read, .owner = @splat(1), .state = state, .commands = &.{ .{ .kind = .get, .key = "a" }, .{ .kind = .list, .key = "", .limit = 2 } } });
            defer alloc.free(result);
            try testing.expect(std.mem.indexOf(u8, result, "alpha") != null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{ client, snapshot.value.state });
}
