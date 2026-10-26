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
    action: enum { begin, observe, observe_committed, read, commit, commit_wal, checkpoint, trace, retire_logs },
    owner: [16]u8,
    state: ?journal.State = null,
    commands: []const Command = &.{},
    page: ?tree.Ref = null,
    keys: []const []const u8 = &.{},
    before: []const u8 = "0",
    checkpoint_commits: u8 = 64,
    wal_commit: ?WalCommit = null,
};
pub const WalCommit = struct {
    wal_key: []const u8,
    expected_etag: ?[]const u8 = null,
    value: []const u8,
    publication_key: []const u8,
    watermark: []const u8,
    now_ms: u64,
    id: []const u8,
    migration_required: bool = false,
};
const HotRecord = struct { key: []const u8, value: ?[]const u8 = null };
const HotRoot = struct { format: u8 = 2, metadata_tree: ?tree.Ref = null, wal: HotRecord, publication: HotRecord };
const Row = struct { key: []const u8, value: []const u8 };
const Root = struct {
    page: ?tree.Ref = null,
    hot: ?HotRoot = null,
    fn load(a: A, bytes: []const u8) !Root {
        if (bytes.len == 0) return .{};
        const value = (try std.json.parseFromSlice(std.json.Value, a, bytes, .{})).value;
        if (value == .object and value.object.contains("format")) {
            const hot = (try std.json.parseFromValue(HotRoot, a, value, .{})).value;
            if (hot.format != 2 or hot.wal.key.len == 0 or hot.publication.key.len == 0 or std.mem.eql(u8, hot.wal.key, hot.publication.key)) return error.InvalidCoordinatorRoot;
            if (hot.metadata_tree) |ref| try ref.validate();
            return .{ .page = hot.metadata_tree, .hot = hot };
        }
        return .{ .page = (try std.json.parseFromValue(?tree.Ref, a, value, .{})).value };
    }
    fn get(self: Root, a: A, scratch: A, store: tree.Store, key: []const u8) !?[]const u8 {
        if (self.hot) |hot| {
            if (std.mem.eql(u8, key, hot.wal.key)) return if (hot.wal.value) |value| try compactRecord(a, value, false) else null;
            if (std.mem.eql(u8, key, hot.publication.key)) return hot.publication.value;
        }
        const values = try tree.lookupMany(scratch, store, self.page, &.{key});
        defer {
            for (values) |value| if (value) |entry| entry.deinit(scratch);
            scratch.free(values);
        }
        return if (values[0]) |entry| try a.dupe(u8, entry.value) else null;
    }
    fn encode(self: Root, a: A) ![]u8 {
        if (self.hot) |value| {
            var hot = value;
            if (hot.wal.value) |record| hot.wal.value = try compactRecord(a, record, true);
            return std.json.Stringify.valueAlloc(a, hot, .{});
        }
        return std.json.Stringify.valueAlloc(a, self.page, .{});
    }
};
// The mutable WAL HEAD is UTF-8 JSON. Inline roots keep its bytes as a JSON
// string instead of decimal byte arrays; generic reads restore the exact byte
// representation. Other record fields and non-UTF-8 payloads are unchanged.
fn compactRecord(a: A, value: []const u8, pack: bool) ![]const u8 {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, value, .{});
    var object = parsed.value;
    if (object != .object) return value;
    const body = object.object.getPtr("body") orelse return value;
    if (body.* != .object or body.object.count() != 1) return value;
    var replacement: std.json.ObjectMap = .empty;
    if (pack) {
        const bytes = body.object.get("__antfly_bytes") orelse return value;
        if (bytes != .array) return value;
        const utf8 = try a.alloc(u8, bytes.array.items.len);
        for (bytes.array.items, utf8) |number, *byte| {
            if (number != .integer or number.integer < 0 or number.integer > 255) return value;
            byte.* = @intCast(number.integer);
        }
        if (!std.unicode.utf8ValidateSlice(utf8)) return value;
        try replacement.put(a, "__antfly_utf8", .{ .string = utf8 });
    } else {
        const utf8 = body.object.get("__antfly_utf8") orelse return value;
        if (utf8 != .string) return error.InvalidCoordinatorRoot;
        var bytes = std.array_list.Managed(std.json.Value).init(a);
        for (utf8.string) |byte| try bytes.append(.{ .integer = byte });
        try replacement.put(a, "__antfly_bytes", .{ .array = bytes });
    }
    body.* = .{ .object = replacement };
    return std.json.Stringify.valueAlloc(a, object, .{});
}
pub const Context = struct {
    alloc: A,
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    io: std.Io,
    cancellation: ?objects.CancellationToken = null,

    pub fn execute(self: Context, req: Request) ![]u8 {
        if (req.checkpoint_commits < 8 or req.checkpoint_commits > 64 or (req.checkpoint_commits & (req.checkpoint_commits - 1)) != 0) return error.InvalidCoordinatorCheckpointCadence;
        var metadata = std.heap.ArenaAllocator.init(self.alloc);
        defer metadata.deinit();
        const m = metadata.allocator();
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.namespace, &digest, .{});
        const base = try std.fmt.allocPrint(m, "{s}/coord-v1/{s}", .{ self.prefix, std.fmt.bytesToHex(digest, .lower) });
        const domain = Pages.domainForNamespace(base);
        const log = journal.Log{ .client = self.client, .bucket = self.bucket, .prefix = base, .owner = req.owner, .io = self.io, .cancellation = self.cancellation, .checkpoint_commits = req.checkpoint_commits };
        if (req.action == .begin or req.action == .observe or req.action == .observe_committed) {
            const recovered = if (req.action == .observe_committed) try log.observeCommitted(m, req.state) else if (req.action == .observe) try log.observe(m, req.state orelse return error.CoordinatorSessionRequired) else try log.recover(m, req.state, req.state == null or !std.mem.eql(u8, &req.state.?.owner, &req.owner));
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
        var uploads = tree.BufferedWrites{ .alloc = self.alloc, .underlying = pages.store() };
        defer uploads.deinit();
        var cache = tree.Cache{ .alloc = self.alloc, .underlying = uploads.store() };
        defer cache.deinit();
        var root = try Root.load(m, state.root);
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
            var output = std.ArrayList([]const Row).empty;
            for (req.commands) |command| {
                if (command.kind == .get) {
                    const value = try root.get(m, self.alloc, cache.store(), command.key);
                    const rows = if (value) |bytes| try m.dupe(Row, &.{.{ .key = command.key, .value = bytes }}) else &.{};
                    try output.append(m, rows);
                } else if (command.kind == .list) {
                    if (command.limit > 128 or command.limit == 0) return error.CoordinatorBatchExceeded;
                    const upper = try upperBound(m, command.key);
                    const lower = if (std.mem.order(u8, command.after, command.key) == .gt) command.after else command.key;
                    var cursor = try tree.Cursor.init(self.alloc, cache.store(), root.page, lower, upper);
                    defer cursor.deinit();
                    var rows = std.ArrayList(Row).empty;
                    while (rows.items.len < command.limit) {
                        const row = (try cursor.next()) orelse break;
                        if (command.after.len != 0 and std.mem.order(u8, row.key, command.after) != .gt) continue;
                        try rows.append(m, .{ .key = try m.dupe(u8, row.key), .value = try m.dupe(u8, row.value) });
                    }
                    if (root.hot) |hot| for ([_]HotRecord{ hot.wal, hot.publication }) |record| {
                        if (record.value) |value| if (std.mem.startsWith(u8, record.key, command.key) and (command.after.len == 0 or std.mem.order(u8, record.key, command.after) == .gt)) {
                            try rows.append(m, .{ .key = record.key, .value = try compactRecord(m, value, false) });
                        };
                    };
                    std.mem.sort(Row, rows.items, {}, struct {
                        fn less(_: void, l: Row, r: Row) bool {
                            return std.mem.lessThan(u8, l.key, r.key);
                        }
                    }.less);
                    try output.append(m, rows.items[0..@min(rows.items.len, command.limit)]);
                } else return error.CoordinatorReadRequired;
            }
            return std.json.Stringify.valueAlloc(self.alloc, .{ .results = output.items }, .{});
        }
        if (req.action == .commit_wal) {
            const commit = req.wal_commit orelse return error.CoordinatorWalCommitRequired;
            if (commit.wal_key.len == 0 or commit.publication_key.len == 0 or std.mem.eql(u8, commit.wal_key, commit.publication_key) or commit.id.len > 64 or commit.value.len > 32768) return error.InvalidCoordinatorWalCommit;
            if (root.hot) |hot| if (!std.mem.eql(u8, hot.wal.key, commit.wal_key) or !std.mem.eql(u8, hot.publication.key, commit.publication_key)) return error.InvalidCoordinatorWalCommit;
            const migration = try root.get(m, self.alloc, cache.store(), "__antfly_r2_import");
            if (migration) |value| {
                const phase = (try std.json.parseFromSlice(struct { phase: []const u8 }, m, value, .{ .ignore_unknown_fields = true })).value.phase;
                if (!std.mem.eql(u8, phase, "active")) return error.CoordinatorMigrationRequired;
            } else if (commit.migration_required) return error.CoordinatorMigrationRequired;
            const previous = try root.get(m, self.alloc, cache.store(), commit.wal_key);
            const previous_etag = if (previous) |value| (try std.json.parseFromSlice(struct { etag: []const u8 }, m, value, .{ .ignore_unknown_fields = true })).value.etag else null;
            const matches = if (commit.expected_etag) |expected| (if (previous_etag) |etag| std.mem.eql(u8, expected, etag) else false) else previous_etag == null;
            if (!matches) return std.json.Stringify.valueAlloc(self.alloc, .{ .applied = false, .state = state }, .{});
            // Reserve derived work in the same authoritative log frame. Later
            // appends update these small records without rewriting the KV tree.
            const Publication = struct { id: ?[]const u8 = null, expires: u64 = 0, last_published: []const u8 = "0" };
            const prior_bytes = try root.get(m, self.alloc, cache.store(), commit.publication_key);
            var publication = if (prior_bytes) |value| (try std.json.parseFromSlice(Publication, m, value, .{})).value else Publication{};
            var enqueue = false;
            if (try std.fmt.parseInt(u64, commit.watermark, 10) > try std.fmt.parseInt(u64, publication.last_published, 10)) {
                if (publication.id == null or publication.expires <= commit.now_ms) {
                    publication.id = commit.id;
                    publication.expires = try std.math.add(u64, commit.now_ms, 120000);
                    enqueue = true;
                }
            }
            if (root.hot == null) {
                var retired = [_]tree.Mutation{ .{ .key = commit.wal_key, .value = null }, .{ .key = commit.publication_key, .value = null } };
                std.mem.sort(tree.Mutation, &retired, {}, struct {
                    fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                        return std.mem.lessThan(u8, l.key, r.key);
                    }
                }.less);
                root.page = try tree.apply(self.alloc, cache.store(), root.page, &retired);
            }
            root.hot = .{ .metadata_tree = root.page, .wal = .{ .key = commit.wal_key, .value = commit.value }, .publication = .{ .key = commit.publication_key, .value = try std.json.Stringify.valueAlloc(m, publication, .{}) } };
            const encoded = try root.encode(m);
            try uploads.flush();
            const next = try log.append(m, state, encoded);
            return std.json.Stringify.valueAlloc(self.alloc, .{ .applied = true, .state = next, .publication = .{ .id = publication.id, .enqueue = enqueue } }, .{});
        }
        if (req.action != .commit) return error.CoordinatorWriteRequired;
        // append authenticates the expected root and fences before/after its
        // immutable conditional create. Recovery was already done at begin.
        var changes = std.ArrayList(tree.Mutation).empty;
        defer changes.deinit(self.alloc);
        var seen = std.StringHashMap(void).init(m);
        for (req.commands) |command| {
            if (!std.unicode.utf8ValidateSlice(command.key) or (command.value != null and !std.unicode.utf8ValidateSlice(command.value.?))) return error.CoordinatorValueEncodingInvalid;
            if (command.key.len == 0 or command.key.len > 16384 or (command.value != null and command.value.?.len > 32768)) return error.CoordinatorValueExceeded;
            if ((try seen.getOrPut(command.key)).found_existing) return error.CoordinatorDuplicateMutation;
            const value = switch (command.kind) {
                .put => command.value orelse return error.CoordinatorValueRequired,
                .delete => null,
                else => return error.CoordinatorWriteRequired,
            };
            if (root.hot) |*hot| {
                if (std.mem.eql(u8, command.key, hot.wal.key)) {
                    hot.wal.value = value;
                    continue;
                }
                if (std.mem.eql(u8, command.key, hot.publication.key)) {
                    hot.publication.value = value;
                    continue;
                }
            }
            try changes.append(self.alloc, .{ .key = command.key, .value = value });
        }
        std.mem.sort(tree.Mutation, changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        if (changes.items.len != 0) root.page = try tree.apply(self.alloc, cache.store(), root.page, changes.items);
        if (root.hot) |*hot| hot.metadata_tree = root.page;
        const encoded = try root.encode(m);
        try uploads.flush();
        const next = try log.append(m, state, encoded);
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

test "object-native buffered pages permit private reads and require explicit publication flush" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages = Pages{ .client = client, .bucket = "db", .prefix = "private", .domain = @splat(1), .attempt = @splat(2), .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var uploads = tree.BufferedWrites{ .alloc = a, .underlying = pages.store() };
    defer uploads.deinit();
    memory.resetOperationCount();
    const root = try tree.apply(a, uploads.store(), null, &.{.{ .key = "key", .value = "private value" }});
    const found = try tree.lookupMany(a, uploads.store(), root, &.{"key"});
    defer {
        for (found) |entry| if (entry) |value| value.deinit(a);
        a.free(found);
    }
    try std.testing.expectEqualStrings("private value", found[0].?.value);
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
    try uploads.flush();
    try std.testing.expect(memory.operationCount() > 0);
    var page = try tree.readPage(a, pages.store(), root.?);
    defer page.deinit(a);
    try std.testing.expectEqualStrings("private value", page.entries[0].value);
    writes = root.?.bytes;
    memory.resetOperationCount();
    const store = pages.store();
    try store.put_many.?(store.ptr, &.{.{ .ref = root.?, .bytes = page.bytes }});
    try std.testing.expectEqual(@as(u64, 0), writes);
    try std.testing.expectEqual(@as(u64, 2), memory.operationCount());
}

test "object-native COW page batches never publish a partially uploaded root" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const Hook = struct {
        var fail: bool = false;
        var largest: usize = 0;
        fn many(raw: *anyopaque, alloc: A, bucket: []const u8, items: []const objects.types.PutObjectRequest) ![]objects.types.PutResult {
            const backend: *objects.MemoryClient = @ptrCast(@alignCast(raw));
            var base = backend.client();
            base.allocator = alloc;
            largest = @max(largest, items.len);
            if (fail) {
                var partial = try base.putObject(bucket, items[0].key, items[0].body, items[0].options);
                partial.deinit(alloc);
                return error.InjectedUploadFailure;
            }
            return base.putObjects(bucket, items);
        }
    };
    var table = client.vtable.*;
    table.put_objects = Hook.many;
    client.vtable = &table;
    const ctx = Context{ .alloc = a, .client = client, .bucket = "db", .prefix = "native", .namespace = "buffered", .io = std.testing.io };
    const Header = struct { state: journal.State };
    const initial = try ctx.execute(.{ .action = .begin, .owner = @splat(1) });
    defer a.free(initial);
    const state = (try std.json.parseFromSlice(Header, m, initial, .{ .ignore_unknown_fields = true })).value.state;
    var keys: [512][16]u8 = undefined;
    var commands: [512]Command = undefined;
    const before: [4096]u8 = @splat('a');
    const after: [4096]u8 = @splat('b');
    for (&keys, &commands, 0..) |*key, *command, i| command.* = .{ .kind = .put, .key = try std.fmt.bufPrint(key, "row/{d:0>4}", .{i}), .value = &before };
    const committed = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &commands });
    defer a.free(committed);
    const snapshot = (try std.json.parseFromSlice(Header, m, committed, .{ .ignore_unknown_fields = true })).value.state;
    var changes: [8]Command = undefined;
    for (&changes, 0..) |*change, i| change.* = .{ .kind = .put, .key = commands[i * 64].key, .value = &after };
    Hook.fail = true;
    defer Hook.fail = false;
    try std.testing.expectError(error.InjectedUploadFailure, ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = snapshot, .commands = &changes }));
    const recovered = try ctx.execute(.{ .action = .begin, .owner = @splat(1), .state = snapshot });
    defer a.free(recovered);
    const prior = (try std.json.parseFromSlice(Header, m, recovered, .{ .ignore_unknown_fields = true })).value.state;
    try std.testing.expectEqual(snapshot.revision, prior.revision);
    Hook.fail = false;
    Hook.largest = 0;
    const applied = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = prior, .commands = &changes });
    defer a.free(applied);
    try std.testing.expect(Hook.largest >= 2 and Hook.largest <= tree.max_write_batch);
    const next = (try std.json.parseFromSlice(Header, m, applied, .{ .ignore_unknown_fields = true })).value.state;
    const result = try ctx.execute(.{ .action = .read, .owner = @splat(1), .state = next, .commands = &.{.{ .kind = .get, .key = commands[0].key }} });
    defer a.free(result);
    const rows = (try std.json.parseFromSlice(struct { results: [][]Row }, m, result, .{})).value.results;
    try std.testing.expectEqualStrings(&after, rows[0][0].value);
}

test "object-native buffered commit allocation failures release private ownership" {
    const Check = struct {
        fn run(alloc: A) !void {
            var memory = objects.MemoryClient.init(std.testing.allocator);
            defer memory.deinit();
            var client = memory.client();
            try client.makeBucket("db");
            const ctx = Context{ .alloc = alloc, .client = client, .bucket = "db", .prefix = "native", .namespace = "buffer-allocation", .io = std.testing.io };
            const initial = try ctx.execute(.{ .action = .begin, .owner = @splat(1) });
            defer alloc.free(initial);
            const header = try std.json.parseFromSlice(struct { state: journal.State }, alloc, initial, .{ .ignore_unknown_fields = true });
            defer header.deinit();
            const result = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = header.value.state, .commands = &.{ .{ .kind = .put, .key = "a", .value = "one" }, .{ .kind = .put, .key = "b", .value = "two" } } });
            defer alloc.free(result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "object-native WAL commit inlines hot records atomically and preserves generic ordered reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const ctx = Context{ .alloc = a, .client = client, .bucket = "fixture", .prefix = "native", .namespace = "hot", .io = std.testing.io };
    const Header = struct { state: journal.State, applied: bool = true };
    const initial = try ctx.execute(.{ .action = .begin, .owner = @splat(1) });
    defer a.free(initial);
    var state = (try std.json.parseFromSlice(Header, m, initial, .{ .ignore_unknown_fields = true })).value.state;
    const generic = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{.{ .kind = .put, .key = "meta", .value = "5" }} });
    defer a.free(generic);
    state = (try std.json.parseFromSlice(Header, m, generic, .{ .ignore_unknown_fields = true })).value.state;
    var commit = WalCommit{ .wal_key = "wal", .value = "{\"etag\":\"one\",\"body\":{\"__antfly_bytes\":[1]}}", .publication_key = "publication", .watermark = "1", .now_ms = 1000, .id = "job" };
    const first = try ctx.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = commit });
    defer a.free(first);
    state = (try std.json.parseFromSlice(Header, m, first, .{ .ignore_unknown_fields = true })).value.state;
    const root = try Root.load(m, state.root);
    try std.testing.expect(root.hot != null);
    commit.expected_etag = "one";
    commit.value = "{\"etag\":\"two\",\"body\":{\"__antfly_bytes\":[2]}}";
    commit.watermark = "2";
    const second = try ctx.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = commit });
    defer a.free(second);
    state = (try std.json.parseFromSlice(Header, m, second, .{ .ignore_unknown_fields = true })).value.state;
    const updated = try Root.load(m, state.root);
    try std.testing.expect(std.meta.eql(root.page, updated.page));
    const read = try ctx.execute(.{ .action = .read, .owner = @splat(1), .state = state, .commands = &.{ .{ .kind = .get, .key = "wal" }, .{ .kind = .list, .key = "", .limit = 2 } } });
    defer a.free(read);
    const rows = (try std.json.parseFromSlice(struct { results: [][]Row }, m, read, .{})).value.results;
    try std.testing.expectEqualStrings(commit.value, rows[0][0].value);
    try std.testing.expectEqualStrings("meta", rows[1][0].key);
    try std.testing.expectEqualStrings("publication", rows[1][1].key);
    const lost = try ctx.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = commit });
    defer a.free(lost);
    const conflict = (try std.json.parseFromSlice(Header, m, lost, .{ .ignore_unknown_fields = true })).value;
    try std.testing.expect(!conflict.applied);
    try std.testing.expectEqual(state.revision, conflict.state.revision);
    const frozen = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{.{ .kind = .put, .key = "__antfly_r2_import", .value = "{\"phase\":\"freeze\"}" }} });
    defer a.free(frozen);
    state = (try std.json.parseFromSlice(Header, m, frozen, .{ .ignore_unknown_fields = true })).value.state;
    commit.expected_etag = "two";
    try std.testing.expectError(error.CoordinatorMigrationRequired, ctx.execute(.{ .action = .commit_wal, .owner = @splat(1), .state = state, .wal_commit = commit }));
}
