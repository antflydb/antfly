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
    inline_read_set: ?InlineReadSet = null,
};
pub const InlineReadSet = struct {
    keys: []const []const u8,
    prefix: ?[]const u8 = null,

    fn validate(self: InlineReadSet) !void {
        if (self.keys.len > 8) return error.InvalidCoordinatorReadSet;
        for (self.keys, 0..) |key, i| {
            if (key.len == 0 or key.len > 1024 or !std.unicode.utf8ValidateSlice(key)) return error.InvalidCoordinatorReadSet;
            for (self.keys[0..i]) |previous| if (std.mem.eql(u8, previous, key)) return error.InvalidCoordinatorReadSet;
        }
        if (self.prefix) |prefix| if (prefix.len == 0 or prefix.len > 1024 or !std.unicode.utf8ValidateSlice(prefix)) return error.InvalidCoordinatorReadSet;
    }
};
pub const WalDependency = struct {
    key: []const u8,
    value: ?[]const u8 = null,
    /// Preparation can depend on a publication/floor value without depending
    /// on lease-only ETag/header changes. The WAL identity remains an exact CAS.
    comparison: enum { exact, record_body } = .exact,
};
pub const WalCommit = struct {
    dependencies: []const WalDependency = &.{},
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
// Cached rows remain in the ordered tree. Native commits update both copies
// atomically; absence is represented explicitly. The selected prefix record
// accelerates a changing pin key without claiming absence for other pins.
const InlineRows = struct { exact: []HotRecord = &.{}, prefix: ?[]const u8 = null, selected: ?HotRecord = null };
const ReadRoot = struct { format: u8 = 3, metadata_tree: ?tree.Ref = null, wal: ?HotRecord = null, publication: ?HotRecord = null, read_rows: InlineRows };
const Row = struct { key: []const u8, value: []const u8 };
const Root = struct {
    page: ?tree.Ref = null,
    hot: ?HotRoot = null,
    read_rows: ?InlineRows = null,
    fn load(a: A, bytes: []const u8) !Root {
        if (bytes.len > journal.max_root_bytes) return error.InvalidCoordinatorRoot;
        if (bytes.len == 0) return .{};
        const value = (try std.json.parseFromSlice(std.json.Value, a, bytes, .{})).value;
        if (value == .object and value.object.contains("format")) {
            const format = value.object.get("format").?;
            if (format == .integer and format.integer == 3) {
                const current = (try std.json.parseFromValue(ReadRoot, a, value, .{})).value;
                if (current.metadata_tree) |ref| try ref.validate();
                if ((current.wal == null) != (current.publication == null) or current.read_rows.exact.len > 8) return error.InvalidCoordinatorRoot;
                var hot: ?HotRoot = null;
                if (current.wal) |wal| {
                    if (wal.key.len == 0 or current.publication.?.key.len == 0 or std.mem.eql(u8, wal.key, current.publication.?.key)) return error.InvalidCoordinatorRoot;
                    hot = .{ .metadata_tree = current.metadata_tree, .wal = wal, .publication = current.publication.? };
                }
                for (current.read_rows.exact, 0..) |record, i| {
                    try validateInlineRecord(record);
                    for (current.read_rows.exact[0..i]) |prior| if (std.mem.eql(u8, prior.key, record.key)) return error.InvalidCoordinatorRoot;
                }
                if (current.read_rows.prefix) |prefix| {
                    if (prefix.len == 0 or prefix.len > 1024 or !std.unicode.utf8ValidateSlice(prefix)) return error.InvalidCoordinatorRoot;
                    if (current.read_rows.selected) |record| {
                        try validateInlineRecord(record);
                        if (!std.mem.startsWith(u8, record.key, prefix)) return error.InvalidCoordinatorRoot;
                    }
                } else if (current.read_rows.selected != null) return error.InvalidCoordinatorRoot;
                return .{ .page = current.metadata_tree, .hot = hot, .read_rows = current.read_rows };
            }
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
        if (self.read_rows) |rows| {
            for (rows.exact) |record| if (std.mem.eql(u8, key, record.key)) return if (record.value) |value| try a.dupe(u8, value) else null;
            if (rows.selected) |record| if (std.mem.eql(u8, key, record.key)) return if (record.value) |value| try a.dupe(u8, value) else null;
        }
        const values = try tree.lookupMany(scratch, store, self.page, &.{key});
        defer {
            for (values) |value| if (value) |entry| entry.deinit(scratch);
            scratch.free(values);
        }
        return if (values[0]) |entry| try a.dupe(u8, entry.value) else null;
    }
    fn encode(self: Root, a: A) ![]u8 {
        if (self.read_rows) |rows| {
            var wal: ?HotRecord = if (self.hot) |hot| hot.wal else null;
            if (wal) |*record| if (record.value) |value| {
                record.value = try compactRecord(a, value, true);
            };
            return std.json.Stringify.valueAlloc(a, ReadRoot{ .metadata_tree = self.page, .wal = wal, .publication = if (self.hot) |hot| hot.publication else null, .read_rows = rows }, .{});
        }
        if (self.hot) |value| {
            var hot = value;
            if (hot.wal.value) |record| hot.wal.value = try compactRecord(a, record, true);
            return std.json.Stringify.valueAlloc(a, hot, .{});
        }
        return std.json.Stringify.valueAlloc(a, self.page, .{});
    }
    fn updateInline(self: *Root, key: []const u8, value: ?[]const u8) void {
        if (self.read_rows) |*rows| {
            for (rows.exact) |*record| if (std.mem.eql(u8, key, record.key)) {
                record.value = value;
            };
            if (rows.prefix) |prefix| if (key.len <= 1024 and std.mem.startsWith(u8, key, prefix)) {
                if (value != null or rows.selected != null and std.mem.eql(u8, rows.selected.?.key, key)) rows.selected = .{ .key = key, .value = value };
            };
        }
    }
    fn captureInline(self: *Root, a: A, scratch: A, store: tree.Store, requested: ?InlineReadSet, commands: []const Command) !void {
        const rules = requested orelse return;
        var exact = std.ArrayList(HotRecord).empty;
        for (rules.keys) |key| {
            if (self.hot) |hot| if (std.mem.eql(u8, key, hot.wal.key) or std.mem.eql(u8, key, hot.publication.key)) continue;
            try exact.append(a, .{ .key = key, .value = try self.get(a, scratch, store, key) });
        }
        var selected: ?HotRecord = null;
        if (rules.prefix) |prefix| {
            if (self.read_rows) |prior| if (prior.prefix != null and std.mem.eql(u8, prefix, prior.prefix.?)) {
                selected = prior.selected;
            };
            for (commands) |command| if (command.key.len <= 1024 and std.mem.startsWith(u8, command.key, prefix)) {
                if (command.kind == .put or selected != null and std.mem.eql(u8, command.key, selected.?.key)) selected = .{ .key = command.key, .value = if (command.kind == .put) command.value else null };
            };
        }
        self.read_rows = .{ .exact = exact.items, .prefix = rules.prefix, .selected = selected };
    }
    fn encodeBounded(self: *Root, a: A, state: journal.State) ![]u8 {
        while (true) {
            const encoded = try self.encode(a);
            if (try journal.rootFits(a, state, encoded)) return encoded;
            if (self.read_rows) |*rows| {
                if (rows.exact.len != 0) {
                    var largest: usize = 0;
                    for (rows.exact, 0..) |record, i| if (inlineCharge(record) > inlineCharge(rows.exact[largest])) {
                        largest = i;
                    };
                    const remaining = try a.alloc(HotRecord, rows.exact.len - 1);
                    @memcpy(remaining[0..largest], rows.exact[0..largest]);
                    @memcpy(remaining[largest..], rows.exact[largest + 1 ..]);
                    rows.exact = remaining;
                } else if (rows.selected != null) rows.selected = null else self.read_rows = null;
            } else return error.CoordinatorRootExceeded;
        }
    }
};
fn recordBody(a: A, bytes: []const u8) ![]const u8 {
    const Body = struct { __antfly_bytes: ?[]const u8 = null, __antfly_utf8: ?[]const u8 = null };
    const value = (try std.json.parseFromSlice(struct { body: Body }, a, bytes, .{ .ignore_unknown_fields = true })).value.body;
    if ((value.__antfly_bytes == null) == (value.__antfly_utf8 == null)) return error.InvalidCoordinatorWalCommit;
    return value.__antfly_bytes orelse value.__antfly_utf8.?;
}

fn validateInlineRecord(record: HotRecord) !void {
    if (record.key.len == 0 or record.key.len > 1024 or !std.unicode.utf8ValidateSlice(record.key) or record.value != null and (record.value.?.len > 32768 or !std.unicode.utf8ValidateSlice(record.value.?))) return error.InvalidCoordinatorRoot;
}
fn inlineCharge(record: HotRecord) usize {
    return record.key.len + if (record.value) |value| value.len else @as(usize, 0);
}
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
        if (req.inline_read_set) |rules| try rules.validate();
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
            if (commit.dependencies.len > 4) return error.InvalidCoordinatorWalCommit;
            var dependency_bytes: usize = 0;
            for (commit.dependencies, 0..) |dependency, i| {
                if (dependency.key.len == 0 or dependency.key.len > 1024 or std.mem.eql(u8, dependency.key, commit.wal_key) or std.mem.eql(u8, dependency.key, commit.publication_key)) return error.InvalidCoordinatorWalCommit;
                for (commit.dependencies[0..i]) |prior| if (std.mem.eql(u8, prior.key, dependency.key)) return error.InvalidCoordinatorWalCommit;
                if (dependency.value) |value| {
                    if (value.len > 8192) return error.InvalidCoordinatorWalCommit;
                    dependency_bytes += value.len;
                    if (dependency_bytes > 16384) return error.InvalidCoordinatorWalCommit;
                }
                const current = try root.get(m, self.alloc, cache.store(), dependency.key);
                const same = if ((current == null) != (dependency.value == null)) false else if (current == null) true else switch (dependency.comparison) {
                    .exact => std.mem.eql(u8, current.?, dependency.value.?),
                    .record_body => std.mem.eql(u8, try recordBody(m, current.?), try recordBody(m, dependency.value.?)),
                };
                if (!same) return std.json.Stringify.valueAlloc(self.alloc, .{ .applied = false, .state = state }, .{});
            }
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
            root.updateInline(commit.wal_key, commit.value);
            root.updateInline(commit.publication_key, root.hot.?.publication.value);
            try root.captureInline(m, self.alloc, cache.store(), req.inline_read_set, &.{});
            const encoded = try root.encodeBounded(m, state);
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
            root.updateInline(command.key, value);
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
        try root.captureInline(m, self.alloc, cache.store(), req.inline_read_set, req.commands);
        const encoded = try root.encodeBounded(m, state);
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

test "object-native inline read capsules preserve ordered authority through updates and pin rotation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const ctx = Context{ .alloc = a, .client = client, .bucket = "fixture", .prefix = "native", .namespace = "inline", .io = std.testing.io };
    const Header = struct { state: journal.State };
    const initial = try ctx.execute(.{ .action = .begin, .owner = @splat(1) });
    defer a.free(initial);
    var state = (try std.json.parseFromSlice(Header, m, initial, .{ .ignore_unknown_fields = true })).value.state;
    const committed = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .inline_read_set = .{ .keys = &.{ "head", "hints", "missing" }, .prefix = "pins/" }, .commands = &.{ .{ .kind = .put, .key = "head", .value = "1" }, .{ .kind = .put, .key = "hints", .value = "proof" }, .{ .kind = .put, .key = "pins/1", .value = "clock-1" } } });
    defer a.free(committed);
    state = (try std.json.parseFromSlice(Header, m, committed, .{ .ignore_unknown_fields = true })).value.state;
    const observed = try ctx.execute(.{ .action = .observe_committed, .owner = @splat(9) });
    defer a.free(observed);
    const cold = (try std.json.parseFromSlice(Header, m, observed, .{ .ignore_unknown_fields = true })).value.state;
    try std.testing.expectEqualStrings(state.root, cold.root);
    const Hook = struct {
        fn deny(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: @import("types.zig").GetOptions) !@import("types.zig").GetResult {
            return error.UnexpectedPhysicalRead;
        }
    };
    var table = client.vtable.*;
    table.get_object = Hook.deny;
    var cached = ctx;
    cached.client.vtable = &table;
    const first = try cached.execute(.{ .action = .read, .owner = cold.owner, .state = cold, .commands = &.{ .{ .kind = .get, .key = "head" }, .{ .kind = .get, .key = "hints" }, .{ .kind = .get, .key = "missing" }, .{ .kind = .get, .key = "pins/1" } } });
    defer a.free(first);
    const before = (try std.json.parseFromSlice(struct { results: [][]Row }, m, first, .{})).value.results;
    try std.testing.expectEqualStrings("1", before[0][0].value);
    try std.testing.expectEqualStrings("proof", before[1][0].value);
    try std.testing.expectEqual(@as(usize, 0), before[2].len);
    try std.testing.expectEqualStrings("clock-1", before[3][0].value);
    // Existing capsules remain coherent even when a caller omits read rules.
    const changed = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .commands = &.{ .{ .kind = .put, .key = "head", .value = "2" }, .{ .kind = .delete, .key = "hints" }, .{ .kind = .put, .key = "pins/2", .value = "clock-2" }, .{ .kind = .put, .key = "cold", .value = "tree-value" } } });
    defer a.free(changed);
    state = (try std.json.parseFromSlice(Header, m, changed, .{ .ignore_unknown_fields = true })).value.state;
    const updated = try cached.execute(.{ .action = .read, .owner = state.owner, .state = state, .commands = &.{ .{ .kind = .get, .key = "head" }, .{ .kind = .get, .key = "hints" }, .{ .kind = .get, .key = "pins/2" } } });
    defer a.free(updated);
    const after = (try std.json.parseFromSlice(struct { results: [][]Row }, m, updated, .{})).value.results;
    try std.testing.expectEqualStrings("2", after[0][0].value);
    try std.testing.expectEqual(@as(usize, 0), after[1].len);
    try std.testing.expectEqualStrings("clock-2", after[2][0].value);
    const listed = try ctx.execute(.{ .action = .read, .owner = state.owner, .state = state, .commands = &.{ .{ .kind = .list, .key = "" }, .{ .kind = .get, .key = "pins/1" } } });
    defer a.free(listed);
    const rows = (try std.json.parseFromSlice(struct { results: [][]Row }, m, listed, .{})).value.results;
    try std.testing.expectEqual(@as(usize, 4), rows[0].len);
    try std.testing.expectEqualStrings("cold", rows[0][0].key);
    try std.testing.expectEqualStrings("clock-1", rows[1][0].value);
    // Deleting a pin ignores its payload in both the tree and its capsule.
    const deleted = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .inline_read_set = .{ .keys = &.{"head"}, .prefix = "pins/" }, .commands = &.{.{ .kind = .delete, .key = "pins/2", .value = "ignored-delete-payload" }} });
    defer a.free(deleted);
    state = (try std.json.parseFromSlice(Header, m, deleted, .{ .ignore_unknown_fields = true })).value.state;
    const absent = try cached.execute(.{ .action = .read, .owner = state.owner, .state = state, .commands = &.{.{ .kind = .get, .key = "pins/2" }} });
    defer a.free(absent);
    try std.testing.expectEqual(@as(usize, 0), (try std.json.parseFromSlice(struct { results: [][]Row }, m, absent, .{})).value.results[0].len);
    // Generic tree keys may exceed the capsule key budget.
    const long_key = try m.alloc(u8, 1100);
    @memset(long_key, 'x');
    @memcpy(long_key[0..5], "pins/");
    const extended = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .inline_read_set = .{ .keys = &.{"head"}, .prefix = "pins/" }, .commands = &.{.{ .kind = .put, .key = long_key, .value = "ordered-long-pin" }} });
    defer a.free(extended);
    state = (try std.json.parseFromSlice(Header, m, extended, .{ .ignore_unknown_fields = true })).value.state;
    const long_pin = try ctx.execute(.{ .action = .read, .owner = state.owner, .state = state, .commands = &.{.{ .kind = .get, .key = long_key }} });
    defer a.free(long_pin);
    try std.testing.expectEqualStrings("ordered-long-pin", (try std.json.parseFromSlice(struct { results: [][]Row }, m, long_pin, .{})).value.results[0][0].value);
}

test "object-native inline read capsule budgets fall back to complete ordered values" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const ctx = Context{ .alloc = a, .client = client, .bucket = "fixture", .prefix = "native", .namespace = "capsule-bound", .io = std.testing.io };
    const Header = struct { state: journal.State, base: []const u8 };
    const initial = try ctx.execute(.{ .action = .begin, .owner = @splat(1) });
    defer a.free(initial);
    const source = (try std.json.parseFromSlice(Header, m, initial, .{ .ignore_unknown_fields = true })).value;
    const large = try m.alloc(u8, 12000);
    @memset(large, '\\');
    const committed = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = source.state, .inline_read_set = .{ .keys = &.{ "large", "small" } }, .commands = &.{ .{ .kind = .put, .key = "large", .value = large }, .{ .kind = .put, .key = "small", .value = "kept" } } });
    defer a.free(committed);
    const state = (try std.json.parseFromSlice(struct { state: journal.State }, m, committed, .{ .ignore_unknown_fields = true })).value.state;
    const root = try Root.load(m, state.root);
    try std.testing.expect(root.read_rows != null);
    try std.testing.expectEqual(@as(usize, 1), root.read_rows.?.exact.len);
    try std.testing.expectEqualStrings("small", root.read_rows.?.exact[0].key);
    const key = try std.fmt.allocPrint(m, "{s}/epochs/{s}/{d:0>20}.json", .{ source.base, std.fmt.bytesToHex(source.state.epoch, .lower), source.state.next });
    var frame = try client.getObject("fixture", key, .{});
    defer frame.deinit(a);
    try std.testing.expect(frame.body.len <= 15360);
    const read = try ctx.execute(.{ .action = .read, .owner = state.owner, .state = state, .commands = &.{ .{ .kind = .get, .key = "large" }, .{ .kind = .get, .key = "small" } } });
    defer a.free(read);
    const rows = (try std.json.parseFromSlice(struct { results: [][]Row }, m, read, .{})).value.results;
    try std.testing.expectEqualStrings(large, rows[0][0].value);
    try std.testing.expectEqualStrings("kept", rows[1][0].value);
    // The new bounded root carries useful hints larger than the legacy 4-KiB
    // state limit, while every log record remains below sixteen KiB.
    const hint = try m.alloc(u8, 5000);
    @memset(hint, 'x');
    const enlarged = try ctx.execute(.{ .action = .commit, .owner = @splat(1), .state = state, .inline_read_set = .{ .keys = &.{ "hint", "small" } }, .commands = &.{.{ .kind = .put, .key = "hint", .value = hint }} });
    defer a.free(enlarged);
    const updated = (try std.json.parseFromSlice(struct { state: journal.State }, m, enlarged, .{ .ignore_unknown_fields = true })).value.state;
    try std.testing.expect(updated.root.len > 4096 and updated.root.len <= journal.max_root_bytes);
    const observed = try ctx.execute(.{ .action = .observe_committed, .owner = @splat(9) });
    defer a.free(observed);
    const recovered = (try std.json.parseFromSlice(struct { state: journal.State }, m, observed, .{ .ignore_unknown_fields = true })).value.state;
    try std.testing.expectEqualStrings(updated.root, recovered.root);
}

test "object-native inline read capsules reject ambiguous declarations and malformed pins" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    try std.testing.expectError(error.InvalidCoordinatorReadSet, (InlineReadSet{ .keys = &.{ "same", "same" } }).validate());
    try std.testing.expectError(error.InvalidCoordinatorReadSet, (InlineReadSet{ .keys = &.{}, .prefix = "" }).validate());
    try std.testing.expectError(error.InvalidCoordinatorRoot, Root.load(m, "{\"format\":3,\"metadata_tree\":null,\"wal\":null,\"publication\":null,\"read_rows\":{\"exact\":[{\"key\":\"same\"},{\"key\":\"same\"}]}}"));
    try std.testing.expectError(error.InvalidCoordinatorRoot, Root.load(m, "{\"format\":3,\"metadata_tree\":null,\"wal\":null,\"publication\":null,\"read_rows\":{\"exact\":[],\"prefix\":\"pins/\",\"selected\":{\"key\":\"other\",\"value\":\"value\"}}}"));
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
