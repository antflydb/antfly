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

//! Public-ID ordered seeks over native tuple/file counts and physical rows.
//! A tie group visits only participating files and the requested row window.
//! Snapshot-bound public digests are computed once; retained row pages remain
//! reusable when a new snapshot changes the digest permutation of the files.
const std = @import("std");
const local = @import("antfly_local_sources");
const ordered = @import("lake_index_ordered_rows.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Cursor = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    group_arena: std.heap.ArenaAllocator,
    file_arena: std.heap.ArenaAllocator,
    directory: ?tree.Cursor = null,
    public_files: ?tree.Cursor = null,
    participants: ?*Participants = null,
    participant_sorts: usize = 0,
    participant_spill_files_peak: usize = 0,
    participant_spill_limit: u64 = 1024 * 1024 * 1024,
    public_digest: [32]u8 = undefined,
    group_participants: u64 = 0,
    promotion_probe_limit: u64 = 0,
    probe_byte_floor: u64 = 0,
    probe_start: usize = 0,
    last_probed: ?[32]u8 = null,
    participant_after: ?[32]u8 = null,
    promotion_disabled: bool = false,
    requested_rows: usize = 1,
    /// Diagnostic counts exact membership probes, independent of cache warmth.
    public_probes: usize = 0,
    group_counts: usize = 0,
    pending: ?tree.Cursor.Record = null,
    reader: *ordered.Reader,
    navigation: ?*Navigation = null,
    digests: []const [32]u8,
    group_digests: std.AutoHashMapUnmanaged(u32, [32]u8) = .empty,
    boundary_digest: ?[32]u8 = null,
    lower: []const u8,
    upper: ?[]const u8,
    boundary: ?[]const u8,
    boundary_file: ?u32 = null,
    boundary_coordinate: [12]u8 = @splat(0),
    reverse: bool,
    reverse_ids: bool,
    group: []const u8 = "",
    files: []const u32 = &.{},
    file_position: usize = 0,
    rows: ?tree.Cursor = null,
    exhausted: bool = false,
    /// One statement-owned cache amortizes seeks across distinct tie groups.
    /// It retains immutable pages only, with the original query's read context.
    const Navigation = struct {
        reader: *ordered.Reader,
        context: ?local.serverless_query_lake_read_context.Context,
        cache: tree.Cache,
        fn check(raw: *anyopaque) !void {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            if (owner.context) |context| try context.ensureActive();
            try owner.reader.pages.cancellation.check();
        }
        fn get(raw: *anyopaque, a_: A, ref: tree.Ref) ![]u8 {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            try check(raw);
            const backing = owner.reader.pages.store();
            return backing.get(backing.ptr, a_, ref);
        }
        fn put(_: *anyopaque, _: tree.Ref, _: []const u8) !void {
            return error.InvalidNativeLakeRowIndex;
        }
    };
    pub fn init(a: A, reader: *ordered.Reader, lower: []const u8, upper: ?[]const u8, boundary: ?[]const u8, id: ?[]const u8, id_descending: bool, before: bool) !Cursor {
        var result: Cursor = .{ .a = a, .arena = .init(a), .group_arena = .init(a), .file_arena = .init(a), .reader = reader, .digests = &.{}, .lower = undefined, .upper = null, .boundary = null, .reverse = before, .reverse_ids = id_descending != before };
        errdefer result.deinit();
        const navigation = try a.create(Navigation);
        navigation.* = .{ .reader = reader, .context = if (reader.cached) |cached| cached.context else null, .cache = .{ .alloc = a, .underlying = .{ .domain = reader.root.domain, .ptr = navigation, .get = Navigation.get, .put = Navigation.put, .check = Navigation.check } } };
        result.navigation = navigation;
        const ca = result.arena.allocator();
        result.lower = try ca.dupe(u8, lower);
        result.upper = if (upper) |end| try ca.dupe(u8, end) else null;
        if (reader.root.file_dictionary == null and reader.root.public_digests.len != reader.root.files.len) return error.InvalidNativeLakeRowIndex;
        const digests = reader.root.public_digests;
        result.digests = digests;
        if (boundary) |prefix| {
            const value = id orelse return error.InvalidRelationalIndexBound;
            const identity = local.storage_rowsource_identity;
            if (!identity.isPublic(value)) return error.InvalidRelationalIndexBound;
            const parts = identity.parse(value) catch return error.InvalidRelationalIndexBound;
            identity.validateSnapshot(parts, reader.root.source, reader.root.snapshot) catch return error.InvalidRelationalIndexBound;
            if (parts.version != reader.identityVersion()) return error.InvalidRelationalIndexBound;
            const group = parts.group;
            const row = parts.row;
            var digest_bytes: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest_bytes, parts.digest) catch return error.InvalidRelationalIndexBound;
            result.boundary_file = try reader.publicSlot(ca, digest_bytes);
            result.boundary_digest = digest_bytes;
            if (result.boundary_file == null) return error.InvalidRelationalIndexBound;
            result.boundary = try ca.dupe(u8, prefix);
            std.mem.writeInt(u32, result.boundary_coordinate[0..4], group, .big);
            std.mem.writeInt(u64, result.boundary_coordinate[4..12], row, .big);
            if (before) {
                if (try orderedSuccessor(ca, prefix)) |end| {
                    if (result.upper == null or std.mem.order(u8, end, result.upper.?) == .lt) result.upper = end;
                }
            } else if (std.mem.order(u8, prefix, result.lower) == .gt) result.lower = result.boundary.?;
        }
        if (result.upper) |end| if (std.mem.order(u8, result.lower, end) != .lt) {
            result.exhausted = true;
        };
        if (!result.exhausted) result.directory = if (before)
            try tree.Cursor.initReverse(a, result.navigation.?.cache.store(), reader.root.ties, result.lower, result.upper)
        else
            try tree.Cursor.init(a, result.navigation.?.cache.store(), reader.root.ties, result.lower, result.upper);
        return result;
    }
    pub fn deinit(self: *Cursor) void {
        self.clearParticipants();
        if (self.rows) |*rows| rows.deinit();
        if (self.public_files) |*files| files.deinit();
        self.group_arena.deinit();
        if (self.navigation) |navigation| {
            navigation.cache.deinit();
            self.a.destroy(navigation);
            self.navigation = null;
        }
        self.file_arena.deinit();
        if (self.directory) |*directory| directory.deinit();
        self.arena.deinit();
    }
    fn loadGroup(self: *Cursor) !bool {
        if (self.exhausted) return false;
        self.clearParticipants();
        self.group_participants = 0;
        self.probe_start = self.public_probes;
        self.last_probed = null;
        self.participant_after = null;
        self.promotion_disabled = false;
        _ = self.group_arena.reset(.retain_capacity);
        self.group_digests = .empty;
        const ca = self.group_arena.allocator();
        const first = (self.pending orelse try self.directory.?.next()) orelse {
            self.exhausted = true;
            return false;
        };
        self.pending = null;
        if (first.key.len < 4) return error.InvalidNativeLakeRowIndex;
        self.group = try ca.dupe(u8, first.key[0 .. first.key.len - 4]);
        var cache_key: ?[32]u8 = null;
        if (self.reader.cached) |cached| {
            try cached.context.ensureActive();
            try self.reader.pages.cancellation.check();
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update("antfly.native-public-tie-files.v2");
            hash.update(&cached.scope);
            hash.update(&self.reader.root.domain);
            hash.update(&self.reader.root.fingerprint);
            const root = self.reader.root.ties orelse return error.InvalidNativeLakeRowIndex;
            // JSON includes the complete immutable tree identity, independent of
            // pointer addresses. Length framing keeps each identity component exact.
            // Cached slots are index-local. Equal tuple trees can have different
            // file mappings after independent index histories.
            const identity = try std.json.Stringify.valueAlloc(ca, .{ root, self.reader.root.file_dictionary, self.reader.root.files }, .{});
            for ([_][]const u8{ identity, self.reader.root.source, self.reader.root.snapshot, self.group }) |part| {
                var length: [8]u8 = undefined;
                std.mem.writeInt(u64, &length, part.len, .little);
                hash.update(&length);
                hash.update(part);
            }
            var key: [32]u8 = undefined;
            hash.final(&key);
            cache_key = key;
            if (cached.cache.decoded.lookup(key)) |lease| {
                defer lease.release();
                const value: *const GroupFiles = @ptrCast(@alignCast(lease.item.payload.extension));
                self.files = try ca.dupe(u32, value.files);
                // Only a warm group needs a seek. A cold scan keeps its existing
                // directory pages and the first record of the following group.
                self.directory.?.deinit();
                self.directory = null;
                self.directory = if (self.reverse)
                    try tree.Cursor.initReverse(self.a, self.navigation.?.cache.store(), self.reader.root.ties, self.lower, self.group)
                else if (try orderedSuccessor(ca, self.group)) |end|
                    try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.ties, end, self.upper)
                else blk: {
                    self.exhausted = true;
                    break :blk null;
                };
                try self.prepareDigests(ca);
                self.seekBoundary();
                return true;
            }
        }
        // Intersect large tie groups with the already sorted snapshot file
        // directory. Dense groups can return their first rows without decoding
        // and sorting every participant. Small/sparse groups keep bounded local
        // sorting; estimates choose work only, never establish membership.
        var seed: std.ArrayList(u32) = .empty;
        var following: ?tree.Cursor.Record = first;
        if (self.reader.root.file_dictionary) |dictionary| if (dictionary.public != null and dictionary.count > 64) {
            if (first.value.len != 8) return error.InvalidNativeLakeRowIndex;
            const first_rows = std.mem.readInt(u64, first.value[0..8], .big);
            // Consume only records needed by the ordinary sequential path.
            // Distinct/small groups never pay a separate tree cardinality seek.
            while (following) |record| {
                if (record.key.len < 4) return error.InvalidNativeLakeRowIndex;
                if (record.key.len != self.group.len + 4 or !std.mem.eql(u8, record.key[0..self.group.len], self.group)) {
                    self.pending = record;
                    following = null;
                    break;
                }
                if (record.value.len != 8 or std.mem.readInt(u64, record.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
                const slot = std.mem.readInt(u32, record.key[record.key.len - 4 ..][0..4], .big);
                if (slot >= dictionary.count) return error.InvalidNativeLakeRowIndex;
                try seed.append(ca, slot);
                following = try self.directory.?.next();
                if (seed.items.len == 65) break;
            }
            if (seed.items.len > 64) {
                const end = try orderedSuccessor(ca, self.group);
                const participating = try tree.countRange(self.a, self.navigation.?.cache.store(), self.reader.root.ties, self.group, end);
                self.group_counts += 1;
                const total: u64 = if (dictionary.public_names) dictionary.public_count else dictionary.count;
                const desired_files = @max(1, self.requested_rows / @max(1, first_rows));
                // A global walk is attractive for dense groups/small pages.
                // Sparse wide groups sort only their participants under a spill
                // quota instead of probing unrelated archive files.
                const global_cost = @min(total, total / @max(1, participating) *| @min(desired_files, participating));
                const participant_cost = participating *| 4;
                // Reserve most metadata admission for participant preparation
                // and returned rows. Count a conservative whole routing path
                // for both public navigation and exact membership/name seeks.
                const ties_root = self.reader.root.ties.?;
                const public_root = dictionary.public.?;
                const probe_bytes = (@as(u64, ties_root.height) + 1) * @max(ties_root.bytes, tree.target_page_bytes * 2) +
                    (@as(u64, public_root.height) + 1) * @max(public_root.bytes, tree.target_page_bytes * 2) +
                    (if (dictionary.public_names) (@as(u64, dictionary.names.?.height) + 1) * @max(dictionary.names.?.bytes, tree.target_page_bytes * 2) else 0);
                const byte_probes = @max(1, self.reader.remaining_reads / 4 / probe_bytes);
                self.promotion_probe_limit = @min(participant_cost, byte_probes);
                self.probe_byte_floor = self.reader.remaining_reads - self.reader.remaining_reads / 4;
                if (participating > 4096 and (global_cost >= participant_cost or global_cost > byte_probes)) {
                    if (self.reader.cached) |cached| if (cached.context.io) |io| {
                        const sorted = self.sortParticipants(following, seed, io) catch |err| switch (err) {
                            error.OutOfMemory, error.SqlMemoryLimitExceeded, error.SqlProgramLimitExceeded, error.NoSpaceLeft => false,
                            else => return err,
                        };
                        if (sorted) return true;
                        // Optional spill admission must not reject a query the
                        // lazy directory path can serve. Its group reseek below
                        // also discards any partially consumed participant walk.
                        self.pending = null;
                        self.promotion_disabled = true;
                    };
                }
                if (participating > 4096 or (participating > 64 and participating *| participating >= total *| desired_files)) {
                    self.directory.?.deinit();
                    self.directory = null;
                    self.directory = if (self.reverse)
                        try tree.Cursor.initReverse(self.a, self.navigation.?.cache.store(), self.reader.root.ties, self.lower, self.group)
                    else if (end) |after|
                        try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.ties, after, self.upper)
                    else
                        null;
                    if (self.directory == null) self.exhausted = true;
                    const boundary = self.boundary != null and std.mem.eql(u8, self.group, self.boundary.?);
                    const lower = if (boundary and !self.reverse_ids) &self.boundary_digest.? else "";
                    const upper = if (boundary and self.reverse_ids) try std.mem.concat(ca, u8, &.{ &self.boundary_digest.?, &.{0} }) else null;
                    self.group_participants = participating;
                    self.public_files = if (self.reverse_ids)
                        try tree.Cursor.initReverse(self.a, self.reader.dictionaryStore(), dictionary.public, lower, upper)
                    else
                        try tree.Cursor.init(self.a, self.reader.dictionaryStore(), dictionary.public, lower, upper);
                    self.files = &.{};
                    self.file_position = 0;
                    return true;
                }
            }
        };
        self.files = try self.readGroup(ca, following, seed);
        // Small groups already fit in a directory page: reseeking a cached one
        // would cost more reads than sequential traversal. Reserve residency
        // for groups whose encoded records span pages.
        if (self.files.len > tree.target_page_bytes / (self.group.len + 32)) if (self.reader.cached) |cached| {
            const Loader = struct {
                files: []const u32,
                fn load(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                    const ctx: *@This() = @ptrCast(@alignCast(raw));
                    const alloc = item.arena.allocator();
                    const value = try alloc.create(GroupFiles);
                    value.files = try alloc.dupe(u32, ctx.files);
                    item.payload = .{ .extension = value };
                }
            };
            var loader: Loader = .{ .files = self.files };
            const Check = struct {
                parent: local.serverless_query_lake_read_context.Context,
                token: @import("antfly_cancellation").CancellationToken,
                fn check(raw: *anyopaque) !void {
                    const ctx: *@This() = @ptrCast(@alignCast(raw));
                    try ctx.parent.ensureActive();
                    try ctx.token.check();
                }
            };
            var check: Check = .{ .parent = cached.context, .token = self.reader.pages.cancellation };
            var context = cached.context;
            context.checkpoint = .{ .ptr = &check, .check = Check.check };
            const lease = try cached.cache.decoded.acquire(cache_key.?, 512 * 1024, context, .{ .ptr = &loader, .load = Loader.load });
            lease.release();
        };
        self.seekBoundary();
        return true;
    }
    fn shouldPromote(self: *Cursor) bool {
        return self.public_files != null and !self.promotion_disabled and self.group_participants > 4096 and
            (self.public_probes - self.probe_start >= self.promotion_probe_limit or self.reader.remaining_reads <= self.probe_byte_floor) and
            self.reader.cached != null and self.reader.cached.?.context.io != null;
    }
    /// Start small pages lazily. If continued traversal spends more probes than
    /// participant preparation, spill only the unvisited suffix. No completed
    /// file is revisited and a half-consumed file finishes before switching.
    fn promoteParticipants(self: *Cursor) !bool {
        const saved = self.directory;
        const pending = self.pending;
        const end = try orderedSuccessor(self.group_arena.allocator(), self.group);
        self.directory = try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.ties, self.group, end);
        defer {
            self.directory.?.deinit();
            self.directory = saved;
            self.pending = pending;
        }
        self.participant_after = self.last_probed;
        const first = (try self.directory.?.next()) orelse return error.InvalidNativeLakeRowIndex;
        const sorted = self.sortParticipants(first, .empty, self.reader.cached.?.context.io.?) catch |err| switch (err) {
            error.OutOfMemory, error.SqlMemoryLimitExceeded, error.SqlProgramLimitExceeded, error.NoSpaceLeft => false,
            else => return err,
        };
        if (!sorted) {
            self.promotion_disabled = true;
            self.participant_after = null;
            return false;
        }
        self.public_files.?.deinit();
        self.public_files = null;
        return true;
    }
    const Participants = struct {
        manager: local.sql_spill.Manager,
        sort: local.sql_spill.Sort = undefined,
        orders: [1]local.sql_operators.Order,
        scratch: std.heap.ArenaAllocator,
    };
    fn clearParticipants(self: *Cursor) void {
        if (self.participants) |owner| {
            owner.sort.deinit();
            owner.manager.deinit();
            owner.scratch.deinit();
            self.a.destroy(owner);
            self.participants = null;
        }
    }
    fn participantCheck(raw: *anyopaque) !void {
        const reader: *ordered.Reader = @ptrCast(@alignCast(raw));
        try reader.pages.cancellation.check();
        if (reader.cached) |cached| try cached.context.ensureActive();
    }
    fn addParticipant(self: *Cursor, slot: u32) !void {
        const owner = self.participants.?;
        try participantCheck(self.reader);
        _ = owner.scratch.reset(.retain_capacity);
        const scratch = owner.scratch.allocator();
        const digest = try self.reader.publicDigest(scratch, slot);
        if (self.boundary) |boundary| if (std.mem.eql(u8, self.group, boundary)) {
            if (std.mem.order(u8, &digest, &self.boundary_digest.?) == (if (self.reverse_ids) std.math.Order.gt else .lt)) return;
        };
        if (self.participant_after) |after| {
            if (std.mem.order(u8, &digest, &after) != (if (self.reverse_ids) std.math.Order.lt else .gt)) return;
        }
        var key: [36]u8 = undefined;
        key[0..32].* = digest;
        key[32..36].* = ordered.file_dictionary.slotKey(slot);
        // Native memcomparable spill runs keep arbitrary digest bytes intact.
        // Complementing the key supplies descending order without JSON encoding.
        if (self.reverse_ids) for (&key) |*byte| {
            byte.* = ~byte.*;
        };
        try owner.sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = slot });
    }
    fn sortParticipants(self: *Cursor, following: ?tree.Cursor.Record, seed: std.ArrayList(u32), io: std.Io) !bool {
        const owner = try self.a.create(Participants);
        owner.* = .{ .manager = .{ .alloc = self.a, .io = io, .context = self.reader, .checkpoint = participantCheck, .max_bytes = self.participant_spill_limit }, .orders = .{.{}}, .scratch = .init(self.a) };
        owner.sort = local.sql_spill.Sort.init(self.a, &owner.manager, &owner.orders, 128 * 1024);
        owner.sort.shared_runs = true;
        owner.sort.encoded_bytes = true;
        self.participants = owner;
        errdefer self.clearParticipants();
        for (seed.items) |slot| try self.addParticipant(slot);
        var next_record = following;
        while (next_record) |record| {
            if (record.key.len != self.group.len + 4 or !std.mem.eql(u8, record.key[0..self.group.len], self.group)) {
                self.pending = record;
                break;
            }
            if (record.value.len != 8 or std.mem.readInt(u64, record.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
            try self.addParticipant(std.mem.readInt(u32, record.key[record.key.len - 4 ..][0..4], .big));
            next_record = try self.directory.?.next();
        }
        self.participant_sorts += 1;
        self.participant_spill_files_peak = @max(self.participant_spill_files_peak, owner.manager.admissionStats().peak_files);
        self.files = &.{};
        self.file_position = 0;
        return true;
    }
    const GroupFiles = struct { files: []const u32 };
    fn readGroup(self: *Cursor, a: A, first: ?tree.Cursor.Record, seed: std.ArrayList(u32)) ![]const u32 {
        var files = seed;
        errdefer files.deinit(a);
        var record_next: ?tree.Cursor.Record = first;
        while (record_next) |record| {
            if (files.items.len % 256 == 0) {
                try self.reader.pages.cancellation.check();
                if (self.reader.cached) |cached| try cached.context.ensureActive();
            }
            if (record.key.len < 4) return error.InvalidNativeLakeRowIndex;
            if (record.key.len != self.group.len + 4 or !std.mem.eql(u8, record.key[0..self.group.len], self.group)) {
                self.pending = record;
                break;
            }
            if (record.value.len != 8 or std.mem.readInt(u64, record.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
            const slot = std.mem.readInt(u32, record.key[record.key.len - 4 ..][0..4], .big);
            if (slot >= self.reader.root.fileCount() or files.items.len >= self.reader.root.fileCount()) return error.InvalidNativeLakeRowIndex;
            try files.append(a, slot);
            record_next = try self.directory.?.next();
        }
        self.files = files.items;
        try self.prepareDigests(a);
        const Order = struct {
            digests: *const std.AutoHashMapUnmanaged(u32, [32]u8),
            fn less(order: @This(), x: u32, y: u32) bool {
                return std.mem.order(u8, &order.digests.get(x).?, &order.digests.get(y).?) == .lt;
            }
        };
        std.mem.sort(u32, files.items, Order{ .digests = &self.group_digests }, Order.less);
        return files.toOwnedSlice(a);
    }
    fn prepareDigests(self: *Cursor, a: A) !void {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (self.files) |slot| {
            _ = scratch.reset(.retain_capacity);
            try self.group_digests.put(a, slot, try self.reader.publicDigest(scratch.allocator(), slot));
        }
    }
    fn seekBoundary(self: *Cursor) void {
        self.file_position = 0;
        const boundary = self.boundary orelse return;
        if (!std.mem.eql(u8, self.group, boundary)) return;
        const digest = self.boundary_digest.?;
        var lo: usize = 0;
        var hi = self.files.len;
        // Lower bound in traversal order, including the boundary file itself:
        // its row-coordinate seek decides which rows remain admissible.
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const pos = if (self.reverse_ids) self.files.len - mid - 1 else mid;
            const relation = std.mem.order(u8, &self.group_digests.get(self.files[pos]).?, &digest);
            if (relation == (if (self.reverse_ids) std.math.Order.gt else .lt)) lo = mid + 1 else hi = mid;
        }
        self.file_position = lo;
    }
    fn openFile(self: *Cursor, slot: u32) !bool {
        _ = self.file_arena.reset(.retain_capacity);
        const ca = self.file_arena.allocator();
        var file: [4]u8 = undefined;
        std.mem.writeInt(u32, &file, slot, .big);
        var lower: []const u8 = try std.mem.concat(ca, u8, &.{ self.group, &file });
        var upper = try orderedSuccessor(ca, lower);
        if (self.boundary) |boundary| if (std.mem.eql(u8, self.group, boundary)) {
            const digest = if (self.public_files != null or self.participants != null) self.public_digest else self.group_digests.get(slot).?;
            const relation = std.mem.order(u8, &digest, &self.boundary_digest.?);
            if (relation == (if (self.reverse_ids) std.math.Order.gt else .lt)) return false;
            if (relation == .eq) {
                const key = try std.mem.concat(ca, u8, &.{ lower, &self.boundary_coordinate });
                if (self.reverse_ids) upper = key else lower = (try orderedSuccessor(ca, key)) orelse return false;
            }
        };
        self.rows = if (self.reverse_ids)
            try tree.Cursor.initReverse(self.a, self.navigation.?.cache.store(), self.reader.root.page, lower, upper)
        else
            try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.page, lower, upper);
        return true;
    }
    pub fn next(self: *Cursor, a: A, count: usize) ![]const local.storage_rowsource_types.RowRef {
        if (count == 0 or count > 65536) return error.InvalidNativeLakeRowIndex;
        self.requested_rows = count;
        var result: std.ArrayList(local.storage_rowsource_types.RowRef) = .empty;
        errdefer result.deinit(a);
        while (result.items.len < count) {
            if (self.rows) |*rows| {
                if (try rows.next()) |record| {
                    try result.append(a, try self.reader.decode(a, record));
                    continue;
                }
                rows.deinit();
                self.rows = null;
            }
            if (self.participants) |owner| {
                _ = owner.scratch.reset(.retain_capacity);
                if (try owner.sort.next(owner.scratch.allocator())) |item| {
                    const bytes = item.keys[0].value.string;
                    if (bytes.len != 36) return error.InvalidNativeLakeRowIndex;
                    var key = bytes[0..36].*;
                    if (self.reverse_ids) for (&key) |*byte| {
                        byte.* = ~byte.*;
                    };
                    self.public_digest = key[0..32].*;
                    _ = try self.openFile(std.mem.readInt(u32, key[32..36], .big));
                    continue;
                }
            }
            if (self.shouldPromote()) {
                const promoted = self.promoteParticipants() catch |err| switch (err) {
                    error.OutOfMemory, error.SqlMemoryLimitExceeded, error.SqlProgramLimitExceeded, error.NoSpaceLeft => fallback: {
                        self.promotion_disabled = true;
                        self.participant_after = null;
                        break :fallback false;
                    },
                    else => return err,
                };
                if (promoted) continue;
            }
            if (self.public_files) |*files| {
                var found = false;
                while (!self.shouldPromote()) {
                    const record = (try files.next()) orelse break;
                    try self.reader.pages.cancellation.check();
                    if (self.reader.cached) |cached| try cached.context.ensureActive();
                    _ = self.file_arena.reset(.retain_capacity);
                    const scratch = self.file_arena.allocator();
                    if (record.key.len != 32) return error.InvalidNativeLakeRowIndex;
                    const slot = try self.reader.publicRecordSlot(scratch, record);
                    const key = try std.mem.concat(scratch, u8, &.{ self.group, &ordered.file_dictionary.slotKey(slot) });
                    var membership = try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.ties, key, null);
                    defer membership.deinit();
                    self.public_probes += 1;
                    self.last_probed = record.key[0..32].*;
                    const entry = (try membership.next()) orelse continue;
                    if (!std.mem.eql(u8, entry.key, key)) continue;
                    if (entry.value.len != 8 or std.mem.readInt(u64, entry.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
                    self.public_digest = record.key[0..32].*;
                    if (try self.openFile(slot)) {
                        found = true;
                        break;
                    }
                }
                if (found or self.shouldPromote()) continue;
                files.deinit();
                self.public_files = null;
            }
            if (self.file_position < self.files.len) {
                const position = if (self.reverse_ids) self.files.len - self.file_position - 1 else self.file_position;
                const slot = self.files[position];
                self.file_position += 1;
                _ = try self.openFile(slot);
                continue;
            }
            if (!try self.loadGroup()) break;
        }
        return result.toOwnedSlice(a);
    }
};
fn orderedSuccessor(a: A, prefix: []const u8) !?[]const u8 {
    var end = prefix.len;
    while (end != 0) {
        end -= 1;
        if (prefix[end] == 255) continue;
        const result = try a.dupe(u8, prefix[0 .. end + 1]);
        result[end] += 1;
        return result;
    }
    return null;
}
