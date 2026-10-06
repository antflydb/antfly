// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Snapshot-bound logical chunk scan. Merge stream heads with legacy members
//! in logical-key order, seeking past shadowed scopes instead of walking their
//! old tails. Memory is bounded by one stream identity and one row per cursor.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const generations = @import("artifact_chunk_generation.zig");
const Entry = @import("../backend_erased.zig").Entry;
const Budget = @import("artifact_scan_budget.zig").Budget;
const ScanPosition = @import("artifact_chunk_scan_position.zig").Position;

/// Non-owning facade for runtime algorithms whose store is type-erased. The
/// caller pins and closes the read; logical chunk iteration owns only cursors.
pub const BorrowedRead = struct {
    read: *@import("../backend_erased.zig").ReadTxn,
    pub const CursorAdapter = @import("../backend_erased.zig").Cursor;
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.read.get(key);
    }
    pub fn openPhysicalCursorAdapter(self: *@This()) !CursorAdapter {
        return self.read.openCursor();
    }
};

pub fn Cursor(comptime Txn: type) type {
    return struct {
        const Self = @This();
        const Generation = generations.View(Txn);
        alloc: std.mem.Allocator,
        txn: *Txn,
        legacy_prefix: []const u8 = "",
        head_prefix: []const u8 = "",
        head_kind_offset: usize = 0,
        legacy_cursor: ?Txn.CursorAdapter = null,
        head_cursor: ?Txn.CursorAdapter = null,
        legacy: ?Entry = null,
        advance_legacy: bool = false,
        view: ?Generation = null,
        members: ?Generation.Cursor = null,
        logical_key: []u8 = &.{},
        last_from_head: ?bool = null,

        pub fn open(alloc: std.mem.Allocator, txn: *Txn, document: []const u8) !Self {
            return openScoped(alloc, txn, document, null);
        }

        /// Index consumers seek directly into their producer's streams rather
        /// than decoding unrelated output sets and filtering their members.
        pub fn openNamed(alloc: std.mem.Allocator, txn: *Txn, document: []const u8, producer: []const u8) !Self {
            return openScoped(alloc, txn, document, producer);
        }

        fn openScoped(alloc: std.mem.Allocator, txn: *Txn, document: []const u8, producer: ?[]const u8) !Self {
            var self: Self = .{ .alloc = alloc, .txn = txn };
            errdefer self.close();
            self.legacy_prefix = if (producer) |name|
                try keys.artifactNamedPrefixAlloc(alloc, document, "chunk", name)
            else
                try keys.artifactTypePrefixAlloc(alloc, document, "chunk");
            var prefix: std.ArrayListUnmanaged(u8) = .empty;
            defer prefix.deinit(alloc);
            try keys.appendDocumentPrefix(&prefix, alloc, document);
            self.head_kind_offset = prefix.items.len;
            try prefix.append(alloc, keys.producer_generation_head_kind);
            if (producer) |name| try keys.appendEncodedComponent(&prefix, alloc, name);
            self.head_prefix = try prefix.toOwnedSlice(alloc);
            self.legacy_cursor = try txn.openPhysicalCursorAdapter();
            self.head_cursor = try txn.openPhysicalCursorAdapter();
            self.legacy = self.inLegacyRange(try self.legacy_cursor.?.seekAtOrAfter(self.legacy_prefix));
            try self.loadHead(try self.head_cursor.?.seekAtOrAfter(self.head_prefix));
            return self;
        }

        pub fn close(self: *Self) void {
            self.clearHead();
            if (self.legacy_cursor) |*cursor| cursor.close();
            if (self.head_cursor) |*cursor| cursor.close();
            self.alloc.free(self.legacy_prefix);
            self.alloc.free(self.head_prefix);
            self.* = undefined;
        }

        fn clearHead(self: *Self) void {
            if (self.members) |*cursor| cursor.close();
            self.members = null;
            if (self.view) |*view| view.deinit();
            self.view = null;
            self.alloc.free(self.logical_key);
            self.logical_key = &.{};
        }

        fn inLegacyRange(self: *const Self, entry: ?Entry) ?Entry {
            const row = entry orelse return null;
            return if (std.mem.startsWith(u8, row.key, self.legacy_prefix)) row else null;
        }

        fn loadHead(self: *Self, entry: ?Entry) !void {
            self.clearHead();
            const row = entry orelse return;
            if (!std.mem.startsWith(u8, row.key, self.head_prefix)) return;
            const scope = try self.alloc.dupe(u8, row.key);
            defer self.alloc.free(scope);
            scope[self.head_kind_offset] = keys.producer_stream_manifest_kind;
            var plan = try generations.Plan.init(self.alloc, scope, try generations.Spec.decode(row.value));
            errdefer plan.deinit();
            // Preserve escaped components without per-member decode/re-encode.
            const suffix = row.key[self.head_prefix.len..];
            const logical = try self.alloc.alloc(u8, self.legacy_prefix.len + suffix.len + 5);
            @memcpy(logical[0..self.legacy_prefix.len], self.legacy_prefix);
            @memcpy(logical[self.legacy_prefix.len..][0..suffix.len], suffix);
            logical[logical.len - 5] = keys.chunk_record_kind;
            @memset(logical[logical.len - 4 ..], 0);
            self.logical_key = logical;
            self.view = .{ .txn = self.txn, .plan = plan };
        }

        /// Resume exclusively after a canonical logical member. Seeks directly
        /// to its stream and ordinal; never walks the preceding generation or
        /// a shadowed legacy tail. A caller crossing snapshots must separately
        /// fence the producer/work revision: a key is not a completion proof.
        pub fn seekAfter(self: *Self, after: []const u8) !void {
            if (!keys.isChunkArtifactRecordKey(after) or !std.mem.startsWith(u8, after, self.legacy_prefix)) return error.InvalidBatchRequest;
            // `after` may be the key borrowed from this cursor's last next().
            const lower = try self.alloc.dupe(u8, after);
            defer self.alloc.free(lower);
            const head = (try @import("artifact_chunk_manifest.zig").keyForMemberAlloc(self.alloc, lower)).?;
            defer self.alloc.free(head);
            head[self.head_kind_offset] = keys.producer_generation_head_kind;
            const ordinal = std.mem.readInt(u32, lower[lower.len - 4 ..][0..4], .big);
            var lower_len = lower.len;
            if (ordinal == std.math.maxInt(u32)) {
                lower_len -= 4;
                lower[lower_len - 1] += 1;
            } else std.mem.writeInt(u32, lower[lower.len - 4 ..][0..4], ordinal + 1, .big);
            self.advance_legacy = false;
            self.legacy = self.inLegacyRange(try self.legacy_cursor.?.seekAtOrAfter(lower[0..lower_len]));
            const entry = try self.head_cursor.?.seekAtOrAfter(head);
            const same_scope = if (entry) |row| std.mem.eql(u8, row.key, head) else false;
            try self.loadHead(entry);
            if (same_scope) {
                try self.skipLegacyScope();
                self.members = try self.view.?.openCursor(self.alloc, ordinal +| 1);
            }
        }

        fn skipLegacyScope(self: *Self) !void {
            const scope_prefix = self.logical_key[0 .. self.logical_key.len - 4];
            if (self.legacy) |old| if (std.mem.startsWith(u8, old.key, scope_prefix)) {
                scope_prefix[scope_prefix.len - 1] += 1;
                defer scope_prefix[scope_prefix.len - 1] -= 1;
                self.legacy = self.inLegacyRange(try self.legacy_cursor.?.seekAtOrAfter(scope_prefix));
            };
        }

        pub fn checkpointAlloc(self: *const Self, alloc: std.mem.Allocator) ![]u8 {
            return self.checkpointPosition().encodeAlloc(alloc);
        }

        fn checkpointPosition(self: *const Self) ScanPosition {
            return (ScanPosition{
                .head = if (self.view) |view| view.plan.head_key else "",
                .legacy = if (self.legacy) |row| row.key else "",
                .ordinal = if (self.members) |cursor| cursor.ordinal else 0,
                .members_open = self.members != null,
                .advance_legacy = self.advance_legacy,
            });
        }

        /// The merge may prefetch a logical member while returning an older
        /// output. Retain the position BEFORE that member, not after it.
        pub fn checkpointBeforeReturnedAlloc(self: *const Self, alloc: std.mem.Allocator) ![]u8 {
            var position = self.checkpointPosition();
            if (self.last_from_head orelse return error.InvalidBatchRequest) position.ordinal -= 1 else position.advance_legacy = false;
            return position.encodeAlloc(alloc);
        }

        /// Only use with an unchanged owner/input witness across snapshots.
        /// Exact physical identities reject cross-producer or vanished rows.
        pub fn resumeFrom(self: *Self, raw: []const u8) !void {
            const position = try ScanPosition.decode(raw);
            if ((position.head.len != 0 and !std.mem.startsWith(u8, position.head, self.head_prefix)) or
                (position.legacy.len != 0 and !std.mem.startsWith(u8, position.legacy, self.legacy_prefix))) return error.InvalidBatchRequest;
            const head = if (position.head.len != 0) try self.head_cursor.?.seekAtOrAfter(position.head) else null;
            if (position.head.len != 0 and (head == null or !std.mem.eql(u8, head.?.key, position.head))) return error.EnrichmentSourceChanged;
            try self.loadHead(head);
            if (position.members_open) {
                if (position.ordinal > self.view.?.plan.spec.output.count) return error.ArtifactCatalogCorrupt;
                self.members = try self.view.?.openCursor(self.alloc, position.ordinal);
            }
            self.legacy = if (position.legacy.len != 0) self.inLegacyRange(try self.legacy_cursor.?.seekAtOrAfter(position.legacy)) else null;
            if (position.legacy.len != 0 and (self.legacy == null or !std.mem.eql(u8, self.legacy.?.key, position.legacy))) return error.EnrichmentSourceChanged;
            self.advance_legacy = position.advance_legacy;
        }

        pub const Step = union(enum) { row: Entry, yielded, end };

        /// Key and value are borrowed until the next cursor operation. A head
        /// owns every ordinal in its scope, even when it declares zero members.
        pub fn next(self: *Self) !?Entry {
            var budget: Budget = .{ .max_visits = std.math.maxInt(usize), .max_bytes = std.math.maxInt(usize) };
            return switch (try self.poll(&budget)) {
                .row => |row| row,
                .end => null,
                .yielded => unreachable,
            };
        }

        pub fn poll(self: *Self, budget: *Budget) !Step {
            self.last_from_head = null;
            while (true) {
                if (self.view == null and self.legacy == null) return .end;
                if (budget.exhausted()) return .yielded;
                if (self.members) |*cursor| {
                    if (try cursor.next()) |row| {
                        std.mem.writeInt(u32, self.logical_key[self.logical_key.len - 4 ..][0..4], row.ordinal, .big);
                        budget.visit(self.logical_key.len +| row.value.len);
                        self.last_from_head = true;
                        return .{ .row = .{ .key = self.logical_key, .value = row.value } };
                    }
                    budget.visit(self.view.?.plan.head_key.len);
                    try self.loadHead(try self.head_cursor.?.next());
                    continue;
                }
                if (self.advance_legacy) {
                    self.legacy = self.inLegacyRange(try self.legacy_cursor.?.next());
                    self.advance_legacy = false;
                }
                if (self.view != null and (self.legacy == null or std.mem.order(u8, self.logical_key, self.legacy.?.key) != .gt)) {
                    // The leaf-kind successor preserves neighboring units.
                    try self.skipLegacyScope();
                    self.members = try self.view.?.openCursor(self.alloc, 0);
                    budget.visit(self.view.?.plan.head_key.len);
                    continue;
                }
                const row = self.legacy orelse return .end;
                self.advance_legacy = true;
                budget.visit(row.key.len +| row.value.len);
                if (keys.isChunkArtifactRecordKey(row.key)) {
                    self.last_from_head = false;
                    return .{ .row = row };
                }
            }
        }
    };
}

test "ordered artifact inventory empty generation skips legacy scope with one seek and no member walk" {
    const alloc = std.testing.allocator;
    const chunks = @import("artifact_chunk_manifest.zig");
    const scope = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", null);
    defer alloc.free(scope);
    const spec = try generations.Spec.init(.{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, scope, @splat(3), chunks.Builder.init().finish(), 1);
    var plan = try generations.Plan.init(alloc, scope, spec);
    defer plan.deinit();
    const legacy = try keys.chunkArtifactKeyAlloc(alloc, "doc", "chunks", 0);
    defer alloc.free(legacy);
    const encoded = spec.encode();
    const Txn = struct {
        legacy: Entry,
        head: Entry,
        opened: usize = 0,
        legacy_seeks: usize = 0,
        head_nexts: usize = 0,
        pub const CursorAdapter = struct {
            txn: *Owner,
            is_head: bool,
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                if (self.is_head) return self.txn.head;
                self.txn.legacy_seeks += 1;
                return if (std.mem.order(u8, self.txn.legacy.key, key) == .lt) null else self.txn.legacy;
            }
            pub fn next(self: *@This()) !?Entry {
                if (!self.is_head) return error.LegacyTailWalk;
                self.txn.head_nexts += 1;
                return null;
            }
        };
        const Owner = @This();
        pub fn openPhysicalCursorAdapter(self: *@This()) !CursorAdapter {
            self.opened += 1;
            if (self.opened > 2) return error.UnexpectedMemberCursor;
            return .{ .txn = self, .is_head = self.opened == 2 };
        }
    };
    var txn: Txn = .{ .legacy = .{ .key = legacy, .value = "obsolete" }, .head = .{ .key = plan.head_key, .value = &encoded } };
    var cursor = try Cursor(Txn).open(alloc, &txn, "doc");
    defer cursor.close();
    try std.testing.expect((try cursor.next()) == null);
    try std.testing.expectEqual(@as(usize, 2), txn.legacy_seeks);
    try std.testing.expectEqual(@as(usize, 1), txn.head_nexts);
}
