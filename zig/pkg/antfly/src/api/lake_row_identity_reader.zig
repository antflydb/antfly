// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Index-scoped resolver views share one pinned, immutable publication owner.
//! Each view carries its exact original space, including same-label rewrites.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const navigation = @import("lake_source_navigation.zig");
const Store = @import("lake_index_store.zig").Store;
const Lease = @import("lake_index_reader_lease.zig").Handle;
const Source = local.serverless_query_lake_serving.ServingSource;
const RowRef = local.storage_rowsource_types.RowRef;
const A = std.mem.Allocator;
const Binding = struct {
    owner: *Owner,
    space: identity.Space,
    relocated: bool,
    translation: ?identity.Translation = null,
    original_slots: [16]?Slot = @splat(null),
    current_slots: [16]?Slot = @splat(null),
    const Slot = struct { file: [32]u8, slot: u32 };
    fn slot(self: *Binding, a: A, reader_: *identity.Reader, space: identity.Space, file_id: []const u8, original: bool) !u32 {
        const digest_ = @import("lake_row_identity_delta.zig").fileKey(file_id);
        const cache = if (original) &self.original_slots else &self.current_slots;
        const bucket = digest_[0] % cache.len;
        try self.owner.lock();
        const cached = cache[bucket];
        self.owner.mutex.unlock();
        if (cached) |value| if (std.mem.eql(u8, &value.file, &digest_)) return value.slot;
        const number = try reader_.fileSlot(a, space, file_id);
        try self.owner.lock();
        cache[bucket] = .{ .file = digest_, .slot = number };
        self.owner.mutex.unlock();
        return number;
    }
    fn from(raw: *anyopaque) *Binding {
        return @ptrCast(@alignCast(raw));
    }
    fn fromIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self = from(raw);
        return self.resolve(a, ref, true) catch |err| return self.owner.failed(err);
    }
    fn toIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self = from(raw);
        return self.resolve(a, ref, false) catch |err| return self.owner.failed(err);
    }
    fn reader(self: *Binding, lane: *ReadLane) identity.Reader {
        var value = self.owner.reader;
        value.pages = &lane.pages;
        value.cache = &lane.cache;
        value.root.indexes = self.space;
        value.verify_values = self.owner.reader.root.reused.len == 0;
        return value;
    }
    fn resolve(self: *Binding, a: A, ref: RowRef, forward: bool) !RowRef {
        const owner = self.owner;
        const source = if (forward) self.space else owner.reader.root.current;
        if (!sameCut(source, ref)) return error.ExternalLakeSnapshotMismatch;
        if (!self.relocated) return ownRef(a, ref);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var lane: ReadLane = undefined;
        lane.init(owner, scratch.allocator(), null);
        defer lane.deinit();
        var value = self.reader(&lane);
        if (self.translation) |translation| {
            const number = try self.slot(scratch.allocator(), &value, source, ref.external.file_id, forward);
            const mapped = value.translateAt(scratch.allocator(), translation, .{ .file = number, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }, forward) catch |err| return owner.failed(err);
            const target_space = if (forward) value.root.current else self.space;
            const version = if (forward) owner.identity_version else (try self.nav(scratch.allocator())).root.public_identity_version;
            var result = try value.coordinateReference(a, target_space, mapped.coordinate);
            result.external.identity_version = version;
            return result;
        }
        const possible = (if (forward) value.fromIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal) else value.toIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) catch |err| return owner.failed(err);
        const resolved = possible orelse return owner.failed(error.InvalidLogicalRowIdentity);
        const version = if (forward) owner.identity_version else (try self.nav(scratch.allocator())).root.public_identity_version;
        var result = try value.reference(a, if (forward) value.root.current else self.space, resolved);
        result.external.identity_version = version;
        return result;
    }
    fn translateMany(raw: *anyopaque, a: A, refs: []const RowRef, forward: bool) ![]const RowRef {
        const self = from(raw);
        return self.translateManyInner(a, refs, forward, null) catch |err| return self.owner.failed(err);
    }
    fn translateManyBounded(raw: *anyopaque, a: A, refs: []const RowRef, forward: bool, allowance: *Source.TranslationAllowance) ![]const RowRef {
        const self = from(raw);
        if (refs.len > allowance.remaining_rows) return error.QueryCandidateBudgetExceeded;
        allowance.remaining_rows -= refs.len;
        return self.translateManyInner(a, refs, forward, allowance) catch |err| return self.owner.failed(err);
    }
    fn translateManyInner(self: *Binding, a: A, refs: []const RowRef, forward: bool, allowance: ?*Source.TranslationAllowance) ![]const RowRef {
        const owner = self.owner;
        const result = try a.alloc(RowRef, refs.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |ref| {
                a.free(ref.external.source_id);
                a.free(ref.external.snapshot_id);
                a.free(ref.external.file_id);
            }
            a.free(result);
        }
        var begin: usize = 0;
        while (begin < refs.len) {
            const end = @min(refs.len, begin + 256);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const source = if (forward) self.space else owner.reader.root.current;
            for (refs[begin..end]) |ref| if (!sameCut(source, ref)) return error.ExternalLakeSnapshotMismatch;
            if (!self.relocated) {
                for (refs[begin..end], result[begin..end]) |ref, *target| {
                    target.* = try ownRef(a, ref);
                    initialized += 1;
                }
                begin = end;
                continue;
            }
            var lane: ReadLane = undefined;
            lane.init(owner, scratch.allocator(), allowance);
            defer lane.deinit();
            var reader_ = self.reader(&lane);
            if (self.translation) |translation| {
                const coordinates = try scratch.allocator().alloc(identity.logical.Coordinate, end - begin);
                for (refs[begin..end], coordinates) |ref, *coordinate| coordinate.* = .{ .file = try self.slot(scratch.allocator(), &reader_, source, ref.external.file_id, forward), .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal };
                const mapped = reader_.translateDenseMany(scratch.allocator(), translation, coordinates, forward) catch |err| return owner.failed(err);
                try self.materialize(a, scratch.allocator(), &reader_, mapped, result[begin..end], forward, &initialized);
            } else {
                const rows = reader_.translateMany(scratch.allocator(), refs[begin..end], forward) catch |err| return owner.failed(err);
                try self.materialize(a, scratch.allocator(), &reader_, rows, result[begin..end], forward, &initialized);
            }
            begin = end;
        }
        return result;
    }
    fn materialize(self: *Binding, a: A, scratch: A, reader_: *identity.Reader, rows: anytype, targets: []RowRef, forward: bool, initialized: *usize) !void {
        const target_space = if (forward) reader_.root.current else self.space;
        const version = if (forward) self.owner.identity_version else (try self.nav(scratch)).root.public_identity_version;
        var files: std.AutoHashMapUnmanaged(u32, []const u8) = .empty;
        for (rows, targets) |row, *target| {
            const slot_ = try files.getOrPut(scratch, row.coordinate.file);
            if (!slot_.found_existing) slot_.value_ptr.* = try reader_.file(scratch, target_space, row.coordinate.file);
            target.* = try ownRef(a, .{ .external = .{ .identity_version = version, .source_id = target_space.source, .snapshot_id = target_space.snapshot, .file_id = slot_.value_ptr.*, .row_group_ordinal = row.coordinate.group, .row_ordinal = row.coordinate.row } });
            initialized.* += 1;
        }
    }
    fn tieRank(raw: *anyopaque, a: A, ref: RowRef) !u64 {
        const self = from(raw);
        return self.tieRankInner(a, ref) catch |err| return self.owner.failed(err);
    }
    fn tieRankInner(self: *Binding, a: A, ref: RowRef) !u64 {
        const owner = self.owner;
        if (!sameCut(self.space, ref)) return error.ExternalLakeSnapshotMismatch;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var lane: ReadLane = undefined;
        lane.init(owner, scratch.allocator(), null);
        defer lane.deinit();
        var value = self.reader(&lane);
        if (self.translation) |translation| {
            const number = try self.slot(scratch.allocator(), &value, self.space, ref.external.file_id, true);
            const translated = value.translateAt(scratch.allocator(), translation, .{ .file = number, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }, true) catch |err| return owner.failed(err);
            if (!translation.ranked) return value.canonicalRank(scratch.allocator(), value.root.current, .{ .state = undefined, .coordinate = translated.coordinate, .tie_rank = 0 });
            if (translated.rank >= value.root.current.rows) return owner.failed(error.InvalidLogicalRowIdentity);
            return translated.rank;
        }
        const current = if (self.relocated)
            (try value.fromIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) orelse return owner.failed(error.InvalidLogicalRowIdentity)
        else blk: {
            const state = (try value.byLocation(scratch.allocator(), value.root.current, ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) orelse return owner.failed(error.InvalidLogicalRowIdentity);
            break :blk (try value.byId(scratch.allocator(), value.root.current, state.id)).?;
        };
        return value.canonicalRank(scratch.allocator(), value.root.current, current) catch |err| return owner.failed(err);
    }
    fn tieRanks(raw: *anyopaque, a: A, refs: []const RowRef, ranks: []u64) !void {
        const self = from(raw);
        self.tieRanksInner(a, refs, ranks) catch |err| return self.owner.failed(err);
    }
    fn tieRanksInner(self: *Binding, a: A, refs: []const RowRef, ranks: []u64) !void {
        if (refs.len != ranks.len or refs.len > 256) return error.InvalidLogicalRowIdentity;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        var lane: ReadLane = undefined;
        lane.init(self.owner, sa, null);
        defer lane.deinit();
        var reader_ = self.reader(&lane);
        const current = reader_.root.current;
        if (current.by_order == null) {
            for (refs, ranks) |ref, *rank| rank.* = try self.tieRankInner(a, ref);
            return;
        }
        const coordinates = try sa.alloc(identity.logical.Coordinate, refs.len);
        if (self.translation) |translation| {
            for (refs, coordinates) |ref, *coordinate| {
                if (!sameCut(self.space, ref)) return error.ExternalLakeSnapshotMismatch;
                coordinate.* = .{ .file = try self.slot(sa, &reader_, self.space, ref.external.file_id, true), .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal };
            }
            const mapped = try reader_.translateDenseMany(sa, translation, coordinates, true);
            if (translation.ranked) {
                for (mapped, ranks) |value, *rank| {
                    if (value.rank >= current.rows) return error.InvalidLogicalRowIdentity;
                    rank.* = value.rank;
                }
                return;
            }
            for (mapped, coordinates) |value, *coordinate| coordinate.* = value.coordinate;
        } else {
            const mapped = try reader_.translateMany(sa, refs, true);
            for (mapped, coordinates) |value, *coordinate| coordinate.* = value.coordinate;
        }
        var keys: [256][44]u8 = undefined;
        var pointers: [256][]const u8 = undefined;
        var names: std.AutoHashMapUnmanaged(u32, []const u8) = .empty;
        for (coordinates, keys[0..refs.len], pointers[0..refs.len]) |coordinate, *key, *pointer| {
            const name = try names.getOrPut(sa, coordinate.file);
            if (!name.found_existing) name.value_ptr.* = try reader_.file(sa, current, coordinate.file);
            key.* = identity.orderKey(name.value_ptr.*, coordinate.group, coordinate.row);
            pointer.* = key;
        }
        try @import("../serverless/graph_segment/page_tree.zig").ranksOf(sa, lane.cache.store(), current.by_order orelse return error.InvalidLogicalRowIdentity, pointers[0..refs.len], ranks);
    }
    fn file(raw: *anyopaque, a: A, digest_: []const u8) !?[]const u8 {
        const self = from(raw);
        const resolver = try self.nav(a);
        return resolver.file(a, digest_, false) catch |err| return self.owner.failed(err);
    }
    fn digest(raw: *anyopaque, a: A, file_: []const u8) ![]const u8 {
        const self = from(raw);
        const resolver = try self.nav(a);
        return resolver.digest(a, file_) catch |err| return self.owner.failed(err);
    }
    fn nav(self: *Binding, a: A) !*navigation.Resolver {
        return self.owner.navigationFor(a, self.space);
    }
    fn close(_: *anyopaque) void {}
    fn capability(self: *Binding) Source.RowResolver {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-coordinate-space-v1");
        hash.update(&self.owner.reader.root.domain);
        if (self.space.by_location) |ref| {
            hash.update(&ref.digest);
            hash.update(&ref.attempt);
        }
        var space: [32]u8 = undefined;
        hash.final(&space);
        return .{ .ptr = self, .source = self.space.source, .snapshot = self.space.snapshot, .current_source = self.owner.reader.root.current.source, .current_snapshot = self.owner.reader.root.current.snapshot, .rows = self.owner.reader.root.current.rows, .relocated = self.relocated, .coordinate_space = space, .from_index = fromIndex, .to_index = toIndex, .file = file, .digest = digest, .tie_rank = tieRank, .tie_ranks = tieRanks, .translate_many = translateMany, .translate_many_bounded = translateManyBounded, .close = close };
    }
};
fn sameCut(space: identity.Space, ref: RowRef) bool {
    return ref == .external and std.mem.eql(u8, space.source, ref.external.source_id) and std.mem.eql(u8, space.snapshot, ref.external.snapshot_id);
}
fn ownRef(a: A, ref: RowRef) !RowRef {
    if (ref != .external) return error.InvalidLogicalRowIdentity;
    var result = ref;
    result.external.source_id = try a.dupe(u8, ref.external.source_id);
    errdefer a.free(result.external.source_id);
    result.external.snapshot_id = try a.dupe(u8, ref.external.snapshot_id);
    errdefer a.free(result.external.snapshot_id);
    result.external.file_id = try a.dupe(u8, ref.external.file_id);
    return result;
}
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    store: Store,
    artifacts: @import("../serverless/artifacts/store.zig").ArtifactStore,
    lease: *Lease,
    reader: identity.Reader,
    navigation: navigation.Resolver,
    navigations: std.StringHashMapUnmanaged(*navigation.Resolver) = .empty,
    bindings: std.StringHashMapUnmanaged(*Binding) = .empty,
    default_binding: Binding = undefined,
    pages: @import("../serverless/graph_segment/page_store.zig").PageStore,
    cache: @import("../serverless/graph_segment/page_tree.zig").Cache = undefined,
    reads: u64 = 256 * 1024 * 1024,
    writes: u64 = 0,
    cached: @import("lake_index_aggregate_artifact.zig").CachedRead,
    mutex: std.atomic.Mutex = .unlocked,
    identity_version: u8,
    index_identity_version: u8,
    table_id: u64,
    generation: u64,
    publication_token: [16]u8,
    repair_hint: @FieldType(Source, "repair_hint"),
    fn lock(self: *Owner) !void {
        while (!self.mutex.tryLock()) {
            try self.cached.context.ensureActive();
            if (self.cached.context.io) |io| try io.sleep(.zero, .awake) else @import("antfly_platform").time.yieldBriefly();
        }
        self.cached.context.ensureActive() catch |err| {
            self.mutex.unlock();
            return err;
        };
    }
    fn failed(self: *Owner, err: anyerror) anyerror {
        const failure = identity.derivedReadFailure(err);
        if (failure == error.LakeIndexArtifactUnavailable or failure == error.InvalidLogicalRowIdentity) if (self.repair_hint) |hint| hint.report(hint.ptr, self.table_id, self.generation, self.publication_token);
        return failure;
    }
    fn fromIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.fromIndex(&self.default_binding, a, ref);
    }
    fn toIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.toIndex(&self.default_binding, a, ref);
    }
    fn file(raw: *anyopaque, a: A, digest_: []const u8) !?[]const u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.file(&self.default_binding, a, digest_);
    }
    fn digest(raw: *anyopaque, a: A, file_: []const u8) ![]const u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.digest(&self.default_binding, a, file_);
    }
    fn tieRank(raw: *anyopaque, a: A, ref: RowRef) !u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.tieRank(&self.default_binding, a, ref);
    }
    fn translateMany(raw: *anyopaque, a: A, refs: []const RowRef, forward: bool) ![]const RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.translateMany(&self.default_binding, a, refs, forward);
    }
    fn translateManyBounded(raw: *anyopaque, a: A, refs: []const RowRef, forward: bool, allowance: *Source.TranslationAllowance) ![]const RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return Binding.translateManyBounded(&self.default_binding, a, refs, forward, allowance);
    }
    fn tieRanks(raw: *anyopaque, a: A, refs: []const RowRef, ranks: []u64) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try Binding.tieRanks(&self.default_binding, a, refs, ranks);
    }
    fn bindIndex(raw: *anyopaque, name: []const u8) !Source.RowResolver {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.lock();
        defer self.mutex.unlock();
        if (self.bindings.get(name)) |binding| return binding.capability();
        const binding = try self.a.create(Binding);
        errdefer self.a.destroy(binding);
        var relocated = self.reader.root.indexes != null;
        var translation: ?identity.Translation = null;
        for (self.reader.root.reused) |entry| if (std.mem.eql(u8, entry.name, name)) {
            relocated = true;
            translation = entry.translation;
            break;
        };
        binding.* = .{ .owner = self, .space = self.reader.root.spaceForIndex(name), .relocated = relocated, .translation = translation };
        const key = try self.a.dupe(u8, name);
        errdefer self.a.free(key);
        try self.bindings.put(self.a, key, binding);
        return binding.capability();
    }
    fn navigationFor(self: *Owner, a: A, space: identity.Space) !*navigation.Resolver {
        try self.cached.context.ensureActive();
        const ref = space.navigation orelse return error.InvalidLogicalRowIdentity;
        if (std.mem.eql(u8, ref.artifact_id, self.reader.root.indexSpace().navigation.?.artifact_id)) return &self.navigation;
        try self.lock();
        const existing = self.navigations.get(ref.artifact_id);
        self.mutex.unlock();
        if (existing) |resolver| return resolver;
        // Load with operation-owned scratch outside the publication mutex.
        // Installation copies only small immutable metadata while locked, so
        // a cold index space cannot serialize unrelated map readers.
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const root = navigation.load(scratch.allocator(), self.artifacts, ref, self.pages.cancellation, self.cached) catch |err| return self.failed(err);
        if (!std.mem.eql(u8, &root.domain, &self.reader.root.domain) or !std.mem.eql(u8, root.source, space.source) or !std.mem.eql(u8, root.snapshot, space.snapshot) or root.count != space.liveFiles()) return error.InvalidLogicalRowIdentity;
        try self.lock();
        defer self.mutex.unlock();
        if (self.navigations.get(ref.artifact_id)) |resolver| return resolver;
        const oa = self.arena.allocator();
        const encoded = try std.json.Stringify.valueAlloc(oa, root, .{});
        const owned = try std.json.parseFromSliceLeaky(navigation.Root, oa, encoded, .{ .allocate = .alloc_always });
        const resolver = try self.a.create(navigation.Resolver);
        errdefer self.a.destroy(resolver);
        resolver.* = .{ .root = owned, .store = self.artifacts, .cached = self.cached, .token = self.pages.cancellation };
        try self.navigations.put(self.a, ref.artifact_id, resolver);
        return resolver;
    }
    fn charge(self: *Owner, amount: u64, allowance: ?*Source.TranslationAllowance) !void {
        try self.lock();
        defer self.mutex.unlock();
        if (allowance) |limited| {
            if (amount > limited.remaining_reads or amount > self.reads -| limited.required_reserve) return error.QueryCandidateBudgetExceeded;
            limited.remaining_reads -= amount;
        }
        try @import("../serverless/artifacts/store.zig").chargeReadBudget(&self.reads, amount);
    }
    fn decodedPage(raw: *anyopaque, a: A, ref: @import("../serverless/graph_segment/page_tree.zig").Ref) !@import("../serverless/graph_segment/page_tree.zig").Page {
        var lane: ReadLane = undefined;
        lane.init(@ptrCast(@alignCast(raw)), a, null);
        defer lane.deinit();
        return ReadLane.decodedPage(&lane, a, ref);
    }
    fn readPage(raw: *anyopaque, a: A, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, token: @import("antfly_cancellation").CancellationToken, _: *u64) ![]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        if (offset != 0 or length != ref.byte_len) return error.InvalidLogicalRowIdentity;
        try self.charge(length, null);
        return @import("lake_index_aggregate_artifact.zig").readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached) catch |err| return self.failed(err);
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.cache.deinit();
        self.navigation.deinit();
        var navigations = self.navigations.valueIterator();
        while (navigations.next()) |value| {
            value.*.deinit();
            a.destroy(value.*);
        }
        self.navigations.deinit(a);
        var bindings = self.bindings.iterator();
        while (bindings.next()) |entry| {
            a.destroy(entry.value_ptr.*);
            a.free(entry.key_ptr.*);
        }
        self.bindings.deinit(a);
        self.store.deinit();
        self.lease.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};

/// Mutable cursor/cache state belongs to one synchronous operation, while
/// authenticated decoded pages and publication authority remain shared.
const ReadLane = struct {
    owner: *Owner,
    allowance: ?*Source.TranslationAllowance,
    pages: @import("../serverless/graph_segment/page_store.zig").PageStore = undefined,
    cache: @import("../serverless/graph_segment/page_tree.zig").Cache = undefined,
    unused_reads: u64 = 0,
    unused_writes: u64 = 0,
    fn init(self: *ReadLane, owner_: *Owner, a: A, allowance: ?*Source.TranslationAllowance) void {
        self.* = .{ .owner = owner_, .allowance = allowance };
        self.pages = .{ .domain = owner_.reader.root.domain, .artifacts = &owner_.artifacts, .cancellation = owner_.pages.cancellation, .remaining_read_bytes = &self.unused_reads, .remaining_write_bytes = &self.unused_writes, .read_cache = .{ .ptr = self, .read = readPage } };
        var storage = self.pages.store();
        storage.decoded_page = .{ .ptr = self, .read = decodedPage };
        self.cache = .{ .alloc = a, .underlying = storage, .max_bytes = 256 * 1024 };
    }
    fn deinit(self: *ReadLane) void {
        self.cache.deinit();
    }
    fn readPage(raw: *anyopaque, a: A, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, token: @import("antfly_cancellation").CancellationToken, _: *u64) ![]u8 {
        const self: *ReadLane = @ptrCast(@alignCast(raw));
        try self.owner.cached.context.ensureActive();
        if (offset != 0 or length != ref.byte_len) return error.InvalidLogicalRowIdentity;
        try self.owner.charge(length, self.allowance);
        return @import("lake_index_aggregate_artifact.zig").readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.owner.cached) catch |err| return self.owner.failed(err);
    }
    const DecodedPage = struct { page: @import("../serverless/graph_segment/page_tree.zig").Page };
    fn decodedPage(raw: *anyopaque, a: A, ref: @import("../serverless/graph_segment/page_tree.zig").Ref) !@import("../serverless/graph_segment/page_tree.zig").Page {
        const lane: *ReadLane = @ptrCast(@alignCast(raw));
        const self = lane.owner;
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        try self.cached.context.ensureActive();
        try self.pages.cancellation.check();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-row-map-decoded-page-v1");
        hash.update(&self.cached.scope);
        hash.update(&self.reader.root.domain);
        hash.update(&ref.digest);
        hash.update(&ref.attempt);
        var shape: [13]u8 = undefined;
        std.mem.writeInt(u32, shape[0..4], ref.bytes, .big);
        shape[4] = ref.height;
        std.mem.writeInt(u64, shape[5..13], ref.records, .big);
        hash.update(&shape);
        var key: [32]u8 = undefined;
        hash.final(&key);
        const Loader = struct {
            lane: *ReadLane,
            ref: tree.Ref,
            fn load(raw_: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                const self_: *@This() = @ptrCast(@alignCast(raw_));
                const out = item.arena.allocator();
                const value = out.create(DecodedPage) catch |err| {
                    if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                };
                value.* = .{ .page = tree.readPage(out, self_.lane.pages.store(), self_.ref) catch |err| {
                    if (err == error.OutOfMemory and item.budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
                    return err;
                } };
                item.payload = .{ .extension = value };
            }
            fn release(owner: *anyopaque, item: *anyopaque) void {
                const lease: local.serverless_query_lake_decoded_cache.Lease = .{ .cache = @ptrCast(@alignCast(owner)), .item = @ptrCast(@alignCast(item)) };
                lease.release();
            }
        };
        var loader: Loader = .{ .lane = lane, .ref = ref };
        const cache = &self.cached.cache.decoded;
        // Optional planning never leads a shared flight with its narrower
        // allowance: required readers must not inherit a planner's decline.
        if (lane.allowance != null) {
            if (cache.lookup(key)) |lease| {
                const value: *const DecodedPage = @ptrCast(@alignCast(lease.item.payload.extension));
                var page = value.page;
                page.lease = .{ .owner = lease.cache, .item = lease.item, .release = Loader.release };
                return page;
            }
            return tree.readPage(a, lane.pages.store(), ref);
        }
        const lease = cache.acquire(key, cache.max_loading_bytes, self.cached.context, .{ .ptr = &loader, .load = Loader.load }) catch |err| {
            if (err != error.SqlMemoryLimitExceeded) return self.failed(err);
            try self.cached.context.ensureActive();
            return tree.readPage(a, lane.pages.store(), ref) catch |failure| return self.failed(failure);
        };
        errdefer lease.release();
        try self.cached.context.ensureActive();
        const value: *const DecodedPage = @ptrCast(@alignCast(lease.item.payload.extension));
        var page = value.page;
        page.lease = .{ .owner = lease.cache, .item = lease.item, .release = Loader.release };
        return page;
    }
};

pub fn validate(a: A, root: identity.Root, publication: local.metadata_lake_index_catalog.Publication, domain: identity.logical.Digest, source: *Source) !void {
    try root.validate(a);
    if (!std.mem.eql(u8, &root.domain, &domain) or !std.mem.eql(u8, &root.cut, &publication.signature.source) or !std.mem.eql(u8, &root.values_recipe, &publication.signature.desired) or !std.mem.eql(u8, &root.artifacts, &try identity.declarationDigest(a, publication.declarations)) or !std.mem.eql(u8, root.current.source, source.inventory.source_id) or !std.mem.eql(u8, root.current.snapshot, source.inventory.snapshot_id) or root.current.liveFiles() != source.fileCount()) return error.InvalidLogicalRowIdentity;
    for (publication.declarations) |declaration| {
        const index = root.spaceForIndex(declaration.name);
        if (!std.mem.eql(u8, declaration.binding.source_id, index.source) or !std.mem.eql(u8, declaration.binding.snapshot_id, index.snapshot)) return error.InvalidLogicalRowIdentity;
    }
}

pub fn install(table_id: u64, source: *Source, store: *Store, publication: local.metadata_lake_index_catalog.Publication, root: identity.Root, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, lease: *Lease) !void {
    if (!root.hasRelocation()) return;
    if (source.row_resolver) |prior| {
        if (prior.bind_index == Owner.bindIndex) {
            const existing: *Owner = @ptrCast(@alignCast(prior.ptr));
            if (existing.table_id == table_id and existing.generation == publication.generation and std.mem.eql(u8, &existing.publication_token, &publication.token) and std.mem.eql(u8, &existing.reader.root.artifacts, &root.artifacts)) return;
        }
        // Borrowed views can already be retained by another consumer. Their
        // owner is immutable until source teardown; replace the source to rebind.
        return error.ExternalLakeSnapshotMismatch;
    }
    const a = source.alloc;
    const owner = try a.create(Owner);
    errdefer a.destroy(owner);
    owner.* = .{ .a = a, .arena = .init(a), .store = undefined, .artifacts = undefined, .lease = undefined, .reader = undefined, .navigation = undefined, .pages = undefined, .cached = cached, .identity_version = source.inventory.row_identity_version, .index_identity_version = 1, .table_id = table_id, .generation = publication.generation, .publication_token = publication.token, .repair_hint = source.repair_hint };
    errdefer owner.arena.deinit();
    owner.lease = try lease.retain();
    errdefer owner.lease.deinit();
    owner.store = try store.reopenReadOnly(a);
    errdefer owner.store.deinit();
    const context = owner.lease.readContext();
    const token: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |value| .{ .ptr = value.ptr, .is_cancelled_fn = value.is_cancelled_fn } else .none;
    owner.store.implementation.cloud_io = context.io;
    try owner.store.bindPublication(publication, token);
    owner.artifacts = owner.store.artifactStore();
    owner.cached.context = context;
    const oa = owner.arena.allocator();
    const encoded = try std.json.Stringify.valueAlloc(oa, root, .{});
    const owned = try identity.decode(oa, encoded);
    const index = owned.indexSpace();
    const nav = try navigation.load(oa, owner.artifacts, index.navigation orelse return error.InvalidLogicalRowIdentity, token, owner.cached);
    if (!std.mem.eql(u8, &nav.domain, &root.domain) or !std.mem.eql(u8, nav.source, index.source) or !std.mem.eql(u8, nav.snapshot, index.snapshot) or nav.count != index.liveFiles()) return error.InvalidLogicalRowIdentity;
    owner.navigation = .{ .root = nav, .store = owner.artifacts, .cached = owner.cached, .token = token };
    owner.index_identity_version = nav.public_identity_version;
    owner.pages = .{ .domain = root.domain, .artifacts = &owner.artifacts, .cancellation = token, .remaining_read_bytes = &owner.reads, .remaining_write_bytes = &owner.writes, .read_cache = .{ .ptr = owner, .read = Owner.readPage } };
    var decoded_storage = owner.pages.store();
    decoded_storage.decoded_page = .{ .ptr = owner, .read = Owner.decodedPage };
    owner.cache = .{ .alloc = a, .underlying = decoded_storage, .max_bytes = 2 * 1024 * 1024 };
    owner.reader = .{ .root = owned, .pages = &owner.pages, .cache = &owner.cache };
    owner.default_binding = .{ .owner = owner, .space = owned.indexSpace(), .relocated = true, .translation = if (owned.reused.len != 0) owned.reused[0].translation else null };
    var resolver = owner.default_binding.capability();
    resolver.ptr = owner;
    resolver.from_index = Owner.fromIndex;
    resolver.to_index = Owner.toIndex;
    resolver.file = Owner.file;
    resolver.digest = Owner.digest;
    resolver.tie_rank = Owner.tieRank;
    resolver.tie_ranks = Owner.tieRanks;
    resolver.translate_many = Owner.translateMany;
    resolver.translate_many_bounded = Owner.translateManyBounded;
    resolver.bind_index = Owner.bindIndex;
    resolver.close = Owner.close;
    source.row_resolver = resolver;
}

test "external lake index scoped views distinguish same snapshot labels and immutable borrowed owners" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-index-scoped-views");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var artifacts = fs.artifactStore();
    artifacts.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call };
    defer manager.deinit();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var original = try identity.Builder.init(a, &manager, &.{"file"}, 64 * 1024, .{ .source = "source", .snapshot = "same" });
    defer original.deinit();
    var replacement = try identity.Builder.init(a, &manager, &.{"file"}, 64 * 1024, .{ .source = "source", .snapshot = "same" });
    defer replacement.deinit();
    for (0..2) |row| {
        var id: [32]u8 = @splat(3);
        id[31] = @intCast(row);
        try original.add(.{ .id = id, .values = @splat(4) }, .{ .file = 0, .group = 0, .row = row });
        try replacement.add(.{ .id = id, .values = @splat(5) }, .{ .file = 0, .group = 0, .row = 1 - row });
    }
    var old = try original.finish(&pages, "source", "same", @splat(1), @splat(2), @splat(3));
    old.current.navigation = .{ .kind = .doc_values, .artifact_id = "test-old-navigation", .checksum = "test", .byte_len = 1 };
    var current = try replacement.finish(&pages, "source", "same", @splat(1), @splat(2), @splat(4));
    current.values_format = 2;
    current.artifacts = @splat(9);
    current.reused = &.{.{ .name = "body", .space = old.current }};
    try current.reseal(a);
    try current.validate(a);
    var serving_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer serving_cache.deinit();
    var owner: Owner = .{ .a = a, .arena = .init(a), .store = undefined, .artifacts = artifacts, .lease = undefined, .reader = .{ .root = current, .pages = &pages }, .navigation = .{ .root = .{ .domain = @splat(7), .source = "source", .schema = "schema", .snapshot = "same", .count = 1 }, .store = artifacts, .cached = .{ .cache = undefined, .scope = @splat(0), .context = .{} }, .token = .none }, .pages = pages, .cached = .{ .cache = &serving_cache, .scope = @splat(0), .context = .{} }, .identity_version = 2, .index_identity_version = 1, .table_id = 1, .generation = 1, .publication_token = @splat(1), .repair_hint = null };
    defer {
        var bindings = owner.bindings.iterator();
        while (bindings.next()) |entry| {
            a.destroy(entry.value_ptr.*);
            a.free(entry.key_ptr.*);
        }
        owner.bindings.deinit(a);
        owner.arena.deinit();
    }
    // Distinct cold map pages must overlap provider reads. The old owner-wide
    // lock serialized this path even when query lanes had independent cursors.
    const Probe = struct {
        var active: *@This() = undefined;
        backend: @import("../serverless/artifacts/store.zig").ArtifactStore,
        current: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        fn read(_: *anyopaque, alloc: A, id: []const u8, maximum: usize, token: @import("antfly_cancellation").CancellationToken) ![]u8 {
            const self = active;
            const current_ = self.current.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.current.fetchSub(1, .acq_rel);
            _ = self.peak.fetchMax(current_, .acq_rel);
            const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (self.peak.load(.acquire) < 2) {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.UnexpectedSerializedRead;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            const bytes = try self.backend.getAllocWithCancellationUsingAllocator(alloc, id, token);
            errdefer alloc.free(bytes);
            if (bytes.len > maximum) return error.ArtifactTooLarge;
            return bytes;
        }
        fn run(binding: *Binding, expected: u64, failure: *?anyerror) void {
            var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer scratch.deinit();
            const ref: RowRef = .{ .external = .{ .source_id = "source", .snapshot_id = "same", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 0 } };
            const result = Binding.fromIndex(binding, scratch.allocator(), ref) catch |err| {
                failure.* = err;
                return;
            };
            if (result.external.row_ordinal != expected) failure.* = error.TestUnexpectedResult;
        }
    };
    var probe: Probe = .{ .backend = artifacts };
    Probe.active = &probe;
    var vtable = artifacts.vtable.*;
    vtable.get_bounded_alloc_with_cancellation = Probe.read;
    owner.artifacts.vtable = &vtable;
    owner.cached.context.io = std.testing.io;
    {
        var failures: [2]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        defer group.cancel(std.testing.io);
        var left: Binding = .{ .owner = &owner, .space = old.current, .relocated = true };
        var right: Binding = .{ .owner = &owner, .space = current.current, .relocated = true };
        try group.concurrent(std.testing.io, Probe.run, .{ &left, 1, &failures[0] });
        try group.concurrent(std.testing.io, Probe.run, .{ &right, 0, &failures[1] });
        try group.await(std.testing.io);
        for (failures) |failure| if (failure) |err| return err;
    }
    try std.testing.expectEqual(@as(usize, 2), probe.peak.load(.acquire));
    owner.artifacts.vtable = artifacts.vtable;
    // Parsed immutable pages are shared across seeks; current authority still
    // applies to a warm hit. Cache pressure falls back to the required read.
    var first_page = try Owner.decodedPage(&owner, a, current.current.by_id.?);
    first_page.deinit(a);
    const hits = serving_cache.decoded.hits;
    var second_page = try Owner.decodedPage(&owner, a, current.current.by_id.?);
    second_page.deinit(a);
    try std.testing.expectEqual(hits + 1, serving_cache.decoded.hits);
    owner.cached.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, Owner.decodedPage(&owner, a, current.current.by_id.?));
    owner.cached.context.deadline_ns = null;
    serving_cache.decoded.max_loading_bytes = 0;
    var fallback = try Owner.decodedPage(&owner, a, old.current.by_id.?);
    fallback.deinit(a);
    const body = try Owner.bindIndex(&owner, "body");
    const cover = try Owner.bindIndex(&owner, "cover");
    const again = try Owner.bindIndex(&owner, "body");
    try std.testing.expect(body.ptr == again.ptr);
    try std.testing.expect(body.relocated and !cover.relocated);
    const ref: RowRef = .{ .external = .{ .source_id = "source", .snapshot_id = "same", .identity_version = 2, .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 0 } };
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const oa = output.allocator();
    // Make the required-versus-optional admission check cold, independently
    // of which pages the parallel translation above happened to warm.
    serving_cache.deinit();
    serving_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    serving_cache.decoded.max_loading_bytes = 0;
    var declined: Source.TranslationAllowance = .{ .remaining_reads = 0, .required_reserve = 0 };
    const before = owner.reads;
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, body.translate_many_bounded.?(body.ptr, oa, &.{ref}, true, &declined));
    try std.testing.expectEqual(before, owner.reads);
    const mapped = try body.from_index(body.ptr, oa, ref);
    const direct = try cover.from_index(cover.ptr, oa, ref);
    try std.testing.expectEqual(@as(u64, 1), mapped.external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 0), direct.external.row_ordinal);
    const reverse = try body.to_index(body.ptr, oa, mapped);
    try std.testing.expectEqual(@as(u64, 0), reverse.external.row_ordinal);
    const batch = try body.translate_many.?(body.ptr, oa, &.{ ref, ref }, true);
    try std.testing.expectEqual(@as(u64, 1), batch[0].external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 1), batch[1].external.row_ordinal);
    body.close(body.ptr); // A borrowed view cannot destroy its shared owner.
    try std.testing.expectEqual(@as(u64, 1), (try body.from_index(body.ptr, oa, ref)).external.row_ordinal);
}

test "external lake row-map optional admission preserves required reads and cancellation" {
    var owner: Owner = undefined;
    owner.mutex = .unlocked;
    owner.reads = 1024;
    owner.cached.context = .{};
    var allowance: Source.TranslationAllowance = .{ .remaining_reads = 128, .required_reserve = 512 };
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, owner.charge(129, &allowance));
    try std.testing.expectEqual(@as(u64, 1024), owner.reads);
    try owner.charge(128, &allowance);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, owner.charge(1, &allowance));
    try owner.charge(700, null);
    try std.testing.expectEqual(@as(u64, 196), owner.reads);
    allowance.remaining_reads = 1024;
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, owner.charge(1, &allowance));
    try std.testing.expectEqual(@as(u64, 196), owner.reads);
    owner.cached.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, owner.charge(1, null));
    try std.testing.expectEqual(@as(u64, 196), owner.reads);
}
