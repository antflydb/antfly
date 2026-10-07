// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Native publication/read/collection authority. Stored independently of the
//! table definition, but updated by the same metadata transaction as publish
//! and DROP. Reader traffic never changes a builder's definition CAS fence.
const std = @import("std");
const catalog = @import("antfly_local_sources").metadata_lake_index_catalog;
const A = std.mem.Allocator;
pub const max_bytes = 1024 * 1024;
pub const max_publications = 256;
pub const max_readers = 256;
pub const lease_ms: u64 = 10 * 60 * 1000;
pub const grace_ms: u64 = 30 * 1000;
pub const Reader = struct { token: catalog.Token, generation: u64, expires_ms: u64 };
pub const StoreBinding = struct { identity: catalog.Digest, locator: catalog.StoreLocator };
pub const Collection = struct {
    token: catalog.Token,
    store: catalog.Digest,
    expires_ms: u64,
    /// Newly created artifacts at or above this generation are never swept.
    upload_cutoff: u64,
    upload_floor: u64 = 0,
    namespace: ?catalog.Digest = null,
    /// Exact generations selected for retirement, not a numerical history
    /// prefix: failed attempts and store rotations leave generation gaps.
    retired: []const u64,
    progress: ?@import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef = null,
};
pub const State = struct {
    version: u16 = 1,
    revision: u64 = 0,
    generation: u64 = 0,
    namespace: ?catalog.Digest = null,
    stores: []const StoreBinding = &.{},
    /// Uploads before this fence may have unleased rolling-upgrade readers.
    /// They remain protected until an independent drain proof can retire them.
    reader_upload_floor: ?u64 = null,
    current: ?u64 = null,
    pending: ?catalog.Attempt = null,
    dropped: bool = false,
    publications: []const catalog.Publication = &.{},
    readers: []const Reader = &.{},
    collection: ?Collection = null,
    completed_collection: ?catalog.Token = null,

    pub fn findPublication(self: State, generation: u64) ?catalog.Publication {
        for (self.publications) |publication| if (publication.generation == generation) return publication;
        return null;
    }
    fn retired(self: State, generation: u64) bool {
        if (self.collection) |collection| for (collection.retired) |old| if (generation == old) return true;
        return false;
    }
    pub fn validate(self: State) !void {
        if (self.version != 1 or self.publications.len > max_publications or self.readers.len > max_readers or self.stores.len > 64) return error.InvalidLakeIndexLifecycle;
        for (self.stores, 0..) |store, index| {
            try store.locator.validate();
            if (std.mem.allEqual(u8, &store.identity, 0)) return error.InvalidLakeIndexLifecycle;
            for (self.stores[0..index]) |other| if (std.mem.eql(u8, &store.identity, &other.identity)) return error.InvalidLakeIndexLifecycle;
        }
        if (self.reader_upload_floor) |floor| if (floor == 0 or floor > self.generation) return error.InvalidLakeIndexLifecycle;
        if (self.namespace) |namespace| if (std.mem.allEqual(u8, &namespace, 0)) return error.InvalidLakeIndexLifecycle;
        var prior: u64 = 0;
        for (self.publications) |publication| {
            try publication.validate();
            if (publication.generation <= prior or publication.generation > self.generation) return error.InvalidLakeIndexLifecycle;
            prior = publication.generation;
        }
        if (self.current) |current| if (self.dropped or self.findPublication(current) == null or self.retired(current)) return error.InvalidLakeIndexLifecycle;
        if (self.pending) |pending| if (self.dropped or pending.generation != self.generation) return error.InvalidLakeIndexLifecycle;
        for (self.readers, 0..) |reader, index| {
            if (reader.expires_ms == 0 or std.mem.allEqual(u8, &reader.token, 0) or self.findPublication(reader.generation) == null) return error.InvalidLakeIndexLifecycle;
            for (self.readers[0..index]) |other| if (std.mem.eql(u8, &reader.token, &other.token)) return error.InvalidLakeIndexLifecycle;
        }
        if (self.collection) |collection| {
            if (collection.upload_floor == 0 or collection.upload_floor > collection.upload_cutoff) return error.InvalidLakeIndexLifecycle;
            if (std.mem.allEqual(u8, &collection.token, 0) or std.mem.allEqual(u8, &collection.store, 0) or collection.expires_ms == 0 or collection.upload_cutoff == 0 or collection.upload_cutoff > self.generation +| 1) return error.InvalidLakeIndexLifecycle;
            if (collection.progress) |progress| try @import("../serverless/artifacts/store.zig").validateSha256ArtifactIdentity(progress.artifact_id, progress.checksum);
            for (collection.retired, 0..) |generation, index| {
                const publication = self.findPublication(generation) orelse return error.InvalidLakeIndexLifecycle;
                if (self.current == generation or !std.meta.eql(publication.namespace, collection.namespace) or !std.mem.eql(u8, &publication.signature.store, &collection.store)) return error.InvalidLakeIndexLifecycle;
                for (collection.retired[0..index]) |other| if (other == generation) return error.InvalidLakeIndexLifecycle;
            }
        }
    }
    fn advance(self: State) !State {
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.LakeIndexGenerationExhausted;
        return next;
    }
    /// Called inside the table mutation's metadata transaction. No independent
    /// object-store HEAD can publish or resurrect a native generation.
    pub fn synchronize(self: State, a: A, catalog_state: catalog.State) !State {
        try self.validate();
        try catalog_state.validate();
        if (self.dropped or catalog_state.generation < self.generation) return error.LakeIndexPublicationFenceChanged;
        if (self.namespace != null and !std.meta.eql(self.namespace, catalog_state.namespace)) return error.LakeIndexPublicationFenceChanged;
        var next = try self.advance();
        next.generation = catalog_state.generation;
        next.namespace = catalog_state.namespace;
        next.pending = catalog_state.pending;
        if (next.reader_upload_floor == null) {
            if (catalog_state.pending) |pending| if (pending.reader_protocol == 24) {
                next.reader_upload_floor = pending.generation;
            };
            if (next.reader_upload_floor == null) if (catalog_state.published) |publication| if (publication.reader_protocol == 24) {
                next.reader_upload_floor = publication.generation;
            };
        }
        next.current = if (catalog_state.published) |publication| publication.generation else null;
        if (catalog_state.pending) |pending| if (pending.store_locator) |locator| try next.retainStore(a, pending.signature.store, locator);
        if (catalog_state.published) |publication| if (publication.store_locator) |locator| try next.retainStore(a, publication.signature.store, locator);
        if (catalog_state.published) |publication| {
            if (self.retired(publication.generation)) return error.LakeIndexPublicationFenceChanged;
            if (self.findPublication(publication.generation)) |prior| {
                if (!try catalog.publicationEqual(a, prior, publication)) return error.LakeIndexPublicationFenceChanged;
            } else {
                if (self.publications.len == max_publications) return error.LakeIndexRetentionBackpressure;
                const all = try a.alloc(catalog.Publication, self.publications.len + 1);
                @memcpy(all[0..self.publications.len], self.publications);
                all[self.publications.len] = publication;
                next.publications = all;
            }
        }
        try next.validate();
        return next;
    }
    fn retainStore(self: *State, a: A, identity: catalog.Digest, locator: catalog.StoreLocator) !void {
        for (self.stores) |store| if (std.mem.eql(u8, &store.identity, &identity)) {
            const before = try std.json.Stringify.valueAlloc(a, store.locator, .{});
            defer a.free(before);
            const after = try std.json.Stringify.valueAlloc(a, locator, .{});
            defer a.free(after);
            if (!std.mem.eql(u8, before, after)) return error.LakeIndexPublicationFenceChanged;
            return;
        };
        if (self.stores.len == 64) return error.LakeIndexRetentionBackpressure;
        const all = try a.alloc(StoreBinding, self.stores.len + 1);
        @memcpy(all[0..self.stores.len], self.stores);
        all[self.stores.len] = .{ .identity = identity, .locator = locator };
        self.stores = all;
    }
    pub fn drop(self: State) !State {
        var next = try self.advance();
        next.dropped = true;
        next.current = null;
        next.pending = null;
        try next.validate();
        return next;
    }
    pub fn acquire(self: State, a: A, token: catalog.Token, generation: u64, now: u64) !State {
        if (now == 0 or self.dropped or self.current != generation or self.retired(generation) or std.mem.allEqual(u8, &token, 0)) return error.LakeIndexGenerationRetired;
        var active: std.ArrayList(Reader) = .empty;
        for (self.readers) |reader| {
            if (std.mem.eql(u8, &token, &reader.token)) return error.LakeIndexReaderTokenReused;
            if (reader.expires_ms +| grace_ms >= now) try active.append(a, reader);
        }
        if (active.items.len == max_readers) return error.LakeIndexReaderCapacityExceeded;
        try active.append(a, .{ .token = token, .generation = generation, .expires_ms = std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle });
        var next = try self.advance();
        next.readers = try active.toOwnedSlice(a);
        try next.validate();
        return next;
    }
    /// A previously admitted reader may renew after replacement/DROP. An
    /// expired session can never revive, so GC's expired-pin snapshot is final.
    pub fn renew(self: State, a: A, token: catalog.Token, now: u64) !State {
        if (now == 0) return error.LakeIndexReaderLeaseExpired;
        const readers = try a.dupe(Reader, self.readers);
        for (readers) |*reader| if (std.mem.eql(u8, &reader.token, &token)) {
            if (now >= reader.expires_ms or self.retired(reader.generation)) return error.LakeIndexReaderLeaseExpired;
            reader.expires_ms = @max(reader.expires_ms, std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle);
            var next = try self.advance();
            next.readers = readers;
            try next.validate();
            return next;
        };
        return error.LakeIndexReaderLeaseExpired;
    }
    pub fn release(self: State, a: A, token: catalog.Token) !State {
        var readers: std.ArrayList(Reader) = .empty;
        for (self.readers) |reader| if (!std.mem.eql(u8, &reader.token, &token)) try readers.append(a, reader);
        var next = try self.advance();
        next.readers = try readers.toOwnedSlice(a);
        try next.validate();
        return next;
    }
    /// Retirement and reader admission are serialized by metadata. The exact
    /// retired set survives interruption; a replacement collector resumes it.
    pub fn beginCollection(self: State, a: A, token: catalog.Token, store: catalog.Digest, now: u64) !State {
        if (now == 0 or std.mem.allEqual(u8, &token, 0) or std.mem.allEqual(u8, &store, 0)) return error.InvalidLakeIndexLifecycle;
        if (self.collection) |previous| {
            if (!std.mem.eql(u8, &previous.store, &store)) return error.LakeIndexCollectionStoreMismatch;
            if (now < previous.expires_ms) return error.LakeIndexCollectionInProgress;
            var next = try self.advance();
            next.collection.?.token = token;
            next.collection.?.expires_ms = std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle;
            return next;
        }
        var retired_generations: std.ArrayList(u64) = .empty;
        for (self.publications) |publication| {
            if (publication.reader_protocol != 24 or !std.meta.eql(publication.namespace, self.namespace) or !std.mem.eql(u8, &publication.signature.store, &store) or self.current == publication.generation) continue;
            const protected = for (self.readers) |reader| {
                if (reader.generation == publication.generation and reader.expires_ms +| grace_ms >= now) break true;
            } else false;
            // An admitted builder can still reuse the current publication.
            if (!protected) try retired_generations.append(a, publication.generation);
        }
        var next = try self.advance();
        const cutoff = if (self.pending) |pending| pending.generation else self.generation +| 1;
        next.collection = .{ .token = token, .store = store, .namespace = self.namespace, .expires_ms = std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle, .upload_cutoff = cutoff, .upload_floor = self.reader_upload_floor orelse cutoff, .retired = try retired_generations.toOwnedSlice(a) };
        try next.validate();
        return next;
    }
    pub fn checkpointCollection(self: State, a: A, token: catalog.Token, previous: ?[]const u8, progress: @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef, now: u64) !State {
        const collection = self.collection orelse return error.LakeIndexCollectionFenceChanged;
        if (now == 0 or now >= collection.expires_ms or !std.mem.eql(u8, &collection.token, &token)) return error.LakeIndexCollectionFenceChanged;
        const old_id = if (collection.progress) |old| old.artifact_id else null;
        if ((old_id == null) != (previous == null) or (old_id != null and !std.mem.eql(u8, old_id.?, previous.?))) return error.LakeIndexCollectionFenceChanged;
        try @import("../serverless/artifacts/store.zig").validateSha256ArtifactIdentity(progress.artifact_id, progress.checksum);
        if (progress.byte_len > 4096) return error.InvalidLakeIndexLifecycle;
        var next = try self.advance();
        var owned = progress;
        owned.artifact_id = try a.dupe(u8, progress.artifact_id);
        owned.checksum = try a.dupe(u8, progress.checksum);
        owned.name = try a.dupe(u8, progress.name);
        next.collection.?.progress = owned;
        // A progressing collector renews ownership; takeover retains this cut
        // and its authenticated checkpoint rather than restarting marking.
        next.collection.?.expires_ms = std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle;
        try next.validate();
        return next;
    }
    pub fn finishCollection(self: State, a: A, token: catalog.Token, now: u64) !State {
        const collection = self.collection orelse return error.LakeIndexCollectionFenceChanged;
        if (now == 0 or now >= collection.expires_ms or !std.mem.eql(u8, &collection.token, &token)) return error.LakeIndexCollectionFenceChanged;
        var retained: std.ArrayList(catalog.Publication) = .empty;
        for (self.publications) |publication| if (!self.retired(publication.generation)) try retained.append(a, publication);
        var readers: std.ArrayList(Reader) = .empty;
        for (self.readers) |reader| if (!self.retired(reader.generation)) try readers.append(a, reader);
        var next = try self.advance();
        next.publications = try retained.toOwnedSlice(a);
        next.readers = try readers.toOwnedSlice(a);
        next.collection = null;
        next.completed_collection = token;
        // Only the swept store can be forgotten. Other bindings may still
        // own uploads from failed builds that never published a root.
        var stores: std.ArrayList(StoreBinding) = .empty;
        for (self.stores) |store| {
            const has_publication = for (next.publications) |publication| {
                if (std.mem.eql(u8, &publication.signature.store, &store.identity)) break true;
            } else false;
            const has_pending = if (next.pending) |pending| std.mem.eql(u8, &pending.signature.store, &store.identity) else false;
            if (!std.mem.eql(u8, &collection.store, &store.identity) or has_publication or has_pending) try stores.append(a, store);
        }
        next.stores = try stores.toOwnedSlice(a);
        try next.validate();
        return next;
    }
};
pub const Mutation = union(enum) {
    acquire: struct { token: catalog.Token, generation: u64, now_ms: u64 },
    renew: struct { token: catalog.Token, now_ms: u64 },
    release: catalog.Token,
    begin_collection: struct { token: catalog.Token, store: catalog.Digest, now_ms: u64 },
    checkpoint_collection: struct { token: catalog.Token, previous: ?[]const u8 = null, progress: @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef, now_ms: u64 },
    finish_collection: struct { token: catalog.Token, now_ms: u64 },
    /// Receipt observation may see a later reader mutation. Unique tokens and
    /// monotonic deadlines prove this command's effect without replaying it.
    pub fn observed(self: Mutation, state: State) bool {
        return switch (self) {
            .acquire => |m| for (state.readers) |reader| {
                if (std.mem.eql(u8, &reader.token, &m.token) and reader.generation == m.generation and reader.expires_ms >= m.now_ms +| lease_ms) break true;
            } else false,
            .renew => |m| for (state.readers) |reader| {
                if (std.mem.eql(u8, &reader.token, &m.token) and reader.expires_ms >= m.now_ms +| lease_ms) break true;
            } else false,
            .release => |token| for (state.readers) |reader| {
                if (std.mem.eql(u8, &reader.token, &token)) break false;
            } else true,
            .begin_collection => |m| if (state.collection) |collection| std.mem.eql(u8, &collection.token, &m.token) else false,
            .checkpoint_collection => |m| if (state.collection) |collection| std.mem.eql(u8, &collection.token, &m.token) and collection.progress != null and std.mem.eql(u8, collection.progress.?.artifact_id, m.progress.artifact_id) else false,
            .finish_collection => |m| if (state.completed_collection) |token| std.mem.eql(u8, &token, &m.token) else false,
        };
    }
    pub fn apply(self: Mutation, a: A, state: State) !State {
        try state.validate();
        return switch (self) {
            .acquire => |m| state.acquire(a, m.token, m.generation, m.now_ms),
            .renew => |m| state.renew(a, m.token, m.now_ms),
            .release => |token| state.release(a, token),
            .begin_collection => |m| state.beginCollection(a, m.token, m.store, m.now_ms),
            .checkpoint_collection => |m| state.checkpointCollection(a, m.token, m.previous, m.progress, m.now_ms),
            .finish_collection => |m| state.finishCollection(a, m.token, m.now_ms),
        };
    }
};
pub fn parse(a: A, bytes: []const u8) !State {
    if (bytes.len > max_bytes) return error.InvalidLakeIndexLifecycle;
    const state = try std.json.parseFromSliceLeaky(State, a, if (bytes.len == 0) "{}" else bytes, .{ .allocate = .alloc_always });
    try state.validate();
    return state;
}
pub fn encode(a: A, state: State) ![]u8 {
    try state.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.LakeIndexRetentionBackpressure;
    return bytes;
}

fn testPublication(generation: u64) catalog.Publication {
    return .{
        .reader_protocol = 24,
        .generation = generation,
        .token = @splat(@intCast(generation)),
        .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) },
        .published_at_ms = generation,
        .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } },
        .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    };
}
test "external lake native lifecycle protects readers across replacement and DROP without reopening retired generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try (State{}).synchronize(a, .{ .generation = 1, .published = testPublication(1) });
    state = try state.acquire(a, @splat(9), 1, 100);
    state = try state.synchronize(a, .{ .generation = 2, .published = testPublication(2) });
    try std.testing.expectError(error.LakeIndexGenerationRetired, state.acquire(a, @splat(8), 1, 101));
    state = try state.renew(a, @splat(9), 101);
    state = try state.beginCollection(a, @splat(7), @splat(4), 102);
    try std.testing.expectEqual(@as(usize, 0), state.collection.?.retired.len);
    state = try state.finishCollection(a, @splat(7), 103);
    state = try state.drop();
    state = try state.renew(a, @splat(9), 104);
    try std.testing.expectError(error.LakeIndexGenerationRetired, state.acquire(a, @splat(8), 2, 105));
    state = try state.beginCollection(a, @splat(7), @splat(4), 105);
    try std.testing.expectEqualSlices(u64, &.{2}, state.collection.?.retired);
    state = try state.finishCollection(a, @splat(7), 106);
    const expiration = state.readers[0].expires_ms;
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, state.renew(a, @splat(9), expiration));
    state = try state.beginCollection(a, @splat(7), @splat(4), expiration + grace_ms + 1);
    try std.testing.expectEqualSlices(u64, &.{1}, state.collection.?.retired);
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, state.renew(a, @splat(9), expiration - 1));
    const encoded = try encode(a, state);
    state = try parse(a, encoded);
    // Crashed collectors leave durable retirement evidence. Takeover resumes
    // the same root set instead of admitting readers into partially swept data.
    state = try state.beginCollection(a, @splat(6), @splat(4), state.collection.?.expires_ms);
    try std.testing.expectEqualSlices(u64, &.{1}, state.collection.?.retired);
    state = try state.finishCollection(a, @splat(6), expiration + grace_ms + lease_ms + 2);
    try std.testing.expectEqual(@as(usize, 0), state.publications.len);
    try std.testing.expectEqual(@as(usize, 0), state.readers.len);
}

test "external lake native collection preserves pending uploads and cannot switch stores during takeover" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try (State{}).synchronize(a, .{ .generation = 1, .published = testPublication(1) });
    state = try state.synchronize(a, .{ .generation = 2, .published = testPublication(1), .pending = .{ .generation = 2, .token = @splat(2), .signature = testPublication(1).signature, .started_at_ms = 1, .lease_expires_at_ms = 100 } });
    state = try state.beginCollection(a, @splat(7), @splat(4), 2);
    try std.testing.expectEqual(@as(u64, 2), state.collection.?.upload_cutoff);
    try std.testing.expectError(error.LakeIndexCollectionInProgress, state.beginCollection(a, @splat(6), @splat(4), 3));
    try std.testing.expectError(error.LakeIndexCollectionStoreMismatch, state.beginCollection(a, @splat(6), @splat(5), state.collection.?.expires_ms));
}

test "external lake native collection protects unleased legacy upload generations during upgrade" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var legacy = testPublication(5);
    legacy.reader_protocol = 0;
    var state = try (State{}).synchronize(a, .{ .generation = 5, .published = legacy });
    state = try state.synchronize(a, .{ .generation = 6, .published = legacy, .pending = .{ .reader_protocol = 24, .generation = 6, .token = @splat(6), .signature = legacy.signature, .started_at_ms = 1, .lease_expires_at_ms = 100 } });
    state = try state.synchronize(a, .{ .generation = 6, .published = testPublication(6) });
    state = try state.drop();
    state = try state.beginCollection(a, @splat(7), @splat(4), 101);
    try std.testing.expectEqual(@as(u64, 6), state.collection.?.upload_floor);
    try std.testing.expectEqualSlices(u64, &.{6}, state.collection.?.retired);
    state = try state.finishCollection(a, @splat(7), 102);
    try std.testing.expectEqual(@as(usize, 1), state.publications.len);
    try std.testing.expectEqual(@as(u64, 5), state.publications[0].generation);
}

test "external lake collection retains failed upload stores through DROP and rotation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const locator: catalog.StoreLocator = .{ .protocol = .filesystem, .bucket = "indexes", .prefix = "", .root = "/artifacts" };
    var building = try (catalog.State{ .namespace = @splat(8) }).begin(testPublication(1).signature, @splat(1), 1, 100);
    building.pending.?.store_locator = locator;
    var state = try (State{}).synchronize(a, building);
    var rotated_signature = building.pending.?.signature;
    rotated_signature.store = @splat(5);
    building = try building.begin(rotated_signature, @splat(2), 2, 100);
    building.pending.?.store_locator = locator;
    state = try state.synchronize(a, building);
    try std.testing.expectEqual(@as(usize, 2), state.stores.len);
    state = try state.drop();
    state = try parse(a, try encode(a, state));
    state = try state.beginCollection(a, @splat(7), @splat(4), 3);
    try std.testing.expectEqual(@as(u64, 1), state.collection.?.upload_floor);
    try std.testing.expectEqual(@as(u64, 3), state.collection.?.upload_cutoff);
    state = try state.finishCollection(a, @splat(7), 4);
    try std.testing.expectEqual(@as(usize, 1), state.stores.len);
    try std.testing.expectEqual(rotated_signature.store, state.stores[0].identity);
    state = try state.beginCollection(a, @splat(6), @splat(5), 5);
    state = try state.finishCollection(a, @splat(6), 6);
    try std.testing.expectEqual(@as(usize, 0), state.stores.len);
}

pub const Write = struct { table_id: u64, expected_revision: u64, mutation: Mutation };
pub const Work = struct { table_id: u64, state: State };
pub const WorkPage = struct { item: ?Work = null, after: ?u64 = null };

pub fn workOnService(svc: anytype, a: A, after: ?u64, request: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.lakeIndexLifecycleWork(a, svc.metadata_group_id, after);
}

pub fn readOnService(svc: anytype, a: A, table_id: u64, request: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.getLakeIndexLifecycle(a, svc.metadata_group_id, table_id);
}

pub fn mutateOnService(svc: anytype, a: A, write: Write, request: @import("antfly_local_sources").api_operation.RequestContext) !void {
    try request.ensureActive();
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const before = try parse(a, try store.getLakeIndexLifecycle(a, svc.metadata_group_id, write.table_id));
    if (before.revision != write.expected_revision) return error.CatalogGenerationChanged;
    _ = try encode(a, try write.mutation.apply(a, before));
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .mutate_lake_index_lifecycle = .{ .table_id = write.table_id, .expected_revision = write.expected_revision, .mutation = write.mutation } });
    svc.waitForTransitionApplied(receipt) catch return error.MetadataMutationOutcomeUnknown;
    const observed = try parse(a, try store.getLakeIndexLifecycle(a, svc.metadata_group_id, write.table_id));
    if (observed.revision <= write.expected_revision or !write.mutation.observed(observed)) return error.MetadataMutationOutcomeUnknown;
}
