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
pub const StoreBinding = struct {
    identity: catalog.Digest,
    locator: catalog.StoreLocator,
    vacuum: ?@import("../serverless/artifacts/cloud_generations.zig").VacuumCut = null,
    // Query ownership is independent of index publications and survives DROP.
    // Clear only after the writer horizon and a complete auxiliary sweep.
    query_until_ms: u64 = 0,
    query_gc_phase: u8 = 0,
    query_gc_receipt: ?catalog.Token = null,
};
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
    current: ?u64 = null,
    pending: ?catalog.Attempt = null,
    checkpoint: ?catalog.Publication = null,
    dropped: bool = false,
    publications: []const catalog.Publication = &.{},
    readers: []const Reader = &.{},
    collection: ?Collection = null,
    completed_collection: ?catalog.Token = null,

    pub fn findPublication(self: State, generation: u64) ?catalog.Publication {
        for (self.publications) |publication| if (publication.generation == generation) return publication;
        return null;
    }
    /// Partial construction is a GC root, but cannot admit a query reader.
    pub fn gcPublications(self: State, a: A) ![]const catalog.Publication {
        const all = try a.alloc(catalog.Publication, self.publications.len + @as(usize, if (self.checkpoint != null) 1 else 0));
        @memcpy(all[0..self.publications.len], self.publications);
        if (self.checkpoint) |saved| all[self.publications.len] = saved;
        return all;
    }
    fn retired(self: State, generation: u64) bool {
        if (self.collection) |collection| for (collection.retired) |old| if (generation == old) return true;
        return false;
    }
    pub fn validate(self: State) !void {
        if (self.version != 1 or self.publications.len > max_publications or self.readers.len > max_readers or self.stores.len > 64) return error.InvalidLakeIndexLifecycle;
        for (self.stores, 0..) |store, index| {
            if (store.query_gc_phase >= 6) return error.InvalidLakeIndexLifecycle;
            try store.locator.validate();
            if (store.vacuum) |cut| {
                try cut.validate();
                if (cut.cutoff > self.generation +| 1) return error.InvalidLakeIndexLifecycle;
            }
            if (std.mem.allEqual(u8, &store.identity, 0)) return error.InvalidLakeIndexLifecycle;
            for (self.stores[0..index]) |other| if (std.mem.eql(u8, &store.identity, &other.identity)) return error.InvalidLakeIndexLifecycle;
        }
        if (self.namespace) |namespace| if (std.mem.allEqual(u8, &namespace, 0)) return error.InvalidLakeIndexLifecycle;
        if (self.checkpoint) |saved| {
            try saved.validate();
            if (self.dropped or saved.generation > self.generation or !std.meta.eql(saved.namespace, self.namespace)) return error.InvalidLakeIndexLifecycle;
        }
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
        next.checkpoint = catalog_state.checkpoint;
        if (catalog_state.checkpoint) |saved| if (saved.store_locator) |locator| try next.retainStore(a, saved.signature.store, locator);
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
    pub fn retainQueryStore(self: State, a: A, binding: StoreBinding, until: u64) !State {
        if (until == 0) return error.InvalidLakeIndexLifecycle;
        var next = try self.advance();
        const owned = try a.dupe(StoreBinding, self.stores);
        next.stores = owned;
        try next.retainStore(a, binding.identity, binding.locator);
        for (@constCast(next.stores)) |*store| if (std.mem.eql(u8, &store.identity, &binding.identity)) {
            store.query_until_ms = @max(store.query_until_ms, until);
        };
        try next.validate();
        return next;
    }
    pub fn queryProgress(self: State, a: A, m: QueryProgress) !State {
        if (m.phase >= 6 or m.now_ms == 0 or std.mem.allEqual(u8, &m.token, 0)) return error.InvalidLakeIndexLifecycle;
        var next = try self.advance();
        var bindings: std.ArrayList(StoreBinding) = .empty;
        for (self.stores) |value| {
            var store = value;
            if (std.mem.eql(u8, &store.identity, &m.store)) {
                store.query_gc_phase = m.phase;
                store.query_gc_receipt = m.token;
                // A concurrent writer renewal is never consumed by an older
                // collector. The revision CAS also fences index retirement.
                if (m.release and store.query_until_ms == m.observed_until_ms and m.now_ms > store.query_until_ms) {
                    store.query_until_ms = 0;
                    const index_owned = (if (self.collection) |c| std.mem.eql(u8, &c.store, &store.identity) else false) or
                        (if (self.pending) |v| std.mem.eql(u8, &v.signature.store, &store.identity) else false) or
                        (if (self.checkpoint) |v| std.mem.eql(u8, &v.signature.store, &store.identity) else false) or
                        for (self.publications) |v| {
                            if (std.mem.eql(u8, &v.signature.store, &store.identity)) break true;
                        } else false;
                    if (!index_owned and store.vacuum == null) continue;
                }
            }
            try bindings.append(a, store);
        }
        next.stores = try bindings.toOwnedSlice(a);
        try next.validate();
        return next;
    }
    pub fn drop(self: State) !State {
        var next = try self.advance();
        next.dropped = true;
        next.checkpoint = null;
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
            if (!std.meta.eql(publication.namespace, self.namespace) or !std.mem.eql(u8, &publication.signature.store, &store) or self.current == publication.generation) continue;
            const protected = for (self.readers) |reader| {
                if (reader.generation == publication.generation and reader.expires_ms +| grace_ms >= now) break true;
            } else false;
            // An admitted builder can still reuse the current publication.
            if (!protected) try retired_generations.append(a, publication.generation);
        }
        var next = try self.advance();
        var cutoff = if (self.pending) |pending| pending.generation else self.generation +| 1;
        if (self.checkpoint) |saved| cutoff = @min(cutoff, saved.generation);
        next.collection = .{ .token = token, .store = store, .namespace = self.namespace, .expires_ms = std.math.add(u64, now, lease_ms) catch return error.InvalidLakeIndexLifecycle, .upload_cutoff = cutoff, .upload_floor = 1, .retired = try retired_generations.toOwnedSlice(a) };
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
        return self.finishCollectionRetainingStore(a, token, now, false);
    }
    pub fn finishCollectionRetainingStore(self: State, a: A, token: catalog.Token, now: u64, retain_store: bool) !State {
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
            if (store.query_until_ms != 0 or retain_store or !std.mem.eql(u8, &collection.store, &store.identity) or has_publication or has_pending or (if (next.checkpoint) |saved| std.mem.eql(u8, &saved.signature.store, &store.identity) else false)) try stores.append(a, store);
        }
        next.stores = try stores.toOwnedSlice(a);
        try next.validate();
        return next;
    }
};
pub const QueryProgress = struct { store: catalog.Digest, observed_until_ms: u64, phase: u8, release: bool, now_ms: u64, token: catalog.Token };
pub const Mutation = union(enum) {
    retain_query_store: struct { binding: StoreBinding, until_ms: u64 },
    query_gc_progress: QueryProgress,
    acquire: struct { token: catalog.Token, generation: u64, now_ms: u64 },
    renew: struct { token: catalog.Token, now_ms: u64 },
    release: catalog.Token,
    begin_collection: struct { token: catalog.Token, store: catalog.Digest, now_ms: u64 },
    checkpoint_collection: struct { token: catalog.Token, previous: ?[]const u8 = null, progress: @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef, now_ms: u64 },
    finish_collection: struct { token: catalog.Token, now_ms: u64, retain_store: bool = false, vacuum: ?@import("../serverless/artifacts/cloud_generations.zig").VacuumCut = null },
    /// Decoded Raft commands own every JSON string independently of the input.
    pub fn deinitOwned(self: *Mutation, a: A) void {
        switch (self.*) {
            .retain_query_store => |m| {
                inline for (.{ "bucket", "prefix", "root" }) |name| a.free(@field(m.binding.locator, name));
                if (m.binding.locator.connection) |name| a.free(name);
            },
            .checkpoint_collection => |m| {
                if (m.previous) |previous| a.free(previous);
                inline for (.{ "name", "artifact_id", "checksum" }) |name| a.free(@field(m.progress, name));
            },
            else => {},
        }
        self.* = undefined;
    }
    pub fn cloneOwned(self: Mutation, a: A) !Mutation {
        return switch (self) {
            .retain_query_store => |m| blk: {
                var next = m;
                const locator = m.binding.locator;
                next.binding.locator.bucket = try a.dupe(u8, locator.bucket);
                errdefer a.free(next.binding.locator.bucket);
                next.binding.locator.prefix = try a.dupe(u8, locator.prefix);
                errdefer a.free(next.binding.locator.prefix);
                next.binding.locator.root = try a.dupe(u8, locator.root);
                errdefer a.free(next.binding.locator.root);
                next.binding.locator.connection = if (locator.connection) |name| try a.dupe(u8, name) else null;
                break :blk .{ .retain_query_store = next };
            },
            .checkpoint_collection => |m| blk: {
                var next = m;
                next.previous = if (m.previous) |previous| try a.dupe(u8, previous) else null;
                errdefer if (next.previous) |previous| a.free(previous);
                next.progress.name = try a.dupe(u8, m.progress.name);
                errdefer a.free(next.progress.name);
                next.progress.artifact_id = try a.dupe(u8, m.progress.artifact_id);
                errdefer a.free(next.progress.artifact_id);
                next.progress.checksum = try a.dupe(u8, m.progress.checksum);
                break :blk .{ .checkpoint_collection = next };
            },
            else => self,
        };
    }
    /// Receipt observation may see a later reader mutation. Unique tokens and
    /// monotonic deadlines prove this command's effect without replaying it.
    pub fn observed(self: Mutation, state: State) bool {
        return switch (self) {
            .retain_query_store => |m| for (state.stores) |store| {
                if (std.mem.eql(u8, &store.identity, &m.binding.identity) and store.query_until_ms >= m.until_ms) break true;
            } else false,
            .query_gc_progress => |m| for (state.stores) |store| {
                if (std.mem.eql(u8, &store.identity, &m.store)) break if (store.query_gc_receipt) |token| std.mem.eql(u8, &token, &m.token) else false;
            } else m.release,
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
            .retain_query_store => |m| state.retainQueryStore(a, m.binding, m.until_ms),
            .query_gc_progress => |m| state.queryProgress(a, m),
            .acquire => |m| state.acquire(a, m.token, m.generation, m.now_ms),
            .renew => |m| state.renew(a, m.token, m.now_ms),
            .release => |token| state.release(a, token),
            .begin_collection => |m| state.beginCollection(a, m.token, m.store, m.now_ms),
            .checkpoint_collection => |m| state.checkpointCollection(a, m.token, m.previous, m.progress, m.now_ms),
            .finish_collection => |m| blk: {
                var next = try state.finishCollectionRetainingStore(a, m.token, m.now_ms, m.retain_store);
                if (m.vacuum) |cut| {
                    const collection = state.collection orelse return error.InvalidLakeIndexLifecycle;
                    try cut.validate();
                    if (cut.floor != collection.upload_floor or cut.cutoff != collection.upload_cutoff) return error.InvalidLakeIndexLifecycle;
                    const progress = collection.progress orelse return error.InvalidLakeIndexLifecycle;
                    const scope = (try @import("../serverless/artifacts/store.zig").uploadScopeFromArtifactId(progress.artifact_id)) orelse return error.InvalidLakeIndexLifecycle;
                    if (!std.mem.eql(u8, &scope.domain, &cut.domain)) return error.InvalidLakeIndexLifecycle;
                    const bindings = try a.dupe(StoreBinding, next.stores);
                    for (bindings) |*binding| if (std.mem.eql(u8, &binding.identity, &collection.store)) {
                        binding.vacuum = cut;
                    };
                    next.stores = bindings;
                    try next.validate();
                }
                break :blk next;
            },
        };
    }
    /// Admission runs before any command is appended. A publication race is
    /// an explicit snapshot conflict, also across the native runtime ABI.
    pub fn preflight(self: Mutation, scratch: A, state: State) !void {
        const next = self.apply(scratch, state) catch |err| switch (err) {
            error.LakeIndexGenerationRetired => return error.ExternalLakeSnapshotMismatch,
            else => return err,
        };
        const bytes = try encode(scratch, next);
        scratch.free(bytes);
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
        .reader_protocol = catalog.native_reader_protocol,
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
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, (Mutation{ .acquire = .{ .token = @splat(8), .generation = 1, .now_ms = 101 } }).preflight(a, state));
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

test "external lake native collection rejects obsolete reader authority" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var obsolete = testPublication(5);
    obsolete.reader_protocol = 0;
    try std.testing.expectError(error.InvalidLakeIndexCatalog, (State{}).synchronize(arena.allocator(), .{ .generation = 5, .published = obsolete }));
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
    state = try state.finishCollectionRetainingStore(a, @splat(7), 4, true);
    try std.testing.expectEqual(@as(usize, 2), state.stores.len);
    state = try parse(a, try encode(a, state));
    state = try state.beginCollection(a, @splat(9), @splat(4), 4);
    state = try state.finishCollection(a, @splat(9), 4);
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
    try write.mutation.preflight(a, before);
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .mutate_lake_index_lifecycle = .{ .table_id = write.table_id, .expected_revision = write.expected_revision, .mutation = write.mutation } });
    svc.waitForTransitionApplied(receipt) catch return error.MetadataMutationOutcomeUnknown;
    const observed = try parse(a, try store.getLakeIndexLifecycle(a, svc.metadata_group_id, write.table_id));
    if (observed.revision <= write.expected_revision or !write.mutation.observed(observed)) return error.MetadataMutationOutcomeUnknown;
}

test "external lake resumable checkpoint is fenced and protected without becoming queryable" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const saved = testPublication(2);
    var building = try (catalog.State{ .generation = 1, .published = testPublication(1) }).begin(saved.signature, saved.token, saved.published_at_ms - 1, 100);
    building.pending.?.reader_protocol = saved.reader_protocol;
    building.pending.?.store_locator = saved.store_locator;
    const checkpointed = try building.saveCheckpoint(saved, saved.published_at_ms);
    try std.testing.expectEqual(@as(u64, 1), checkpointed.published.?.generation);
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, building.saveCheckpoint(saved, saved.published_at_ms + 100));
    var forged = saved;
    forged.token = @splat(9);
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, building.saveCheckpoint(forged, saved.published_at_ms));
    const failed = try checkpointed.fail(saved.token, "interrupted", 200);
    building = try failed.begin(saved.signature, @splat(3), 200, 100);
    try std.testing.expect(building.checkpoint != null);
    var changed = saved.signature;
    changed.source = @splat(8);
    try std.testing.expect((try failed.begin(changed, @splat(3), 200, 100)).checkpoint == null);
    var lifecycle = try (State{}).synchronize(ca, failed);
    try std.testing.expectEqual(@as(usize, 2), (try lifecycle.gcPublications(ca)).len);
    try std.testing.expectError(error.LakeIndexGenerationRetired, lifecycle.acquire(ca, @splat(4), 2, 200));
    lifecycle = try lifecycle.beginCollection(ca, @splat(5), saved.signature.store, 200);
    try std.testing.expectEqual(@as(u64, 2), lifecycle.collection.?.upload_cutoff);
    try std.testing.expect((try lifecycle.drop()).checkpoint == null);
}

test "external lake query ownership survives DROP rotation restart and stale cleanup" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const locator: catalog.StoreLocator = .{ .protocol = .filesystem, .bucket = "indexes", .prefix = "", .root = "/artifacts" };
    const old: StoreBinding = .{ .identity = @splat(4), .locator = locator };
    var state = try (State{}).retainQueryStore(a, old, 100);
    try std.testing.expect(state.namespace == null);
    state = try state.retainQueryStore(a, .{ .identity = @splat(5), .locator = locator }, 200);
    state = try state.drop();
    state = try parse(a, try encode(a, state));
    state = try state.queryProgress(a, .{ .store = old.identity, .observed_until_ms = 100, .phase = 4, .release = false, .now_ms = 101, .token = @splat(1) });
    state = try parse(a, try encode(a, state));
    try std.testing.expectEqual(@as(u8, 4), state.stores[0].query_gc_phase);
    // A writer renews after discovery. A stale collector cannot consume it.
    state = try state.retainQueryStore(a, old, 300);
    state = try state.queryProgress(a, .{ .store = old.identity, .observed_until_ms = 100, .phase = 5, .release = true, .now_ms = 201, .token = @splat(2) });
    try std.testing.expectEqual(@as(u64, 300), state.stores[0].query_until_ms);
    state = try state.queryProgress(a, .{ .store = old.identity, .observed_until_ms = 300, .phase = 0, .release = true, .now_ms = 301, .token = @splat(3) });
    try std.testing.expectEqual(@as(usize, 1), state.stores.len);
    try std.testing.expectEqual(@as([32]u8, @splat(5)), state.stores[0].identity);
    state = try state.queryProgress(a, .{ .store = @splat(5), .observed_until_ms = 200, .phase = 0, .release = true, .now_ms = 301, .token = @splat(4) });
    try std.testing.expectEqual(@as(usize, 0), state.stores.len);
}

test "external lake index collection cannot forget independent query work" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const locator: catalog.StoreLocator = .{ .protocol = .filesystem, .bucket = "indexes", .prefix = "", .root = "/artifacts" };
    var building = try (catalog.State{ .namespace = @splat(8) }).begin(testPublication(1).signature, @splat(1), 1, 100);
    building.pending.?.store_locator = locator;
    var state = try (State{}).synchronize(a, building);
    state = try state.retainQueryStore(a, state.stores[0], 1000);
    state = try state.drop();
    state = try state.beginCollection(a, @splat(7), state.stores[0].identity, 3);
    state = try state.finishCollection(a, @splat(7), 4);
    try std.testing.expectEqual(@as(usize, 1), state.stores.len);
    try std.testing.expectEqual(@as(u64, 1000), state.stores[0].query_until_ms);
}

test "external lake lifecycle command ownership unwinds every allocation failure" {
    const Probe = struct {
        fn run(a: A, mutation: Mutation) !void {
            var owned = try mutation.cloneOwned(a);
            defer owned.deinitOwned(a);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{Mutation{ .retain_query_store = .{ .binding = .{ .identity = @splat(4), .locator = .{ .protocol = .s3, .connection = "artifacts", .bucket = "bucket", .prefix = "prefix", .root = "https://objects.example" } }, .until_ms = 100 } }});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{Mutation{ .checkpoint_collection = .{ .token = @splat(7), .previous = "previous", .progress = .{ .kind = .doc_values, .name = "checkpoint", .artifact_id = "artifact", .byte_len = 42, .checksum = "checksum" }, .now_ms = 100 } }});
}
