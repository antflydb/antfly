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

//! Exact native aggregate roots authenticate bounded immutable column blocks.
const std = @import("std");
const local = @import("antfly_local_sources");
const operators = local.sql_operators;
const recipes = local.sql_aggregate_materialization;
const spill = local.sql_spill;
const stores = @import("../serverless/artifacts/store.zig");
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const ReadContext = local.serverless_query_lake_read_context.Context;
const Execution = local.common_request_admission.runnable_scheduler.Execution;
pub const CachedRead = struct {
    /// Admission mode for field-sort windows; never changes artifact authority.
    lazy_lineage: bool = false,
    metrics: ?*@import("lake_query_metrics.zig").Metrics = null,
    cache: *local.serverless_query_lake_serving_cache.Cache,
    scope: [32]u8,
    context: local.serverless_query_lake_read_context.Context,
};

pub fn readArtifact(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead) ![]u8 {
    return readArtifactFor(a, store, ref, cancellation, cached, if (cached) |cache| cache.context else .{});
}
pub fn readArtifactFor(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead, context: ReadContext) ![]u8 {
    var loader = struct {
        store: stores.ArtifactStore,
        ref: ChunkRef,
        cancellation: Cancellation,
        context: local.serverless_query_lake_read_context.Context,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.getVerifiedAllocFor(alloc, self.ref.artifact_id, self.ref.byte_len, self.ref.checksum, self.cancellation, self.context);
        }
    }{ .store = store, .ref = ref, .cancellation = cancellation, .context = context };
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    try cancellation.check();
    if (cached) |cache| return cache.cache.readImmutableAlloc(a, cache.scope, ref.artifact_id, std.math.cast(usize, ref.byte_len) orelse return error.ArtifactTooLarge, try stores.sha256DigestFromChecksum(ref.checksum), context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
    return @TypeOf(loader).load(&loader, a);
}
/// Authenticate a page range independently of its containing immutable object.
/// Range cache identities bind the pack, offset, length and page digest. A cold
/// seek reads only that page; it never downloads the whole navigation pack.
pub fn readPageRange(a: A, store: stores.ArtifactStore, ref: ChunkRef, offset: u64, length: usize, digest: [32]u8, cancellation: Cancellation, cached: ?CachedRead) ![]u8 {
    return readPageRangeFor(a, store, ref, offset, length, digest, cancellation, cached, if (cached) |cache| cache.context else .{});
}
pub fn readPageRangeFor(a: A, store: stores.ArtifactStore, ref: ChunkRef, offset: u64, length: usize, digest: [32]u8, cancellation: Cancellation, cached: ?CachedRead, context: ReadContext) ![]u8 {
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    if (offset > ref.byte_len or length > ref.byte_len - offset or length > @import("../serverless/graph_segment/page_tree.zig").max_page_bytes) return error.InvalidGraphPage;
    if (offset == 0 and length == ref.byte_len) {
        const object_digest = try stores.sha256DigestFromChecksum(ref.checksum);
        if (!std.mem.eql(u8, &object_digest, &digest)) return error.ArtifactIntegrityMismatch;
        return readArtifactFor(a, store, ref, cancellation, cached, context);
    }
    const Loader = struct {
        store: stores.ArtifactStore,
        ref: ChunkRef,
        offset: u64,
        length: usize,
        digest: [32]u8,
        token: Cancellation,
        context: ReadContext,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = try self.store.getRangeAllocFor(alloc, self.ref.artifact_id, self.offset, self.length, self.token, self.context);
            errdefer alloc.free(bytes);
            if (bytes.len != self.length) return error.ArtifactIntegrityMismatch;
            try stores.validatePayloadSha256For(bytes, &std.fmt.bytesToHex(&self.digest, .lower), self.token, self.context);
            return bytes;
        }
    };
    var loader: Loader = .{ .store = store, .ref = ref, .offset = offset, .length = length, .digest = digest, .token = cancellation, .context = context };
    try cancellation.check();
    if (cached) |cache| {
        var identity: [256]u8 = undefined;
        const key = try std.fmt.bufPrint(&identity, "{s}:page:{x}:{x}", .{ ref.artifact_id, offset, length });
        return cache.cache.readImmutableAlloc(a, cache.scope, key, length, digest, context, .{ .ptr = &loader, .load = Loader.load });
    }
    return Loader.load(&loader, a);
}
/// Explicit synchronous publication read. Verified transport completes and CPU
/// admission resumes before callers parse metadata or advance reducers.
/// Serving and asynchronous readers never borrow this serial capability.
pub fn readPublicationArtifact(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation) ![]u8 {
    var phase = try store.beginIo(1);
    defer phase.abort();
    var backend = store;
    backend.io_phase = null;
    const bytes = readArtifact(a, backend, ref, cancellation, null) catch |err| return phase.failed(err);
    errdefer a.free(bytes);
    try phase.finish();
    return bytes;
}
/// Borrow a verified bounded block until the caller has copied/decoded its
/// requested range. Current authority is checked even when payloads are warm.
pub fn readArtifactLease(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    return readArtifactLeaseFor(a, store, ref, cancellation, cached, if (cached) |cache| cache.context else .{});
}
pub fn readArtifactLeaseFor(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead, context: ReadContext) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    const Loader = struct {
        store: stores.ArtifactStore,
        ref: ChunkRef,
        cancellation: Cancellation,
        context: local.serverless_query_lake_read_context.Context,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.getVerifiedAllocFor(alloc, self.ref.artifact_id, self.ref.byte_len, self.ref.checksum, self.cancellation, self.context);
        }
    };
    var loader: Loader = .{ .store = store, .ref = ref, .cancellation = cancellation, .context = context };
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    try cancellation.check();
    if (cached) |cache| return cache.cache.readImmutableBlockLease(a, cache.scope, ref.artifact_id, std.math.cast(usize, ref.byte_len) orelse return error.ArtifactTooLarge, try stores.sha256DigestFromChecksum(ref.checksum), context, .{ .ptr = &loader, .load = Loader.load });
    return .{ .heap = .{ .alloc = a, .bytes = try Loader.load(&loader, a) } };
}

pub const metadata_version: u16 = 2;
pub fn supportsMetadataVersion(version: u16) bool {
    return version == 1 or version == metadata_version or version == 6 or isTree(version) or isPartitioned(version);
}
pub fn isTree(version: u16) bool {
    return version == 7 or version == 8;
}
pub fn isPartitioned(version: u16) bool {
    return version == 3 or version == 4 or version == 5;
}
pub const partition_leaf_groups: u64 = 1024;
pub const max_partition_depth: u8 = 9;
pub fn partitionFanout(depth: u8) usize {
    return if (depth == 0) 64 else 4;
}
fn partitionShift(depth: u8) u6 {
    return if (depth == 0) 0 else @intCast(6 + (depth - 1) * 2);
}
const group_keys = @import("lake_index_aggregate_keys.zig");
const ordered_keys = @import("lake_index_aggregate_order.zig");
const native_partial = local.sql_aggregate_partial;
pub const state_tree = @import("lake_index_aggregate_tree.zig");
pub const max_root_bytes = 4 * 1024 * 1024;
pub const max_block_bytes = 4 * 1024 * 1024;
pub const max_blocks = 8192;
pub const ChunkRef = struct { artifact_id: []const u8, checksum: []const u8, byte_len: u64 };
const Block = struct {
    artifact: ChunkRef,
    rows: u16,
    bounds: ?group_keys.Bounds = null,
    state_layout: u8 = 0,
    pub fn jsonStringify(self: Block, writer: anytype) !void {
        if (self.state_layout != 0) return writer.write(.{ .artifact = self.artifact, .rows = self.rows, .bounds = self.bounds, .state_layout = self.state_layout });
        if (self.bounds != null) return writer.write(.{ .artifact = self.artifact, .rows = self.rows, .bounds = self.bounds });
        return writer.write(.{ .artifact = self.artifact, .rows = self.rows });
    }
};
/// Build-local proof returned by a publisher or an authenticated parent.
/// It is bound to the exact logical child and never changes the wire format.
pub const StateProof = struct {
    logical: [32]u8,
    physical: ?[32]u8,
    recipe: [32]u8,
    groups: u64,
    slot: u16,
    depth: ?u8,
    bounds: ?group_keys.Bounds = null,
};
pub const Partition = struct {
    bucket: u8,
    artifact: Ref,
    groups: u64,
    proof: ?StateProof = null,
    bounds: ?group_keys.Bounds = null,
    pub fn jsonStringify(self: Partition, writer: anytype) !void {
        if (self.bounds != null) return writer.write(.{ .bucket = self.bucket, .artifact = self.artifact, .groups = self.groups, .bounds = self.bounds });
        return writer.write(.{ .bucket = self.bucket, .artifact = self.artifact, .groups = self.groups });
    }
};
const Root = struct {
    format: []const u8 = "native-sql-aggregate-v2",
    name: []const u8,
    recipe: recipes.Recipe,
    groups: u64,
    blocks: []const Block = &.{},
    partitions: []const Partition = &.{},
    radix_depth: u8 = 0,
    bounds: ?group_keys.Bounds = null,
    key_index: ?ordered_keys.Index = null,
    state_tree: ?state_tree.Index = null,
    row_state: ?RowState = null,
    cohort: ?ChunkRef = null,
    state_recipe: ?recipes.Recipe = null,
    state_slot: ?u16 = null,
    pub fn jsonStringify(self: Root, writer: anytype) !void {
        if (self.state_tree != null) return writer.write(.{ .format = self.format, .name = self.name, .recipe = self.recipe, .groups = self.groups, .state_tree = self.state_tree, .row_state = self.row_state, .state_recipe = self.state_recipe, .state_slot = self.state_slot });
        if (self.key_index != null) return writer.write(.{ .format = self.format, .name = self.name, .recipe = self.recipe, .groups = self.groups, .blocks = self.blocks, .partitions = self.partitions, .radix_depth = self.radix_depth, .bounds = self.bounds, .key_index = self.key_index, .cohort = self.cohort, .state_recipe = self.state_recipe, .state_slot = self.state_slot });
        if (self.bounds != null) return writer.write(.{ .format = self.format, .name = self.name, .recipe = self.recipe, .groups = self.groups, .blocks = self.blocks, .partitions = self.partitions, .radix_depth = self.radix_depth, .bounds = self.bounds, .cohort = self.cohort, .state_recipe = self.state_recipe, .state_slot = self.state_slot });
        return writer.write(.{ .format = self.format, .name = self.name, .recipe = self.recipe, .groups = self.groups, .blocks = self.blocks, .partitions = self.partitions, .radix_depth = self.radix_depth, .cohort = self.cohort, .state_recipe = self.state_recipe, .state_slot = self.state_slot });
    }
};

/// A publication binds each logical root and a bounded child slot map to one
/// shared descriptor. Child identities and recipe slots are checked on demand.
const Cohort = struct {
    version: u8 = 1,
    recipe: recipes.Recipe,
    depth: u8,
    groups: u64,
    members: []const [32]u8,
    children: []const Child,
    const Child = struct { bucket: u8, groups: u64, identity: [32]u8, slots: []const u16 };
};
fn rootCommitment(a: A, input: Root) ![32]u8 {
    var root = input;
    root.cohort = null;
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return hash;
}
fn physicalIdentity(a: A, root: Root) !?[32]u8 {
    if (root.state_tree) |index| {
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .recipe = root.state_recipe.?, .index = index }, .{});
        defer a.free(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return digest;
    }
    if (root.cohort) |ref| return try stores.sha256DigestFromChecksum(ref.checksum);
    if (root.partitions.len != 0 or root.state_recipe == null) return null;
    const bytes = if (root.key_index != null) try std.json.Stringify.valueAlloc(a, .{ .recipe = root.state_recipe.?, .groups = root.groups, .blocks = root.blocks, .bounds = root.bounds, .key_index = root.key_index }, .{}) else if (root.bounds != null) try std.json.Stringify.valueAlloc(a, .{ .recipe = root.state_recipe.?, .groups = root.groups, .blocks = root.blocks, .bounds = root.bounds }, .{}) else try std.json.Stringify.valueAlloc(a, .{ .recipe = root.state_recipe.?, .groups = root.groups, .blocks = root.blocks }, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return hash;
}
fn decodeCohort(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead) !Cohort {
    return decodeCohortFor(a, store, ref, token, cached, if (cached) |cache| cache.context else .{});
}
fn decodeCohortFor(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead, context: ReadContext) !Cohort {
    if (ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifactFor(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached, context);
    defer a.free(bytes);
    const cohort = try std.json.parseFromSliceLeaky(Cohort, a, bytes, .{ .allocate = .alloc_always });
    if (cohort.version != 1 or cohort.recipe.inputs.len == 0 or cohort.recipe.inputs.len > 256 or cohort.members.len != cohort.recipe.inputs.len or cohort.children.len > 64 or cohort.depth > max_partition_depth) return error.InvalidNativeAggregateArtifact;
    for (cohort.children) |child| if (child.slots.len != cohort.members.len or std.mem.allEqual(u8, &child.identity, 0)) return error.InvalidNativeAggregateArtifact;
    return cohort;
}
fn cohortReference(ref: ChunkRef) Ref {
    return .{ .kind = .algebraic_segment, .metadata_version = 5, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
/// Reader-specific commitment checks run even when the common proof is cached.
fn validateCohort(a: A, root: Root, cohort: Cohort) !void {
    const state = root.state_recipe orelse return error.InvalidNativeAggregateArtifact;
    const slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact;
    if (!cohort.recipe.eql(state) or cohort.depth != root.radix_depth or cohort.groups != root.groups or cohort.members.len != state.inputs.len or slot >= cohort.members.len or cohort.children.len != root.partitions.len) return error.InvalidNativeAggregateArtifact;
    if (!std.mem.eql(u8, &cohort.members[slot], &try rootCommitment(a, root))) return error.InvalidNativeAggregateArtifact;
    for (cohort.children, root.partitions) |child, part| {
        if (child.bucket != part.bucket or child.groups != part.groups) return error.InvalidNativeAggregateArtifact;
    }
}
/// Bind the build hint to immutable object identity and aggregate
/// interpretation: namespace, checksum, kind, size, version and logical name.
fn logicalIdentity(ref: Ref) ![32]u8 {
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("native-aggregate-state-proof-v1");
    for ([_][]const u8{ ref.artifact_id, ref.checksum, @tagName(ref.kind), ref.name }) |bytes| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, bytes.len, .little);
        hash.update(&size);
        hash.update(bytes);
    }
    var fields: [10]u8 = undefined;
    std.mem.writeInt(u64, fields[0..8], ref.byte_len, .little);
    std.mem.writeInt(u16, fields[8..10], ref.metadata_version, .little);
    hash.update(&fields);
    return hash.finalResult();
}
fn stateProof(a: A, ref: Ref, root: Root) !StateProof {
    return .{ .logical = try logicalIdentity(ref), .physical = try physicalIdentity(a, root), .recipe = root.recipe.fingerprint(), .groups = root.groups, .slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact, .depth = if (isPartitioned(ref.metadata_version)) root.radix_depth else null, .bounds = root.bounds };
}
/// Publication-local fallback for legacy or alias references without a parent
/// proof. No request authority or decoded root is retained; the full immutable
/// artifact identity, interpretation and exact recipe bind every hit.
pub const ProofMemo = struct {
    entries: std.AutoHashMapUnmanaged([32]u8, StateProof) = .empty,
    limit: usize = 4096,
    pub fn deinit(self: *ProofMemo, a: A) void {
        self.entries.deinit(a);
    }
    pub fn resolve(self: *ProofMemo, a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, token: Cancellation) !StateProof {
        try token.check();
        const key = try logicalIdentity(ref);
        if (self.entries.get(key)) |proof| {
            if (!std.mem.eql(u8, &proof.recipe, &recipe.fingerprint())) return error.InvalidNativeAggregateArtifact;
            return proof;
        }
        const reader = try Reader.open(a, store, ref, recipe, token);
        defer reader.cursor().close(reader);
        const proof = try stateProof(a, ref, reader.root);
        if (self.limit != 0) {
            if (self.entries.count() >= @min(self.limit, 4096)) self.entries.clearRetainingCapacity();
            // A failed optional memo allocation must not invalidate a proof
            // already checked against the durable immutable child.
            self.entries.put(a, key, proof) catch return proof;
        }
        return proof;
    }
};
fn loadValidatedRoot(a: A, store: stores.ArtifactStore, artifact: Ref, token: Cancellation) !Root {
    const root = try loadRoot(a, store, artifact, token, null);
    if (root.cohort) |ref| try validateCohort(a, root, try decodeCohort(a, store, cohortReference(ref), token, null));
    return root;
}
/// Build planning reads authenticated recipe metadata, without aggregate blocks.
pub fn loadRecipe(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation) !recipes.Recipe {
    if (!supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const root = try loadValidatedRoot(a, store, artifact, cancellation);
    return root.recipe;
}

/// GC traverses authenticated root references without reading every leaf.
/// Unknown formats fail closed rather than guessing that a root has no edges.
pub const RowState = struct {
    rows: ?state_tree.tree.Ref = null,
    reverse: ?state_tree.tree.Ref = null,
    /// Ordered (group, reducer slot, typed value, row ordinal) multiset.
    /// Version distinguishes an indexed all-NULL cohort from legacy state.
    extrema_version: u8 = 0,
    extrema: ?state_tree.tree.Ref = null,
    reductions_version: u8 = 0,
    reductions: ?state_tree.tree.Ref = null,
};
pub const LeafReferences = struct { blocks: []const ChunkRef, key_index: ?ordered_keys.Index, state_tree: ?state_tree.Index, row_state: ?RowState = null };
pub fn leafReferences(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation) !LeafReferences {
    var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    const root = try loadValidatedRoot(scratch.allocator(), store, artifact, cancellation);
    const refs = try a.alloc(ChunkRef, if (artifact.metadata_version == 6) 0 else root.blocks.len);
    for (refs, root.blocks[0..refs.len]) |*ref, block| ref.* = .{ .artifact_id = try a.dupe(u8, block.artifact.artifact_id), .checksum = try a.dupe(u8, block.artifact.checksum), .byte_len = block.artifact.byte_len };
    // Embedded navigation has no external GC edge. Do not return its borrowed
    // page bytes after releasing the temporary metadata owner.
    const external_index = if (root.key_index) |index| (if (index.inline_page.len == 0) index else null) else null;
    return .{ .blocks = refs, .key_index = external_index, .state_tree = root.state_tree, .row_state = root.row_state };
}
pub fn descendantsAlloc(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation) ![]const ChunkRef {
    return (try leafReferences(a, store, artifact, cancellation)).blocks;
}

/// Consumes a completed reducer, including disk partitions. The caller owns
/// the fenced upload capability. Only a bounded output page is retained.
pub fn publish(a: A, result_alloc: A, store: *stores.ArtifactStore, name: []const u8, group: *operators.Grouped, recipe: recipes.Recipe, cancellation: Cancellation) !Ref {
    if (recipe.inputs.len != 1) return error.InvalidNativeAggregateArtifact;
    const references = try publishCohort(a, result_alloc, store, &.{name}, group, recipe, cancellation);
    defer result_alloc.free(references);
    return references[0];
}

/// One immutable block stores the common keys and all typed reducer slots.
/// Distinct logical roots authenticate their slot, complete state recipe and
/// shared block directory. Older readers decline metadata version 2 safely.
pub fn publishCohort(a: A, result_alloc: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, cancellation: Cancellation) ![]Ref {
    return publishCohortProven(a, result_alloc, store, names, group, recipe, cancellation, null);
}
pub fn publishCohortProven(a: A, result_alloc: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, cancellation: Cancellation, proofs: ?[]StateProof) ![]Ref {
    if (proofs) |values| if (values.len != names.len) return error.InvalidNativeAggregateArtifact;
    if (names.len == 0 or names.len != recipe.inputs.len or names.len > 256 or group.specs.len != names.len) return error.InvalidNativeAggregateArtifact;
    for (names, group.specs, recipe.inputs) |name, spec, input| if (name.len == 0 or !std.meta.eql(spec, input.spec)) return error.InvalidNativeAggregateArtifact;
    var input = PartialStream.init(group, recipe);
    const remaining = group.groups.items.len - group.result_cursor;
    if (group.external == null and remaining != 0 and remaining <= 256) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        return publishPackedRows(a, result_alloc, store, names, try input.next(arena.allocator()), recipe, cancellation, proofs, input.state_layout);
    }
    return publishPartialStream(a, result_alloc, store, names, &input, recipe, cancellation, proofs);
}

/// New native file reductions use exact ordered state pages. Unsupported or
/// spilled inputs retain the established radix/block publisher.
pub fn publishKeyed(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, manager: *spill.Manager, token: Cancellation) ![]Ref {
    if (!state_tree.eligible(recipe)) return publishPartitioned(a, out, store, names, group, recipe, manager, token);
    const record_layout = try state_tree.record.Layout.init(a, recipe);
    defer record_layout.deinit(a);
    var admitted = true;
    var ordinal: u64 = 0;
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var sort = spill.Sort.init(a, manager, &.{.{}}, 8 * 1024 * 1024);
    defer sort.deinit();
    sort.encoded_bytes = true;
    sort.shared_runs = true;
    sort.parallel_runs = false;
    while (true) {
        _ = scratch.reset(.free_all);
        const sa = scratch.allocator();
        const input = (try group.nextTypedPartialResult(sa)) orelse break;
        if (!group_keys.compatible(recipe, input.keys)) admitted = false;
        var fallback: [8]u8 = undefined;
        std.mem.writeInt(u64, &fallback, ordinal, .big);
        const key = group_keys.encode(sa, input.keys) catch |err| failed: {
            if (err != error.RelationalIndexKeyTooLarge and err != error.InvalidColumnValue and err != error.InvalidRelationalIndexBound) return err;
            admitted = false;
            break :failed &fallback;
        };
        const value = try state_tree.record.encodeWithLayout(sa, input, record_layout);
        if (key.len > state_tree.tree.max_key_bytes or key.len + value.len + 8 > state_tree.tree.max_record_bytes) admitted = false;
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = key })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = value })}, .ordinal = ordinal });
        ordinal += 1;
    }
    if (!admitted) {
        var input: PartialStream = .{ .encoded_sort = &sort, .recipe = recipe, .state_layout = 1 };
        var pool = try spill.Partitions.init(manager);
        defer pool.deinit();
        return publishRadixStream(a, out, store, names, &input, recipe, &pool, token, 0, null);
    }
    var access: @import("lake_index_native_directory.zig").Access = undefined;
    access.initPublication(store.*, scope.domain, token);
    var builder: state_tree.tree.SortedBuilder = .{ .alloc = a, .store = access.backing() };
    defer builder.deinit();
    while (true) {
        _ = scratch.reset(.free_all);
        const input = (try sort.next(scratch.allocator())) orelse break;
        try builder.add(input.keys[0].value.string, input.values[0].value.string);
    }
    const page = try builder.finish();
    return publishTree(a, out, store, names, recipe, .{ .domain = scope.domain, .layout = 2, .page = page }, token);
}
const TreeControl = struct {
    format: []const u8 = "native-sql-aggregate-v7",
    recipe: recipes.Recipe,
    names: []const []const u8,
    index: state_tree.Index,
    row_state: ?RowState = null,
    fn root(self: @This(), name: []const u8) !Root {
        for (self.names, 0..) |candidate, slot| if (std.mem.eql(u8, name, candidate)) return .{ .format = self.format, .name = candidate, .recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot..][0..1] }, .groups = self.index.count(), .state_recipe = self.recipe, .state_slot = @intCast(slot), .state_tree = self.index, .row_state = self.row_state };
        return error.InvalidNativeAggregateArtifact;
    }
};
fn loadTreeControl(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead) !TreeControl {
    return loadTreeControlFor(a, store, ref, token, cached, if (cached) |cache| cache.context else .{});
}
fn loadTreeControlFor(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead, context: ReadContext) !TreeControl {
    return loadTreeControlInnerFor(a, store, ref, token, cached, false, context);
}
fn loadTreeControlInner(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead, publication: bool) !TreeControl {
    return loadTreeControlInnerFor(a, store, ref, token, cached, publication, if (cached) |cache| cache.context else .{});
}
fn loadTreeControlInnerFor(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead, publication: bool, context: ReadContext) !TreeControl {
    if (ref.kind != .algebraic_segment or !isTree(ref.metadata_version) or ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const chunk: ChunkRef = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
    const bytes = if (publication) try readPublicationArtifact(a, store, chunk, token) else try readArtifactFor(a, store, chunk, token, cached, context);
    defer a.free(bytes);
    const value = try std.json.parseFromSliceLeaky(TreeControl, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, value.format, if (ref.metadata_version == 8) "native-sql-aggregate-v8" else "native-sql-aggregate-v7") or value.names.len == 0 or value.names.len > 256 or value.names.len != value.recipe.inputs.len) return error.InvalidNativeAggregateArtifact;
    for (value.names, 0..) |name, i| {
        if (name.len == 0) return error.InvalidNativeAggregateArtifact;
        for (value.names[0..i]) |previous| if (std.mem.eql(u8, name, previous)) return error.InvalidNativeAggregateArtifact;
        var selected = ref;
        selected.name = name;
        try validateRoot(selected, try value.root(name));
    }
    return value;
}
pub const RowSeed = struct { index: state_tree.Index, rows: RowState };
pub fn rowSeed(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, token: Cancellation) !?RowSeed {
    if (ref.metadata_version != 8) return null;
    const control = try loadTreeControlInner(a, store, ref, token, null, true);
    if (!control.recipe.eql(recipe) or control.index.layout != 2 or control.row_state == null) return null;
    return .{ .index = control.index, .rows = control.row_state.? };
}
pub fn publishTree(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, recipe: recipes.Recipe, index: state_tree.Index, token: Cancellation) ![]Ref {
    return publishTreeWithRows(a, out, store, names, recipe, index, null, token);
}
pub fn publishTreeWithRows(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, recipe: recipes.Recipe, index: state_tree.Index, rows: ?RowState, token: Cancellation) ![]Ref {
    if (names.len != recipe.inputs.len or names.len == 0 or !state_tree.eligible(recipe) or (index.layout != 1 and index.layout != 2)) return error.InvalidNativeAggregateArtifact;
    const bytes = try std.json.Stringify.valueAlloc(a, TreeControl{ .format = if (index.layout == 2) "native-sql-aggregate-v8" else "native-sql-aggregate-v7", .recipe = recipe, .names = names, .index = index, .row_state = rows }, .{});
    defer a.free(bytes);
    if (bytes.len > max_root_bytes) return error.NativeAggregateArtifactTooLarge;
    var saved = try store.putWithCancellation(bytes, token);
    defer saved.deinit(store.allocator);
    const refs = try out.alloc(Ref, names.len);
    var completed: usize = 0;
    errdefer {
        for (refs[0..completed]) |ref| {
            out.free(ref.artifact_id);
            out.free(ref.checksum);
        }
        out.free(refs);
    }
    for (refs, names) |*ref, name| {
        const id = try out.dupe(u8, saved.artifact_id);
        errdefer out.free(id);
        const checksum = try out.dupe(u8, saved.checksum);
        ref.* = .{ .kind = .algebraic_segment, .metadata_version = if (index.layout == 2) 8 else 7, .name = name, .artifact_id = id, .checksum = checksum, .byte_len = saved.byte_len };
        completed += 1;
    }
    return refs;
}
/// Finished unique partials cross publication boundaries without another hash
/// reducer. State checkpoints remain opaque until serving imports them.
const PartialStream = struct {
    group: ?*operators.Grouped = null,
    recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{} },
    state_layout: u8 = 0,
    rows: ?[]const operators.Row = null,
    file: ?*spill.Sequential = null,
    encoded_sort: ?*spill.Sort = null,
    retirement: ?struct { pool: *spill.Partitions, index: usize } = null,
    offset: u64 = 0,
    finished: bool = false,
    fn init(group: *operators.Grouped, recipe: recipes.Recipe) PartialStream {
        const native = native_partial.physicalWidth(recipe.inputs) <= 1024 and for (recipe.inputs) |input| {
            if (!native_partial.physicalEligible(input.spec)) break false;
        } else true;
        return .{ .group = group, .recipe = recipe, .state_layout = @intFromBool(native) };
    }
    fn finish(self: *PartialStream) void {
        if (self.retirement) |owner| owner.pool.close(owner.index);
        self.retirement = null;
        self.file = null;
        self.finished = true;
    }
    fn next(self: *PartialStream, a: A) ![]const operators.Row {
        if (self.finished) return &.{};
        if (self.encoded_sort) |sort| {
            var rows: std.ArrayList(operators.Row) = .empty;
            while (rows.items.len < 256) {
                const input = (try sort.next(a)) orelse break;
                const block = try state_tree.block(a, input.values[0].value.string, self.recipe);
                const key_cells = try a.alloc(local.sql_scalar.Datum, block.keys.len);
                const cells = try a.alloc(local.sql_scalar.Datum, block.values.len);
                for (key_cells, 0..) |*cell, i| cell.* = try block.keyCell(0, i);
                for (cells, 0..) |*cell, i| cell.* = try block.cell(0, i);
                try rows.append(a, .{ .keys = key_cells, .values = cells, .ordinal = input.ordinal });
            }
            return rows.items;
        }
        if (self.rows) |rows| {
            const begin: usize = @intCast(self.offset);
            const end = @min(rows.len, begin + 256);
            self.offset = end;
            return rows[begin..end];
        }
        if (self.file) |file| {
            if (self.offset == file.size) return &.{};
            // Coalesce small scatter records into full output pages. Clone
            // payloads only across the spill read-buffer ownership boundary.
            const maximum: usize = @intCast(@min(256, file.size - self.offset));
            const rows = try a.alloc(operators.Row, maximum);
            const first = try file.readBatchBorrowed(self.offset, maximum);
            const width = first.rows[0].values.len;
            const key_width = first.rows[0].keys.len;
            const values = try a.alloc(local.sql_scalar.Datum, maximum * width);
            const keys = try a.alloc(local.sql_scalar.Datum, maximum * key_width);
            var count: usize = 0;
            while (count < maximum) {
                const batch = try file.readBatchBorrowed(self.offset, maximum - count);
                for (batch.rows) |row| {
                    if (row.values.len != width or row.keys.len != key_width) return error.InvalidSqlSpill;
                    const value_cells = values[count * width ..][0..width];
                    const key_cells = keys[count * key_width ..][0..key_width];
                    for (value_cells, row.values) |*cell, value| cell.* = try operators.cloneDatum(a, value);
                    for (key_cells, row.keys) |*cell, value| cell.* = try operators.cloneDatum(a, value);
                    rows[count] = .{ .values = value_cells, .keys = key_cells, .ordinal = row.ordinal };
                    count += 1;
                }
                self.offset = batch.following;
            }
            return rows;
        }
        var rows: std.ArrayList(operators.Row) = .empty;
        while (rows.items.len < 256) {
            const row = (if (self.state_layout == 0) try self.group.?.nextPartialResult(a) else try self.group.?.nextTypedPartialResult(a)) orelse break;
            var values = row.aggregates;
            if (self.state_layout != 0) {
                const cells = try a.alloc(local.sql_scalar.Datum, native_partial.physicalWidth(self.recipe.inputs));
                for (self.recipe.inputs, 0..) |input, column| {
                    const state = try native_partial.read(a, row, column, input.spec);
                    const physical = try native_partial.physicalCells(a, state);
                    const offset = native_partial.physicalOffset(self.recipe.inputs, column);
                    for (native_partial.physicalColumns(input.spec), 0..) |lane, index| cells[offset + index] = physical[lane];
                }
                values = cells;
            }
            try rows.append(a, .{ .keys = row.keys, .values = values, .ordinal = row.ordinal });
        }
        return rows.items;
    }
};
fn publishPartialStream(a: A, result_alloc: A, store: *stores.ArtifactStore, names: []const []const u8, input: *PartialStream, recipe: recipes.Recipe, cancellation: Cancellation, proofs: ?[]StateProof) ![]Ref {
    var control = std.heap.ArenaAllocator.init(a);
    defer control.deinit();
    const ca = control.allocator();
    var key_builder: ordered_keys.Builder = .{ .store = store, .token = cancellation, .recipe = recipe };
    defer key_builder.deinit();
    var blocks: std.ArrayList(Block) = .empty;
    var groups: u64 = 0;
    var output_bytes: u64 = 0;
    while (true) {
        try cancellation.check();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const pa = page.allocator();
        const rows = try input.next(pa);
        if (rows.len == 0) break;
        for (rows) |row| if (row.keys.len != recipe.keys.len or row.values.len != (if (input.state_layout != 0) native_partial.physicalWidth(recipe.inputs) else recipe.inputs.len)) return error.InvalidSqlBackendResponse;
        var begin: usize = 0;
        while (begin < rows.len) {
            try cancellation.check();
            if (blocks.items.len == max_blocks) return error.NativeAggregateArtifactTooLarge;
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var count = rows.len - begin;
            const encoded = encode: while (true) {
                const bytes = spill.encodeColumnarBlockAlloc(scratch.allocator(), rows[begin..][0..count], max_block_bytes) catch |err| {
                    if (err != error.SqlProgramLimitExceeded or count == 1) return err;
                    _ = scratch.reset(.free_all);
                    count = (count + 1) / 2;
                    continue;
                };
                break :encode bytes;
            };
            var upload = store.*;
            upload.allocator = ca;
            output_bytes = std.math.add(u64, output_bytes, encoded.len) catch return error.NativeAggregateArtifactTooLarge;
            if (output_bytes > 512 * 1024 * 1024) return error.NativeAggregateArtifactTooLarge;
            const artifact = try upload.putWithCancellation(encoded, cancellation);
            defer if (artifact.provider_version) |version| upload.allocator.free(version);
            try key_builder.add(a, rows[begin..][0..count], @intCast(blocks.items.len));
            try blocks.append(ca, .{ .state_layout = input.state_layout, .artifact = .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum }, .rows = @intCast(count), .bounds = try group_keys.rowBoundsTyped(scratch.allocator(), rows[begin..][0..count], recipe) });
            groups = std.math.add(u64, groups, count) catch return error.NativeAggregateArtifactTooLarge;
            begin += count;
        }
    }
    input.finish();
    const references = try result_alloc.alloc(Ref, names.len);
    var initialized: usize = 0;
    errdefer {
        for (references[0..initialized]) |ref| {
            result_alloc.free(ref.artifact_id);
            result_alloc.free(ref.checksum);
        }
        result_alloc.free(references);
    }
    for (references, names, 0..) |*reference, name, slot| {
        var root_arena = std.heap.ArenaAllocator.init(a);
        defer root_arena.deinit();
        const root: Root = .{ .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot .. slot + 1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = groups, .blocks = blocks.items, .bounds = allBounds(blocks.items), .key_index = key_builder.result() };
        const bytes = try std.json.Stringify.valueAlloc(root_arena.allocator(), root, .{});
        if (bytes.len > max_root_bytes or bytes.len > 512 * 1024 * 1024 -| output_bytes) return error.NativeAggregateArtifactTooLarge;
        output_bytes += bytes.len;
        var upload = store.*;
        upload.allocator = result_alloc;
        const artifact = try upload.putWithCancellation(bytes, cancellation);
        defer if (artifact.provider_version) |version| upload.allocator.free(version);
        reference.* = .{ .kind = .algebraic_segment, .name = name, .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .metadata_version = metadata_version };
        initialized += 1;
        if (proofs) |values| values[slot] = try stateProof(a, reference.*, root);
    }
    return references;
}

/// Cache only authenticated immutable directory metadata. The expected query
/// recipe and the live reader lease are checked on every open, including hits.
fn loadRoot(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation, cached: ?CachedRead) !Root {
    return loadRootFor(a, store, artifact, cancellation, cached, if (cached) |cache| cache.context else .{});
}
fn loadRootFor(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation, cached: ?CachedRead, context: ReadContext) !Root {
    if (isTree(artifact.metadata_version)) return (try loadTreeControlFor(a, store, artifact, cancellation, cached, context)).root(artifact.name);
    if (artifact.metadata_version == 6) return (try loadPackedFor(a, store, artifact, cancellation, cached, context)).root(artifact.name);
    if (artifact.kind != .algebraic_segment or !supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifactFor(a, store, .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum }, cancellation, cached, context);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    try validateRoot(artifact, root);
    return root;
}
fn allBounds(entries: anytype) ?group_keys.Bounds {
    if (entries.len == 0) return null;
    var result: ?group_keys.Bounds = null;
    for (entries) |entry| result = group_keys.envelope(result, entry.bounds orelse return null);
    return result;
}
fn validateRoot(artifact: Ref, root: Root) !void {
    if (!std.mem.eql(u8, root.format, if (artifact.metadata_version == 1) "native-sql-aggregate-v1" else if (isTree(artifact.metadata_version)) (if (artifact.metadata_version == 8) "native-sql-aggregate-v8" else "native-sql-aggregate-v7") else if (artifact.metadata_version == 3) "native-sql-aggregate-v3" else if (artifact.metadata_version == 5) "native-sql-aggregate-v5" else if (artifact.metadata_version == 4) "native-sql-aggregate-v4" else "native-sql-aggregate-v2") or !std.mem.eql(u8, root.name, artifact.name) or root.blocks.len > max_blocks) return error.InvalidNativeAggregateArtifact;
    if (artifact.metadata_version == 1) {
        if (root.state_recipe != null or root.state_slot != null) return error.InvalidNativeAggregateArtifact;
    } else {
        const state_recipe = root.state_recipe orelse return error.InvalidNativeAggregateArtifact;
        const slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact;
        if (state_recipe.inputs.len > 256 or slot >= state_recipe.inputs.len or !root.recipe.eql(.{ .keys = state_recipe.keys, .inputs = state_recipe.inputs[slot .. slot + 1] })) return error.InvalidNativeAggregateArtifact;
    }
    if (isTree(artifact.metadata_version)) {
        const index = root.state_tree orelse return error.InvalidNativeAggregateArtifact;
        if (index.layout != (if (artifact.metadata_version == 8) @as(u8, 2) else 1)) return error.InvalidNativeAggregateArtifact;
        if (root.blocks.len != 0 or root.partitions.len != 0 or root.key_index != null or root.bounds != null or root.cohort != null or !state_tree.eligible(root.state_recipe.?) or index.count() != root.groups) return error.InvalidNativeAggregateArtifact;
        const scope = (try stores.uploadScopeFromArtifactId(artifact.artifact_id)) orelse return error.InvalidNativeAggregateArtifact;
        if (!std.mem.eql(u8, &index.domain, &scope.domain)) return error.InvalidNativeAggregateArtifact;
        if (index.page) |page| try page.validate();
        if (root.row_state) |rows| {
            if (artifact.metadata_version != 8) return error.InvalidNativeAggregateArtifact;
            if (rows.rows) |page| try page.validate();
            if (rows.reverse) |page| try page.validate();
            if (rows.extrema_version > 1 or (rows.extrema_version == 0 and rows.extrema != null)) return error.InvalidNativeAggregateArtifact;
            if (rows.extrema) |page| try page.validate();
            if (rows.reductions_version > 2 or (rows.reductions_version == 0 and rows.reductions != null)) return error.InvalidNativeAggregateArtifact;
            if (rows.reductions) |page| try page.validate();
            if ((if (rows.rows) |page| page.records else 0) != (if (rows.reverse) |page| page.records else 0)) return error.InvalidNativeAggregateArtifact;
        }
        return;
    }
    if (root.state_tree != null or root.row_state != null) return error.InvalidNativeAggregateArtifact;
    if (!isPartitioned(artifact.metadata_version) and root.partitions.len != 0) return error.InvalidNativeAggregateArtifact;
    if (isPartitioned(artifact.metadata_version) and (root.blocks.len != 0 or root.partitions.len > 64 or root.radix_depth > max_partition_depth)) return error.InvalidNativeAggregateArtifact;
    if ((artifact.metadata_version == 5) != (root.cohort != null)) return error.InvalidNativeAggregateArtifact;
    if (root.cohort) |ref| {
        if (ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    }
    if (root.key_index) |index| {
        try index.page.validate();
        if (index.inline_page.len != 0 and (index.inline_page.len != @as(usize, index.page.bytes) * 2 or index.page.height != 0)) return error.InvalidNativeAggregateArtifact;
        const scope = (try stores.uploadScopeFromArtifactId(artifact.artifact_id)) orelse return error.InvalidNativeAggregateArtifact;
        if (!std.mem.eql(u8, &index.domain, &scope.domain)) return error.InvalidNativeAggregateArtifact;
        if (root.partitions.len != 0 or index.page.records != root.groups or std.mem.allEqual(u8, &index.domain, 0)) return error.InvalidNativeAggregateArtifact;
    }
    if (root.bounds) |bounds| {
        try bounds.validate();
        const expected = if (root.partitions.len != 0) allBounds(root.partitions) else allBounds(root.blocks);
        if (expected == null or !std.meta.eql(bounds, expected.?)) return error.InvalidNativeAggregateArtifact;
    }
    var count: u64 = 0;
    for (root.partitions, 0..) |part, i| {
        if (part.bucket >= partitionFanout(root.radix_depth) or (i != 0 and root.partitions[i - 1].bucket >= part.bucket) or part.artifact.kind != .algebraic_segment or (part.artifact.metadata_version != 2 and part.artifact.metadata_version != 6 and !(artifact.metadata_version >= 4 and (part.artifact.metadata_version == 4 or part.artifact.metadata_version == 5) and root.radix_depth < max_partition_depth)) or !std.mem.eql(u8, part.artifact.name, root.name) or part.artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        if (part.bounds) |bounds| try bounds.validate();
        try stores.validateSha256ArtifactIdentity(part.artifact.artifact_id, part.artifact.checksum);
        count = std.math.add(u64, count, part.groups) catch return error.InvalidNativeAggregateArtifact;
    }
    for (root.blocks) |block| {
        if (block.bounds) |bounds| try bounds.validate();
        if (block.state_layout > 1) return error.InvalidNativeAggregateArtifact;
        if (block.state_layout == 1) for ((root.state_recipe orelse root.recipe).inputs) |input| if (!native_partial.physicalEligible(input.spec)) return error.InvalidNativeAggregateArtifact;
        if (block.rows == 0 or block.rows > 256 or block.artifact.byte_len > max_block_bytes) return error.InvalidNativeAggregateArtifact;
        try stores.validateSha256ArtifactIdentity(block.artifact.artifact_id, block.artifact.checksum);
        count = std.math.add(u64, count, block.rows) catch return error.InvalidNativeAggregateArtifact;
    }
    if (count != root.groups) return error.InvalidNativeAggregateArtifact;
}

/// A small leaf is one immutable object: all logical roots and their shared
/// column block. Names select logical slots; the whole pack is authenticated
/// before a directory or cell is exposed. Legacy standalone blocks stay valid.
const PackedControl = struct {
    version: u8 = 1,
    recipe: recipes.Recipe,
    names: []const []const u8,
    blocks: []const Block,
    key_index: ?ordered_keys.Index = null,
};
const Packed = struct {
    control: PackedControl,
    bytes: []const u8,
    fn rootAt(self: Packed, slot: usize) Root {
        return .{ .name = self.control.names[slot], .recipe = .{ .keys = self.control.recipe.keys, .inputs = self.control.recipe.inputs[slot..][0..1] }, .state_recipe = self.control.recipe, .state_slot = @intCast(slot), .groups = self.control.blocks[0].rows, .blocks = self.control.blocks, .bounds = self.control.blocks[0].bounds, .key_index = self.control.key_index };
    }
    fn root(self: Packed, name: []const u8) !Root {
        for (self.control.names, 0..) |value, slot| if (std.mem.eql(u8, value, name)) return self.rootAt(slot);
        return error.InvalidNativeAggregateArtifact;
    }
};
const pack_magic = "AGPK\x01";
fn loadPacked(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead) !Packed {
    return loadPackedFor(a, store, ref, token, cached, if (cached) |cache| cache.context else .{});
}
fn loadPackedFor(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead, context: ReadContext) !Packed {
    if (ref.kind != .algebraic_segment or ref.metadata_version != 6 or ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifactFor(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached, context);
    if (bytes.len < 9 or !std.mem.eql(u8, bytes[0..5], pack_magic)) return error.InvalidNativeAggregateArtifact;
    const length = std.mem.readInt(u32, bytes[5..9], .little);
    if (length > bytes.len - 9) return error.InvalidNativeAggregateArtifact;
    const control = try std.json.parseFromSliceLeaky(PackedControl, a, bytes[9..][0..length], .{ .allocate = .alloc_always });
    if (control.version != 1 or control.names.len == 0 or control.names.len > 256 or control.recipe.inputs.len != control.names.len or control.blocks.len != 1) return error.InvalidNativeAggregateArtifact;
    const body = bytes[9 + length ..];
    const block = control.blocks[0];
    if (body.len != block.artifact.byte_len or body.len > max_block_bytes) return error.InvalidNativeAggregateArtifact;
    try stores.validateSha256ArtifactIdentity(block.artifact.artifact_id, block.artifact.checksum);
    const digest = try stores.sha256DigestFromChecksum(block.artifact.checksum);
    var actual: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &actual, .{});
    if (!std.mem.eql(u8, &digest, &actual)) return error.InvalidNativeAggregateArtifact;
    const pack: Packed = .{ .control = control, .bytes = body };
    for (control.names, 0..) |name, slot| {
        if (name.len == 0) return error.InvalidNativeAggregateArtifact;
        for (control.names[0..slot]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidNativeAggregateArtifact;
        var logical = ref;
        logical.name = name;
        try validateRoot(logical, pack.rootAt(slot));
    }
    return pack;
}
fn publishPackedRows(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, rows: []const operators.Row, recipe: recipes.Recipe, token: Cancellation, proofs: ?[]StateProof, state_layout: u8) ![]Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    try token.check();
    const block = spill.encodeColumnarBlockAlloc(pa, rows, max_block_bytes) catch |err| {
        if (err != error.SqlProgramLimitExceeded or rows.len <= 1) return err;
        var input: PartialStream = .{ .rows = rows, .recipe = recipe, .state_layout = state_layout };
        return publishPartialStream(a, out, store, names, &input, recipe, token, proofs);
    };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(block, &digest, .{});
    const checksum = try pa.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    const id = try std.fmt.allocPrint(pa, "sha256:{s}", .{checksum});
    var key_builder: ordered_keys.Builder = .{ .store = store, .token = token, .recipe = recipe, .allow_inline = true };
    defer key_builder.deinit();
    try key_builder.add(a, rows, 0);
    const roots = try pa.alloc(Root, names.len);
    const blocks = [_]Block{.{ .state_layout = state_layout, .artifact = .{ .artifact_id = id, .checksum = checksum, .byte_len = block.len }, .rows = @intCast(rows.len), .bounds = try group_keys.rowBoundsTyped(pa, rows, recipe) }};
    for (roots, names, 0..) |*root, name, slot| root.* = .{ .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = rows.len, .blocks = &blocks, .bounds = blocks[0].bounds, .key_index = key_builder.result() };
    var control_arena = std.heap.ArenaAllocator.init(a);
    defer control_arena.deinit();
    var control_budget: local.sql_memory_budget = .{ .backing = control_arena.allocator(), .limit = max_root_bytes -| (9 + block.len), .monotonic = true };
    const control = std.json.Stringify.valueAlloc(control_budget.allocator(), PackedControl{ .recipe = recipe, .names = names, .blocks = &blocks, .key_index = key_builder.result() }, .{}) catch |err| {
        if (err != error.OutOfMemory or !control_budget.isLocallyExhausted()) return err;
        var input: PartialStream = .{ .rows = rows, .recipe = recipe, .state_layout = state_layout };
        return publishPartialStream(a, out, store, names, &input, recipe, token, proofs);
    };
    const length = std.math.add(usize, 9 + control.len, block.len) catch return error.NativeAggregateArtifactTooLarge;
    if (length > max_root_bytes) {
        var input: PartialStream = .{ .rows = rows, .recipe = recipe, .state_layout = state_layout };
        return publishPartialStream(a, out, store, names, &input, recipe, token, proofs);
    }
    const bytes = try pa.alloc(u8, length);
    @memcpy(bytes[0..5], pack_magic);
    std.mem.writeInt(u32, bytes[5..9], @intCast(control.len), .little);
    @memcpy(bytes[9..][0..control.len], control);
    @memcpy(bytes[9 + control.len ..], block);
    var uploaded = try store.putWithCancellation(bytes, token);
    defer uploaded.deinit(store.allocator);
    const refs = try out.alloc(Ref, names.len);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| {
            out.free(ref.artifact_id);
            out.free(ref.checksum);
        }
        out.free(refs);
    }
    const shared_physical = if (proofs != null) try physicalIdentity(a, roots[0]) else null;
    for (refs, names, roots, 0..) |*ref, name, root, index| {
        const artifact_id = try out.dupe(u8, uploaded.artifact_id);
        const hash = out.dupe(u8, uploaded.checksum) catch |err| {
            out.free(artifact_id);
            return err;
        };
        ref.* = .{ .kind = .algebraic_segment, .metadata_version = 6, .name = name, .artifact_id = artifact_id, .checksum = hash, .byte_len = uploaded.byte_len };
        initialized += 1;
        if (proofs) |values| values[index] = .{ .logical = try logicalIdentity(ref.*), .physical = shared_physical, .recipe = root.recipe.fingerprint(), .groups = root.groups, .slot = @intCast(index), .depth = null, .bounds = root.bounds };
    }
    return refs;
}

/// One selected physical state column. Dictionary checkpoints are expanded once
/// and all row headers borrow those immutable reducers. The encoded block lease
/// is acquired first, so interpretation never nests another decode-cache flight.
const TypedColumn = struct {
    states: ?[]const operators.Aggregate = null,
    decoded_states: usize = 0,
    fn physicalIdentityAt(block: spill.ColumnarBlock, row: usize, column: usize, spec: operators.AggregateSpec) !?[native_partial.physical_width * 2]u64 {
        var tuple: [native_partial.physical_width * 2]u64 = @splat(0);
        for (native_partial.physicalColumns(spec), 0..) |_, lane| {
            const physical = column + lane;
            if (try block.dictionaryIdentity(row, physical, false)) |id| {
                tuple[lane * 2] = 1;
                tuple[lane * 2 + 1] = id;
                continue;
            }
            const value = try block.cell(row, physical);
            if (value.sql_null) continue;
            if (value.numeric != null or value.array != null or value.patterns != null) return null;
            switch (value.value) {
                .integer => |n| {
                    tuple[lane * 2] = 2;
                    tuple[lane * 2 + 1] = @bitCast(n);
                },
                .bool => |v| {
                    tuple[lane * 2] = 3;
                    tuple[lane * 2 + 1] = @intFromBool(v);
                },
                else => return null,
            }
        }
        return tuple;
    }
    const Args = struct { block: spill.ColumnarBlock, recipe: recipes.Recipe, column: usize, state_layout: u8 = 0 };
    fn decode(a: A, _: stores.ArtifactStore, _: Ref, token: Cancellation, _: ?CachedRead, args: Args) !TypedColumn {
        const block = args.block;
        if (block.values.len != (if (args.state_layout != 0) native_partial.physicalWidth(args.recipe.inputs) else args.recipe.inputs.len) or block.keys.len != args.recipe.keys.len or args.column >= args.recipe.inputs.len) return error.InvalidNativeAggregateArtifact;
        const states = try a.alloc(operators.Aggregate, block.count());
        var dictionary: std.AutoHashMapUnmanaged([native_partial.physical_width * 2]u64, usize) = .empty;
        defer dictionary.deinit(a);
        var decoded: usize = 0;
        for (states, 0..) |*state, row| {
            try token.check();
            const identity = if (args.state_layout == 0) legacy: {
                const id = (try block.dictionaryIdentity(row, args.column, false)) orelse break :legacy null;
                var tuple: [native_partial.physical_width * 2]u64 = @splat(0);
                tuple[0] = id;
                break :legacy @as(?@TypeOf(tuple), tuple);
            } else try physicalIdentityAt(block, row, native_partial.physicalOffset(args.recipe.inputs, args.column), args.recipe.inputs[args.column].spec);
            if (identity) |id| if (dictionary.get(id)) |prior| {
                state.* = states[prior];
                continue;
            };
            state.* = if (args.state_layout == 0) try native_partial.decode(a, try block.cell(row, args.column), args.recipe.inputs[args.column].spec) else try native_partial.readPhysical(a, block, row, native_partial.physicalOffset(args.recipe.inputs, args.column), args.recipe.inputs[args.column].spec);
            decoded += 1;
            if (identity) |id| try dictionary.put(a, id, row);
        }
        return .{ .states = states, .decoded_states = decoded };
    }
};

pub const Reader = struct {
    const PartitionMap = struct { states: []const u16, children: ?[]const PartitionMap = null };
    a: A,
    store: stores.ArtifactStore,
    cancellation: Cancellation,
    cached: ?CachedRead = null,
    context: ReadContext = .{},
    execution: ?*Execution = null,
    pack_data: ?Packed = null,
    pack_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(Packed) = null,
    tree_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(TreeControl) = null,
    control: std.heap.ArenaAllocator,
    page: std.heap.ArenaAllocator,
    root: Root,
    metadata: ?@import("lake_index_decoded_metadata.zig").Owned(Root) = null,
    block: ?spill.ColumnarBlock = null,
    typed_columns: []const @import("lake_index_decoded_metadata.zig").Owned(TypedColumn) = &.{},
    /// Bound the complete selected page, independently of each cached column.
    max_typed_bytes: usize = 32 * 1024 * 1024,
    block_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(spill.ColumnarBlock) = null,
    cohort_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(Cohort) = null,
    block_index: usize = 0,
    position: usize = 0,
    indexed_position: ?usize = null,
    emitted: u64 = 0,
    output_slots: ?[]const u16 = null,
    state_slots: ?[]const u16 = null,
    child: ?*Reader = null,
    partition_index: usize = 0,
    partition_states: ?[]const PartitionMap = null,
    cohort: ?Cohort = null,
    blocks_decoded: usize = 0,
    point_hash: ?u64 = null,
    key_range: ?group_keys.Range = null,
    ordered_scan: bool = false,
    ordered_plan: ?*ordered_keys.Plan(Reader) = null,
    reverse: bool = false,
    raw_states: bool = false,
    selection_spill: ?*spill.Manager = null,
    selection_memory: usize = 8 * 1024 * 1024,
    selection_started: bool = false,
    tree_record_layout: ?*const state_tree.record.Layout = null,
    tree_cursor: ?state_tree.tree.Cursor = null,
    tree_access: ?*@import("lake_index_native_directory.zig").Access = null,
    tree_maximum: ?usize = null,
    point_key: ?[]const u8 = null,
    tree_frame: ?*TreeFrame = null,
    tree_frame_position: usize = 0,
    tree_pending: ?state_tree.tree.Cursor.Record = null,
    tree_loaded: usize = 0,
    tree_prefetch: ?local.sql_parallel_scheduler.Task(anyerror!?*TreeFrame) = null,
    tree_prefetches: usize = 0,
    selection_rows_left: usize = std.math.maxInt(usize),
    selection_quota: local.sql_catalog.ScanQuota = .{ .capacity = 4096, .row_limit = std.math.maxInt(usize), .page_limit = std.math.maxInt(usize) },
    const TreeFrame = struct {
        allocator: A,
        arena: std.heap.ArenaAllocator,
        records: []const state_tree.tree.Cursor.Record,
        fn destroy(self: *@This()) void {
            self.arena.deinit();
            self.allocator.destroy(self);
        }
    };
    fn fetchTreeFrame(self: *Reader) anyerror!?*TreeFrame {
        try self.cancellation.check();
        try self.currentContext().ensureActive();
        const backing = local.sql_memory_budget.reclaimingAllocator(self.a);
        const frame = try backing.create(TreeFrame);
        frame.* = .{ .allocator = backing, .arena = .init(backing), .records = &.{} };
        errdefer frame.destroy();
        const a = frame.arena.allocator();
        var records: std.ArrayList(state_tree.tree.Cursor.Record) = .empty;
        var bytes: usize = 0;
        const byte_limit = @min(@as(usize, 4 * 1024 * 1024), @max(@as(usize, 64 * 1024), self.selection_memory / 4));
        while (records.items.len < 256) {
            if (self.tree_maximum) |goal| if (self.tree_loaded >= goal) break;
            const record = if (self.tree_pending) |record| record else (try self.tree_cursor.?.next()) orelse break;
            self.tree_pending = null;
            const size = record.key.len + record.value.len;
            if (records.items.len != 0 and size > byte_limit -| bytes) {
                self.tree_pending = record;
                break;
            }
            try records.append(a, .{ .key = try a.dupe(u8, record.key), .value = try a.dupe(u8, record.value) });
            bytes += size;
            self.tree_loaded += 1;
        }
        if (records.items.len == 0) {
            frame.destroy();
            return null;
        }
        frame.records = records.items;
        return frame;
    }
    fn retireTree(self: *Reader) void {
        if (self.tree_prefetch) |*task| {
            if (task.future != null) if (task.cancel(self.cached.?.context.io.?)) |result| {
                if (result) |frame| frame.destroy();
            } else |_| {};
        }
        self.tree_prefetch = null;
        if (self.tree_frame) |frame| frame.destroy();
        self.tree_frame = null;
    }

    /// The caller selects an authorized publication first. Errors after that
    /// selection abort the query, never combine partials with a fresh scan.
    pub fn open(a: A, store: stores.ArtifactStore, artifact: Ref, recipe: recipes.Recipe, cancellation: Cancellation) !*Reader {
        return openWithCache(a, store, artifact, recipe, cancellation, null);
    }
    pub fn openWithCache(a: A, store: stores.ArtifactStore, artifact: Ref, recipe: recipes.Recipe, cancellation: Cancellation, cached: ?CachedRead) !*Reader {
        return openWithCacheFor(a, store, artifact, recipe, cancellation, cached, null);
    }
    pub fn openWithCacheFor(a: A, store: stores.ArtifactStore, artifact: Ref, recipe: recipes.Recipe, cancellation: Cancellation, cached: ?CachedRead, execution: ?*Execution) !*Reader {
        const context: ReadContext = if (cached) |cache| cache.context else .{};
        const active = if (execution) |owner| context.withExecution(owner) else context;
        try active.ensureActive();
        const active_cache: ?CachedRead = if (cached) |cache| active_read: {
            var current = cache;
            current.context = active;
            break :active_read current;
        } else null;
        if (artifact.kind != .algebraic_segment or !supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        const self = try a.create(Reader);
        errdefer a.destroy(self);
        var control = std.heap.ArenaAllocator.init(a);
        errdefer control.deinit();
        const ca = control.allocator();
        var pack_ref = artifact;
        pack_ref.name = ""; // Every logical slot shares one authenticated pack.
        const pack_metadata = if (artifact.metadata_version == 6 and active_cache != null) try @import("lake_index_decoded_metadata.zig").acquire(Packed, active_cache.?, store, pack_ref, cancellation, loadPacked) else null;
        errdefer if (pack_metadata) |owned| owned.release();
        const pack_data = if (pack_metadata) |owned| owned.value.* else if (artifact.metadata_version == 6) try loadPackedFor(ca, store, artifact, cancellation, active_cache, active) else null;
        const tree_metadata = if (isTree(artifact.metadata_version) and active_cache != null) try @import("lake_index_decoded_metadata.zig").acquire(TreeControl, active_cache.?, store, pack_ref, cancellation, loadTreeControl) else null;
        errdefer if (tree_metadata) |owned| owned.release();
        const metadata = if (active_cache != null and pack_data == null and tree_metadata == null) metadata: {
            const cache = active_cache.?;
            break :metadata try @import("lake_index_decoded_metadata.zig").acquire(Root, cache, store, artifact, cancellation, loadRoot);
        } else null;
        errdefer if (metadata) |owned| owned.release();
        const root = if (pack_data) |pack| try pack.root(artifact.name) else if (tree_metadata) |tree| try tree.value.root(artifact.name) else if (metadata) |owned| owned.value.* else try loadRootFor(ca, store, artifact, cancellation, null, active);
        try active.ensureActive();
        if (!root.recipe.eql(recipe)) return error.InvalidNativeAggregateArtifact;
        const cohort_metadata = if (active_cache) |cache| if (root.cohort) |ref| try @import("lake_index_decoded_metadata.zig").acquire(Cohort, cache, store, cohortReference(ref), cancellation, decodeCohort) else null else null;
        errdefer if (cohort_metadata) |owned| owned.release();
        const cohort: ?Cohort = if (cohort_metadata) |owned| owned.value.* else if (root.cohort) |ref| try decodeCohortFor(ca, store, cohortReference(ref), cancellation, null, active) else null;
        if (cohort) |proof| try validateCohort(ca, root, proof);
        self.* = .{ .tree_metadata = tree_metadata, .pack_metadata = pack_metadata, .pack_data = pack_data, .cohort_metadata = cohort_metadata, .cohort = cohort, .a = a, .store = store, .cancellation = cancellation, .cached = cached, .context = context, .control = control, .page = .init(a), .root = root, .metadata = metadata };
        return self;
    }
    /// Roots may fuse only when authenticated state recipes and the entire
    /// block directory agree. Independent materializations remain independent.
    pub fn setOutputSlot(self: *Reader, slot: u16) !void {
        self.output_slots = try self.control.allocator().dupe(u16, &.{slot});
        self.state_slots = try self.control.allocator().dupe(u16, &.{self.root.state_slot orelse 0});
    }
    pub fn fuseFor(self: *Reader, other: *const Reader, output_slot: u16, execution: ?*Execution) !bool {
        if (execution) |owner| try bindExecution(self, owner);
        defer if (execution != null) releaseExecution(self);
        const result = try self.fuse(other, output_slot);
        if (execution) |owner| try owner.checkpoint();
        return result;
    }
    pub fn fuse(self: *Reader, other: *const Reader, output_slot: u16) !bool {
        if (self.root.partitions.len != 0 or other.root.partitions.len != 0) return self.fusePartitions(other, output_slot);
        if (!sameFlatCohort(self, other)) return false;
        try self.appendSlot(other, output_slot);
        return true;
    }
    fn sameFlatCohort(self: *const Reader, other: *const Reader) bool {
        const state = self.root.state_recipe orelse return false;
        const incoming = other.root.state_recipe orelse return false;
        if (!state.eql(incoming) or self.root.groups != other.root.groups or self.root.blocks.len != other.root.blocks.len) return false;
        if (!std.meta.eql(self.root.state_tree, other.root.state_tree)) return false;
        for (self.root.blocks, other.root.blocks) |left, right| {
            if (left.state_layout != right.state_layout or left.rows != right.rows or left.artifact.byte_len != right.artifact.byte_len or !std.mem.eql(u8, left.artifact.artifact_id, right.artifact.artifact_id) or !std.mem.eql(u8, left.artifact.checksum, right.artifact.checksum)) return false;
        }
        return true;
    }
    fn appendSlot(self: *Reader, other: *const Reader, output_slot: u16) !void {
        const output = self.output_slots orelse return error.InvalidNativeAggregateArtifact;
        const physical = self.state_slots orelse return error.InvalidNativeAggregateArtifact;
        const a = self.control.allocator();
        const slots = try a.alloc(u16, output.len + 1);
        const states = try a.alloc(u16, physical.len + 1);
        @memcpy(slots[0..output.len], output);
        @memcpy(states[0..physical.len], physical);
        slots[output.len] = output_slot;
        states[physical.len] = other.root.state_slot.?;
        self.output_slots = slots;
        self.state_slots = states;
    }
    pub fn partitionReader(self: *Reader, index: usize) !*Reader {
        return self.partitionReaderAllocated(index, self.a);
    }
    pub fn partitionReaderAllocated(self: *Reader, index: usize, allocator: A) !*Reader {
        const part = self.root.partitions[index];
        const reader = try Reader.openWithCacheFor(allocator, self.store, part.artifact, self.root.recipe, self.cancellation, self.cached, self.execution);
        errdefer reader.cursor().close(reader);
        reader.max_typed_bytes = self.max_typed_bytes;
        reader.point_hash = self.point_hash;
        reader.key_range = self.key_range;
        reader.raw_states = self.raw_states;
        if (part.bounds != null and !std.meta.eql(part.bounds, reader.root.bounds)) return error.InvalidNativeAggregateArtifact;
        if (isPartitioned(part.artifact.metadata_version) and reader.root.radix_depth != self.root.radix_depth + 1) return error.InvalidNativeAggregateArtifact;
        if (reader.root.groups != part.groups) return error.InvalidNativeAggregateArtifact;
        if (self.partition_states) |maps| {
            reader.output_slots = if (self.output_slots) |slots| try reader.control.allocator().dupe(u16, slots) else null;
            const owned = try clonePartitionMap(reader.control.allocator(), maps[index]);
            reader.state_slots = owned.states;
            reader.partition_states = owned.children;
        } else if (self.cohort) |cohort| {
            const proof = cohort.children[index];
            const identity = (try physicalIdentity(self.a, reader.root)) orelse return error.InvalidNativeAggregateArtifact;
            if (!std.mem.eql(u8, &identity, &proof.identity)) return error.InvalidNativeAggregateArtifact;
            const selected = self.state_slots orelse &.{self.root.state_slot.?};
            const states = try reader.control.allocator().alloc(u16, selected.len);
            const physical = reader.root.state_recipe orelse return error.InvalidNativeAggregateArtifact;
            for (states, selected) |*state, slot| {
                if (slot >= proof.slots.len) return error.InvalidNativeAggregateArtifact;
                state.* = proof.slots[slot];
                if (state.* >= physical.inputs.len or !(recipes.Recipe{ .keys = physical.keys, .inputs = physical.inputs[state.*..][0..1] }).eql(.{ .keys = cohort.recipe.keys, .inputs = cohort.recipe.inputs[slot..][0..1] })) return error.InvalidNativeAggregateArtifact;
            }
            reader.state_slots = states;
            reader.output_slots = if (self.output_slots) |slots| try reader.control.allocator().dupe(u16, slots) else null;
        } else if (self.output_slots) |slots| try reader.setOutputSlot(slots[0]);
        return reader;
    }
    fn clonePartitionMap(a: A, map: PartitionMap) anyerror!PartitionMap {
        var result: PartitionMap = .{ .states = try a.dupe(u16, map.states) };
        if (map.children) |children| {
            const owned = try a.alloc(PartitionMap, children.len);
            for (owned, children) |*child, value| child.* = try clonePartitionMap(a, value);
            result.children = owned;
        }
        return result;
    }
    /// Build the complete slot proof before mutating the caller. Each recursive
    /// directory may borrow flat cohorts with different physical slot orders.
    fn mergePartitionMap(a: A, left: *const Reader, right: *const Reader, prior: ?PartitionMap) anyerror!?PartitionMap {
        const state = left.root.state_recipe orelse return null;
        const incoming = right.root.state_recipe orelse return null;
        if (!state.eql(incoming) or left.root.groups != right.root.groups or left.root.partitions.len != right.root.partitions.len or left.root.radix_depth != right.root.radix_depth) return null;
        const previous = if (prior) |map| map.states else &.{left.root.state_slot.?};
        const slots = try a.alloc(u16, previous.len + 1);
        @memcpy(slots[0..previous.len], previous);
        slots[previous.len] = right.root.state_slot.?;
        if (left.root.partitions.len == 0) return if (sameFlatCohort(left, right)) .{ .states = slots } else null;
        const children = try a.alloc(PartitionMap, left.root.partitions.len);
        for (left.root.partitions, right.root.partitions, children, 0..) |l, r, *child, index| {
            if (l.bucket != r.bucket or l.groups != r.groups) return null;
            const lreader = try Reader.openWithCacheFor(left.a, left.store, l.artifact, left.root.recipe, left.cancellation, left.cached, left.execution);
            defer lreader.cursor().close(lreader);
            const rreader = try Reader.openWithCacheFor(right.a, right.store, r.artifact, right.root.recipe, right.cancellation, right.cached, left.execution);
            defer rreader.cursor().close(rreader);
            child.* = (try mergePartitionMap(a, lreader, rreader, if (prior) |map| (if (map.children) |maps| maps[index] else null) else null)) orelse return null;
        }
        return .{ .states = slots, .children = children };
    }
    fn fusePartitions(self: *Reader, other: *const Reader, output_slot: u16) !bool {
        if (self.partition_index != 0 or self.emitted != 0 or self.root.partitions.len == 0) return false;
        if (self.state_slots == null or self.output_slots == null) return error.InvalidNativeAggregateArtifact;
        if (self.partition_states == null and self.cohort != null and other.cohort != null) {
            const left = self.root.cohort.?;
            const right = other.root.cohort.?;
            if (left.byte_len != right.byte_len or !std.mem.eql(u8, left.checksum, right.checksum)) return false;
            try self.appendSlot(other, output_slot);
            return true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const plan = (try mergePartitionMap(scratch.allocator(), self, other, .{ .states = self.state_slots.?, .children = self.partition_states })) orelse return false;
        const owned = try clonePartitionMap(self.control.allocator(), plan);
        try self.appendSlot(other, output_slot);
        self.partition_states = owned.children;
        return true;
    }
    pub fn groupCount(self: *const Reader) u64 {
        return self.root.groups;
    }
    fn selectPoint(raw: *anyopaque, keys: []const local.sql_scalar.Datum) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        if (self.selection_started or self.emitted != 0 or self.child != null or keys.len != self.root.recipe.keys.len) return error.InvalidNativeAggregateArtifact;
        self.point_hash = try groupHash(keys);
        if (self.root.state_tree != null) self.point_key = try group_keys.encode(self.control.allocator(), keys);
    }
    fn selectKeys(raw: *anyopaque, request: local.sql_catalog.GroupSelection) !bool {
        const self: *Reader = @ptrCast(@alignCast(raw));
        if (self.selection_started or self.ordered_plan != null or self.emitted != 0 or self.child != null) return error.InvalidNativeAggregateArtifact;
        self.selection_spill = request.spill;
        self.selection_memory = request.memory_bytes;
        self.selection_rows_left = request.row_limit;
        if (request.batch_rows == 0) return error.InvalidSqlLimit;
        self.selection_quota = .{ .capacity = request.batch_rows, .row_limit = request.row_limit, .page_limit = request.page_limit };
        self.key_range = try group_keys.range(self.control.allocator(), self.root.recipe, request.conditions);
        if (self.point_key) |point| {
            const upper = try group_keys.successor(self.control.allocator(), point);
            if (std.mem.order(u8, point, self.key_range.?.lower) == .gt) self.key_range.?.lower = point;
            if (upper) |value| if (self.key_range.?.upper == null or std.mem.order(u8, value, self.key_range.?.upper.?) == .lt) {
                self.key_range.?.upper = value;
            };
        }
        if (self.root.state_tree != null and request.unordered_prefix and request.order.len == 0) {
            self.tree_maximum = request.maximum_groups;
            return false;
        }
        if (request.memory_bytes < 64 * 1024 or request.order.len == 0 or request.order.len > self.root.recipe.keys.len or (self.root.bounds == null and self.root.state_tree == null)) return false;
        const reverse = request.order[0].descending;
        for (request.order, self.root.recipe.keys[0..request.order.len]) |order, key| {
            if (!std.mem.eql(u8, order.column, key.path) or order.descending != reverse or (key.nullable and order.nulls_first != reverse)) return false;
        }
        for (self.root.recipe.inputs) |input| if (input.spec.distinct or input.spec.kind == .pattern_set) return false;
        self.reverse = reverse;
        if (self.root.state_tree != null) {
            self.tree_maximum = request.maximum_groups;
            self.ordered_scan = true;
            return true;
        }
        self.ordered_plan = (try ordered_keys.Plan(Reader).prepare(self, request.memory_bytes, request.maximum_groups)) orelse return false;
        self.ordered_scan = true;
        return true;
    }
    pub fn cursor(self: *Reader) local.sql_catalog.AggregatePartialCursor {
        return .{ .ptr = self, .next = next, .close = close, .select_point = selectPoint, .select_keys = selectKeys, .coordinator_execution = .{ .bind = bindExecution, .release = releaseExecution }, .final_group_proof = .{ .count = self.root.groups, .slots = self.output_slots orelse &.{0} } };
    }
    fn bindExecution(raw: *anyopaque, execution: *Execution) !void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        if (self.execution != null) return error.InvalidRunnableTransition;
        try self.context.withExecution(execution).ensureActive();
        self.execution = execution;
    }
    fn releaseExecution(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        self.execution = null;
        if (self.tree_access) |access| {
            access.context = self.context;
            access.cached = self.cached;
        }
    }
    fn currentCache(self: *Reader) ?CachedRead {
        var cache = self.cached orelse return null;
        cache.context = self.currentContext();
        return cache;
    }
    fn currentContext(self: *Reader) ReadContext {
        return if (self.execution) |owner| self.context.withExecution(owner) else self.context;
    }
    fn next(raw: *anyopaque, a: A, maximum: u32) !?[]const operators.GroupResult {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.currentContext().ensureActive();
        self.selection_started = true;
        if (self.ordered_plan) |plan| return plan.next(a, maximum);
        return nextRaw(raw, a, maximum);
    }
    pub fn nextRaw(raw: *anyopaque, a: A, maximum: u32) anyerror!?[]const operators.GroupResult {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.currentContext().ensureActive();
        try self.cancellation.check();
        if (self.cached) |cached| try cached.context.ensureActive();
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.root.state_tree != null) return self.nextTree(a, maximum);
        if (self.root.partitions.len != 0) while (true) {
            if (self.child == null) {
                if (self.partition_index == self.root.partitions.len) {
                    if (self.point_hash == null and self.key_range == null and self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                    return null;
                }
                if (self.point_hash) |hash| if (self.root.partitions[self.partition_index].bucket != (hash >> partitionShift(self.root.radix_depth)) % partitionFanout(self.root.radix_depth)) {
                    self.partition_index += 1;
                    continue;
                };
                if (self.key_range) |range| if (!range.overlaps(self.root.partitions[self.partition_index].bounds)) {
                    self.partition_index += 1;
                    continue;
                };
                self.child = try self.partitionReader(self.partition_index);
                self.partition_index += 1;
            }
            if (try self.child.?.cursor().nextFor(self.execution, a, maximum)) |rows| {
                self.emitted += rows.len;
                return rows;
            }
            self.blocks_decoded += self.child.?.blocks_decoded;
            self.child.?.cursor().close(self.child.?);
            self.child = null;
        };
        if (self.block == null or self.position == self.block.?.count()) {
            self.block = null;
            if (self.block_metadata) |owned| owned.release();
            self.block_metadata = null;
            for (self.typed_columns) |owned| owned.release();
            self.typed_columns = &.{};
            self.position = 0;
            if (self.key_range) |range| while (self.block_index < self.root.blocks.len and !range.overlaps(self.root.blocks[self.block_index].bounds)) {
                self.block_index += 1;
            };
            if (self.block_index == self.root.blocks.len) {
                if (self.key_range == null and self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                return null;
            }
            if (!self.page.reset(.{ .retain_with_limit = max_block_bytes })) return error.OutOfMemory;
            const pa = self.page.allocator();
            const ref = self.root.blocks[self.block_index];
            const state_recipe = self.root.state_recipe orelse self.root.recipe;
            const fingerprint = state_recipe.fingerprint();
            const owned = if (self.currentCache()) |cached| if (self.pack_data) |pack_data| try @import("lake_index_decoded_metadata.zig").acquireColumnBlockBytes(cached, self.store, ref.artifact, self.cancellation, pack_data.bytes) else try @import("lake_index_decoded_metadata.zig").acquireColumnBlock(cached, self.store, ref.artifact, self.cancellation) else null;
            errdefer if (owned) |value| value.release();
            const block = if (owned) |value| value.value.* else decode: {
                const bytes = if (self.pack_data) |pack_data| pack_data.bytes else try readArtifactFor(pa, self.store, ref.artifact, self.cancellation, null, self.currentContext());
                break :decode try spill.decodeColumnarBlockInArena(pa, bytes, max_block_bytes);
            };
            if (block.count() != ref.rows or block.keys.len != self.root.recipe.keys.len or block.values.len != (if (ref.state_layout != 0) native_partial.physicalWidth(state_recipe.inputs) else state_recipe.inputs.len)) return error.InvalidNativeAggregateArtifact;
            const width = if (self.state_slots) |slots| slots.len else self.root.recipe.inputs.len;
            var columns: std.ArrayList(@import("lake_index_decoded_metadata.zig").Owned(TypedColumn)) = .empty;
            var typed_bytes: usize = 0;
            errdefer for (columns.items) |column| column.release();
            if (!self.raw_states) if (self.currentCache()) |cached| for (0..width) |logical| {
                const physical = self.physicalSlot(logical);
                var interpretation: [35]u8 = undefined;
                @memcpy(interpretation[0..32], &fingerprint);
                std.mem.writeInt(u16, interpretation[32..34], @intCast(physical), .little);
                interpretation[34] = ref.state_layout;
                const column = @import("lake_index_decoded_metadata.zig").acquireInterpretedBounded(TypedColumn, cached, self.store, .{ .kind = .algebraic_segment, .artifact_id = ref.artifact.artifact_id, .checksum = ref.artifact.checksum, .byte_len = ref.artifact.byte_len }, self.cancellation, &interpretation, TypedColumn.Args{ .block = block, .recipe = state_recipe, .column = physical, .state_layout = ref.state_layout }, TypedColumn.decode, 16 * 1024 * 1024, .{}) catch |err| fallback: {
                    if (err != error.OutOfMemory) return err;
                    break :fallback null;
                };
                if (column) |value| {
                    const bytes = value.lease.item.budget.live;
                    if (value.value.states != null and bytes <= self.max_typed_bytes -| typed_bytes) {
                        typed_bytes += bytes;
                        columns.append(pa, value) catch |err| {
                            value.release();
                            return err;
                        };
                        continue;
                    }
                    value.release();
                }
                // The encoded fallback handles the complete selected row. Do
                // not retain a partially expanded set of logical columns.
                for (columns.items) |value| value.release();
                columns.clearRetainingCapacity();
                break;
            };
            self.block_metadata = owned;
            self.typed_columns = columns.items;
            self.block = block;
            self.block_index += 1;
            self.blocks_decoded += 1;
        }
        const block = self.block.?;
        if (self.indexed_position) |position| {
            self.position = position;
            self.indexed_position = null;
        }
        const count = @min(maximum, block.count() - self.position);
        const result = try a.alloc(operators.GroupResult, count);
        const width = if (self.state_slots) |slots| slots.len else self.root.recipe.inputs.len;
        const key_cells = try a.alloc(local.sql_scalar.Datum, try std.math.mul(usize, count, block.keys.len));
        const native_layout = self.root.blocks[self.block_index - 1].state_layout != 0;
        const typed_cells = if (self.typed_columns.len != 0) try a.alloc(operators.Aggregate, try std.math.mul(usize, count, width)) else null;
        const encoded_cells = if (typed_cells == null and !native_layout) try a.alloc(local.sql_scalar.Datum, try std.math.mul(usize, count, width)) else null;
        for (result, 0..) |*row, index| {
            if (index % 64 == 0) try self.currentContext().ensureActive();
            const keys = key_cells[index * block.keys.len ..][0..block.keys.len];
            for (keys, 0..) |*key, column| key.* = try block.keyCell(self.position, column);
            const states = if (typed_cells) |cells| cells[index * width ..][0..width] else null;
            const cells: []local.sql_scalar.Datum = if (encoded_cells) |values| values[index * width ..][0..width] else &.{};
            for (0..width) |column| {
                const physical = self.physicalSlot(column);
                if (states) |values| values[column] = self.typed_columns[column].value.states.?[self.position] else if (!native_layout) cells[column] = try block.cell(self.position, physical);
            }
            var state_reader: ?operators.GroupResult.StateReader = null;
            if (native_layout and states == null) {
                const view = try a.create(PhysicalRow);
                view.* = .{ .reader = self, .block = block, .row = self.position, .width = width };
                state_reader = .{ .ptr = view, .count = width, .read = PhysicalRow.read, .finish = PhysicalRow.finish };
            }
            row.* = .{ .keys = keys, .aggregates = cells, .typed_states = states, .state_reader = state_reader, .ordinal = block.ordinals[self.position], .aggregate_slots = self.output_slots };
            self.position += 1;
            self.emitted += 1;
        }
        return result;
    }
    fn nextTree(self: *Reader, a: A, maximum: u32) !?[]const operators.GroupResult {
        if (self.tree_maximum) |goal| if (self.emitted >= goal) return null;
        const index = self.root.state_tree.?;
        if (index.layout == 2 and self.tree_record_layout == null) {
            const compiled = try self.control.allocator().create(state_tree.record.Layout);
            compiled.* = try state_tree.record.Layout.init(self.control.allocator(), self.root.state_recipe.?);
            self.tree_record_layout = compiled;
        }
        if (self.tree_cursor == null) {
            const ca = self.control.allocator();
            const access = try ca.create(@import("lake_index_native_directory.zig").Access);
            access.init(self.store, index.domain, self.cancellation, self.currentCache());
            access.context = self.currentContext();
            self.tree_access = access;
            const range = self.key_range orelse if (self.point_key) |point| group_keys.Range{ .lower = point, .upper = try group_keys.successor(ca, point) } else group_keys.Range{};
            self.tree_cursor = if (self.reverse) try state_tree.tree.Cursor.initReverse(local.sql_memory_budget.reclaimingAllocator(self.a), access.backing(), index.page, range.lower, range.upper) else try state_tree.tree.Cursor.init(local.sql_memory_budget.reclaimingAllocator(self.a), access.backing(), index.page, range.lower, range.upper);
        }
        self.tree_access.?.context = self.currentContext();
        self.tree_access.?.cached = self.currentCache();
        // Rows borrow their bounded frame until the next pull. Prefetch owns a
        // separate frame and never invalidates the one being consumed.
        if (self.tree_frame) |frame| if (self.tree_frame_position == frame.records.len) {
            frame.destroy();
            self.tree_frame = null;
        };
        if (self.tree_frame == null) {
            const frame = if (self.tree_prefetch) |*task| try task.await(self.cached.?.context.io.?) else try self.fetchTreeFrame();
            self.tree_prefetch = null;
            self.tree_frame = frame;
            self.tree_frame_position = 0;
            if (frame == null) {
                if (self.point_hash == null and self.key_range == null and self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                return null;
            }
            if (self.execution == null and self.context.execution == null) if (self.cached) |cached| if (cached.context.io) |io| if (self.tree_maximum == null or self.tree_loaded < self.tree_maximum.?) {
                const available = local.sql_memory_budget.optionalHeadroom(local.sql_memory_budget.reclaimingAllocator(self.a)) orelse std.math.maxInt(usize);
                if (available >= 8 * 1024 * 1024) {
                    self.tree_prefetch = local.sql_parallel_scheduler.global().submitIoTransient(io, 8 * 1024 * 1024, Reader.fetchTreeFrame, .{self});
                    self.tree_prefetches += @intFromBool(self.tree_prefetch != null);
                }
            };
        }
        const remaining = self.tree_frame.?.records.len - self.tree_frame_position;
        const wanted = @min(remaining, @min(@min(maximum, 256), if (self.tree_maximum) |goal| goal - @as(usize, @intCast(self.emitted)) else std.math.maxInt(usize)));
        // Physical row descriptors also consume memory. Wide recipes must
        // shrink their decode batch even when their encoded payloads are tiny.
        const width = native_partial.physicalWidth(self.root.state_recipe.?.inputs) + self.root.recipe.keys.len;
        const descriptors = if (index.layout == 2) self.root.recipe.keys.len *| @sizeOf(local.sql_scalar.Datum) *| 2 +| @sizeOf(state_tree.View) +| @sizeOf(operators.GroupResult) else width *| @sizeOf(std.meta.Elem(@FieldType(spill.ColumnarBlock, "values"))) *| 2 +| @sizeOf(operators.GroupResult);
        var count: usize = 0;
        var decoded_bytes: usize = 0;
        while (count < wanted) {
            const record = self.tree_frame.?.records[self.tree_frame_position + count];
            const cost = record.value.len +| record.key.len *| 2 +| descriptors;
            if (count != 0 and cost > self.selection_memory -| decoded_bytes) break;
            decoded_bytes +|= cost;
            count += 1;
        }
        const result = try a.alloc(operators.GroupResult, count);
        const slots = self.state_slots orelse try a.dupe(u16, &.{self.root.state_slot.?});
        var n: usize = 0;
        while (n < count) {
            if (n % 64 == 0) try self.currentContext().ensureActive();
            const record = self.tree_frame.?.records[self.tree_frame_position];
            self.tree_frame_position += 1;
            if (self.selection_rows_left == 0) return error.SqlProgramLimitExceeded;
            self.selection_rows_left -= 1;
            const value = try state_tree.view(a, record.value, self.root.state_recipe.?, index.layout, self.tree_record_layout);
            self.blocks_decoded += 1;
            const input = try state_tree.row(a, value, self.root.state_recipe.?, slots, self.output_slots, self.emitted);
            if (!group_keys.compatible(self.root.state_recipe.?, input.keys)) return error.InvalidNativeAggregateArtifact;
            // Validation bytes are transient, not part of the returned page.
            var validation = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
            defer validation.deinit();
            if (!std.mem.eql(u8, record.key, try group_keys.encode(validation.allocator(), input.keys))) return error.InvalidNativeAggregateArtifact;
            result[n] = input;
            self.emitted += 1;
            n += 1;
        }
        if (n == 0) {
            if (self.point_hash == null and self.key_range == null and self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
            return null;
        }
        try self.selection_quota.admit(n, false);
        return result[0..n];
    }
    const PhysicalRow = struct {
        reader: *Reader,
        block: spill.ColumnarBlock,
        row: usize,
        width: usize,
        fn finish(raw: *const anyopaque, a: A, logical: usize, spec: operators.AggregateSpec) anyerror!local.sql_scalar.Datum {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (logical >= self.width) return error.InvalidSqlBackendResponse;
            const inputs = (self.reader.root.state_recipe orelse self.reader.root.recipe).inputs;
            const physical = self.reader.physicalSlot(logical);
            if (!std.meta.eql(spec, inputs[physical].spec)) return error.InvalidSqlBackendResponse;
            return native_partial.finishPhysical(a, self.block, self.row, native_partial.physicalOffset(inputs, physical), spec);
        }
        fn read(raw: *const anyopaque, a: A, logical: usize) anyerror!operators.Aggregate {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (logical >= self.width) return error.InvalidSqlBackendResponse;
            const physical = self.reader.physicalSlot(logical);
            const inputs = (self.reader.root.state_recipe orelse self.reader.root.recipe).inputs;
            return native_partial.readPhysical(a, self.block, self.row, native_partial.physicalOffset(inputs, physical), inputs[physical].spec);
        }
    };
    pub fn readIndexed(self: *Reader, a: A, block_index: usize, row: usize) !operators.GroupResult {
        if (block_index >= self.root.blocks.len or row >= self.root.blocks[block_index].rows) return error.InvalidNativeAggregateArtifact;
        if (self.block == null or self.block_index != block_index + 1) {
            self.discardBlock();
            self.block_index = block_index;
        } else self.position = row;
        self.indexed_position = row;
        const result = (try nextRaw(self, a, 1)) orelse return error.InvalidNativeAggregateArtifact;
        if (self.block_index != block_index + 1) return error.InvalidNativeAggregateArtifact;
        return result[0];
    }
    pub fn discardBlock(self: *Reader) void {
        self.block = null;
        if (self.block_metadata) |owned| owned.release();
        self.block_metadata = null;
        for (self.typed_columns) |owned| owned.release();
        self.typed_columns = &.{};
        self.position = 0;
        _ = self.page.reset(.free_all);
    }
    pub fn physicalSlot(self: *const Reader, logical: usize) usize {
        return if (self.state_slots) |slots| slots[logical] else if (self.root.state_slot) |slot| @as(usize, slot) else logical;
    }
    fn close(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.retireTree();
        if (self.tree_cursor) |*cursor_| cursor_.deinit();
        if (self.ordered_plan) |plan| plan.deinit();
        if (self.child) |child| child.cursor().close(child);
        if (self.block_metadata) |owned| owned.release();
        for (self.typed_columns) |owned| owned.release();
        if (self.cohort_metadata) |owned| owned.release();
        self.page.deinit();
        if (self.metadata) |owned| owned.release();
        if (self.pack_metadata) |owned| owned.release();
        if (self.tree_metadata) |owned| owned.release();
        self.control.deinit();
        a.destroy(self);
    }
};

test "external lake aggregate readers retain version one root compatibility" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-v1");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const spec: operators.AggregateSpec = .{ .kind = .count };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = null }} };
    const group = try operators.Grouped.create(a, &.{spec}, .{});
    defer group.deinit();
    try group.ensureGlobalGroup();
    try group.addGlobalCount(23);
    var legacy_input: PartialStream = .{ .group = group };
    const legacy_refs = try publishPartialStream(a, a, &store, &.{"stats.count"}, &legacy_input, recipe, .none, null);
    defer a.free(legacy_refs);
    const current = legacy_refs[0];
    defer a.free(current.artifact_id);
    defer a.free(current.checksum);
    const bytes = try store.getVerifiedAllocWithCancellation(current.artifact_id, current.byte_len, current.checksum, .none);
    defer a.free(bytes);
    var document = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer document.deinit();
    _ = document.value.object.swapRemove("state_recipe");
    _ = document.value.object.swapRemove("state_slot");
    try document.value.object.put(document.arena.allocator(), "format", .{ .string = "native-sql-aggregate-v1" });
    const legacy_bytes = try std.json.Stringify.valueAlloc(a, document.value, .{});
    defer a.free(legacy_bytes);
    var legacy = try store.put(legacy_bytes);
    defer legacy.deinit(a);
    const reference: Ref = .{ .kind = .algebraic_segment, .name = "stats.count", .metadata_version = 1, .artifact_id = legacy.artifact_id, .byte_len = legacy.byte_len, .checksum = legacy.checksum };
    const cursor = (try Reader.open(a, store, reference, recipe, .none)).cursor();
    defer cursor.close(cursor.ptr);
    var page = std.heap.ArenaAllocator.init(a);
    defer page.deinit();
    const rows = (try cursor.next(cursor.ptr, page.allocator(), 32)).?;
    var state = try local.sql_aggregate_partial.read(page.allocator(), rows[0], 0, spec);
    defer state.deinit();
    try std.testing.expectEqual(@as(u64, 23), state.count);
}

test "external lake native aggregate artifacts retain exact state across bounded column blocks" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-artifact");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const spec: operators.AggregateSpec = .{ .kind = .sum, .input_type = .integer };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "key", .type = .integer, .nullable = false }}, .inputs = &.{.{ .spec = spec, .column = .{ .path = "amount", .type = .integer, .nullable = true } }} };
    const Checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Checkpoint.check, .async_writes = false };
    defer manager.deinit();
    const group = try operators.Grouped.create(a, &.{spec}, .{ .groups = 1024, .bytes = 16 * 1024, .spill = &manager });
    defer group.deinit();
    for (0..600) |i| {
        const key = local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(i) });
        const value = local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 });
        try group.add(&.{key}, &.{value});
        try group.add(&.{key}, &.{value});
    }
    try std.testing.expect(group.external != null);
    const ref = try publish(a, a, &store, "stats.exact", group, recipe, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const cursor = (try Reader.open(a, store, ref, recipe, .none)).cursor();
    defer cursor.close(cursor.ptr);
    const imported = try operators.Grouped.create(a, &.{spec}, .{ .groups = 1024, .bytes = 8 * 1024 * 1024 });
    defer imported.deinit();
    var count: usize = 0;
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const partials = (try cursor.next(cursor.ptr, page.allocator(), 17)) orelse break;
        try std.testing.expect(partials.len <= 17);
        for (partials) |partial| {
            try imported.importPartialBatch(&.{partial});
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 600), count);
    var page = std.heap.ArenaAllocator.init(a);
    defer page.deinit();
    while (try imported.nextResult(page.allocator())) |row| try std.testing.expectEqual(@as(i64, 18014398509481986), row.aggregates[0].value.integer);
    var wrong = recipe;
    wrong.keys = &.{};
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.open(a, store, ref, wrong, .none));
}

fn readFailureScenario(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe) !void {
    const cursor = (try Reader.open(a, store, ref, recipe, .none)).cursor();
    defer cursor.close(cursor.ptr);
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        if (try cursor.next(cursor.ptr, page.allocator(), 1) == null) break;
    }
}

test "external lake native aggregate readers unwind every allocation failure" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-reader-fault");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const spec: operators.AggregateSpec = .{ .kind = .count };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = null }} };
    const group = try operators.Grouped.create(a, &.{spec}, .{});
    defer group.deinit();
    try group.ensureGlobalGroup();
    try group.addGlobalCount(123);
    const ref = try publish(a, a, &store, "stats.count", group, recipe, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    try std.testing.checkAllAllocationFailures(a, readFailureScenario, .{ store, ref, recipe });
}

test "external lake cohort readers decode shared keys and all selected slots once" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-aggregate-fusion");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{ .{ .spec = specs[0], .column = .{ .path = "amount", .type = .integer, .nullable = false } }, .{ .spec = specs[1], .column = null } } };
    const group = try operators.Grouped.create(a, &specs, .{});
    defer group.deinit();
    try group.add(&.{}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }), local.sql_scalar.Datum.fromJson(.{ .integer = 1 }) });
    const refs = try publishCohort(a, a, &store, &.{ "stats.sum", "stats.count" }, group, recipe, .none);
    defer {
        for (refs) |ref| {
            a.free(ref.artifact_id);
            a.free(ref.checksum);
        }
        a.free(refs);
    }
    const left = try Reader.open(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none);
    defer left.cursor().close(left);
    const right = try Reader.open(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none);
    defer right.cursor().close(right);
    try left.setOutputSlot(1);
    try std.testing.expect(try left.fuse(right, 0));
    var page = std.heap.ArenaAllocator.init(a);
    defer page.deinit();
    const rows = (try left.cursor().next(left, page.allocator(), 1)).?;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualSlices(u16, &.{ 1, 0 }, rows[0].aggregate_slots.?);
    const imported = try operators.Grouped.create(a, &.{ specs[1], specs[0] }, .{});
    defer imported.deinit();
    try imported.importPartialBatch(rows[0..1]);
    const result = (try imported.nextResult(page.allocator())).?;
    try std.testing.expectEqual(@as(i64, 1), result.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.aggregates[1].value.integer);
    try std.testing.expect(try left.cursor().next(left, page.allocator(), 1) == null);
    try std.testing.expectEqual(@as(usize, 1), left.block_index);
    try std.testing.expectEqual(@as(usize, 0), right.block_index);
}

/// Fixed hash partitions keep unchanged group ranges immutable across file
/// reduction branches. Exact typed reducers still own every affected range.
pub fn publishPartitioned(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, manager: *spill.Manager, cancellation: Cancellation) ![]Ref {
    return publishPartitionedAt(a, out, store, names, group, recipe, manager, cancellation, 0);
}
pub fn publishPartitionedAt(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, manager: *spill.Manager, cancellation: Cancellation, depth: u8) anyerror![]Ref {
    return publishPartitionedAtProven(a, out, store, names, group, recipe, manager, cancellation, depth, null);
}
pub fn publishPartitionedAtProven(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, manager: *spill.Manager, cancellation: Cancellation, depth: u8, proofs: ?[]StateProof) anyerror![]Ref {
    if (depth > max_partition_depth or names.len == 0 or names.len != recipe.inputs.len or names.len > 256 or group.specs.len != names.len) return error.InvalidNativeAggregateArtifact;
    if (proofs) |values| if (values.len != names.len) return error.InvalidNativeAggregateArtifact;
    for (names, group.specs, recipe.inputs) |name, spec, input| if (name.len == 0 or !std.meta.eql(spec, input.spec)) return error.InvalidNativeAggregateArtifact;
    try cancellation.check();
    var input = PartialStream.init(group, recipe);
    if (group.external == null and group.groups.items.len - group.result_cursor <= 256)
        return publishSmallRadix(a, out, store, names, &input, recipe, cancellation, depth, proofs, false);
    var pool = try spill.Partitions.init(manager);
    defer pool.deinit();
    return publishRadixStream(a, out, store, names, &input, recipe, &pool, cancellation, depth, proofs);
}
/// A merge of differently shaped trees needs an explicit directory at this
/// radix depth even when one child is small. Ordinary publication stays flat.
pub fn publishPartitionDirectoryAt(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, group: *operators.Grouped, recipe: recipes.Recipe, manager: *spill.Manager, cancellation: Cancellation, depth: u8) ![]Ref {
    if (depth > max_partition_depth or names.len == 0 or names.len != recipe.inputs.len or names.len > 256 or group.specs.len != names.len) return error.InvalidNativeAggregateArtifact;
    for (names, group.specs, recipe.inputs) |name, spec, input| if (name.len == 0 or !std.meta.eql(spec, input.spec)) return error.InvalidNativeAggregateArtifact;
    var input = PartialStream.init(group, recipe);
    if (group.external == null and group.groups.items.len - group.result_cursor <= 256) return publishSmallRadix(a, out, store, names, &input, recipe, cancellation, depth, null, true);
    var pool = try spill.Partitions.init(manager);
    defer pool.deinit();
    return publishRadixStream(a, out, store, names, &input, recipe, &pool, cancellation, depth, null);
}
fn groupHash(keys: []const local.sql_scalar.Datum) !u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (keys) |key| {
        var bytes: [9]u8 = undefined;
        bytes[0] = @intFromBool(key.sql_null);
        std.mem.writeInt(u64, bytes[1..9], try local.sql_scalar.semanticHashDatum(key), .little);
        hasher.update(&bytes);
    }
    return hasher.final();
}
/// Counting scatter visits each row once, independent of directory fanout.
const RadixSelection = struct {
    positions: [256]usize = undefined,
    ranges: [65]usize = @splat(0),
    fn init(rows: []const operators.Row, recipe: recipes.Recipe, depth: u8) !RadixSelection {
        if (rows.len > 256) return error.InvalidNativeAggregateArtifact;
        var result: RadixSelection = .{};
        var counts: [64]usize = @splat(0);
        var buckets: [256]u8 = undefined;
        for (rows, 0..) |row, index| {
            if (row.keys.len != recipe.keys.len) return error.InvalidSqlBackendResponse;
            const bucket: u8 = @intCast((try groupHash(row.keys) >> partitionShift(depth)) % partitionFanout(depth));
            buckets[index] = bucket;
            counts[bucket] += 1;
        }
        for (counts, 0..) |count, index| result.ranges[index + 1] = result.ranges[index] + count;
        var next = result.ranges[0..64].*;
        for (buckets[0..rows.len], 0..) |bucket, index| {
            result.positions[next[bucket]] = index;
            next[bucket] += 1;
        }
        return result;
    }
    fn indices(self: *const RadixSelection, bucket: usize) []const usize {
        return self.positions[self.ranges[bucket]..self.ranges[bucket + 1]];
    }
};
/// Small categorical reductions remain entirely resident. Partitioning copies
/// only bounded row descriptors and writes each finished checkpoint once.
fn publishSmallRadix(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, input: *PartialStream, recipe: recipes.Recipe, cancellation: Cancellation, depth: u8, proofs: ?[]StateProof, route: bool) ![]Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const rows = try input.next(pa);
    input.finish();
    if (!route and rows.len != 0) return publishPackedRows(a, out, store, names, rows, recipe, cancellation, proofs, input.state_layout);
    const selection = try RadixSelection.init(rows, recipe, depth);
    const lists = try pa.alloc(std.ArrayList(Partition), names.len);
    @memset(lists, .empty);
    const child_proofs = try pa.alloc(StateProof, names.len);
    var selected: [256]operators.Row = undefined;
    for (0..partitionFanout(depth)) |bucket| {
        try cancellation.check();
        const positions = selection.indices(bucket);
        const count = positions.len;
        if (count == 0) continue;
        for (positions, 0..) |index, lane| selected[lane] = rows[index];
        const refs = try publishPackedRows(a, pa, store, names, selected[0..count], recipe, cancellation, child_proofs, input.state_layout);
        for (refs, lists, child_proofs) |ref, *list, proof| try list.append(pa, .{ .bucket = @intCast(bucket), .artifact = ref, .groups = count, .proof = proof, .bounds = proof.bounds });
    }
    return publishPartitionRootsAtProven(a, out, store, names, recipe, lists, cancellation, depth, proofs);
}
fn publishRadixStream(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, input: *PartialStream, recipe: recipes.Recipe, pool: *spill.Partitions, cancellation: Cancellation, depth: u8, proofs: ?[]StateProof) anyerror![]Ref {
    var partitions = pool.child();
    defer partitions.deinit();
    while (true) {
        try cancellation.check();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const pa = page.allocator();
        const rows = try input.next(pa);
        if (rows.len == 0) break;
        const selection = try RadixSelection.init(rows, recipe, depth);
        for (0..partitionFanout(depth)) |bucket| {
            const positions = selection.indices(bucket);
            const count = positions.len;
            if (count == 0) continue;
            // Gather descriptors only. Payloads borrow the page until the
            // synchronous columnar write ends; no state decoding is needed.
            var values: [256][]const local.sql_scalar.Datum = undefined;
            var keys: [256][]const local.sql_scalar.Datum = undefined;
            var ordinals: [256]u64 = undefined;
            for (positions, 0..) |index, lane| {
                values[lane] = rows[index].values;
                keys[lane] = rows[index].keys;
                ordinals[lane] = rows[index].ordinal;
            }
            try partitions.appendBatch(bucket, .{ .rows = values[0..count] }, .{ .rows = keys[0..count] }, ordinals[0..count]);
        }
    }
    // Retire the parent extent stream before recursively opening children.
    input.finish();
    try partitions.seal();
    const lists = try a.alloc(std.ArrayList(Partition), names.len);
    defer a.free(lists);
    @memset(lists, .empty);
    defer for (lists) |*list| {
        for (list.items) |part| {
            a.free(part.artifact.artifact_id);
            a.free(part.artifact.checksum);
        }
        list.deinit(a);
    };
    const child_proofs = try a.alloc(StateProof, names.len);
    defer a.free(child_proofs);
    for (0..partitionFanout(depth)) |bucket| {
        const file = if (partitions.streams[bucket]) |*stream| stream else continue;
        const count = file.size;
        var child: PartialStream = .{ .recipe = recipe, .state_layout = input.state_layout, .file = file, .retirement = .{ .pool = &partitions, .index = bucket } };
        const refs = if (count > partition_leaf_groups and depth < max_partition_depth)
            try publishRadixStream(a, a, store, names, &child, recipe, pool, cancellation, depth + 1, child_proofs)
        else if (count <= 256) leaf: {
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            const rows = try child.next(page.allocator());
            child.finish();
            break :leaf try publishPackedRows(a, a, store, names, rows, recipe, cancellation, child_proofs, child.state_layout);
        } else try publishPartialStream(a, a, store, names, &child, recipe, cancellation, child_proofs);
        defer a.free(refs);
        var transferred: usize = 0;
        defer for (refs[transferred..]) |ref| {
            a.free(ref.artifact_id);
            a.free(ref.checksum);
        };
        for (refs, lists, child_proofs) |ref, *list, proof| {
            try list.append(a, .{ .bucket = @intCast(bucket), .artifact = ref, .groups = count, .proof = proof, .bounds = proof.bounds });
            transferred += 1;
        }
        partitions.close(bucket);
    }
    return publishPartitionRootsAtProven(a, out, store, names, recipe, lists, cancellation, depth, proofs);
}
pub fn publishPartitionRoots(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, recipe: recipes.Recipe, lists: []const std.ArrayList(Partition), cancellation: Cancellation) ![]Ref {
    return publishPartitionRootsAt(a, out, store, names, recipe, lists, cancellation, 0);
}
pub fn publishPartitionRootsAt(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, recipe: recipes.Recipe, lists: []const std.ArrayList(Partition), cancellation: Cancellation, depth: u8) ![]Ref {
    return publishPartitionRootsAtProven(a, out, store, names, recipe, lists, cancellation, depth, null);
}
pub fn publishPartitionRootsAtProven(a: A, out: A, store: *stores.ArtifactStore, names: []const []const u8, recipe: recipes.Recipe, lists: []const std.ArrayList(Partition), cancellation: Cancellation, depth: u8, proofs: ?[]StateProof) ![]Ref {
    if (proofs) |values| if (values.len != names.len) return error.InvalidNativeAggregateArtifact;
    if (names.len == 0 or names.len != recipe.inputs.len or lists.len != names.len or names.len > 256) return error.InvalidNativeAggregateArtifact;
    const refs = try out.alloc(Ref, names.len);
    var initialized: usize = 0;
    errdefer {
        for (refs[0..initialized]) |ref| {
            out.free(ref.artifact_id);
            out.free(ref.checksum);
        }
        out.free(refs);
    }
    const roots = try a.alloc(Root, names.len);
    defer a.free(roots);
    for (roots, names, lists, 0..) |*root, name, list, slot| {
        var count: u64 = 0;
        for (list.items) |part| count = try std.math.add(u64, count, part.groups);
        root.* = .{ .format = "native-sql-aggregate-v5", .radix_depth = depth, .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = count, .partitions = list.items, .bounds = allBounds(list.items) };
    }
    const certificate = try publishCohortDescriptor(a, store, roots, cancellation);
    defer if (certificate) |ref| {
        a.free(ref.artifact_id);
        a.free(ref.checksum);
    };
    for (roots) |*root| {
        root.cohort = certificate;
        if (certificate == null) root.format = "native-sql-aggregate-v4";
    }
    for (refs, names, roots, 0..) |*ref, name, root, index| {
        const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
        defer a.free(bytes);
        if (bytes.len > max_root_bytes) return error.NativeAggregateArtifactTooLarge;
        var upload = store.*;
        upload.allocator = out;
        const artifact = try upload.putWithCancellation(bytes, cancellation);
        defer if (artifact.provider_version) |version| upload.allocator.free(version);
        ref.* = .{ .kind = .algebraic_segment, .name = name, .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .metadata_version = if (certificate != null) 5 else 4 };
        initialized += 1;
        if (proofs) |values| values[index] = try stateProof(a, ref.*, root);
    }
    return refs;
}
fn publishCohortDescriptor(a: A, store: *stores.ArtifactStore, roots: []const Root, token: Cancellation) !?ChunkRef {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const root = roots[0];
    const children = try ca.alloc(Cohort.Child, root.partitions.len);
    for (children, root.partitions, 0..) |*child, part, index| {
        const slots = try ca.alloc(u16, roots.len);
        var identity: ?[32]u8 = null;
        for (roots, slots) |member, *slot| {
            if (member.partitions.len != root.partitions.len or member.groups != root.groups) return error.InvalidNativeAggregateArtifact;
            const candidate = member.partitions[index];
            if (candidate.bucket != part.bucket or candidate.groups != part.groups or candidate.artifact.kind != .algebraic_segment or !std.mem.eql(u8, candidate.artifact.name, member.name) or candidate.artifact.byte_len > max_root_bytes or (candidate.artifact.metadata_version != 2 and candidate.artifact.metadata_version != 6 and candidate.artifact.metadata_version != 4 and candidate.artifact.metadata_version != 5)) return error.InvalidNativeAggregateArtifact;
            const proof = candidate.proof orelse load: {
                const reader = try Reader.open(a, store.*, candidate.artifact, member.recipe, token);
                defer reader.cursor().close(reader);
                break :load try stateProof(ca, candidate.artifact, reader.root);
            };
            if (!std.mem.eql(u8, &proof.logical, &try logicalIdentity(candidate.artifact)) or !std.mem.eql(u8, &proof.recipe, &member.recipe.fingerprint()) or proof.groups != candidate.groups) return error.InvalidNativeAggregateArtifact;
            if (proof.depth) |depth| {
                if (depth != member.radix_depth + 1 or depth > max_partition_depth) return error.InvalidNativeAggregateArtifact;
            } else if (isPartitioned(candidate.artifact.metadata_version)) return error.InvalidNativeAggregateArtifact;
            const incoming = proof.physical orelse return null; // Legacy trees retain complete proof.
            if (identity) |prior| {
                if (!std.mem.eql(u8, &prior, &incoming)) return null;
            } else identity = incoming;
            slot.* = proof.slot;
        }
        child.* = .{ .bucket = part.bucket, .groups = part.groups, .identity = identity.?, .slots = slots };
    }
    const members = try ca.alloc([32]u8, roots.len);
    for (members, roots) |*digest, member| digest.* = try rootCommitment(ca, member);
    const bytes = try std.json.Stringify.valueAlloc(ca, Cohort{ .recipe = root.state_recipe.?, .depth = root.radix_depth, .groups = root.groups, .members = members, .children = children }, .{});
    if (bytes.len > max_root_bytes) return error.NativeAggregateArtifactTooLarge;
    var upload = store.*;
    upload.allocator = a;
    const artifact = try upload.putWithCancellation(bytes, token);
    defer if (artifact.provider_version) |version| a.free(version);
    return .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len };
}
/// GC follows both logical child references and their shared descriptor once.
pub fn partitionReferences(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation) !struct { children: []const Ref, cohort: ?ChunkRef } {
    if (!isPartitioned(ref.metadata_version)) return error.InvalidNativeAggregateArtifact;
    const root = try loadRoot(a, store, ref, token, null);
    const children = try a.alloc(Ref, root.partitions.len);
    for (children, root.partitions) |*child, part| child.* = part.artifact;
    return .{ .children = children, .cohort = root.cohort };
}
pub fn loadPartitions(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, cancellation: Cancellation) ![]const Partition {
    return (try loadPartitionDirectory(a, store, ref, recipe, cancellation)).parts;
}
pub const PartitionDirectory = struct { depth: u8, parts: []const Partition };
pub fn loadPartitionDirectory(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, cancellation: Cancellation) !PartitionDirectory {
    if (!isPartitioned(ref.metadata_version)) return error.InvalidNativeAggregateArtifact;
    return loadPartitionDirectoryWithCache(a, store, ref, recipe, cancellation, null);
}
pub fn loadPartitionDirectoryWithCache(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, cancellation: Cancellation, cached: ?CachedRead) !PartitionDirectory {
    if (!isPartitioned(ref.metadata_version)) return error.InvalidNativeAggregateArtifact;
    const reader = try Reader.openWithCache(a, store, ref, recipe, cancellation, cached);
    defer reader.cursor().close(reader);
    // Copy only owned descriptors. The authenticated cache lease remains
    // private to the reader; no JSON encode/decode round trip is necessary.
    const parts = try a.dupe(Partition, reader.root.partitions);
    for (parts) |*part| {
        part.artifact.artifact_id = try a.dupe(u8, part.artifact.artifact_id);
        part.artifact.checksum = try a.dupe(u8, part.artifact.checksum);
        part.artifact.name = try a.dupe(u8, part.artifact.name);
    }
    if (reader.cohort) |cohort| for (parts, cohort.children) |*part, child| {
        part.proof = .{ .logical = try logicalIdentity(part.artifact), .physical = child.identity, .recipe = recipe.fingerprint(), .groups = part.groups, .slot = child.slots[reader.root.state_slot.?], .depth = if (isPartitioned(part.artifact.metadata_version)) reader.root.radix_depth + 1 else null, .bounds = part.bounds };
    };
    return .{ .depth = reader.root.radix_depth, .parts = parts };
}
pub fn partitionDepth(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, token: Cancellation) !u8 {
    const reader = try Reader.open(a, store, ref, recipe, token);
    defer reader.cursor().close(reader);
    return reader.root.radix_depth;
}
pub fn partitionChildren(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation) ![]const Ref {
    const recipe = try loadRecipe(a, store, ref, cancellation);
    const parts = try loadPartitions(a, store, ref, recipe, cancellation);
    const refs = try a.alloc(Ref, parts.len);
    for (refs, parts) |*child, part| child.* = part.artifact;
    return refs;
}

test "external lake partitioned cohorts fuse once per block and decline late mismatches atomically" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-partition-fusion");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "id", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = specs[0], .column = .{ .path = "amount", .type = .integer, .nullable = false } }, .{ .spec = specs[1], .column = null } } };
    const group = try operators.Grouped.create(a, &specs, .{});
    defer group.deinit();
    for (0..128) |id| for (0..2) |_| try group.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(id) })}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }), local.sql_scalar.Datum.fromJson(.{ .integer = 1 }) });
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const out = output.allocator();
    var manager_context: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &manager_context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const refs = try publishPartitionDirectoryAt(a, out, &store, &.{ "stats.sum", "stats.count" }, group, recipe, &manager, .none, 0);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: CachedRead = .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } };
    const same = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, cached);
    defer same.cursor().close(same);
    const left = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, cached);
    try std.testing.expect(left.metadata.?.value == same.metadata.?.value);
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, cached));
    defer left.cursor().close(left);
    const right = try Reader.openWithCache(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, cached);
    defer right.cursor().close(right);
    try std.testing.expect(left.root.partitions.len > 1);
    try left.setOutputSlot(1);
    const Denied = struct {
        fn get(_: *anyopaque, _: A, _: []const u8) ![]u8 {
            return error.UnexpectedAggregateNavigationRead;
        }
        fn canceled(_: *anyopaque, _: A, _: []const u8, _: Cancellation) ![]u8 {
            return error.UnexpectedAggregateNavigationRead;
        }
        fn bounded(_: *anyopaque, _: A, _: []const u8, _: usize, _: Cancellation) ![]u8 {
            return error.UnexpectedAggregateNavigationRead;
        }
    };
    var denied = store.vtable.*;
    denied.get_alloc = Denied.get;
    denied.get_alloc_with_cancellation = Denied.canceled;
    denied.get_bounded_alloc_with_cancellation = Denied.bounded;
    denied.get_bounded_alloc_for = null;
    left.store.vtable = &denied;
    right.store.vtable = &denied;
    try std.testing.expect(left.cohort != null);
    try std.testing.expect(try left.fuse(right, 0));
    left.store = store;
    right.store = store;
    const imported = try operators.Grouped.create(a, &.{ specs[1], specs[0] }, .{});
    defer imported.deinit();
    while (try left.cursor().next(left, out, 7)) |rows| for (rows) |row| {
        try std.testing.expectEqualSlices(u16, &.{ 1, 0 }, row.aggregate_slots.?);
        try imported.importPartialBatch(&.{row});
    };
    var count: usize = 0;
    while (try imported.nextResult(out)) |row| : (count += 1) {
        try std.testing.expectEqual(@as(i64, 2), row.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 18014398509481986), row.aggregates[1].value.integer);
    }
    try std.testing.expectEqual(@as(usize, 128), count);
    try std.testing.expectEqual(left.root.partitions.len, left.blocks_decoded);
    try std.testing.expectEqual(@as(usize, 0), right.blocks_decoded);

    // Replace only the last range with a valid, independently built cohort.
    // All earlier ranges agree; declining fusion must leave them untouched.
    const parts = try out.dupe(Partition, right.root.partitions);
    const replacement = try operators.Grouped.create(a, &specs, .{});
    defer replacement.deinit();
    for (0..@intCast(parts[parts.len - 1].groups)) |id| try replacement.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(id + 1000) })}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 1 }), local.sql_scalar.Datum.fromJson(.{ .integer = 1 }) });
    const independent = try publishCohort(a, out, &store, &.{ "stats.sum", "stats.count" }, replacement, recipe, .none);
    parts[parts.len - 1].artifact = independent[1];
    // A copied descriptor cannot authorize mutated logical child references.
    var tampered_root = right.root;
    tampered_root.partitions = parts;
    const tampered_bytes = try std.json.Stringify.valueAlloc(out, tampered_root, .{});
    var tampered_store = store;
    tampered_store.allocator = out;
    var uploaded_tampered = try tampered_store.put(tampered_bytes);
    defer uploaded_tampered.deinit(out);
    var tampered = refs[1];
    tampered.artifact_id = uploaded_tampered.artifact_id;
    tampered.checksum = uploaded_tampered.checksum;
    tampered.byte_len = uploaded_tampered.byte_len;
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.open(a, store, tampered, .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none));
    var root = right.root;
    root.partitions = parts;
    root.cohort = null;
    root.format = "native-sql-aggregate-v4";
    const bytes = try std.json.Stringify.valueAlloc(out, root, .{});
    var upload = store;
    upload.allocator = out;
    const artifact = try upload.put(bytes);
    defer if (artifact.provider_version) |version| upload.allocator.free(version);
    var mismatched = refs[1];
    mismatched.metadata_version = 4;
    mismatched.artifact_id = artifact.artifact_id;
    mismatched.checksum = artifact.checksum;
    mismatched.byte_len = artifact.byte_len;
    const fresh = try Reader.open(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none);
    defer fresh.cursor().close(fresh);
    const other = try Reader.open(a, store, mismatched, .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none);
    defer other.cursor().close(other);
    try fresh.setOutputSlot(1);
    try std.testing.expect(!try fresh.fuse(other, 0));
    try std.testing.expectEqualSlices(u16, &.{1}, fresh.output_slots.?);
    try std.testing.expect(fresh.partition_states == null);
    count = 0;
    while (try fresh.cursor().next(fresh, out, 11)) |rows| for (rows) |row| {
        try std.testing.expectEqualSlices(u16, &.{1}, row.aggregate_slots.?);
        count += 1;
    };
    try std.testing.expectEqual(@as(usize, 128), count);
}

test "external lake aggregate cursors attach each call including lazy roots and detach before retirement" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Case = enum { success, canceled, expired, closed };
    const Clock = struct {
        var value: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return value.load(.acquire);
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Fixture = struct {
        base: stores.ArtifactStore,
        scheduler: *Runnable.Scheduler,
        signal: *std.atomic.Value(bool),
        block_id: []const u8,
        case: Case,
        calls: usize = 0,
        controls: usize = 0,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn parked(self: *@This()) !void {
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().resource_blocked);
            self.calls += 1;
            var control: Runnable.Execution = .{};
            try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try control.checkpoint();
            control.finish();
            self.controls += 1;
        }
        fn stat(raw: *anyopaque, a: A, id: []const u8, token: Cancellation) !stores.ArtifactMetadata {
            const self = from(raw);
            try self.parked();
            return self.base.statWithCancellationUsingAllocator(a, id, token);
        }
        fn rangeFor(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize, token: Cancellation, context: ReadContext) ![]u8 {
            return local.sql_execution_wait.runOwned(context.wait, range, .{ raw, a, id, offset, len, token }, discard, .{a});
        }
        fn boundedFor(raw: *anyopaque, a: A, id: []const u8, len: usize, token: Cancellation, context: ReadContext) ![]u8 {
            // This deterministic provider models stat and range readiness
            // separately, preserving the cursor's original two-call contract.
            var metadata = try local.sql_execution_wait.runOwned(context.wait, stat, .{ raw, a, id, token }, discardMetadata, .{a});
            defer metadata.deinit(a);
            try context.ensureActive();
            if (metadata.byte_len != len) return error.ArtifactIntegrityMismatch;
            return rangeFor(raw, a, id, 0, len, token, context);
        }
        fn discard(bytes: []u8, a: A) void {
            a.free(bytes);
        }
        fn discardMetadata(value: stores.ArtifactMetadata, a: A) void {
            var owned = value;
            owned.deinit(a);
        }
        fn range(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize, token: Cancellation) ![]u8 {
            const self = from(raw);
            try self.parked();
            const bytes = try self.base.getRangeAllocWithCancellationUsingAllocator(a, id, offset, len, token);
            if (std.mem.eql(u8, id, self.block_id)) switch (self.case) {
                .canceled => self.signal.store(true, .release),
                .expired => Clock.value.store(10, .release),
                .closed => self.scheduler.close(),
                else => {},
            };
            return bytes;
        }
    };
    const Factory = struct {
        store: stores.ArtifactStore,
        reference: Ref,
        recipe: recipes.Recipe,
        cached: ?CachedRead,
        opens: usize = 0,
        fn open(raw: *anyopaque, slot: usize) !local.sql_catalog.AggregatePartialCursor {
            return openFor(raw, slot, null);
        }
        fn scoped(raw: *anyopaque, slot: usize, execution: *Execution) !local.sql_catalog.AggregatePartialCursor {
            return openFor(raw, slot, execution);
        }
        fn openFor(raw: *anyopaque, slot: usize, execution: ?*Execution) !local.sql_catalog.AggregatePartialCursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), slot);
            self.opens += 1;
            return (try Reader.openWithCacheFor(std.testing.allocator, self.store, self.reference, self.recipe, .none, self.cached, execution)).cursor();
        }
    };
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-call-owner");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var base = fs.artifactStore();
    const spec: operators.AggregateSpec = .{ .kind = .count };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = null }} };
    const group = try operators.Grouped.create(a, &.{spec}, .{});
    defer group.deinit();
    try group.ensureGlobalGroup();
    try group.addGlobalCount(23);
    // Keep the payload separate from its root so the fixture can revoke
    // authority during a later pull, after the opening execution has retired.
    var input = PartialStream.init(group, recipe);
    const references = try publishPartialStream(a, a, &base, &.{"stats.count"}, &input, recipe, .none, null);
    defer a.free(references);
    const reference = references[0];
    defer a.free(reference.artifact_id);
    defer a.free(reference.checksum);
    var metadata = std.heap.ArenaAllocator.init(a);
    defer metadata.deinit();
    const root = try loadRoot(metadata.allocator(), base, reference, .none, null);
    for ([_]bool{ false, true }) |cached| for ([_]bool{ false, true }) |composed| for (std.enums.values(Case)) |case| {
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .max_bytes = 64 * 1024 * 1024 });
        var signal: std.atomic.Value(bool) = .init(false);
        Clock.value.store(0, .release);
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        var fixture: Fixture = .{ .base = base, .scheduler = &scheduler, .signal = &signal, .block_id = root.blocks[0].artifact.artifact_id, .case = case };
        var vtable = base.vtable.*;
        vtable.stat_with_cancellation = Fixture.stat;
        vtable.get_range_alloc_with_cancellation = Fixture.range;
        vtable.get_range_alloc_for = Fixture.rangeFor;
        vtable.get_bounded_alloc_for = Fixture.boundedFor;
        const store: stores.ArtifactStore = .{ .allocator = a, .ptr = &fixture, .vtable = &vtable };
        var factory: Factory = .{ .store = store, .reference = reference, .recipe = recipe, .cached = if (cached) .{ .cache = &cache, .scope = @splat(9), .context = .{ .io = std.testing.io } } else null };
        var opening: Execution = .{};
        try opening.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10 }, .query);
        var composition: ?*@import("lake_index_aggregate_composition.zig").Composition = null;
        const cursor = if (composed) lazy: {
            composition = try @import("lake_index_aggregate_composition.zig").Composition.create(a, recipe, .{ .ptr = &factory, .open = Factory.open, .open_for = Factory.scoped });
            break :lazy composition.?.cursor();
        } else try Factory.openFor(&factory, 0, &opening);
        defer cursor.close(cursor.ptr);
        opening.finish();
        if (composed) try std.testing.expectEqual(@as(usize, 0), fixture.calls);
        var execution: Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &signal, .requested = Clock.requested } }, .query);
        defer execution.finish();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const result = cursor.nextFor(&execution, page.allocator(), 1);
        if (case == .success) {
            const rows = (try result).?;
            try std.testing.expectEqual(@as(usize, 1), rows.len);
            var state = try local.sql_aggregate_partial.read(page.allocator(), rows[0], 0, spec);
            defer state.deinit();
            try std.testing.expectEqual(@as(u64, 23), state.count);
        } else try std.testing.expectError(switch (case) {
            .canceled => error.Canceled,
            .expired => error.DeadlineExceeded,
            .closed => error.SchedulerClosed,
            else => unreachable,
        }, result);
        const reader: *Reader = @ptrCast(@alignCast(if (composition) |owner| owner.child.?.ptr else cursor.ptr));
        try std.testing.expect(reader.execution == null and reader.context.execution == null);
        if (reader.cached) |capability| try std.testing.expect(capability.context.execution == null);
        if (composition) |owner| try std.testing.expect(owner.execution == null);
        execution.finish();
        try std.testing.expectEqual(fixture.calls, fixture.controls);
        if (case == .success) {
            var final: Execution = .{};
            try final.start(&scheduler, .{ .io = std.testing.io }, .query);
            defer final.finish();
            try std.testing.expect((try cursor.nextFor(&final, page.allocator(), 1)) == null);
        }
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    };
}

test "external lake shared cohort maps preserve reordered physical slots and reject invalid child slots" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-cohort-slots");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = specs[0], .column = .{ .path = "n", .type = .integer, .nullable = false } }, .{ .spec = specs[1], .column = null } } };
    const reversed: recipes.Recipe = .{ .keys = recipe.keys, .inputs = &.{ recipe.inputs[1], recipe.inputs[0] } };
    const names = [_][]const u8{ "stats.sum", "stats.count" };
    var lists = [_]std.ArrayList(Partition){ .empty, .empty };
    for ([_]bool{ false, true }, 0..) |reverse, bucket| {
        const state = if (reverse) reversed else recipe;
        const group = try operators.Grouped.create(a, &.{ state.inputs[0].spec, state.inputs[1].spec }, .{});
        defer group.deinit();
        const value = local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 });
        const count = local.sql_scalar.Datum.fromJson(.{ .integer = 1 });
        try group.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(bucket) })}, if (reverse) &.{ count, value } else &.{ value, count });
        const refs = try publishCohort(a, out, &store, if (reverse) &.{ names[1], names[0] } else &names, group, state, .none);
        for (&lists, 0..) |*list, slot| try list.append(out, .{ .bucket = @intCast(bucket), .groups = 1, .artifact = refs[if (reverse) 1 - slot else slot] });
    }
    const refs = try publishPartitionRoots(a, out, &store, &names, recipe, &lists, .none);
    const sum = try Reader.open(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none);
    defer sum.cursor().close(sum);
    const count = try Reader.open(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none);
    defer count.cursor().close(count);
    try sum.setOutputSlot(0);
    try std.testing.expect(try sum.fuse(count, 1));
    try std.testing.expect(sum.partition_states == null);
    const result = try operators.Grouped.create(a, &specs, .{});
    defer result.deinit();
    while (try sum.cursor().next(sum, out, 1)) |rows| for (rows) |row| try result.importPartialBatch(&.{row});
    var seen: usize = 0;
    while (try result.nextResult(out)) |row| : (seen += 1) {
        try std.testing.expectEqual(@as(i64, 9007199254740993), row.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 1), row.aggregates[1].value.integer);
    }
    try std.testing.expectEqual(@as(usize, 2), seen);

    // A structurally valid descriptor still has to prove every consumed slot's
    // exact recipe, rather than trusting an out-of-range physical column.
    var invalid = sum.cohort.?;
    const children = try out.dupe(Cohort.Child, invalid.children);
    const slots = try out.dupe(u16, children[0].slots);
    slots[0] = std.math.maxInt(u16);
    children[0].slots = slots;
    invalid.children = children;
    var upload = store;
    upload.allocator = out;
    var proof = try upload.put(try std.json.Stringify.valueAlloc(out, invalid, .{}));
    defer proof.deinit(out);
    var root = sum.root;
    root.cohort = .{ .artifact_id = proof.artifact_id, .checksum = proof.checksum, .byte_len = proof.byte_len };
    var artifact = try upload.put(try std.json.Stringify.valueAlloc(out, root, .{}));
    defer artifact.deinit(out);
    var reference = refs[0];
    reference.artifact_id = artifact.artifact_id;
    reference.checksum = artifact.checksum;
    reference.byte_len = artifact.byte_len;
    const reader = try Reader.open(a, store, reference, root.recipe, .none);
    defer reader.cursor().close(reader);
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, reader.cursor().next(reader, out, 1));
}

test "external lake aggregate publishers carry authenticated proofs without child rereads and share decoded leases" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-proof-handoff");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = specs[0], .column = .{ .path = "n", .type = .integer, .nullable = false } }, .{ .spec = specs[1], .column = null } } };
    const names = [_][]const u8{ "stats.sum", "stats.count" };
    const group = try operators.Grouped.create(a, &specs, .{});
    defer group.deinit();
    for (0..256) |i| try group.add(&.{local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(i) })}, &.{ local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }), local.sql_scalar.Datum.fromJson(.{ .integer = 1 }) });
    const Check = struct {
        fn check(_: *anyopaque) !void {}
        fn value(alloc: A, row: operators.GroupResult, spec: operators.AggregateSpec) !i64 {
            const reducer = try operators.Grouped.create(alloc, &.{spec}, .{});
            defer reducer.deinit();
            try reducer.importPartialBatch(&.{row});
            return (try reducer.nextResult(alloc)).?.aggregates[0].value.integer;
        }
        fn denied(_: *anyopaque, _: A, _: []const u8, _: usize, _: Cancellation) ![]u8 {
            return error.UnexpectedAggregateChildRead;
        }
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, publishPartitioned(a, out, &store, &names, group, recipe, &manager, Cancellation.fromAtomic(&canceled)));
    try std.testing.expectEqual(@as(usize, 0), group.result_cursor);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    const original = store.vtable;
    var denied = original.*;
    denied.get_bounded_alloc_with_cancellation = Check.denied;
    denied.get_bounded_alloc_for = null;
    store.vtable = &denied;
    // Fresh partitions provide their physical identity and logical slot at the
    // upload boundary. Even a completely cold store needs no child-root GETs.
    const refs = try publishPartitionDirectoryAt(a, out, &store, &names, group, recipe, &manager, .none, 0);
    try std.testing.expectEqual(@as(usize, 0), manager.peak_files);
    store.vtable = original;
    var lists = [_]std.ArrayList(Partition){ .empty, .empty };
    for (&lists, refs, 0..) |*list, ref, slot| try list.appendSlice(out, try loadPartitions(out, store, ref, .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] }, .none));
    try std.testing.expect(lists[0].items.len > 1);
    var memo: ProofMemo = .{ .limit = 1 };
    defer memo.deinit(a);
    const first_child = lists[0].items[0].artifact;
    const sum_recipe: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] };
    const memoized = try memo.resolve(a, store, first_child, sum_recipe, .none);
    store.vtable = &denied;
    try std.testing.expectEqualDeep(memoized, try memo.resolve(a, store, first_child, sum_recipe, .none));
    var changed_reference = first_child;
    changed_reference.byte_len += 1;
    try std.testing.expectError(error.UnexpectedAggregateChildRead, memo.resolve(a, store, changed_reference, sum_recipe, .none));
    store.vtable = original;
    _ = try memo.resolve(a, store, lists[0].items[1].artifact, sum_recipe, .none);
    try std.testing.expectEqual(@as(usize, 1), memo.entries.count());
    store.vtable = &denied;
    try std.testing.expectError(error.UnexpectedAggregateChildRead, memo.resolve(a, store, first_child, sum_recipe, .none));
    store.vtable = original;
    for (lists[0].items) |part| try std.testing.expect(part.proof != null);
    // Inherited authenticated parents carry exactly the same bounded proof.
    store.vtable = &denied;
    const inherited = try publishPartitionRoots(a, out, &store, &names, recipe, &lists, .none);
    const saved_proof = lists[0].items[0].proof.?;
    lists[0].items[0].proof.?.logical[0] ^= 1;
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, publishPartitionRoots(a, out, &store, &names, recipe, &lists, .none));
    lists[0].items[0].proof = saved_proof;
    store.vtable = original;
    const wire = try readArtifact(out, store, .{ .artifact_id = inherited[0].artifact_id, .checksum = inherited[0].checksum, .byte_len = inherited[0].byte_len }, .none, null);
    try std.testing.expect(std.mem.indexOf(u8, wire, "\"proof\"") == null);

    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    const sum = try Reader.openWithCache(a, store, inherited[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, cached);
    var sum_open = true;
    defer if (sum_open) sum.cursor().close(sum);
    const count = try Reader.openWithCache(a, store, inherited[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, cached);
    defer count.cursor().close(count);
    try std.testing.expect(sum.cohort_metadata.?.value == count.cohort_metadata.?.value);
    var tampered_root = sum.root;
    const tampered_parts = try out.dupe(Partition, tampered_root.partitions);
    tampered_parts[0].artifact = tampered_parts[1].artifact;
    tampered_root.partitions = tampered_parts;
    var upload = store;
    upload.allocator = out;
    var tampered = try upload.put(try std.json.Stringify.valueAlloc(out, tampered_root, .{}));
    defer tampered.deinit(out);
    var tampered_ref = inherited[0];
    tampered_ref.artifact_id = tampered.artifact_id;
    tampered_ref.checksum = tampered.checksum;
    tampered_ref.byte_len = tampered.byte_len;
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.openWithCache(a, store, tampered_ref, .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, cached));
    const sum_rows = (try sum.cursor().next(sum, out, 1)).?;
    const count_rows = (try count.cursor().next(count, out, 1)).?;
    try std.testing.expectEqual(@as(i64, 9007199254740993), try Check.value(out, sum_rows[0], specs[0]));
    try std.testing.expectEqual(@as(i64, 1), try Check.value(out, count_rows[0], specs[1]));
    try std.testing.expect(sum.child.?.block_metadata.?.value == count.child.?.block_metadata.?.value);
    const block = count.child.?.typed_columns[0].value;
    sum.cursor().close(sum);
    sum_open = false;
    try std.testing.expectEqual(@as(u64, 1), block.states.?[0].count);
    var isolated = cached;
    isolated.scope = @splat(2);
    const other = try Reader.openWithCache(a, store, inherited[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, isolated);
    defer other.cursor().close(other);
    try std.testing.expect(other.cohort_metadata.?.value != count.cohort_metadata.?.value);
    _ = try other.cursor().next(other, out, 1);
    try std.testing.expect(other.child.?.typed_columns[0].value != block);
    count.cached.?.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, count.cursor().next(count, out, 1));
}

test "external lake typed aggregate blocks preserve distinct exact sums extrema and nulls on warm reads" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-typed-partials");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const specs = [_]operators.AggregateSpec{
        .{ .kind = .count, .input_type = .integer, .distinct = true },
        .{ .kind = .sum, .input_type = .integer },
        .{ .kind = .min, .input_type = .string },
        .{ .kind = .count },
    };
    const recipe: recipes.Recipe = .{ .keys = &.{}, .inputs = &.{
        .{ .spec = specs[0], .column = .{ .path = "n", .type = .integer, .nullable = true } },
        .{ .spec = specs[1], .column = .{ .path = "n", .type = .integer, .nullable = true } },
        .{ .spec = specs[2], .column = .{ .path = "s", .type = .string, .nullable = true } },
        .{ .spec = specs[3], .column = null },
    } };
    const group = try operators.Grouped.create(a, &specs, .{});
    defer group.deinit();
    const D = local.sql_scalar.Datum;
    for ([_]?i64{ 9007199254740993, 9007199254740993, null, 3 }, [_]?[]const u8{ "z", "alpha", null, "b" }) |number, text| {
        const n: D = if (number) |v| D.json(.{ .integer = v }) else .{};
        try group.add(&.{}, &.{ n, n, if (text) |v| D.json(.{ .string = v }) else .{}, D.json(.{ .integer = 1 }) });
    }
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const out = output.allocator();
    const refs = try publishCohort(a, out, &store, &.{ "stats.distinct", "stats.sum", "stats.min", "stats.count" }, group, recipe, .none);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: CachedRead = .{ .cache = &cache, .scope = @splat(21), .context = .{ .io = std.testing.io } };
    const reader = try Reader.openWithCache(a, store, refs[0], .{ .keys = &.{}, .inputs = recipe.inputs[0..1] }, .none, cached);
    defer reader.cursor().close(reader);
    try reader.setOutputSlot(0);
    for (1..4) |slot| {
        const other = try Reader.openWithCache(a, store, refs[slot], .{ .keys = &.{}, .inputs = recipe.inputs[slot..][0..1] }, .none, cached);
        defer other.cursor().close(other);
        try std.testing.expect(try reader.fuse(other, @intCast(slot)));
    }
    const partials = (try reader.cursor().next(reader, out, 256)).?;
    try std.testing.expectEqual(@as(usize, 0), partials[0].aggregates.len);
    try std.testing.expectEqual(@as(usize, 4), partials[0].typed_states.?.len);
    for (0..2) |_| {
        // Repeated imports must leave every cached reducer immutable.
        const result = try operators.Grouped.create(a, &specs, .{});
        defer result.deinit();
        try result.importPartialBatch(partials);
        const row = (try result.nextResult(out)).?;
        try std.testing.expectEqual(@as(i64, 2), row.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 18014398509481989), row.aggregates[1].value.integer);
        try std.testing.expectEqualStrings("alpha", row.aggregates[2].value.string);
        try std.testing.expectEqual(@as(i64, 4), row.aggregates[3].value.integer);
    }
    const warm = try Reader.openWithCache(a, store, refs[1], .{ .keys = &.{}, .inputs = recipe.inputs[1..2] }, .none, cached);
    defer warm.cursor().close(warm);
    const warm_rows = (try warm.cursor().next(warm, out, 1)).?;
    try std.testing.expect(reader.typed_columns[1].value == warm.typed_columns[0].value);
    try std.testing.expectEqual(@as(i128, 18014398509481989), warm_rows[0].typed_states.?[0].integer_sum);
    // Several individually admissible columns must not multiply the page's
    // retained expansion allowance. Declining after the first lease releases
    // the partial set and preserves exact encoded imports without poisoning
    // reusable column entries for other readers.
    const bounded = try Reader.openWithCache(a, store, refs[0], .{ .keys = &.{}, .inputs = recipe.inputs[0..1] }, .none, cached);
    defer bounded.cursor().close(bounded);
    try bounded.setOutputSlot(0);
    for (1..4) |slot| {
        const other = try Reader.openWithCache(a, store, refs[slot], .{ .keys = &.{}, .inputs = recipe.inputs[slot..][0..1] }, .none, cached);
        defer other.cursor().close(other);
        try std.testing.expect(try bounded.fuse(other, @intCast(slot)));
    }
    bounded.max_typed_bytes = reader.typed_columns[0].lease.item.budget.live + reader.typed_columns[1].lease.item.budget.live - 1;
    const fallback_rows = (try bounded.cursor().next(bounded, out, 256)).?;
    try std.testing.expectEqual(@as(usize, 0), bounded.typed_columns.len);
    try std.testing.expect(fallback_rows[0].typed_states == null);
    const fallback_group = try operators.Grouped.create(a, &specs, .{});
    defer fallback_group.deinit();
    try fallback_group.importPartialBatch(fallback_rows);
    const fallback_row = (try fallback_group.nextResult(out)).?;
    try std.testing.expectEqual(@as(i64, 2), fallback_row.aggregates[0].value.integer);
    try std.testing.expectEqual(@as(i64, 18014398509481989), fallback_row.aggregates[1].value.integer);
    try std.testing.expectEqualStrings("alpha", fallback_row.aggregates[2].value.string);
    try std.testing.expectEqual(@as(i64, 4), fallback_row.aggregates[3].value.integer);
    // A different interpretation of identical physical bytes cannot hit the
    // typed cache. Signature validation still fails before importing a state.
    var incompatible = recipe;
    const inputs = try out.dupe(@TypeOf(recipe.inputs[0]), recipe.inputs);
    inputs[1].spec.input_type = .number;
    incompatible.inputs = inputs;
    const ref = reader.root.blocks[0].artifact;
    const fingerprint = incompatible.fingerprint();
    try std.testing.expectError(error.InvalidSqlSpill, @import("lake_index_decoded_metadata.zig").acquireInterpreted(TypedColumn, cached, store, .{ .kind = .algebraic_segment, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, &fingerprint, TypedColumn.Args{ .block = reader.block.?, .recipe = incompatible, .column = 1 }, TypedColumn.decode));
}

test "external lake typed aggregate caching selects columns and shares dictionary reducers" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-selected-partials");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .min, .input_type = .string }, .{ .kind = .max, .input_type = .string } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "key", .type = .integer, .nullable = false }}, .inputs = &.{
        .{ .spec = specs[0], .column = null },
        .{ .spec = specs[1], .column = .{ .path = "s", .type = .string, .nullable = false } },
        .{ .spec = specs[2], .column = .{ .path = "s", .type = .string, .nullable = false } },
    } };
    const group = try operators.Grouped.create(a, &specs, .{ .groups = 1024, .bytes = 128 * 1024 * 1024 });
    defer group.deinit();
    const text = try a.alloc(u8, 64 * 1024);
    defer a.free(text);
    @memset(text, 'a');
    const D = local.sql_scalar.Datum;
    for (0..256) |row| try group.add(&.{D.json(.{ .integer = @intCast(row) })}, &.{ D.json(.{ .integer = 1 }), D.json(.{ .string = text }), D.json(.{ .string = text }) });
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const out = output.allocator();
    const refs = try publishCohort(a, out, &store, &.{ "stats.count", "stats.min", "stats.max" }, group, recipe, .none);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: CachedRead = .{ .cache = &cache, .scope = @splat(22), .context = .{ .io = std.testing.io } };
    const count = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, cached);
    defer count.cursor().close(count);
    try std.testing.expectEqual(@as(u8, 1), count.root.blocks[0].state_layout);
    const counts = (try count.cursor().next(count, out, 256)).?;
    try std.testing.expectEqual(native_partial.physicalWidth(recipe.inputs), count.block.?.values.len);
    try std.testing.expectEqual(@as(usize, 256), counts.len);
    try std.testing.expectEqual(@as(usize, 1), count.typed_columns.len);
    try std.testing.expectEqual(@as(usize, 1), count.typed_columns[0].value.decoded_states);
    const minimum = try Reader.openWithCache(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, cached);
    defer minimum.cursor().close(minimum);
    const minima = (try minimum.cursor().next(minimum, out, 256)).?;
    try std.testing.expectEqual(@as(usize, 1), minimum.typed_columns.len);
    try std.testing.expectEqual(@as(usize, 1), minimum.typed_columns[0].value.decoded_states);
    try std.testing.expect(count.block_metadata.?.value == minimum.block_metadata.?.value);
    try std.testing.expect(minima[0].typed_states.?[0].selected.?.row.values[0].value.string.ptr == minima[255].typed_states.?[0].selected.?.row.values[0].value.string.ptr);
    const warm = try Reader.openWithCache(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, cached);
    defer warm.cursor().close(warm);
    _ = try warm.cursor().next(warm, out, 1);
    try std.testing.expect(warm.typed_columns[0].value == minimum.typed_columns[0].value);
}

// Fault exploration must not depend on whether the host can grow an arena's
// backing mapping in place after a different allocation history.
fn readPackedFailureScenario(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe) !void {
    const Stable = struct {
        backing: A,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ret);
        }
    };
    var stable: Stable = .{ .backing = a };
    try readFailureScenario(.{ .ptr = &stable, .vtable = &.{ .alloc = Stable.alloc, .resize = Stable.resize, .remap = Stable.remap, .free = Stable.free } }, store, ref, recipe);
}

test "external lake packed leaves bound writes and build directories share authenticated metadata" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-leaf-packs");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const Counter = struct {
        var base: stores.ArtifactStore = undefined;
        var writes: usize = 0;
        fn put(_: *anyopaque, alloc: A, bytes: []const u8, token: Cancellation) !stores.ArtifactMetadata {
            writes += 1;
            var copy = base;
            copy.allocator = alloc;
            return copy.putWithCancellation(bytes, token);
        }
        fn check(_: *anyopaque) !void {}
    };
    Counter.base = store;
    Counter.writes = 0;
    var counted = store.vtable.*;
    counted.put_with_cancellation = Counter.put;
    store.vtable = &counted;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const specs: [4]operators.AggregateSpec = @splat(.{ .kind = .count });
    const inputs: [4]recipes.Input = @splat(.{ .spec = specs[0], .column = null });
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "id", .type = .integer, .nullable = false }}, .inputs = &inputs };
    const group = try operators.Grouped.create(a, &specs, .{});
    defer group.deinit();
    const values: [4]local.sql_scalar.Datum = @splat(local.sql_scalar.Datum.json(.{ .integer = 1 }));
    for (0..32) |i| try group.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, &values);
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Counter.check, .async_writes = false };
    defer manager.deinit();
    const small = try publishPartitioned(a, out, &store, &.{ "a", "b", "c", "d" }, group, recipe, &manager, .none);
    try std.testing.expectEqual(@as(usize, 1), Counter.writes); // 120 -> 28 -> one bounded pack.
    try std.testing.expectEqual(@as(u16, 6), small[0].metadata_version);
    const routed = try operators.Grouped.create(a, &specs, .{});
    defer routed.deinit();
    for (0..512) |i| try routed.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, &values);
    const refs = try publishPartitioned(a, out, &store, &.{ "a", "b", "c", "d" }, routed, recipe, &manager, .none);
    try std.testing.expect(Counter.writes <= 70);
    var cache = local.serverless_query_lake_serving_cache.Cache.initWithMemoryLimit(a, 2 * 1024 * 1024);
    defer cache.deinit();
    cache.decoded.max_bytes = 4 * 1024 * 1024;
    const cached: CachedRead = .{ .cache = &cache, .scope = @splat(67), .context = .{ .io = std.testing.io } };
    var first: ?Ref = null;
    for (0..2) |pass| {
        const before = cache.stats.provider_reads;
        for (refs, 0..) |ref, slot| {
            const single: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] };
            const parts = (try loadPartitionDirectoryWithCache(out, store, ref, single, .none, cached)).parts;
            try std.testing.expectEqual(@as(usize, 64), parts.len);
            if (slot == 0) first = parts[0].artifact;
        }
        try std.testing.expectEqual(@as(u64, if (pass == 0) 5 else 0), cache.stats.provider_reads - before);
    }
    for (refs, 0..) |ref, slot| {
        const single: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] };
        const reader = try Reader.openWithCache(a, store, ref, single, .none, cached);
        defer reader.cursor().close(reader);
        var rows: usize = 0;
        while (try reader.cursor().next(reader, out, 7)) |batch| {
            for (batch) |row| {
                var state = try local.sql_aggregate_partial.read(out, row, 0, specs[slot]);
                defer state.deinit();
                try std.testing.expectEqual(@as(u64, 1), state.count);
                rows += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 512), rows);
    }
    var wide_recipe = recipe;
    const long_path = try out.alloc(u8, 512 * 1024);
    @memset(long_path, 'k');
    wide_recipe.keys = try out.dupe(recipes.Key, recipe.keys);
    @constCast(wide_recipe.keys)[0].path = long_path;
    // Use actual producer checkpoints, keeping their state width/signatures.
    const tiny = try operators.Grouped.create(a, &specs, .{});
    defer tiny.deinit();
    try tiny.add(&.{local.sql_scalar.Datum.json(.{ .integer = 1 })}, &values);
    var partial_stream: PartialStream = .{ .group = tiny };
    var wide_names: [4][]const u8 = undefined;
    for (&wide_names, 0..) |*name, slot| {
        const text = try out.alloc(u8, 1024 * 1024);
        @memset(text, @as(u8, 'a') + @as(u8, @intCast(slot)));
        name.* = text;
    }
    const fallback = try publishPackedRows(a, out, &store, &wide_names, try partial_stream.next(out), wide_recipe, .none, null, 0);
    try std.testing.expectEqual(@as(u16, 2), fallback[0].metadata_version);
    const leaf = first.?;
    try std.testing.expectEqual(@as(u16, 6), leaf.metadata_version);
    const single: recipes.Recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] };
    try std.testing.checkAllAllocationFailures(a, readPackedFailureScenario, .{ store, leaf, single });
    try std.testing.expectEqual(@as(usize, 0), (try descendantsAlloc(out, store, leaf, .none)).len);
    const bytes = try store.getVerifiedAllocWithCancellation(leaf.artifact_id, leaf.byte_len, leaf.checksum, .none);
    defer a.free(bytes);
    bytes[bytes.len - 1] ^= 1;
    var corrupt = try store.putWithCancellation(bytes, .none);
    defer corrupt.deinit(store.allocator);
    var bad = leaf;
    bad.artifact_id = corrupt.artifact_id;
    bad.checksum = corrupt.checksum;
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.open(a, store, bad, single, .none));
    bad = leaf;
    bad.name = "missing";
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.open(a, store, bad, single, .none));
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, descendantsAlloc(out, store, bad, .none));
}

test "external lake adaptive packs split wide resident groups by encoded bytes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("wide-aggregate-pages");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    const specs = [_]operators.AggregateSpec{.{ .kind = .count }};
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = &.{.{ .spec = specs[0], .column = null }} };
    const group = try operators.Grouped.create(a, &specs, .{ .bytes = 8 * 1024 * 1024 });
    defer group.deinit();
    const key = try a.alloc(u8, 34004);
    defer a.free(key);
    @memset(key, 'x');
    for (0..128) |i| {
        std.mem.writeInt(u32, key[0..4], @intCast(i), .little);
        try group.add(&.{local.sql_scalar.Datum.json(.{ .string = key })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    }
    const refs = try publishPartitioned(a, out, &store, &.{"stats.count"}, group, recipe, &manager, .none);
    try std.testing.expectEqual(metadata_version, refs[0].metadata_version);
    const reader = try Reader.open(a, store, refs[0], recipe, .none);
    defer reader.cursor().close(reader);
    try std.testing.expect(reader.root.blocks.len > 1);
    for (reader.root.blocks) |block| try std.testing.expect(block.artifact.byte_len <= max_block_bytes);
    var count: usize = 0;
    while (try reader.cursor().next(reader, out, 256)) |rows| count += rows.len;
    try std.testing.expectEqual(@as(usize, 128), count);
}

test "external lake aggregate point selection reads one authenticated radix bucket" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var temporary = try local.common_test_directory.TestDirectory.init("aggregate-point-read");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    const specs = [_]operators.AggregateSpec{.{ .kind = .count }};
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = true }}, .inputs = &.{.{ .spec = specs[0], .column = null }} };
    const group = try operators.Grouped.create(a, &specs, .{ .bytes = 8 * 1024 * 1024 });
    defer group.deinit();
    for (0..512) |index| try group.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(index) })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    try group.add(&.{.{}}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    const refs = try publishPartitioned(a, out, &store, &.{"stats.count"}, group, recipe, &manager, .none);
    const reader = try Reader.open(a, store, refs[0], recipe, .none);
    defer reader.cursor().close(reader);
    try std.testing.expect(reader.root.partitions.len > 1);
    try reader.cursor().select_point.?(reader, &.{local.sql_scalar.Datum.json(.{ .integer = 123 })});
    var found = false;
    var seen: usize = 0;
    while (try reader.cursor().next(reader, out, 256)) |rows| for (rows) |row| {
        seen += 1;
        if (!row.keys[0].sql_null and row.keys[0].value.integer == 123) found = true;
    };
    try std.testing.expect(found);
    try std.testing.expect(seen < 32);
    try std.testing.expectEqual(@as(usize, 1), reader.blocks_decoded);
    const null_reader = try Reader.open(a, store, refs[0], recipe, .none);
    defer null_reader.cursor().close(null_reader);
    try null_reader.cursor().select_point.?(null_reader, &.{.{}});
    found = false;
    seen = 0;
    while (try null_reader.cursor().next(null_reader, out, 256)) |rows| for (rows) |row| {
        seen += 1;
        found = found or row.keys[0].sql_null;
    };
    try std.testing.expect(found);
    try std.testing.expect(seen < 32);
    try std.testing.expectEqual(@as(usize, 1), null_reader.blocks_decoded);
}

test "external lake aggregate ordered frontiers prune payloads and page both directions" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var temporary = try local.common_test_directory.TestDirectory.init("aggregate-ordered-frontier");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    const specs = [_]operators.AggregateSpec{.{ .kind = .count }};
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = true }}, .inputs = &.{.{ .spec = specs[0], .column = null }} };
    const group = try operators.Grouped.create(a, &specs, .{ .bytes = 8 * 1024 * 1024 });
    defer group.deinit();
    for (0..1024) |index| try group.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(1023 - index) })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    try group.add(&.{.{}}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    const refs = try publishPartitioned(a, out, &store, &.{"stats.count"}, group, recipe, &manager, .none);
    for ([_]bool{ false, true }) |reverse| {
        const reader = try Reader.open(a, store, refs[0], recipe, .none);
        defer reader.cursor().close(reader);
        try std.testing.expect(try reader.cursor().select_keys.?(reader, .{ .maximum_groups = 9, .order = &.{.{ .column = "k", .descending = reverse, .nulls_first = reverse }} }));
        for (0..3) |page_index| {
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            const rows = (try reader.cursor().next(reader, page.allocator(), 3)).?;
            try std.testing.expectEqual(@as(usize, 3), rows.len);
            for (rows, page_index * 3..) |row, ordinal| {
                if (reverse and ordinal == 0) try std.testing.expect(row.keys[0].sql_null) else try std.testing.expectEqual(@as(i64, @intCast(if (reverse) 1024 - ordinal else ordinal)), row.keys[0].value.integer);
                const state = try local.sql_aggregate_partial.read(page.allocator(), row, 0, specs[0]);
                try std.testing.expectEqual(@as(u64, 1), state.count);
            }
        }
        try std.testing.expect(reader.blocks_decoded < 24);
        try std.testing.expect(reader.ordered_plan.?.bounded);
        try std.testing.expect(reader.ordered_plan.?.leaves.items.len < reader.root.partitions.len / 2);
        try std.testing.expect((try reader.cursor().next(reader, out, 3)) == null);
    }
    for ([_]bool{ false, true }) |reverse| {
        const fragmented = try Reader.open(a, store, refs[0], recipe, .none);
        defer fragmented.cursor().close(fragmented);
        try std.testing.expect(try fragmented.cursor().select_keys.?(fragmented, .{ .maximum_groups = 10, .batch_rows = 8, .page_limit = 2, .order = &.{.{ .column = "k", .descending = reverse, .nulls_first = reverse }} }));
        for (0..10) |i| {
            const row = (try fragmented.cursor().next(fragmented, out, 1)).?[0];
            if (reverse and i == 0) try std.testing.expect(row.keys[0].sql_null) else try std.testing.expectEqual(@as(i64, @intCast(if (reverse) 1024 - i else i)), row.keys[0].value.integer);
        }
        try std.testing.expect((try fragmented.cursor().next(fragmented, out, 1)) == null);
    }
    const reader = try Reader.open(a, store, refs[0], recipe, .none);
    defer reader.cursor().close(reader);
    try std.testing.expect(try reader.cursor().select_keys.?(reader, .{ .conditions = &.{.{ .column = "k", .op = .gte, .value = .{ .integer = 1000 } }}, .order = &.{.{ .column = "k" }} }));
    const rows = (try reader.cursor().next(reader, out, 5)).?;
    for (rows, 1000..) |row, ordinal| try std.testing.expectEqual(@as(i64, @intCast(ordinal)), row.keys[0].value.integer);
    const exact_range = try Reader.open(a, store, refs[0], recipe, .none);
    defer exact_range.cursor().close(exact_range);
    try std.testing.expect(try exact_range.cursor().select_keys.?(exact_range, .{ .memory_bytes = 128 * 1024, .row_limit = 1, .conditions = &.{ .{ .column = "k", .op = .gte, .value = .{ .integer = 500 } }, .{ .column = "k", .op = .lt, .value = .{ .integer = 501 } } }, .order = &.{.{ .column = "k" }} }));
    try std.testing.expectEqual(@as(usize, 1), exact_range.ordered_plan.?.leaves.items.len);
    try std.testing.expectEqual(@as(i64, 500), (try exact_range.cursor().next(exact_range, out, 1)).?[0].keys[0].value.integer);
    try std.testing.expect((try exact_range.cursor().next(exact_range, out, 1)) == null);
    const bounded = try Reader.open(a, store, refs[0], recipe, .none);
    defer bounded.cursor().close(bounded);
    try std.testing.expect(try bounded.cursor().select_keys.?(bounded, .{ .row_limit = 1, .order = &.{.{ .column = "k" }} }));
    _ = try bounded.cursor().next(bounded, out, 1);
    try std.testing.expectError(error.SqlProgramLimitExceeded, bounded.cursor().next(bounded, out, 1));
    const ranged = try Reader.open(a, store, refs[0], recipe, .none);
    defer ranged.cursor().close(ranged);
    try std.testing.expect(!try ranged.cursor().select_keys.?(ranged, .{ .conditions = &.{.{ .column = "k", .op = .gte, .value = .{ .integer = 1000 } }} }));
    var matches: usize = 0;
    while (try ranged.cursor().next(ranged, out, 256)) |page| for (page) |row| {
        if (!row.keys[0].sql_null and row.keys[0].value.integer >= 1000) matches += 1;
    };
    try std.testing.expectEqual(@as(usize, 24), matches);
    try std.testing.expect(ranged.blocks_decoded < 40);
    const pair_recipe: recipes.Recipe = .{ .keys = &.{ .{ .path = "category", .type = .integer, .nullable = false }, .{ .path = "id", .type = .integer, .nullable = false } }, .inputs = recipe.inputs };
    const pairs = try operators.Grouped.create(a, &specs, .{ .bytes = 1024 * 1024 });
    defer pairs.deinit();
    for (0..32) |index| try pairs.add(&.{ local.sql_scalar.Datum.json(.{ .integer = @intCast((31 - index) / 8) }), local.sql_scalar.Datum.json(.{ .integer = @intCast(31 - index) }) }, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    const pair_refs = try publishPartitionDirectoryAt(a, out, &store, &.{"stats.count"}, pairs, pair_recipe, &manager, .none, 0);
    const prefix = try Reader.open(a, store, pair_refs[0], pair_recipe, .none);
    defer prefix.cursor().close(prefix);
    try std.testing.expect(try prefix.cursor().select_keys.?(prefix, .{ .order = &.{.{ .column = "category" }} }));
    for (0..3) |page_index| {
        const page = (try prefix.cursor().next(prefix, out, 3)).?;
        for (page, page_index * 3..) |row, ordinal| {
            try std.testing.expectEqual(ordinal, row.ordinal);
            try std.testing.expectEqual(@as(i64, @intCast(ordinal)), row.keys[1].value.integer);
        }
    }
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, orderedFaultScenario, .{ store, pair_refs[0], pair_recipe });
    const wide_recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "text", .type = .string, .nullable = false }}, .inputs = recipe.inputs };
    const wide = try operators.Grouped.create(a, &specs, .{ .bytes = 2 * 1024 * 1024 });
    defer wide.deinit();
    var label: [1028]u8 = @splat('x');
    for (0..256) |index| {
        _ = try std.fmt.bufPrint(label[1024..], "{d:0>4}", .{255 - index});
        try wide.add(&.{local.sql_scalar.Datum.json(.{ .string = &label })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    }
    var limited_index: ordered_keys.Builder = .{ .store = &store, .token = .none, .recipe = wide_recipe, .writes = 1 };
    defer limited_index.deinit();
    try limited_index.add(a, &.{.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = "sample" })}, .values = &.{}, .ordinal = 0 }}, 0);
    try std.testing.expect(limited_index.result() == null);
    const wide_refs = try publishCohort(a, out, &store, &.{"stats.count"}, wide, wide_recipe, .none);
    const spilled = try Reader.open(a, store, wide_refs[0], wide_recipe, .none);
    defer spilled.cursor().close(spilled);
    try std.testing.expect(try spilled.cursor().select_keys.?(spilled, .{ .memory_bytes = 256 * 1024, .spill = &manager, .order = &.{.{ .column = "text" }} }));
    const before_spill = manager.written_bytes;
    const spilled_rows = (try spilled.cursor().next(spilled, out, 100)).?;
    try std.testing.expectEqual(@as(usize, 100), spilled_rows.len);
    try std.testing.expectEqualStrings("0000", spilled_rows[0].keys[0].value.string[1024..]);
    try std.testing.expectEqualStrings("0099", spilled_rows[99].keys[0].value.string[1024..]);
    try std.testing.expectEqual(before_spill, manager.written_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    for ([_]bool{ false, true }) |reverse| {
        const paged = try Reader.open(a, store, wide_refs[0], wide_recipe, .none);
        defer paged.cursor().close(paged);
        try std.testing.expect(paged.root.key_index != null);
        try std.testing.expect(try paged.cursor().select_keys.?(paged, .{ .row_limit = 256, .page_limit = 3, .memory_bytes = 512 * 1024, .order = &.{.{ .column = "text", .descending = reverse }} }));
        var delivered: usize = 0;
        while (try paged.cursor().next(paged, out, 100)) |page| {
            for (page) |row| {
                const expected = if (reverse) 255 - delivered else delivered;
                var suffix: [4]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&suffix, "{d:0>4}", .{expected}), row.keys[0].value.string[1024..]);
                try std.testing.expectEqual(delivered, row.ordinal);
                delivered += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 256), delivered);
        try std.testing.expectEqual(@as(usize, 0), paged.selection_rows_left);
        try std.testing.expectEqual(@as(usize, 1), paged.blocks_decoded);
    }
    const declined = try Reader.open(a, store, wide_refs[0], wide_recipe, .none);
    defer declined.cursor().close(declined);
    const denied = try ordered_keys.Plan(Reader).prepare(declined, 1, null);
    defer if (denied) |plan| plan.deinit();
    try std.testing.expect(denied == null);
    try std.testing.expect(!try declined.cursor().select_keys.?(declined, .{ .memory_bytes = 32 * 1024, .order = &.{.{ .column = "text" }} }));
    try std.testing.expectEqual(@as(u64, 0), declined.emitted);
    // Declining navigation preserves the ordinary one-pass sort input.
    try std.testing.expectEqual(@as(usize, 256), (try declined.cursor().next(declined, out, 256)).?.len);
}
fn orderedFaultScenario(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe) !void {
    for ([_]?usize{ null, 3 }) |maximum| {
        const reader = try Reader.open(a, store, ref, recipe, .none);
        defer reader.cursor().close(reader);
        _ = try reader.cursor().select_keys.?(reader, .{ .maximum_groups = maximum, .order = &.{.{ .column = "category" }} });
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const rows = (try reader.cursor().next(reader, page.allocator(), 3)).?;
        try std.testing.expectEqual(@as(usize, 3), rows.len);
    }
}

test "external lake aggregate ordered persistent group state prunes long prefixes and rewrites changed groups" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("ordered-group-state");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @import("lake_index_publication.zig").uploadDomain(4, @splat(9)), .attempt = @splat(1) };
    const specs = [_]operators.AggregateSpec{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer }, .{ .kind = .min, .input_type = .integer }, .{ .kind = .max, .input_type = .integer } };
    const inputs = [_]recipes.Input{ .{ .spec = specs[0], .column = null }, .{ .spec = specs[1], .column = .{ .path = "v", .type = .integer, .nullable = false } }, .{ .spec = specs[2], .column = .{ .path = "v", .type = .integer, .nullable = false } }, .{ .spec = specs[3], .column = .{ .path = "v", .type = .integer, .nullable = false } } };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = &inputs };
    const names = &.{ "stats.count", "stats.sum", "stats.min", "stats.max" };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    var indexes: [2]state_tree.Index = undefined;
    var label: [84]u8 = @splat('x');
    for (&indexes, 0..) |*index, side| {
        const grouped = try operators.Grouped.create(a, &specs, .{ .bytes = 8 * 1024 * 1024 });
        defer grouped.deinit();
        for (0..2048) |i| {
            _ = try std.fmt.bufPrint(label[80..], "{d:0>4}", .{2047 - i});
            const value = local.sql_scalar.Datum.json(.{ .integer = @as(i64, if (side == 0) 10 else 20) });
            try grouped.add(&.{local.sql_scalar.Datum.json(.{ .string = &label })}, &.{ value, value, value, value });
        }
        const refs = try publishKeyed(a, out, &store, names, grouped, recipe, &manager, .none);
        try std.testing.expectEqual(@as(u16, 8), refs[0].metadata_version);
        const reader = try Reader.open(a, store, refs[0], .{ .keys = recipe.keys, .inputs = inputs[0..1] }, .none);
        defer reader.cursor().close(reader);
        index.* = reader.root.state_tree.?;
    }
    var cold_metrics: state_tree.Metrics = .{};
    const parent = try state_tree.merge(a, &store, recipe, indexes[0], indexes[1], null, .none, &cold_metrics);
    const refs = try publishTree(a, out, &store, names, recipe, parent, .none);
    for (refs[1..]) |ref| try std.testing.expectEqualStrings(refs[0].artifact_id, ref.artifact_id);
    var unknown = refs[0];
    unknown.name = "missing.slot";
    try std.testing.expectError(error.InvalidNativeAggregateArtifact, Reader.open(a, store, unknown, .{ .keys = recipe.keys, .inputs = inputs[0..1] }, .none));
    // ORDER BY absence is explicit; an unsupported requested sort cannot
    // accidentally turn its LIMIT into an arbitrary source prefix.
    for ([_]bool{ true, false }) |unordered| {
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        const limited = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = inputs[0..1] }, .none, .{ .cache = &cache, .scope = @splat(8), .context = .{ .io = std.testing.io } });
        defer limited.cursor().close(limited);
        try std.testing.expect(!try limited.cursor().select_keys.?(limited, .{ .maximum_groups = 1, .unordered_prefix = unordered }));
        const result = (try limited.cursor().next(limited, out, 4096)).?;
        try std.testing.expectEqual(@as(usize, if (unordered) 1 else 256), result.len);
        if (unordered) {
            try std.testing.expectEqual(@as(usize, 0), limited.tree_prefetches);
            try std.testing.expect((try limited.cursor().next(limited, out, 4096)) == null);
        }
    }
    for ([_]bool{ false, true }) |reverse| {
        const reader = try Reader.open(a, store, refs[0], .{ .keys = recipe.keys, .inputs = inputs[0..1] }, .none);
        defer reader.cursor().close(reader);
        try std.testing.expect(try reader.cursor().select_keys.?(reader, .{ .maximum_groups = 1, .order = &.{.{ .column = "k", .descending = reverse }} }));
        const result = (try reader.cursor().next(reader, out, 256)).?;
        try std.testing.expectEqual(@as(usize, 1), result.len);
        try std.testing.expectEqualStrings(if (reverse) "2047" else "0000", result[0].keys[0].value.string[80..]);
        try std.testing.expect(reader.tree_access.?.reads > 512 * 1024 * 1024 - 256 * 1024);
        try std.testing.expectEqual(@as(usize, 1), reader.blocks_decoded);
        try std.testing.expect((try reader.cursor().next(reader, out, 256)) == null);
    }
    {
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        const cached: CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{ .io = std.testing.io } };
        for ([_]bool{ false, true }) |early| {
            const reader = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = inputs[0..1] }, .none, cached);
            defer reader.cursor().close(reader);
            const other = try Reader.openWithCache(a, store, refs[1], .{ .keys = recipe.keys, .inputs = inputs[1..2] }, .none, cached);
            defer other.cursor().close(other);
            try std.testing.expect(reader.tree_metadata.?.value == other.tree_metadata.?.value);
            try reader.setOutputSlot(0);
            try std.testing.expect(try reader.fuse(other, 1));
            try std.testing.expect(try reader.cursor().select_keys.?(reader, .{ .maximum_groups = 700, .order = &.{.{ .column = "k" }} }));
            const limit: usize = if (early) 1 else 700;
            for (0..limit) |i| {
                const result = (try reader.cursor().next(reader, out, 1)).?;
                var suffix: [4]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&suffix, "{d:0>4}", .{i}), result[0].keys[0].value.string[80..]);
            }
            try std.testing.expect(reader.tree_prefetches > 0);
            if (!early) try std.testing.expect((try reader.cursor().next(reader, out, 1)) == null);
        }
    }
    _ = try std.fmt.bufPrint(label[80..], "{d:0>4}", .{@as(usize, 500)});
    const key = try group_keys.encode(out, &.{local.sql_scalar.Datum.json(.{ .string = &label })});
    const changed = try operators.Grouped.create(a, &specs, .{});
    defer changed.deinit();
    const five = local.sql_scalar.Datum.json(.{ .integer = 5 });
    try changed.add(&.{local.sql_scalar.Datum.json(.{ .string = &label })}, &.{ five, five, five, five });
    const transition = (try changed.nextTypedPartialResult(out)).?;
    const bytes = try state_tree.encode(out, transition, recipe);
    var access: @import("lake_index_native_directory.zig").Access = undefined;
    store.upload_scope.?.attempt = @splat(2);
    access.init(store, parent.domain, .none, null);
    var right = indexes[1];
    right.page = try state_tree.tree.apply(a, access.backing(), right.page, &.{.{ .key = key, .value = bytes }});
    var metrics: state_tree.Metrics = .{};
    var current = try state_tree.merge(a, &store, recipe, indexes[0], right, .{ .parent = parent, .left = indexes[0], .right = indexes[1] }, .none, &metrics);
    try std.testing.expectEqual(@as(usize, 1), metrics.changed_groups);
    try std.testing.expect(metrics.written_bytes < cold_metrics.written_bytes / 4);
    const one_group_writes = metrics.written_bytes;
    for (0..3) |phase| {
        if (phase == 1) {
            const before = right;
            right.page = try state_tree.tree.apply(a, access.backing(), right.page, &.{.{ .key = key, .value = null }});
            current = try state_tree.merge(a, &store, recipe, indexes[0], right, .{ .parent = current, .left = indexes[0], .right = before }, .none, &metrics);
        } else if (phase == 2) {
            var left = indexes[0];
            left.page = try state_tree.tree.apply(a, access.backing(), left.page, &.{.{ .key = key, .value = null }});
            current = try state_tree.merge(a, &store, recipe, left, right, .{ .parent = current, .left = indexes[0], .right = right }, .none, &metrics);
        }
        const fresh = try publishTree(a, out, &store, names, recipe, current, .none);
        for (fresh, 0..) |ref, slot| {
            const reader = try Reader.open(a, store, ref, .{ .keys = recipe.keys, .inputs = inputs[slot..][0..1] }, .none);
            defer reader.cursor().close(reader);
            try reader.cursor().select_point.?(reader, &.{local.sql_scalar.Datum.json(.{ .string = &label })});
            const result = try reader.cursor().next(reader, out, 8);
            if (phase == 2) {
                try std.testing.expect(result == null);
                continue;
            }
            const expected: i64 = if (phase == 1) (if (slot == 0) 1 else 10) else switch (slot) {
                0 => 2,
                1 => 15,
                2 => 5,
                else => 10,
            };
            try std.testing.expectEqual(expected, (try native_partial.finish(out, out, result.?[0], 0, specs[slot])).value.integer);
            try std.testing.expect((try reader.cursor().next(reader, out, 8)) == null);
        }
        var collector: @import("lake_index_gc.zig").Collector = .{ .a = out, .table = 4, .authority = undefined, .store = store, .identity = @splat(9), .context = .{}, .token = @splat(7), .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = 32 * 1024 * 1024 };
        try collector.markArtifact(out, fresh[0]);
        try std.testing.expect(collector.marked.count() > 1);
    }
    std.debug.print("group state: cold_writes={d} one_group_update={d} one_group_delete={d}\n", .{ cold_metrics.written_bytes, one_group_writes, metrics.written_bytes });
}

test "external lake aggregate persistent state admits spilled groups and preserves oversized record fallback" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var temporary = try local.common_test_directory.TestDirectory.init("persistent-state-admission");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const spec: operators.AggregateSpec = .{ .kind = .count };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = true }}, .inputs = &.{.{ .spec = spec, .column = null }} };
    const grouped = try operators.Grouped.create(a, &.{spec}, .{ .bytes = 32 * 1024, .spill = &manager });
    defer grouped.deinit();
    for (0..2000) |i| try grouped.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    try grouped.add(&.{.{}}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    try std.testing.expect(grouped.external != null);
    const refs = try publishKeyed(a, out, &store, &.{"stats.count"}, grouped, recipe, &manager, .none);
    try std.testing.expectEqual(@as(u16, 8), refs[0].metadata_version);
    {
        const reader = try Reader.open(a, store, refs[0], recipe, .none);
        defer reader.cursor().close(reader);
        try std.testing.expect(try reader.cursor().select_keys.?(reader, .{ .maximum_groups = 2, .order = &.{.{ .column = "k", .descending = true, .nulls_first = true }} }));
        const rows = (try reader.cursor().next(reader, out, 8)).?;
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expect(rows[0].keys[0].sql_null);
        try std.testing.expectEqual(@as(i64, 1999), rows[1].keys[0].value.integer);
    }
    const Fault = struct {
        fn run(alloc: A, backend: stores.ArtifactStore, ref: Ref, plan: recipes.Recipe) !void {
            const reader = try Reader.open(alloc, backend, ref, plan, .none);
            defer reader.cursor().close(reader);
            var page = std.heap.ArenaAllocator.init(alloc);
            defer page.deinit();
            _ = try reader.cursor().next(reader, page.allocator(), 2);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fault.run, .{ store, refs[0], recipe });
    const wide = try operators.Grouped.create(a, &.{spec}, .{ .bytes = 8 * 1024 * 1024 });
    defer wide.deinit();
    const label = try out.alloc(u8, 300 * 1024);
    @memset(label, 'x');
    try wide.add(&.{local.sql_scalar.Datum.json(.{ .string = label })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    const wide_recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = recipe.inputs };
    const fallback = try publishKeyed(a, out, &store, &.{"stats.count"}, wide, wide_recipe, &manager, .none);
    try std.testing.expect(fallback[0].metadata_version != 8);
    const reader = try Reader.open(a, store, fallback[0], wide_recipe, .none);
    defer reader.cursor().close(reader);
    const result = (try reader.cursor().next(reader, out, 2)).?;
    try std.testing.expectEqualStrings(label, result[0].keys[0].value.string);
    try std.testing.expectEqual(@as(i64, 1), (try native_partial.finish(out, out, result[0], 0, spec)).value.integer);
}

test "external lake aggregate compact trees read historical columnar records and upgrade mixed merges" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-record-upgrade");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @import("lake_index_publication.zig").uploadDomain(4, @splat(9)), .attempt = @splat(1) };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = false }}, .inputs = &.{.{ .spec = .{ .kind = .count }, .column = null }} };
    const keys = &.{local.sql_scalar.Datum.json(.{ .integer = 7 })};
    const legacy = try spill.encodeColumnarBlockAlloc(out, &.{.{ .keys = keys, .values = &.{local.sql_scalar.Datum.json(.{ .integer = 3 })}, .ordinal = 0 }}, max_block_bytes);
    var access: @import("lake_index_native_directory.zig").Access = undefined;
    access.init(store, store.upload_scope.?.domain, .none, null);
    var builder: state_tree.tree.SortedBuilder = .{ .alloc = a, .store = access.backing() };
    defer builder.deinit();
    try builder.add(try group_keys.encode(out, keys), legacy);
    const old_index: state_tree.Index = .{ .domain = store.upload_scope.?.domain, .page = try builder.finish() };
    const old_refs = try publishTree(a, out, &store, &.{"count"}, recipe, old_index, .none);
    try std.testing.expectEqual(@as(u16, 7), old_refs[0].metadata_version);
    const old_reader = try Reader.open(a, store, old_refs[0], recipe, .none);
    defer old_reader.cursor().close(old_reader);
    const before = (try old_reader.cursor().next(old_reader, out, 8)).?;
    try std.testing.expectEqual(@as(i64, 3), (try native_partial.finish(out, out, before[0], 0, recipe.inputs[0].spec)).value.integer);
    const group = try operators.Grouped.create(a, &.{recipe.inputs[0].spec}, .{});
    defer group.deinit();
    try group.add(keys, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })});
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const fresh = try publishKeyed(a, out, &store, &.{"count"}, group, recipe, &manager, .none);
    const new_reader = try Reader.open(a, store, fresh[0], recipe, .none);
    defer new_reader.cursor().close(new_reader);
    const mixed = try state_tree.merge(a, &store, recipe, old_index, new_reader.root.state_tree.?, null, .none, null);
    try std.testing.expectEqual(@as(u8, 2), mixed.layout);
    const refs = try publishTree(a, out, &store, &.{"count"}, recipe, mixed, .none);
    const reader = try Reader.open(a, store, refs[0], recipe, .none);
    defer reader.cursor().close(reader);
    const after = (try reader.cursor().next(reader, out, 8)).?;
    try std.testing.expectEqual(@as(i64, 4), (try native_partial.finish(out, out, after[0], 0, recipe.inputs[0].spec)).value.integer);
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = out, .table = 4, .authority = undefined, .store = store, .identity = @splat(9), .context = .{}, .token = @splat(7), .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = 32 * 1024 * 1024 };
    try collector.markArtifact(out, old_refs[0]);
    try collector.markArtifact(out, refs[0]);
    try std.testing.expect(collector.marked.count() >= 4);
}

test "external lake aggregate tree frames lend wide unselected states until next pull" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("aggregate-borrowed-frames");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(1) };
    const count: operators.AggregateSpec = .{ .kind = .count };
    const minimum: operators.AggregateSpec = .{ .kind = .min, .input_type = .string };
    const recipe: recipes.Recipe = .{ .keys = &.{.{ .path = "k", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = count, .column = null }, .{ .spec = minimum, .column = .{ .path = "v", .type = .string, .nullable = false } } } };
    const group = try operators.Grouped.create(a, &.{ count, minimum }, .{ .bytes = 8 * 1024 * 1024 });
    defer group.deinit();
    const text: [8192]u8 = @splat('x');
    for (0..512) |i| try group.add(&.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, &.{ local.sql_scalar.Datum.json(.{ .integer = 1 }), local.sql_scalar.Datum.json(.{ .string = &text }) });
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const refs = try publishKeyed(a, out, &store, &.{ "stats.count", "stats.min" }, group, recipe, &manager, .none);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const reader = try Reader.openWithCache(a, store, refs[0], .{ .keys = recipe.keys, .inputs = recipe.inputs[0..1] }, .none, .{ .cache = &cache, .scope = @splat(3), .context = .{ .io = std.testing.io } });
    defer reader.cursor().close(reader);
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 512 * 1024 };
    var page = std.heap.ArenaAllocator.init(budget.allocator());
    defer page.deinit();
    const retained_minimum = retained: {
        const selected = try Reader.open(a, store, refs[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none);
        defer selected.cursor().close(selected);
        const rows = (try selected.cursor().next(selected, page.allocator(), 1)).?;
        break :retained try native_partial.finish(out, page.allocator(), rows[0], 0, minimum);
    };
    var delivered: usize = 0;
    while (true) {
        _ = page.reset(.free_all);
        const rows = (try reader.cursor().next(reader, page.allocator(), 256)) orelse break;
        // The full record has an 8 KiB MIN value which COUNT never reads.
        // Returning it must not duplicate this payload into the result arena.
        try std.testing.expect(reader.tree_frame != null);
        for (rows) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(delivered)), row.keys[0].value.integer);
            try std.testing.expectEqual(@as(i64, 1), (try native_partial.finish(page.allocator(), page.allocator(), row, 0, count)).value.integer);
            delivered += 1;
        }
    }
    try std.testing.expectEqualStrings(&text, retained_minimum.value.string);
    try std.testing.expectEqual(@as(usize, 512), delivered);
    try std.testing.expect(budget.peak < 256 * 1024);
    try std.testing.expect(reader.tree_prefetches > 0);
    std.debug.print("borrowed aggregate frames: groups={} output_peak={}\n", .{ delivered, budget.peak });
}
