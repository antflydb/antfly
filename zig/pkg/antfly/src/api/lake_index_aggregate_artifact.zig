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
pub const CachedRead = struct {
    cache: *local.serverless_query_lake_serving_cache.Cache,
    scope: [32]u8,
    context: local.serverless_query_lake_read_context.Context,
};

pub fn readArtifact(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead) ![]u8 {
    var loader = struct {
        store: stores.ArtifactStore,
        ref: ChunkRef,
        cancellation: Cancellation,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.getVerifiedAllocWithCancellationUsingAllocator(alloc, self.ref.artifact_id, self.ref.byte_len, self.ref.checksum, self.cancellation);
        }
    }{ .store = store, .ref = ref, .cancellation = cancellation };
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    try cancellation.check();
    if (cached) |cache| return cache.cache.readImmutableAlloc(a, cache.scope, ref.artifact_id, std.math.cast(usize, ref.byte_len) orelse return error.ArtifactTooLarge, try stores.sha256DigestFromChecksum(ref.checksum), cache.context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
    return @TypeOf(loader).load(&loader, a);
}
/// Borrow a verified bounded block until the caller has copied/decoded its
/// requested range. Current authority is checked even when payloads are warm.
pub fn readArtifactLease(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead) !local.serverless_query_lake_serving_cache.Cache.ImmutableLease {
    const Loader = struct {
        store: stores.ArtifactStore,
        ref: ChunkRef,
        cancellation: Cancellation,
        fn load(raw: *anyopaque, alloc: A) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.store.getVerifiedAllocWithCancellationUsingAllocator(alloc, self.ref.artifact_id, self.ref.byte_len, self.ref.checksum, self.cancellation);
        }
    };
    var loader: Loader = .{ .store = store, .ref = ref, .cancellation = cancellation };
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    try cancellation.check();
    if (cached) |cache| return cache.cache.readImmutableBlockLease(a, cache.scope, ref.artifact_id, std.math.cast(usize, ref.byte_len) orelse return error.ArtifactTooLarge, try stores.sha256DigestFromChecksum(ref.checksum), cache.context, .{ .ptr = &loader, .load = Loader.load });
    return .{ .heap = .{ .alloc = a, .bytes = try Loader.load(&loader, a) } };
}

pub const metadata_version: u16 = 2;
pub fn supportsMetadataVersion(version: u16) bool {
    return version == 1 or version == metadata_version or isPartitioned(version);
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
pub const max_root_bytes = 4 * 1024 * 1024;
pub const max_block_bytes = 4 * 1024 * 1024;
pub const max_blocks = 8192;
pub const ChunkRef = struct { artifact_id: []const u8, checksum: []const u8, byte_len: u64 };
const Block = struct { artifact: ChunkRef, rows: u16 };
/// Build-local proof returned by a publisher or an authenticated parent.
/// It is bound to the exact logical child and never changes the wire format.
pub const StateProof = struct {
    logical: [32]u8,
    physical: ?[32]u8,
    recipe: [32]u8,
    groups: u64,
    slot: u16,
    depth: ?u8,
};
pub const Partition = struct {
    bucket: u8,
    artifact: Ref,
    groups: u64,
    proof: ?StateProof = null,
    pub fn jsonStringify(self: Partition, writer: anytype) !void {
        try writer.write(.{ .bucket = self.bucket, .artifact = self.artifact, .groups = self.groups });
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
    cohort: ?ChunkRef = null,
    state_recipe: ?recipes.Recipe = null,
    state_slot: ?u16 = null,
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
    if (root.cohort) |ref| return try stores.sha256DigestFromChecksum(ref.checksum);
    if (root.partitions.len != 0 or root.state_recipe == null) return null;
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .recipe = root.state_recipe.?, .groups = root.groups, .blocks = root.blocks }, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return hash;
}
fn decodeCohort(a: A, store: stores.ArtifactStore, ref: Ref, token: Cancellation, cached: ?CachedRead) !Cohort {
    if (ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
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
    return .{ .logical = try logicalIdentity(ref), .physical = try physicalIdentity(a, root), .recipe = root.recipe.fingerprint(), .groups = root.groups, .slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact, .depth = if (isPartitioned(ref.metadata_version)) root.radix_depth else null };
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
/// Build planning reads authenticated recipe metadata, without aggregate blocks.
pub fn loadRecipe(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation) !recipes.Recipe {
    if (!supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifact(a, store, .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len }, cancellation, null);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    const reader = try Reader.open(a, store, artifact, root.recipe, cancellation);
    defer reader.cursor().close(reader);
    return root.recipe;
}

/// GC traverses authenticated root references without reading every leaf.
/// Unknown formats fail closed rather than guessing that a root has no edges.
pub fn descendantsAlloc(a: A, store: stores.ArtifactStore, artifact: Ref, cancellation: Cancellation) ![]const ChunkRef {
    if (!supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifact(a, store, .{ .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len }, cancellation, null);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    const reader = try Reader.open(a, store, artifact, root.recipe, cancellation);
    defer reader.cursor().close(reader);
    const refs = try a.alloc(ChunkRef, root.blocks.len);
    for (refs, root.blocks) |*ref, block| ref.* = block.artifact;
    return refs;
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
    var control = std.heap.ArenaAllocator.init(a);
    defer control.deinit();
    const ca = control.allocator();
    var blocks: std.ArrayList(Block) = .empty;
    var groups: u64 = 0;
    var output_bytes: u64 = 0;
    while (true) {
        try cancellation.check();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const pa = page.allocator();
        var rows: std.ArrayList(operators.Row) = .empty;
        while (rows.items.len < 256) {
            const partial = try group.nextPartialResult(pa) orelse break;
            if (partial.keys.len != recipe.keys.len or partial.aggregates.len != recipe.inputs.len) return error.InvalidSqlBackendResponse;
            try rows.append(pa, .{ .keys = partial.keys, .values = partial.aggregates, .ordinal = partial.ordinal });
        }
        if (rows.items.len == 0) break;
        if (blocks.items.len == max_blocks) return error.NativeAggregateArtifactTooLarge;
        const encoded = try spill.encodeColumnarBlockAlloc(pa, rows.items, max_block_bytes);
        var upload = store.*;
        upload.allocator = ca;
        output_bytes = std.math.add(u64, output_bytes, encoded.len) catch return error.NativeAggregateArtifactTooLarge;
        if (output_bytes > 512 * 1024 * 1024) return error.NativeAggregateArtifactTooLarge;
        const artifact = try upload.putWithCancellation(encoded, cancellation);
        defer if (artifact.provider_version) |version| upload.allocator.free(version);
        try blocks.append(ca, .{ .artifact = .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum }, .rows = @intCast(rows.items.len) });
        groups = std.math.add(u64, groups, rows.items.len) catch return error.NativeAggregateArtifactTooLarge;
    }
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
        const root: Root = .{ .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot .. slot + 1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = groups, .blocks = blocks.items };
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
    if (artifact.kind != .algebraic_segment or !supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
    const bytes = try readArtifact(a, store, .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum }, cancellation, cached);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, root.format, if (artifact.metadata_version == 1) "native-sql-aggregate-v1" else if (artifact.metadata_version == 3) "native-sql-aggregate-v3" else if (artifact.metadata_version == 5) "native-sql-aggregate-v5" else if (artifact.metadata_version == 4) "native-sql-aggregate-v4" else "native-sql-aggregate-v2") or !std.mem.eql(u8, root.name, artifact.name) or root.blocks.len > max_blocks) return error.InvalidNativeAggregateArtifact;
    if (artifact.metadata_version == 1) {
        if (root.state_recipe != null or root.state_slot != null) return error.InvalidNativeAggregateArtifact;
    } else {
        const state_recipe = root.state_recipe orelse return error.InvalidNativeAggregateArtifact;
        const slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact;
        if (state_recipe.inputs.len > 256 or slot >= state_recipe.inputs.len or !root.recipe.eql(.{ .keys = state_recipe.keys, .inputs = state_recipe.inputs[slot .. slot + 1] })) return error.InvalidNativeAggregateArtifact;
    }
    if (!isPartitioned(artifact.metadata_version) and root.partitions.len != 0) return error.InvalidNativeAggregateArtifact;
    if (isPartitioned(artifact.metadata_version) and (root.blocks.len != 0 or root.partitions.len > 64 or root.radix_depth > max_partition_depth)) return error.InvalidNativeAggregateArtifact;
    if ((artifact.metadata_version == 5) != (root.cohort != null)) return error.InvalidNativeAggregateArtifact;
    if (root.cohort) |ref| {
        if (ref.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    }
    var count: u64 = 0;
    for (root.partitions, 0..) |part, i| {
        if (part.bucket >= partitionFanout(root.radix_depth) or (i != 0 and root.partitions[i - 1].bucket >= part.bucket) or part.artifact.kind != .algebraic_segment or (part.artifact.metadata_version != 2 and !(artifact.metadata_version >= 4 and (part.artifact.metadata_version == 4 or part.artifact.metadata_version == 5) and root.radix_depth < max_partition_depth)) or !std.mem.eql(u8, part.artifact.name, root.name) or part.artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        try stores.validateSha256ArtifactIdentity(part.artifact.artifact_id, part.artifact.checksum);
        count = std.math.add(u64, count, part.groups) catch return error.InvalidNativeAggregateArtifact;
    }
    for (root.blocks) |block| {
        if (block.rows == 0 or block.rows > 256 or block.artifact.byte_len > max_block_bytes) return error.InvalidNativeAggregateArtifact;
        try stores.validateSha256ArtifactIdentity(block.artifact.artifact_id, block.artifact.checksum);
        count = std.math.add(u64, count, block.rows) catch return error.InvalidNativeAggregateArtifact;
    }
    if (count != root.groups) return error.InvalidNativeAggregateArtifact;
    return root;
}

pub const Reader = struct {
    const PartitionMap = struct { states: []const u16, children: ?[]const PartitionMap = null };
    a: A,
    store: stores.ArtifactStore,
    cancellation: Cancellation,
    cached: ?CachedRead = null,
    control: std.heap.ArenaAllocator,
    page: std.heap.ArenaAllocator,
    root: Root,
    metadata: ?@import("lake_index_decoded_metadata.zig").Owned(Root) = null,
    block: ?spill.ColumnarBlock = null,
    block_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(spill.ColumnarBlock) = null,
    cohort_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(Cohort) = null,
    block_index: usize = 0,
    position: usize = 0,
    emitted: u64 = 0,
    output_slots: ?[]const u16 = null,
    state_slots: ?[]const u16 = null,
    child: ?*Reader = null,
    partition_index: usize = 0,
    partition_states: ?[]const PartitionMap = null,
    cohort: ?Cohort = null,
    blocks_decoded: usize = 0,

    /// The caller selects an authorized publication first. Errors after that
    /// selection abort the query, never combine partials with a fresh scan.
    pub fn open(a: A, store: stores.ArtifactStore, artifact: Ref, recipe: recipes.Recipe, cancellation: Cancellation) !*Reader {
        return openWithCache(a, store, artifact, recipe, cancellation, null);
    }
    pub fn openWithCache(a: A, store: stores.ArtifactStore, artifact: Ref, recipe: recipes.Recipe, cancellation: Cancellation, cached: ?CachedRead) !*Reader {
        if (artifact.kind != .algebraic_segment or !supportsMetadataVersion(artifact.metadata_version) or artifact.byte_len > max_root_bytes) return error.InvalidNativeAggregateArtifact;
        const self = try a.create(Reader);
        errdefer a.destroy(self);
        var control = std.heap.ArenaAllocator.init(a);
        errdefer control.deinit();
        const ca = control.allocator();
        const metadata = if (cached) |cache| try @import("lake_index_decoded_metadata.zig").acquire(Root, cache, store, artifact, cancellation, loadRoot) else null;
        errdefer if (metadata) |owned| owned.release();
        const root = if (metadata) |owned| owned.value.* else try loadRoot(ca, store, artifact, cancellation, null);
        if (!root.recipe.eql(recipe)) return error.InvalidNativeAggregateArtifact;
        const cohort_metadata = if (cached) |cache| if (root.cohort) |ref| try @import("lake_index_decoded_metadata.zig").acquire(Cohort, cache, store, cohortReference(ref), cancellation, decodeCohort) else null else null;
        errdefer if (cohort_metadata) |owned| owned.release();
        const cohort: ?Cohort = if (cohort_metadata) |owned| owned.value.* else if (root.cohort) |ref| try decodeCohort(ca, store, cohortReference(ref), cancellation, null) else null;
        if (cohort) |proof| try validateCohort(ca, root, proof);
        self.* = .{ .cohort_metadata = cohort_metadata, .cohort = cohort, .a = a, .store = store, .cancellation = cancellation, .cached = cached, .control = control, .page = .init(a), .root = root, .metadata = metadata };
        return self;
    }
    /// Roots may fuse only when authenticated state recipes and the entire
    /// block directory agree. Independent materializations remain independent.
    pub fn setOutputSlot(self: *Reader, slot: u16) !void {
        self.output_slots = try self.control.allocator().dupe(u16, &.{slot});
        self.state_slots = try self.control.allocator().dupe(u16, &.{self.root.state_slot orelse 0});
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
        for (self.root.blocks, other.root.blocks) |left, right| {
            if (left.rows != right.rows or left.artifact.byte_len != right.artifact.byte_len or !std.mem.eql(u8, left.artifact.artifact_id, right.artifact.artifact_id) or !std.mem.eql(u8, left.artifact.checksum, right.artifact.checksum)) return false;
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
    fn partitionReader(self: *Reader, index: usize) !*Reader {
        const part = self.root.partitions[index];
        const reader = try Reader.openWithCache(self.a, self.store, part.artifact, self.root.recipe, self.cancellation, self.cached);
        errdefer reader.cursor().close(reader);
        if (part.artifact.metadata_version >= 4 and reader.root.radix_depth != self.root.radix_depth + 1) return error.InvalidNativeAggregateArtifact;
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
            const lreader = try Reader.openWithCache(left.a, left.store, l.artifact, left.root.recipe, left.cancellation, left.cached);
            defer lreader.cursor().close(lreader);
            const rreader = try Reader.openWithCache(right.a, right.store, r.artifact, right.root.recipe, right.cancellation, right.cached);
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
    pub fn cursor(self: *Reader) local.sql_catalog.AggregatePartialCursor {
        return .{ .ptr = self, .next = next, .close = close };
    }
    fn next(raw: *anyopaque, a: A, maximum: u32) !?[]const operators.GroupResult {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        if (self.cached) |cached| try cached.context.ensureActive();
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.root.partitions.len != 0) while (true) {
            if (self.child == null) {
                if (self.partition_index == self.root.partitions.len) {
                    if (self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                    return null;
                }
                self.child = try self.partitionReader(self.partition_index);
                self.partition_index += 1;
            }
            if (try self.child.?.cursor().next(self.child.?, a, maximum)) |rows| {
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
            self.position = 0;
            if (self.block_index == self.root.blocks.len) {
                if (self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                return null;
            }
            if (!self.page.reset(.{ .retain_with_limit = max_block_bytes })) return error.OutOfMemory;
            const pa = self.page.allocator();
            const ref = self.root.blocks[self.block_index];
            const owned = if (self.cached) |cached| try @import("lake_index_decoded_metadata.zig").acquireColumnBlock(cached, self.store, ref.artifact, self.cancellation) else null;
            errdefer if (owned) |value| value.release();
            const block = if (owned) |value| value.value.* else decode: {
                const bytes = try readArtifact(pa, self.store, ref.artifact, self.cancellation, null);
                break :decode try spill.decodeColumnarBlockInArena(pa, bytes, max_block_bytes);
            };
            const state_width = if (self.root.state_recipe) |recipe| recipe.inputs.len else self.root.recipe.inputs.len;
            if (block.count() != ref.rows or block.keys.len != self.root.recipe.keys.len or block.values.len != state_width) return error.InvalidNativeAggregateArtifact;
            self.block_metadata = owned;
            self.block = block;
            self.block_index += 1;
            self.blocks_decoded += 1;
        }
        const block = self.block.?;
        const count = @min(maximum, block.count() - self.position);
        const result = try a.alloc(operators.GroupResult, count);
        for (result) |*row| {
            const keys = try a.alloc(local.sql_scalar.Datum, block.keys.len);
            for (keys, 0..) |*key, column| key.* = try block.keyCell(self.position, column);
            const cells = try a.alloc(local.sql_scalar.Datum, if (self.state_slots) |slots| slots.len else self.root.recipe.inputs.len);
            for (cells, 0..) |*cell, column| cell.* = try block.cell(self.position, if (self.state_slots) |slots| slots[column] else if (self.root.state_slot) |slot| @as(usize, slot) else column);
            row.* = .{ .keys = keys, .aggregates = cells, .ordinal = block.ordinals[self.position], .aggregate_slots = self.output_slots };
            self.position += 1;
            self.emitted += 1;
        }
        return result;
    }
    fn close(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const a = self.a;
        if (self.child) |child| child.cursor().close(child);
        if (self.block_metadata) |owned| owned.release();
        if (self.cohort_metadata) |owned| owned.release();
        self.page.deinit();
        if (self.metadata) |owned| owned.release();
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
    const current = try publish(a, a, &store, "stats.count", group, recipe, .none);
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
    var state = try local.sql_aggregate_partial.decode(page.allocator(), rows[0].aggregates[0], spec);
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
            try imported.importPartial(partial.keys, partial.aggregates, partial.ordinal);
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
    try imported.importPartialMapped(rows[0].keys, rows[0].aggregates, rows[0].aggregate_slots, rows[0].ordinal);
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
    if (depth > max_partition_depth) return error.InvalidNativeAggregateArtifact;
    // One shared-run sort bounds descriptors independently of radix depth.
    var sort = spill.Sort.init(a, manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    sort.shared_runs = true;
    while (true) {
        try cancellation.check();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const pa = page.allocator();
        const row = try group.nextPartialResult(pa) orelse break;
        var hasher = std.hash.Wyhash.init(0);
        for (row.keys) |key| {
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], if (key.sql_null) 0 else try local.sql_scalar.semanticHash(key.value), .little);
            hasher.update(&bytes);
        }
        const bucket = (hasher.final() >> partitionShift(depth)) % partitionFanout(depth);
        const values = try std.mem.concat(pa, local.sql_scalar.Datum, &.{ row.keys, row.aggregates });
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(bucket) })}, .values = values, .ordinal = row.ordinal });
    }
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
    const specs = try a.alloc(operators.AggregateSpec, recipe.inputs.len);
    defer a.free(specs);
    for (specs, recipe.inputs) |*spec, input| spec.* = input.spec;
    const child_proofs = try a.alloc(StateProof, names.len);
    defer a.free(child_proofs);
    var partial: ?*operators.Grouped = null;
    defer if (partial) |value| value.deinit();
    var bucket: i64 = -1;
    var count: u64 = 0;
    while (true) {
        try cancellation.check();
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const row = try sort.next(page.allocator());
        const next = if (row) |value| value.keys[0].value.integer else -1;
        if (partial != null and (row == null or next != bucket)) {
            const refs = if (count > partition_leaf_groups and depth < max_partition_depth)
                try publishPartitionedAtProven(a, a, store, names, partial.?, recipe, manager, cancellation, depth + 1, child_proofs)
            else
                try publishCohortProven(a, a, store, names, partial.?, recipe, cancellation, child_proofs);
            defer a.free(refs);
            for (refs, lists, child_proofs) |ref, *list, proof| try list.append(a, .{ .bucket = @intCast(bucket), .artifact = ref, .groups = count, .proof = proof });
            partial.?.deinit();
            partial = null;
            count = 0;
        }
        const value = row orelse break;
        if (partial == null) partial = try operators.Grouped.create(a, specs, .{ .groups = 2_000_000, .bytes = 8 * 1024 * 1024, .spill = manager });
        bucket = next;
        try partial.?.importPartial(value.values[0..recipe.keys.len], value.values[recipe.keys.len..], value.ordinal);
        count += 1;
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
        root.* = .{ .format = "native-sql-aggregate-v5", .radix_depth = depth, .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot..][0..1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = count, .partitions = list.items };
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
            if (candidate.bucket != part.bucket or candidate.groups != part.groups or candidate.artifact.kind != .algebraic_segment or !std.mem.eql(u8, candidate.artifact.name, member.name) or candidate.artifact.byte_len > max_root_bytes or (candidate.artifact.metadata_version != 2 and candidate.artifact.metadata_version != 4 and candidate.artifact.metadata_version != 5)) return error.InvalidNativeAggregateArtifact;
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
pub fn loadPartitionDirectory(a: A, store: stores.ArtifactStore, ref: Ref, recipe: recipes.Recipe, cancellation: Cancellation) !struct { depth: u8, parts: []const Partition } {
    if (!isPartitioned(ref.metadata_version)) return error.InvalidNativeAggregateArtifact;
    const reader = try Reader.open(a, store, ref, recipe, cancellation);
    defer reader.cursor().close(reader);
    const bytes = try std.json.Stringify.valueAlloc(a, reader.root.partitions, .{});
    defer a.free(bytes);
    const parts = try std.json.parseFromSliceLeaky([]Partition, a, bytes, .{ .allocate = .alloc_always });
    if (reader.cohort) |cohort| for (parts, cohort.children) |*part, child| {
        part.proof = .{ .logical = try logicalIdentity(part.artifact), .physical = child.identity, .recipe = recipe.fingerprint(), .groups = part.groups, .slot = child.slots[reader.root.state_slot.?], .depth = if (isPartitioned(part.artifact.metadata_version)) reader.root.radix_depth + 1 else null };
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
    const refs = try publishPartitioned(a, out, &store, &.{ "stats.sum", "stats.count" }, group, recipe, &manager, .none);
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
        try imported.importPartialMapped(row.keys, row.aggregates, row.aggregate_slots, row.ordinal);
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
    while (try sum.cursor().next(sum, out, 1)) |rows| for (rows) |row| try result.importPartialMapped(row.keys, row.aggregates, row.aggregate_slots, row.ordinal);
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
            try reducer.importPartial(row.keys, row.aggregates, row.ordinal);
            return (try reducer.nextResult(alloc)).?.aggregates[0].value.integer;
        }
        fn denied(_: *anyopaque, _: A, _: []const u8, _: usize, _: Cancellation) ![]u8 {
            return error.UnexpectedAggregateChildRead;
        }
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const original = store.vtable;
    var denied = original.*;
    denied.get_bounded_alloc_with_cancellation = Check.denied;
    store.vtable = &denied;
    // Fresh partitions provide their physical identity and logical slot at the
    // upload boundary. Even a completely cold store needs no child-root GETs.
    const refs = try publishPartitioned(a, out, &store, &names, group, recipe, &manager, .none);
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
    const block = count.child.?.block_metadata.?.value;
    sum.cursor().close(sum);
    sum_open = false;
    try std.testing.expectEqualDeep(sum_rows[0].aggregates[0], try block.cell(0, 0));
    var isolated = cached;
    isolated.scope = @splat(2);
    const other = try Reader.openWithCache(a, store, inherited[1], .{ .keys = recipe.keys, .inputs = recipe.inputs[1..2] }, .none, isolated);
    defer other.cursor().close(other);
    try std.testing.expect(other.cohort_metadata.?.value != count.cohort_metadata.?.value);
    _ = try other.cursor().next(other, out, 1);
    try std.testing.expect(other.child.?.block_metadata.?.value != block);
    count.cached.?.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, count.cursor().next(count, out, 1));
}
