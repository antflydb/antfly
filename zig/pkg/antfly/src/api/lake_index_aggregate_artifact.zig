// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

fn readArtifact(a: A, store: stores.ArtifactStore, ref: ChunkRef, cancellation: Cancellation, cached: ?CachedRead) ![]u8 {
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
pub const metadata_version: u16 = 2;
pub fn supportsMetadataVersion(version: u16) bool {
    return version == 1 or version == metadata_version;
}
pub const max_root_bytes = 4 * 1024 * 1024;
pub const max_block_bytes = 4 * 1024 * 1024;
pub const max_blocks = 8192;
const ChunkRef = struct { artifact_id: []const u8, checksum: []const u8, byte_len: u64 };
const Block = struct { artifact: ChunkRef, rows: u16 };
const Root = struct {
    format: []const u8 = "native-sql-aggregate-v2",
    name: []const u8,
    recipe: recipes.Recipe,
    groups: u64,
    blocks: []const Block,
    state_recipe: ?recipes.Recipe = null,
    state_slot: ?u16 = null,
};

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
        const bytes = try std.json.Stringify.valueAlloc(root_arena.allocator(), Root{ .name = name, .recipe = .{ .keys = recipe.keys, .inputs = recipe.inputs[slot .. slot + 1] }, .state_recipe = recipe, .state_slot = @intCast(slot), .groups = groups, .blocks = blocks.items }, .{});
        if (bytes.len > max_root_bytes or bytes.len > 512 * 1024 * 1024 -| output_bytes) return error.NativeAggregateArtifactTooLarge;
        output_bytes += bytes.len;
        var upload = store.*;
        upload.allocator = result_alloc;
        const artifact = try upload.putWithCancellation(bytes, cancellation);
        reference.* = .{ .kind = .algebraic_segment, .name = name, .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .metadata_version = metadata_version };
        initialized += 1;
    }
    return references;
}

pub const Reader = struct {
    a: A,
    store: stores.ArtifactStore,
    cancellation: Cancellation,
    cached: ?CachedRead = null,
    control: std.heap.ArenaAllocator,
    page: std.heap.ArenaAllocator,
    root: Root,
    block: ?spill.ColumnarBlock = null,
    block_index: usize = 0,
    position: usize = 0,
    emitted: u64 = 0,

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
        const bytes = try readArtifact(ca, store, .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum }, cancellation, cached);
        const root = try std.json.parseFromSliceLeaky(Root, ca, bytes, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, root.format, if (artifact.metadata_version == 1) "native-sql-aggregate-v1" else "native-sql-aggregate-v2") or !std.mem.eql(u8, root.name, artifact.name) or !root.recipe.eql(recipe) or root.blocks.len > max_blocks) return error.InvalidNativeAggregateArtifact;
        if (artifact.metadata_version == 1) {
            if (root.state_recipe != null or root.state_slot != null) return error.InvalidNativeAggregateArtifact;
        } else {
            const state_recipe = root.state_recipe orelse return error.InvalidNativeAggregateArtifact;
            const slot = root.state_slot orelse return error.InvalidNativeAggregateArtifact;
            if (state_recipe.inputs.len > 256 or slot >= state_recipe.inputs.len or !root.recipe.eql(.{ .keys = state_recipe.keys, .inputs = state_recipe.inputs[slot .. slot + 1] })) return error.InvalidNativeAggregateArtifact;
        }
        var count: u64 = 0;
        for (root.blocks) |block| {
            if (block.rows == 0 or block.rows > 256 or block.artifact.byte_len > max_block_bytes) return error.InvalidNativeAggregateArtifact;
            try stores.validateSha256ArtifactIdentity(block.artifact.artifact_id, block.artifact.checksum);
            count = std.math.add(u64, count, block.rows) catch return error.InvalidNativeAggregateArtifact;
        }
        if (count != root.groups) return error.InvalidNativeAggregateArtifact;
        self.* = .{ .a = a, .store = store, .cancellation = cancellation, .cached = cached, .control = control, .page = .init(a), .root = root };
        return self;
    }
    pub fn cursor(self: *Reader) local.sql_catalog.AggregatePartialCursor {
        return .{ .ptr = self, .next = next, .close = close };
    }
    fn next(raw: *anyopaque, a: A, maximum: u32) !?[]const operators.GroupResult {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.block == null or self.position == self.block.?.count()) {
            self.block = null;
            self.position = 0;
            if (self.block_index == self.root.blocks.len) {
                if (self.emitted != self.root.groups) return error.InvalidNativeAggregateArtifact;
                return null;
            }
            if (!self.page.reset(.{ .retain_with_limit = max_block_bytes })) return error.OutOfMemory;
            const pa = self.page.allocator();
            const ref = self.root.blocks[self.block_index];
            const bytes = try readArtifact(pa, self.store, ref.artifact, self.cancellation, self.cached);
            const block = try spill.decodeColumnarBlockInArena(pa, bytes, max_block_bytes);
            const state_width = if (self.root.state_recipe) |recipe| recipe.inputs.len else self.root.recipe.inputs.len;
            if (block.count() != ref.rows or block.keys.len != self.root.recipe.keys.len or block.values.len != state_width) return error.InvalidNativeAggregateArtifact;
            self.block = block;
            self.block_index += 1;
        }
        const block = self.block.?;
        const count = @min(maximum, block.count() - self.position);
        const result = try a.alloc(operators.GroupResult, count);
        for (result) |*row| {
            const keys = try a.alloc(local.sql_scalar.Datum, block.keys.len);
            for (keys, 0..) |*key, column| key.* = try block.keyCell(self.position, column);
            const cells = try a.alloc(local.sql_scalar.Datum, self.root.recipe.inputs.len);
            for (cells, 0..) |*cell, column| cell.* = try block.cell(self.position, if (self.root.state_slot) |slot| @as(usize, slot) else column);
            row.* = .{ .keys = keys, .aggregates = cells, .ordinal = block.ordinals[self.position] };
            self.position += 1;
            self.emitted += 1;
        }
        return result;
    }
    fn close(raw: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.page.deinit();
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
