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

//! Fenced, non-serving bulk construction receipts. Immutable source runs and
//! the sampler survive process loss; routing scratch is reconstructed from exact
//! vectors and authenticated identities. Local scratch is always disposable.
const std = @import("std");
const local = @import("antfly_local_sources");
const files = @import("lake_index_native_files.zig");
const directory = @import("lake_index_native_directory.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
pub const Root = struct {
    version: u16 = 1,
    index_complete: bool = false,
    spool_from_exact: bool = false,
    census_order: bool = false,
    binding: local.serverless_segment_source_binding.Binding,
    recipe: [32]u8,
    dims: u32,
    algorithm: []const u8,
    input_allowance: u64,
    generation: files.Root,
    partition: local.storage_hbc_adapter.HBCIndex.BulkSpool.Partition,
    file_directory: ?directory.tree.Ref,
    rng_hi: u64,
    rng_lo: u64,
};
pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, token: Token) !Root {
    if (ref.kind != .doc_values or ref.metadata_version != 1 or ref.byte_len > 1024 * 1024) return error.InvalidNativeLakeFiles;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, null);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != 1 or root.dims == 0 or root.dims > 65536 or root.input_allowance == 0 or root.partition.sample_count > root.partition.count) return error.InvalidNativeLakeFiles;
    try root.binding.validate();
    try root.generation.validate();
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFiles;
    if (!std.mem.eql(u8, &scope.domain, &root.generation.domain)) return error.InvalidNativeLakeFiles;
    if (root.file_directory) |page| try page.validate();
    return root;
}
pub fn save(a: A, provider: *Provider, store: *stores.ArtifactStore, name: []const u8, root: Root, token: Token) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    if (bytes.len > 1024 * 1024) return error.NativeLakeFilesTooLarge;
    var uploaded = try store.putWithCancellation(bytes, token);
    defer uploaded.deinit(store.allocator);
    if (comptime @hasDecl(@import("root"), "lakeExecutionProbeFamily")) {
        const report = try std.json.Stringify.valueAlloc(a, .{ .name = name, .manifest = uploaded.artifact_id, .phase = if (root.index_complete) "tree" else "source", .rows = root.partition.count }, .{});
        defer a.free(report);
        std.debug.print("PARQUET_BULK_RECEIPT {s}\n", .{report});
    }
    try provider.checkpointWork(.{ .name = name, .manifest = .{ .kind = .doc_values, .metadata_version = 1, .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len } });
}
pub fn publishPartition(a: A, store: *stores.ArtifactStore, source: local.storage_lsm_backend.Storage, root: files.Root, path: []const u8, partition: local.storage_hbc_adapter.HBCIndex.BulkSpool.Partition, token: Token, budget: files.OutputBudget, sample_only: bool) !files.Root {
    var result = root;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    for ([_]bool{ false, true }) |sample| {
        if ((!sample and sample_only) or (sample and partition.sample_count == 0)) continue;
        const relative = try std.fmt.allocPrint(out, "bulk-spool/{s}{s}", .{ partition.name, if (sample) ".sample" else "" });
        const physical = try std.fmt.allocPrint(out, "{s}/{s}{s}", .{ path, partition.name, if (sample) ".sample" else "" });
        var file = try files.publishPrefix(a, out, source, physical, relative, try source.fileSize(physical), store, token, budget, null);
        file = try directory.pageFile(a, out, store, root.domain, file, token, budget.work);
        result = try directory.applyChanges(a, store, result, &.{.{ .path = relative, .file = file }}, token, budget.work);
    }
    return result;
}
pub fn restorePartition(a: A, store: stores.ArtifactStore, root: Root, spool: *local.storage_hbc_adapter.HBCIndex.BulkSpool, context: local.serverless_query_lake_read_context.Context, token: Token) !void {
    var reader: files.Reader = .{ .root = root.generation, .prefix = "/bulk-work", .store = store, .context = context, .cancellation = token, .scratch = a };
    defer reader.deinit();
    for ([_]bool{ false, true }) |sample| {
        if ((!sample and root.spool_from_exact) or (sample and root.partition.sample_count == 0)) continue;
        const name = try std.fmt.allocPrint(a, "{s}{s}", .{ root.partition.name, if (sample) ".sample" else "" });
        defer a.free(name);
        const physical = try std.fmt.allocPrint(a, "/bulk-work/bulk-spool/{s}", .{name});
        defer a.free(physical);
        const size = try reader.storage().fileSize(physical);
        const file = try spool.dir.createFile(spool.io, name, .{ .exclusive = true, .permissions = .fromMode(0o600) });
        defer file.close(spool.io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(spool.io, &buffer);
        var offset: u64 = 0;
        while (offset < size) {
            try token.check();
            try context.ensureActive();
            const bytes = try reader.storage().readFileRangeAlloc(a, physical, offset, @intCast(@min(files.chunk_bytes, size - offset)));
            defer a.free(bytes);
            try writer.interface.writeAll(bytes);
            offset += bytes.len;
        }
        try writer.interface.flush();
    }
    spool.next_file = 1; // source phase always owns the initial partition
}

/// Recover routing input from the one durable canonical vector plane. Key
/// chunks preserve within-file producer order; the snapshot fixes file order.
/// Only the small reservoir needs a separate immutable vector representation.
pub fn rebuildPartition(a: A, store: stores.ArtifactStore, root: Root, spool: *local.storage_hbc_adapter.HBCIndex.BulkSpool, provider: *Provider, index: *local.storage_hbc_adapter.HBCIndex, vectors: *local.storage_lsm_backend.Backend, token: Token) !void {
    const state = @import("lake_index_native_state.zig");
    const dense = @import("lake_index_native_dense.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const file_root = root.file_directory orelse {
        if (root.partition.count != 0) return error.InvalidNativeLakeFileState;
        var buffer: [64 * 1024]u8 = undefined;
        var empty = try spool.create(&buffer);
        var open = true;
        defer if (open) empty.abort(spool.*);
        _ = try empty.finish(spool.io);
        open = false;
        return;
    };
    const entries = @constCast(try state.directoryFiles(out, store, file_root, root.generation.domain, token));
    if (entries.len != provider.source.inventory.files.len) return error.InvalidNativeLakeFileState;
    if (!root.census_order) {
        var positions: std.StringHashMapUnmanaged(usize) = .empty;
        defer positions.deinit(a);
        for (provider.source.inventory.files, 0..) |file, ordinal| try positions.put(a, file.file_id, ordinal);
        for (entries) |file| if (!positions.contains(file.id)) return error.InvalidNativeLakeFileState;
        std.mem.sort(state.File, entries, &positions, struct {
            fn less(order: *const std.StringHashMapUnmanaged(usize), left: state.File, right: state.File) bool {
                return order.get(left.id).? < order.get(right.id).?;
            }
        }.less);
    }
    var current: state.Plan = .{ .a = a };
    defer current.deinit();
    var keys: state.Deletes = .{ .files = entries, .current = &current, .domain = root.generation.domain, .budget = root.input_allowance };
    defer keys.deinit();
    var buffer: [64 * 1024]u8 = undefined;
    var output = try spool.create(&buffer);
    var output_open = true;
    defer if (output_open) output.abort(spool.*);
    var loader: dense.VectorLoader = .{ .backend = vectors, .dims = root.dims, .context = provider.context };
    while (try keys.next(a, store, token)) |chunk| {
        defer a.free(chunk);
        const count = chunk.len / keys.key_width;
        var first: usize = 0;
        while (first < count) {
            try token.check();
            try provider.context.ensureActive();
            var region = std.heap.ArenaAllocator.init(a);
            defer region.deinit();
            const ra = region.allocator();
            const end = @min(count, first + @max(1, (8 * 1024 * 1024) / (@as(usize, root.dims) * 4)));
            const ids = try ra.alloc(u64, end - first);
            const metadata = try ra.alloc(?[]const u8, ids.len);
            const views = try ra.alloc([]const f32, ids.len);
            const matrix = try ra.alloc(f32, ids.len * root.dims);
            const transformed = try ra.alloc(f32, root.dims);
            for (ids, metadata, first..) |*id, *key, ordinal| {
                key.* = chunk[ordinal * keys.key_width ..][0..keys.key_width];
                id.* = dense.vectorId(key.*.?);
            }
            try dense.VectorLoader.loadMany(&loader, ids, metadata, views, matrix, root.dims);
            for (ids, metadata, views) |id, key, vector| try output.append(id, key.?, index.transformVector(vector, transformed));
            first = end;
        }
    }
    const rebuilt = try output.finish(spool.io);
    output_open = false;
    if (!std.mem.eql(u8, &rebuilt.name, &root.partition.name) or rebuilt.count != root.partition.count or rebuilt.metadata_bytes != root.partition.metadata_bytes) return error.InvalidBulkSpoolCount;
}
