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

//! Immutable native file generations exposed through the existing LSM storage
//! port. Each independently authenticated block is shared by the lake cache;
//! authorization, cancellation, and reader leases remain query-owned.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const Storage = local.storage_lsm_backend.Storage;
pub const chunk_bytes = 256 * 1024;
pub const max_chunks = 65536;
pub const max_root_bytes = 16 * 1024 * 1024;
const native_directory = @import("lake_index_native_directory.zig");
/// Attempt-local proof of the descriptor a native producer actually opened.
/// It is never serialized or accepted from a persisted directory record.
pub const Origin = struct {
    directory: native_directory.tree.Ref,
    domain: [32]u8,
    path: [32]u8,
    bytes: u64,
    chunk_root: ?native_directory.tree.Ref,
    pub fn matches(self: Origin, root: Root, path: []const u8) bool {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
        return root.directory != null and root.directory.?.eql(self.directory) and std.mem.eql(u8, &root.domain, &self.domain) and std.mem.eql(u8, &digest, &self.path);
    }
};
pub const File = struct {
    path: []const u8,
    bytes: u64,
    chunks: []const artifacts.ChunkRef = &.{},
    chunk_root: ?native_directory.tree.Ref = null,
    origin: ?Origin = null,
    pub fn jsonStringify(self: File, writer: anytype) !void {
        return writer.write(.{ .path = self.path, .bytes = self.bytes, .chunks = self.chunks, .chunk_root = self.chunk_root });
    }
    pub fn validate(self: File, domain: [32]u8) !void {
        if (!validPath(self.path)) return error.InvalidNativeLakeFiles;
        const expected = self.bytes / chunk_bytes + @intFromBool(self.bytes % chunk_bytes != 0);
        if (expected > max_chunks) return error.InvalidNativeLakeFiles;
        if (self.chunk_root) |root| {
            try root.validate();
            if (self.chunks.len != 0 or root.records != expected) return error.InvalidNativeLakeFiles;
        } else {
            if (self.chunks.len != expected) return error.InvalidNativeLakeFiles;
            for (self.chunks, 0..) |ref, ordinal| {
                if (ref.byte_len != @min(chunk_bytes, self.bytes - @as(u64, @intCast(ordinal)) * chunk_bytes)) return error.InvalidNativeLakeFiles;
                try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
                const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFiles;
                if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeFiles;
            }
        }
    }
};
pub const Root = struct {
    version: u16 = 1,
    domain: [32]u8,
    files: []const File = &.{},
    directory: ?native_directory.tree.Ref = null,
    file_count: u32 = 0,
    chunk_count: u64 = 0,
    pub fn validate(self: Root) !void {
        if (std.mem.allEqual(u8, &self.domain, 0)) return error.InvalidNativeLakeFiles;
        if (self.version == 2) {
            if (self.files.len != 0 or self.file_count > 8192 or self.chunk_count > max_chunks) return error.InvalidNativeLakeFiles;
            if (self.directory) |root| {
                try root.validate();
                if (root.records != self.file_count) return error.InvalidNativeLakeFiles;
            } else if (self.file_count != 0 or self.chunk_count != 0) return error.InvalidNativeLakeFiles;
            return;
        }
        if (self.version != 1 or self.directory != null or self.files.len > 8192) return error.InvalidNativeLakeFiles;
        var count: usize = 0;
        var previous: ?[]const u8 = null;
        for (self.files) |file| {
            if (!validPath(file.path)) return error.InvalidNativeLakeFiles;
            if (previous) |prior| if (std.mem.order(u8, prior, file.path) != .lt) return error.InvalidNativeLakeFiles;
            previous = file.path;
            try file.validate(self.domain);
            count = std.math.add(usize, count, @intCast(file.bytes / chunk_bytes + @intFromBool(file.bytes % chunk_bytes != 0))) catch return error.InvalidNativeLakeFiles;
            if (count > max_chunks) return error.InvalidNativeLakeFiles;
        }
    }
    pub fn find(self: Root, path: []const u8) ?File {
        var lo: usize = 0;
        var hi = self.files.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.files[mid].path, path)) {
                .eq => return self.files[mid],
                .lt => lo = mid + 1,
                .gt => hi = mid,
            }
        }
        return null;
    }
};
fn validPath(path: []const u8) bool {
    if (path.len == 0 or path.len > 4096 or path[0] == '/' or std.mem.indexOfScalar(u8, path, '\\') != null or std.mem.indexOfScalar(u8, path, 0) != null) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}
/// Reuse is available only after the authenticated prior generation has been
/// restored into this private candidate. Native run/segment names are immutable;
/// WAL generation names identify append-only committed prefixes.
pub const Reuse = struct {
    root: Root,
    prefix: []const u8 = "",
    navigation: ?*native_directory.Navigation = null,
    pub fn find(self: Reuse, path: []const u8) ?File {
        var buffer: [4096]u8 = undefined;
        if (self.prefix.len + path.len > buffer.len) return null;
        @memcpy(buffer[0..self.prefix.len], self.prefix);
        @memcpy(buffer[self.prefix.len..][0..path.len], path);
        return self.root.find(buffer[0 .. self.prefix.len + path.len]);
    }
    pub fn lookup(self: Reuse, store: stores.ArtifactStore, path: []const u8, token: Cancellation) !?File {
        var buffer: [4096]u8 = undefined;
        if (self.prefix.len + path.len > buffer.len) return null;
        @memcpy(buffer[0..self.prefix.len], self.prefix);
        @memcpy(buffer[self.prefix.len..][0..path.len], path);
        const name = buffer[0 .. self.prefix.len + path.len];
        var file = (if (self.navigation) |navigation| try navigation.lookupPublication(self.root, store, name, token) else try native_directory.lookupPublication(self.root, store, name, token)) orelse return null;
        if (self.root.directory) |directory| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
            file.origin = .{ .directory = directory, .domain = self.root.domain, .path = digest, .bytes = file.bytes, .chunk_root = file.chunk_root };
        }
        file.path = path;
        return file;
    }
    fn prior(self: Reuse, a: A, store: *stores.ArtifactStore, path: []const u8, ordinal: usize, token: Cancellation) !?artifacts.ChunkRef {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        if (!std.mem.eql(u8, &scope.domain, &self.root.domain)) return error.InvalidNativeLakeFiles;
        const file = (try self.lookup(store.*, path, token)) orelse return null;
        if (ordinal >= file.bytes / chunk_bytes + @intFromBool(file.bytes % chunk_bytes != 0)) return null;
        if (self.navigation) |navigation| return (try navigation.chunksPublication(a, self.root, store.*, file, ordinal, 1, token))[0];
        var access: native_directory.Access = undefined;
        access.initPublication(store.*, self.root.domain, token);
        return try native_directory.chunk(a, &access, file, ordinal);
    }
};
/// Logical generation size and new physical publication work have independent
/// limits. Reusing an immutable chunk charges no read/upload work.
pub const OutputBudget = struct {
    generation: *u64,
    work: ?*u64 = null,
    fn from(value: anytype) OutputBudget {
        if (@TypeOf(value) == *u64) return .{ .generation = value };
        return value;
    }
    fn produced(self: OutputBudget, bytes: usize) !void {
        if (self.work) |remaining| try stores.chargeReadBudget(remaining, bytes);
    }
};
/// Optional physical read-ahead uses known allocator headroom, including arena
/// capacity overhead. Required inline and opaque allocators retain one chunk.
pub fn captureWidth(a: A, store: *const stores.ArtifactStore) usize {
    const phase = store.io_phase orelse return 1;
    const available = @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.headroom(a) orelse local.sql_memory_budget.headroom(a) orelse return 1;
    return @max(1, @min(stores.ArtifactStore.max_upload_batch, phase.max_parallel, available / (2 * chunk_bytes)));
}

/// One bounded wave, with ordered metadata and no detached payload ownership.
/// Publication workers provide the shared I/O capability and a synchronized
/// allocator. Required inline execution retains the same cancellation contract.
pub fn uploadChunks(out: A, store: *stores.ArtifactStore, token: Cancellation, budget: OutputBudget, contents: []const []const u8, refs: []artifacts.ChunkRef) !void {
    std.debug.assert(contents.len == refs.len and contents.len <= stores.ArtifactStore.max_upload_batch);
    for (contents) |bytes| {
        if (bytes.len > chunk_bytes) return error.NativeLakeFilesTooLarge;
        try budget.produced(bytes.len);
    }
    var metadata: [stores.ArtifactStore.max_upload_batch]stores.ArtifactMetadata = undefined;
    try store.putBatchWithCancellation(if (store.io_phase) |phase| phase.io else null, contents, metadata[0..contents.len], token);
    defer for (metadata[0..contents.len]) |*item| item.deinit(store.allocator);
    var initialized: usize = 0;
    errdefer for (refs[0..initialized]) |ref| {
        out.free(ref.artifact_id);
        out.free(ref.checksum);
    };
    for (metadata[0..contents.len], refs) |item, *ref| {
        const id = try out.dupe(u8, item.artifact_id);
        errdefer out.free(id);
        const checksum = try out.dupe(u8, item.checksum);
        ref.* = .{ .artifact_id = id, .checksum = checksum, .byte_len = item.byte_len };
        initialized += 1;
    }
}
fn putChunkBatch(out: A, path: []const u8, first: usize, contents: []const []const u8, store: *stores.ArtifactStore, token: Cancellation, reuse: ?Reuse, budget: OutputBudget, result: []artifacts.ChunkRef) !void {
    var pending: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
    var positions: [stores.ArtifactStore.max_upload_batch]usize = undefined;
    var count: usize = 0;
    for (contents, first..) |bytes, ordinal| {
        try token.check();
        if (reuse) |old| if (try old.prior(out, store, path, ordinal, token)) |ref| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            if (ref.byte_len == bytes.len and std.mem.eql(u8, &digest, &try stores.sha256DigestFromChecksum(ref.checksum))) {
                result[ordinal - first] = .{ .artifact_id = try out.dupe(u8, ref.artifact_id), .checksum = try out.dupe(u8, ref.checksum), .byte_len = ref.byte_len };
                continue;
            }
        };
        pending[count] = bytes;
        positions[count] = ordinal - first;
        count += 1;
    }
    var refs: [stores.ArtifactStore.max_upload_batch]artifacts.ChunkRef = undefined;
    try uploadChunks(out, store, token, budget, pending[0..count], refs[0..count]);
    for (positions[0..count], refs[0..count]) |position, ref| result[position] = ref;
}

pub fn publishBytes(out: A, path: []const u8, bytes: []const u8, store: *stores.ArtifactStore, cancellation: Cancellation, output_budget: anytype, reuse: ?Reuse) !File {
    const budget = OutputBudget.from(output_budget);
    if (!validPath(path)) return error.InvalidNativeLakeFiles;
    try stores.chargeReadBudget(budget.generation, bytes.len);
    const chunks = try out.alloc(artifacts.ChunkRef, std.math.divCeil(usize, bytes.len, chunk_bytes) catch return error.InvalidNativeLakeFiles);
    if (chunks.len > max_chunks) return error.NativeLakeFilesTooLarge;
    var begin: usize = 0;
    while (begin < chunks.len) {
        const end = @min(chunks.len, begin + stores.ArtifactStore.max_upload_batch);
        var contents: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
        for (contents[0 .. end - begin], begin..) |*part, ordinal| {
            const first = ordinal * chunk_bytes;
            part.* = bytes[first..@min(first + chunk_bytes, bytes.len)];
        }
        try putChunkBatch(out, path, begin, contents[0 .. end - begin], store, cancellation, reuse, budget, chunks[begin..end]);
        begin = end;
    }
    return .{ .path = try out.dupe(u8, path), .bytes = bytes.len, .chunks = chunks, .origin = if (reuse) |old| if (try old.lookup(store.*, path, cancellation)) |prior| prior.origin else null else null };
}
/// Mutable native WALs are captured only through their retained committed tip.
pub fn publishPrefix(a: A, out: A, source: Storage, physical: []const u8, relative: []const u8, size: u64, store: *stores.ArtifactStore, cancellation: Cancellation, output_budget: anytype, reuse: ?Reuse) !File {
    const budget = OutputBudget.from(output_budget);
    if (!validPath(relative)) return error.InvalidNativeLakeFiles;
    const n = std.math.cast(usize, size / chunk_bytes + @intFromBool(size % chunk_bytes != 0)) orelse return error.NativeLakeFilesTooLarge;
    if (n > max_chunks) return error.NativeLakeFilesTooLarge;
    if (n > 8 or (if (reuse) |old| old.root.directory != null else false)) return native_directory.publishPrefix(a, out, source, physical, relative, size, store, cancellation, budget, reuse);
    try stores.chargeReadBudget(budget.generation, size);
    const chunks = try out.alloc(artifacts.ChunkRef, n);
    var begin: usize = 0;
    while (begin < chunks.len) {
        const end = @min(chunks.len, begin + captureWidth(a, store));
        var wave = std.heap.ArenaAllocator.init(a);
        defer wave.deinit();
        var contents: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
        var positions: [stores.ArtifactStore.max_upload_batch]usize = undefined;
        var count: usize = 0;
        for (begin..end) |ordinal| {
            const offset = @as(u64, @intCast(ordinal)) * chunk_bytes;
            try cancellation.check();
            const length = @min(chunk_bytes, size - offset);
            if (reuse) |old| if (try old.prior(out, store, relative, ordinal, cancellation)) |ref| {
                if (ref.byte_len == length) {
                    chunks[ordinal] = .{ .artifact_id = try out.dupe(u8, ref.artifact_id), .checksum = try out.dupe(u8, ref.checksum), .byte_len = ref.byte_len };
                    continue;
                }
            };
            contents[count] = try source.readFileRangeAlloc(wave.allocator(), physical, offset, @intCast(length));
            positions[count] = ordinal;
            count += 1;
        }
        var refs: [stores.ArtifactStore.max_upload_batch]artifacts.ChunkRef = undefined;
        try uploadChunks(out, store, cancellation, budget, contents[0..count], refs[0..count]);
        for (positions[0..count], refs[0..count]) |position, ref| chunks[position] = ref;
        begin = end;
    }
    return .{ .path = try out.dupe(u8, relative), .bytes = size, .chunks = chunks };
}
/// Exact membership from a pinned native cut. Paths and sizes are local native
/// metadata; retained objects need no remote descriptor census. Controls carry
/// their pinned bytes, while immutable runs carry their exact storage paths.
pub const Extent = struct {
    path: []const u8,
    bytes: u64,
    physical: ?[]const u8 = null,
    data: ?[]const u8 = null,
    append_only: bool = false,
};
pub fn checkpointExtents(out: A, checkpoint: *const local.storage_lsm_backend.Backend.NativeCheckpoint, prefix: []const u8) ![]Extent {
    if (checkpoint.run_paths.len != checkpoint.run_ids.len or checkpoint.run_sizes.len != checkpoint.run_ids.len) return error.InvalidNativeLakeFiles;
    const result = try out.alloc(Extent, checkpoint.run_ids.len + 1);
    result[0] = .{ .path = try std.fmt.allocPrint(out, "{s}manifest.bin", .{prefix}), .bytes = checkpoint.manifest_bytes.len, .data = checkpoint.manifest_bytes };
    for (checkpoint.run_paths, checkpoint.run_ids, checkpoint.run_sizes, result[1..]) |physical, id, size, *extent| {
        extent.* = .{ .path = try std.fmt.allocPrint(out, "{s}runs/{d}.tbl", .{ prefix, id }), .physical = physical, .bytes = size };
    }
    sortExtents(result);
    return result;
}
pub fn sortExtents(extents: []Extent) void {
    std.mem.sort(Extent, extents, {}, struct {
        fn less(_: void, l: Extent, r: Extent) bool {
            return std.mem.lessThan(u8, l.path, r.path);
        }
    }.less);
}
fn validateExtents(extents: []const Extent) !void {
    if (extents.len > 8192) return error.InvalidNativeLakeFiles;
    for (extents, 0..) |extent, i| {
        if (!validPath(extent.path) or (i != 0 and !std.mem.lessThan(u8, extents[i - 1].path, extent.path))) return error.InvalidNativeLakeFiles;
        if (extent.data) |data| if (data.len != extent.bytes) return error.InvalidNativeLakeFiles;
    }
}
/// Callers capture both cuts from the same authenticated private candidate.
/// Obsolete physical files are deliberately absent: retention and publication
/// membership are independent. Deletions are explicit, including compaction.
pub fn publishExtentDelta(a: A, out: A, source: Storage, before: []const Extent, after: []const Extent, prior: Root, store: *stores.ArtifactStore, token: Cancellation, output_budget: anytype) !Root {
    try prior.validate();
    try validateExtents(before);
    try validateExtents(after);
    if (prior.version != 2 or prior.file_count != before.len) return error.InvalidNativeLakeFiles;
    const budget = OutputBudget.from(output_budget);
    var navigation: native_directory.Navigation = .{ .allocator = a };
    defer navigation.deinit();
    const reuse: Reuse = .{ .root = prior, .navigation = &navigation };
    var changes: std.ArrayList(native_directory.FileChange) = .empty;
    defer changes.deinit(a);
    var old: usize = 0;
    for (after) |extent| {
        try token.check();
        while (old < before.len and std.mem.lessThan(u8, before[old].path, extent.path)) : (old += 1) try changes.append(a, .{ .path = before[old].path, .file = null });
        const previous: ?Extent = if (old < before.len and std.mem.eql(u8, before[old].path, extent.path)) blk: {
            const value = before[old];
            old += 1;
            break :blk value;
        } else null;
        if (previous) |value| {
            if (value.append_only != extent.append_only or ((value.data == null) != (extent.data == null))) return error.InvalidNativeLakeFiles;
            if (extent.data == null and (if (extent.append_only) extent.bytes < value.bytes else extent.bytes != value.bytes)) return error.InvalidNativeLakeFiles;
        }
        if (previous) |value| if (value.bytes == extent.bytes and ((extent.data == null and value.data == null) or (extent.data != null and value.data != null and std.mem.eql(u8, extent.data.?, value.data.?)))) {
            try stores.chargeReadBudget(budget.generation, extent.bytes);
            continue;
        };
        var file = if (extent.data) |data|
            try publishBytes(out, extent.path, data, store, token, budget, reuse)
        else
            try publishPrefix(a, out, source, extent.physical orelse return error.InvalidNativeLakeFiles, extent.path, extent.bytes, store, token, budget, reuse);
        // Only changed chunk maps are constructed. Their old descriptor is
        // authenticated once and retained in the navigation cache.
        if (file.chunks.len != 0) {
            const edits = try out.alloc(native_directory.tree.Mutation, file.chunks.len);
            for (file.chunks, edits, 0..) |chunk_ref, *edit, ordinal| edit.* = .{ .key = try out.dupe(u8, &native_directory.key(ordinal)), .value = try std.json.Stringify.valueAlloc(out, chunk_ref, .{}) };
            var access: native_directory.Access = undefined;
            access.initPublication(store.*, prior.domain, token);
            access.work = budget.work;
            file.chunk_root = try native_directory.tree.apply(a, access.backing(), null, edits);
            file.chunks = &.{};
        }
        try changes.append(a, .{ .path = extent.path, .file = file });
    }
    for (before[old..]) |extent| try changes.append(a, .{ .path = extent.path, .file = null });
    const result = try native_directory.applyChanges(a, store, prior, changes.items, token, budget.work);
    if (result.file_count != after.len) return error.InvalidNativeLakeFiles;
    return result;
}

/// Capture the retained manifest bytes and only its pinned immutable runs.
/// A live manifest pathname may already describe a different generation.
pub fn publishCheckpoint(a: A, out: A, checkpoint: *const local.storage_lsm_backend.Backend.NativeCheckpoint, store: *stores.ArtifactStore, cancellation: Cancellation, output_budget: anytype, reuse: ?Reuse) !Root {
    const budget = OutputBudget.from(output_budget);
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (checkpoint.run_paths.len != checkpoint.run_ids.len or checkpoint.run_paths.len >= 8192) return error.InvalidNativeLakeFiles;
    const files = try out.alloc(File, checkpoint.run_paths.len + 1);
    files[0] = try publishBytes(out, "manifest.bin", checkpoint.manifest_bytes, store, cancellation, budget, reuse);
    for (checkpoint.run_paths, checkpoint.run_ids, files[1..]) |physical, id, *file| {
        const relative = try std.fmt.allocPrint(out, "runs/{d}.tbl", .{id});
        const size = try checkpoint.storage.fileSize(physical);
        file.* = try publishPrefix(a, out, checkpoint.storage, physical, relative, size, store, cancellation, budget, reuse);
    }
    std.mem.sort(File, files, {}, struct {
        fn less(_: void, l: File, r: File) bool {
            return std.mem.order(u8, l.path, r.path) == .lt;
        }
    }.less);
    const root: Root = .{ .domain = scope.domain, .files = files };
    try root.validate();
    return root;
}
/// Paths must be the exact files of a pinned native checkpoint, never an
/// unbounded directory census containing mutable or obsolete generations.
pub fn publishFiles(a: A, out: A, source: Storage, source_root: []const u8, paths: []const []const u8, store: *stores.ArtifactStore, cancellation: Cancellation, output_budget: anytype, reuse: ?Reuse) !Root {
    const budget = OutputBudget.from(output_budget);
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (paths.len > 8192) return error.InvalidNativeLakeFiles;
    const files = try out.alloc(File, paths.len);
    var total: usize = 0;
    for (paths, files) |path, *file| {
        try cancellation.check();
        if (!validPath(path)) return error.InvalidNativeLakeFiles;
        const physical = try std.fmt.allocPrint(a, "{s}/{s}", .{ source_root, path });
        defer a.free(physical);
        const size = try source.fileSize(physical);
        const n = std.math.cast(usize, size / chunk_bytes + @intFromBool(size % chunk_bytes != 0)) orelse return error.NativeLakeFilesTooLarge;
        total = std.math.add(usize, total, n) catch return error.NativeLakeFilesTooLarge;
        if (total > max_chunks) return error.NativeLakeFilesTooLarge;
        try stores.chargeReadBudget(budget.generation, size);
        const chunks = try out.alloc(artifacts.ChunkRef, n);
        var begin: usize = 0;
        while (begin < chunks.len) {
            const end = @min(chunks.len, begin + captureWidth(a, store));
            var wave = std.heap.ArenaAllocator.init(a);
            defer wave.deinit();
            var contents: [stores.ArtifactStore.max_upload_batch][]const u8 = undefined;
            for (contents[0 .. end - begin], begin..) |*bytes, ordinal| {
                try cancellation.check();
                const offset = @as(u64, @intCast(ordinal)) * chunk_bytes;
                bytes.* = try source.readFileRangeAlloc(wave.allocator(), physical, offset, @intCast(@min(chunk_bytes, size - offset)));
            }
            try putChunkBatch(out, path, begin, contents[0 .. end - begin], store, cancellation, reuse, budget, chunks[begin..end]);
            begin = end;
        }
        file.* = .{ .path = try out.dupe(u8, path), .bytes = size, .chunks = chunks };
    }
    std.mem.sort(File, files, {}, struct {
        fn less(_: void, left: File, right: File) bool {
            return std.mem.order(u8, left.path, right.path) == .lt;
        }
    }.less);
    const root: Root = .{ .domain = scope.domain, .files = files };
    try root.validate();
    return root;
}
/// Stable for the lifetime of a native index and its workers. Owns no shared
/// authentication state: the request's lease-bearing Context is checked even
/// for metadata-only and fully cached reads.
pub const Reader = struct {
    root: Root,
    prefix: []const u8,
    store: stores.ArtifactStore,
    context: local.serverless_query_lake_read_context.Context,
    cancellation: Cancellation = .none,
    cache: ?artifacts.CachedRead = null,
    navigation: native_directory.Navigation = .{},
    scratch: A,
    // Disabled for short-lived readers; pooled runtimes retain bounded leases.
    max_block_bytes: usize = 0,
    block_mutex: std.atomic.Mutex = .unlocked,
    blocks: [32]?*Block = @splat(null),
    block_bytes: usize = 0,
    block_tick: u64 = 0,
    const Block = struct {
        id: []u8,
        size: usize,
        used: u64,
        refs: std.atomic.Value(usize) = .init(1),
        state: std.atomic.Value(enum(u8) { loading, ready, failed }) = .init(.loading),
        ready: std.Io.Event = .unset,
        data: local.index.SegmentData = undefined,
        failure: anyerror = error.InvalidNativeLakeFileRoot,
        fn release(self: *Block) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.state.load(.acquire) == .ready) self.data.deinit(std.heap.page_allocator);
            std.heap.page_allocator.free(self.id);
            std.heap.page_allocator.destroy(self);
        }
    };
    pub fn deinit(self: *Reader) void {
        self.navigation.deinit();
        // Native owners join all workers before closing this reader.
        for (&self.blocks) |*slot| if (slot.*) |block| {
            std.debug.assert(block.refs.load(.acquire) == 1);
            block.release();
            slot.* = null;
        };
        self.block_bytes = 0;
    }
    fn loadBlock(self: *Reader, ref: artifacts.ChunkRef) !local.index.SegmentData {
        // Concurrent loaders never allocate through the native owner's arena.
        const a = std.heap.page_allocator;
        var mapped = if (self.cache) |cached| @import("lake_index_native_text.zig").CachedSegments{ .store = self.store, .cache = .{ .cache = cached.cache, .scope = cached.scope, .context = self.context } } else null;
        return if (mapped) |*loader| try loader.loader().load(loader, a, ref, self.cancellation) else local.index.SegmentData.fromOwnedHeap(try artifacts.readArtifact(a, self.store, ref, self.cancellation, null));
    }
    fn awaitBlock(self: *Reader, block: *Block) !*Block {
        errdefer block.release();
        while (block.state.load(.acquire) == .loading) {
            try self.check();
            if (self.context.io) |io| {
                block.ready.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                    error.Timeout => continue,
                    else => return err,
                };
            } else @import("antfly_platform").time.yieldNow();
        }
        try self.check();
        if (block.state.load(.acquire) == .failed) return block.failure;
        return block;
    }
    /// Pin the chunk before dropping the cache lock. Single-flight loading
    /// applies to each admitted chunk; unrelated remote reads can overlap.
    fn acquireBlock(self: *Reader, ref: artifacts.ChunkRef) !*Block {
        try self.check();
        @import("antfly_platform").sync.lockYielding(&self.block_mutex);
        self.block_tick +|= 1;
        for (self.blocks) |slot| if (slot) |block| {
            if (std.mem.eql(u8, block.id, ref.artifact_id)) {
                block.used = self.block_tick;
                _ = block.refs.fetchAdd(1, .monotonic);
                self.block_mutex.unlock();
                return self.awaitBlock(block);
            }
        };
        var target: ?usize = null;
        while (ref.byte_len <= self.max_block_bytes and self.max_block_bytes != 0) {
            var oldest: ?usize = null;
            for (self.blocks, 0..) |slot, i| {
                if (slot) |block| {
                    if (block.refs.load(.acquire) == 1 and block.state.load(.acquire) != .loading and (oldest == null or block.used < self.blocks[oldest.?].?.used)) oldest = i;
                } else if (target == null) target = i;
            }
            if (target != null and ref.byte_len <= self.max_block_bytes - self.block_bytes) break;
            const victim = oldest orelse {
                target = null;
                break;
            };
            const block = self.blocks[victim].?;
            self.blocks[victim] = null;
            self.block_bytes -= block.size;
            block.release();
        }
        const block = std.heap.page_allocator.create(Block) catch |err| {
            self.block_mutex.unlock();
            return err;
        };
        const owned_id = std.heap.page_allocator.dupe(u8, ref.artifact_id) catch |err| {
            std.heap.page_allocator.destroy(block);
            self.block_mutex.unlock();
            return err;
        };
        block.* = .{ .id = owned_id, .size = @intCast(ref.byte_len), .used = self.block_tick };
        if (target) |slot| {
            _ = block.refs.fetchAdd(1, .monotonic);
            self.blocks[slot] = block;
            self.block_bytes += block.size;
        }
        self.block_mutex.unlock();
        block.data = self.loadBlock(ref) catch |err| {
            block.failure = err;
            block.state.store(.failed, .release);
            if (self.context.io) |io| block.ready.set(io);
            @import("antfly_platform").sync.lockYielding(&self.block_mutex);
            if (target) |slot| {
                std.debug.assert(self.blocks[slot] == block);
                self.blocks[slot] = null;
                self.block_bytes -= block.size;
                block.release();
            }
            self.block_mutex.unlock();
            block.release();
            return err;
        };
        block.state.store(.ready, .release);
        if (self.context.io) |io| block.ready.set(io);
        return self.awaitBlock(block);
    }
    fn prefetch(self: *Reader, ref: artifacts.ChunkRef) anyerror!void {
        const block = try self.acquireBlock(ref);
        block.release();
    }
    pub fn storage(self: *Reader) Storage {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn from(raw: *anyopaque) *Reader {
        return @ptrCast(@alignCast(raw));
    }
    fn check(self: *Reader) !void {
        try self.context.ensureActive();
        try self.cancellation.check();
    }
    fn file(self: *Reader, path: []const u8) !File {
        try self.check();
        if (!std.mem.startsWith(u8, path, self.prefix) or path.len <= self.prefix.len or path[self.prefix.len] != '/') return error.FileNotFound;
        return (try self.navigation.lookup(self.root, self.store, path[self.prefix.len + 1 ..], self.cancellation, self.cache)) orelse error.FileNotFound;
    }
    fn readInto(raw: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
        const self = from(raw);
        const entry = try self.file(path);
        var metadata = std.heap.ArenaAllocator.init(self.scratch);
        defer metadata.deinit();
        if (offset > entry.bytes or out.len > entry.bytes - offset) return error.EndOfStream;
        if (out.len == 0) return;
        const first: usize = @intCast(offset / chunk_bytes);
        const last: usize = @intCast((offset + out.len - 1) / chunk_bytes);
        const refs = try self.navigation.chunks(metadata.allocator(), self.root, self.store, entry, first, last - first + 1, self.cancellation, self.cache);
        // Start at most two requested successor chunks, and join them before
        // returning so no task can outlive this request's reader/context.
        var tasks: [2]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer for (&tasks) |*task| if (task.*) |*future| if (future.future != null) {
            _ = future.cancel(self.context.io.?) catch {};
        };
        if (out.len != 0 and self.max_block_bytes != 0) if (self.context.io) |io| {
            for (0..@min(2, last - first)) |slot| {
                const ref = refs[slot + 1];
                tasks[slot] = local.sql_parallel_scheduler.global().submitTransient(io, @intCast(ref.byte_len), prefetch, .{ self, ref });
            }
        };
        var copied: usize = 0;
        while (copied < out.len) {
            try self.check();
            const position = offset + copied;
            const ordinal: usize = @intCast(position / chunk_bytes);
            const inside: usize = @intCast(position % chunk_bytes);
            const block = try self.acquireBlock(refs[ordinal - first]);
            defer block.release();
            const bytes = block.data.bytes();
            const n = @min(out.len - copied, bytes.len - inside);
            @memcpy(out[copied..][0..n], bytes[inside..][0..n]);
            copied += n;
        }
        try self.check();
    }
    fn readRange(raw: *anyopaque, a: A, path: []const u8, offset: u64, len: usize) ![]u8 {
        const self = from(raw);
        const entry = try self.file(path);
        if (offset > entry.bytes or len > entry.bytes - offset) return error.EndOfStream;
        const bytes = try a.alloc(u8, len);
        errdefer a.free(bytes);
        try readInto(raw, path, offset, bytes);
        return bytes;
    }
    fn readAll(raw: *anyopaque, a: A, path: []const u8, limit: usize) ![]u8 {
        const entry = try from(raw).file(path);
        if (entry.bytes > limit) return error.FileTooBig;
        return readRange(raw, a, path, 0, @intCast(entry.bytes));
    }
    fn size(raw: *anyopaque, path: []const u8) !u64 {
        return (try from(raw).file(path)).bytes;
    }
    fn create(raw: *anyopaque, _: []const u8) !void {
        try from(raw).check();
        return error.ReadOnly;
    }
    fn write(raw: *anyopaque, _: []const u8, _: []const u8) !void {
        try from(raw).check();
        return error.ReadOnly;
    }
    fn rename(raw: *anyopaque, _: []const u8, _: []const u8) !void {
        try from(raw).check();
        return error.ReadOnly;
    }
    fn erase(raw: *anyopaque, _: []const u8) !void {
        try from(raw).check();
        return error.ReadOnly;
    }
    fn now(_: *anyopaque) u64 {
        return @import("antfly_platform").time.realtimeNs();
    }
    fn identity(raw: *anyopaque, a: A, _: []const u8) ![]u8 {
        const self = from(raw);
        try self.check();
        return a.dupe(u8, self.prefix);
    }
    fn list(raw: *anyopaque, a: A, path: []const u8) ![][]u8 {
        return listKind(raw, a, path, false);
    }
    fn directories(raw: *anyopaque, a: A, path: []const u8) ![][]u8 {
        return listKind(raw, a, path, true);
    }
    fn listKind(raw: *anyopaque, a: A, path: []const u8, dirs: bool) ![][]u8 {
        const self = from(raw);
        try self.check();
        if (!std.mem.startsWith(u8, path, self.prefix) or (path.len != self.prefix.len and path[self.prefix.len] != '/')) return error.FileNotFound;
        const relative = if (path.len == self.prefix.len) "" else path[self.prefix.len + 1 ..];
        if (relative.len != 0 and !validPath(relative)) return error.FileNotFound;
        return native_directory.names(a, self.root, self.store, relative, dirs, self.cancellation, self.cache);
    }
    const vtable: Storage.VTable = .{ .list_file_names_alloc = list, .list_directory_names_alloc = directories, .create_dir_path = create, .read_file_alloc = readAll, .read_file_range_alloc = readRange, .read_file_range_into = readInto, .file_size = size, .write_file_absolute = write, .rename_absolute = rename, .delete_file_absolute = erase, .delete_tree = erase, .now_ns = now, .root_identity_alloc = identity };
};

/// Restore a private mutable candidate through authenticated bounded reads.
/// Final publication still pins a new native checkpoint and catalog CAS.
pub fn materialize(a: A, io: std.Io, root: Root, store: stores.ArtifactStore, target: []const u8, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) !void {
    try root.validate();
    var reader: Reader = .{ .root = root, .prefix = "/native-lake-seed", .store = store, .context = context, .cancellation = cancellation, .scratch = a };
    defer reader.deinit();
    const buffer = try a.alloc(u8, chunk_bytes);
    defer a.free(buffer);
    var budget: u64 = 1024 * 1024 * 1024;
    var metadata = std.heap.ArenaAllocator.init(a);
    defer metadata.deinit();
    for (try native_directory.entries(metadata.allocator(), root, store, cancellation)) |file| {
        try context.ensureActive();
        try cancellation.check();
        try stores.chargeReadBudget(&budget, file.bytes);
        const destination = try std.fmt.allocPrint(a, "{s}/{s}", .{ target, file.path });
        defer a.free(destination);
        if (std.fs.path.dirname(destination)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
        const output = try std.Io.Dir.cwd().createFile(io, destination, .{ .permissions = .fromMode(0o600) });
        defer output.close(io);
        const input = try std.fmt.allocPrint(a, "{s}/{s}", .{ reader.prefix, file.path });
        defer a.free(input);
        var offset: u64 = 0;
        while (offset < file.bytes) {
            const bytes = buffer[0..@min(buffer.len, file.bytes - offset)];
            try reader.storage().readFileRangeInto(a, input, offset, bytes);
            try output.writePositionalAll(io, bytes, offset);
            offset += bytes.len;
        }
    }
}

test "external lake immutable native files reopen sparse checkpoints through authenticated range reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-files");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const path = try std.fmt.allocPrintSentinel(a, "{s}/index", .{directory.path()}, 0);
    defer a.free(path);
    const sparse = local.sparse_sparse;
    var index = try sparse.SparseIndex.open(a, path, .{});
    defer index.close();
    try index.batchWithOptions(&.{
        .{ .doc_id = "physical-a", .vec = .{ .indices = &.{ 1, 7 }, .values = &.{ 2, 3 } } },
        .{ .doc_id = "physical-b", .vec = .{ .indices = &.{1}, .values = &.{5} } },
    }, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true });
    var checkpoint = try index.pinNativeCheckpoint();
    defer checkpoint.deinit();
    var budget: u64 = 64 * 1024 * 1024;
    const root = try publishCheckpoint(a, ca, &checkpoint, &store, .none, &budget, null);
    var reader: Reader = .{ .root = root, .prefix = "/immutable-publication", .store = store, .context = .{}, .scratch = a };
    var reopened = try sparse.SparseIndex.open(a, "/immutable-publication", .{ .lsm_storage = reader.storage(), .lsm_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
    defer reopened.close();
    const query: sparse.SparseVector = .{ .indices = &.{1}, .values = &.{2} };
    const hits = try reopened.search(a, &query, 10);
    defer {
        for (hits) |hit| a.free(hit.doc_id);
        a.free(hits);
    }
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("physical-b", hits[0].doc_id);
    try std.testing.expectApproxEqAbs(@as(f32, 10), hits[0].score, 0.001);
    try std.testing.expectError(error.ReadOnly, reader.storage().writeFileAbsolute("/immutable-publication/manifest.bin", "x"));
    const payload = try a.alloc(u8, chunk_bytes * 2 + 37);
    defer a.free(payload);
    for (payload, 0..) |*byte, ordinal| byte.* = @truncate(ordinal);
    const physical = try std.fmt.allocPrint(a, "{s}/range.bin", .{path});
    defer a.free(physical);
    try checkpoint.storage.writeFileAbsolute(physical, payload);
    const range_root = try publishFiles(a, ca, checkpoint.storage, path, &.{"range.bin"}, &store, .none, &budget, null);
    {
        const candidate_path = try std.fmt.allocPrint(ca, "{s}/candidate", .{directory.path()});
        try std.Io.Dir.cwd().createDirPath(std.testing.io, candidate_path);
        const candidate = try Overlay.create(a, range_root, store, candidate_path, .{}, .none);
        defer candidate.release();
        const remote_file = try std.fmt.allocPrint(ca, "{s}/range.bin", .{candidate_path});
        const moved_file = try std.fmt.allocPrint(ca, "{s}/moved.bin", .{candidate_path});
        try std.testing.expectError(error.FileNotFound, candidate.native.storage().fileSize(remote_file));
        const lazy = try candidate.storage().readFileRangeAlloc(a, remote_file, chunk_bytes - 4, 8);
        defer a.free(lazy);
        try std.testing.expectEqualSlices(u8, payload[chunk_bytes - 4 ..][0..8], lazy);
        try std.testing.expectError(error.FileNotFound, candidate.native.storage().fileSize(remote_file));
        var lease = try candidate.storage().acquireLease();
        defer lease.deinit();
        try candidate.storage().appendFileAbsolute(a, remote_file, "tail", false);
        try std.testing.expectEqual(payload.len + 4, try candidate.storage().fileSize(remote_file));
        try candidate.storage().renameAbsolute(remote_file, moved_file);
        try std.testing.expectError(error.FileNotFound, candidate.storage().fileSize(remote_file));
        try candidate.storage().deleteFileAbsolute(moved_file);
        try std.testing.expectError(error.FileNotFound, candidate.storage().fileSize(moved_file));
        candidate.base.context.deadline_ns = 1;
        try std.testing.expectError(error.DeadlineExceeded, candidate.storage().fileSize(remote_file));
    }
    store.upload_scope.?.attempt = @splat(4);
    var reuse_budget: u64 = 16 * 1024 * 1024;
    const retained_checkpoint = try publishCheckpoint(a, ca, &checkpoint, &store, .none, &reuse_budget, .{ .root = root });
    for (retained_checkpoint.files, root.files) |current, prior| for (current.chunks, prior.chunks) |current_chunk, prior_chunk| {
        try std.testing.expectEqualStrings(prior_chunk.artifact_id, current_chunk.artifact_id);
    };
    const extended = try std.mem.concat(a, u8, &.{ payload, "appended tail" });
    defer a.free(extended);
    try checkpoint.storage.writeFileAbsolute(physical, extended);
    const appended_prefix = try publishPrefix(a, ca, checkpoint.storage, physical, "range.bin", extended.len, &store, .none, &reuse_budget, .{ .root = range_root });
    for (appended_prefix.chunks[0..2], range_root.files[0].chunks[0..2]) |current, prior| try std.testing.expectEqualStrings(prior.artifact_id, current.artifact_id);
    try std.testing.expect(!std.mem.eql(u8, appended_prefix.chunks[2].artifact_id, range_root.files[0].chunks[2].artifact_id));
    const appended_root: Root = .{ .domain = root.domain, .files = &.{appended_prefix} };
    try appended_root.validate();
    var appended_reader: Reader = .{ .root = appended_root, .prefix = "/appended", .store = store, .context = .{}, .scratch = a };
    defer appended_reader.deinit();
    const appended_bytes = try appended_reader.storage().readFileRangeAlloc(a, "/appended/range.bin", 0, extended.len);
    defer a.free(appended_bytes);
    try std.testing.expectEqualSlices(u8, extended, appended_bytes);
    var range_reader: Reader = .{ .root = range_root, .prefix = "/range-publication", .store = store, .context = .{}, .scratch = a, .max_block_bytes = chunk_bytes };
    defer range_reader.deinit();
    const boundary = try range_reader.storage().readFileRangeAlloc(a, "/range-publication/range.bin", chunk_bytes - 13, chunk_bytes + 29);
    defer a.free(boundary);
    try std.testing.expectEqualSlices(u8, payload[chunk_bytes - 13 .. chunk_bytes * 2 + 16], boundary);
    try std.testing.expect(range_reader.block_bytes <= chunk_bytes);
    const warm = try range_reader.storage().readFileRangeAlloc(a, "/range-publication/range.bin", chunk_bytes * 2, 16);
    defer a.free(warm);
    try std.testing.expectEqualSlices(u8, payload[chunk_bytes * 2 ..][0..16], warm);
    range_reader.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, range_reader.storage().readFileRangeAlloc(a, "/range-publication/range.bin", chunk_bytes * 2, 16));
    range_reader.context = .{};
    try std.testing.expectError(error.EndOfStream, range_reader.storage().readFileRangeAlloc(a, "/range-publication/range.bin", payload.len - 1, 2));
    try std.testing.expectError(error.FileNotFound, range_reader.storage().fileSize("/range-publication/../range.bin"));
    var foreign = range_root;
    foreign.domain = @splat(9);
    try std.testing.expectError(error.InvalidNativeLakeFiles, foreign.validate());
    const paged = try native_directory.persist(a, ca, &store, root, null, .none, null);
    try index.batchWithOptions(&.{.{ .doc_id = "physical-c", .vec = .{ .indices = &.{1}, .values = &.{10} } }}, &.{"physical-a"}, .{});
    var current = try index.pinNativeCheckpoint();
    defer current.deinit();
    const delta = try publishExtentDelta(a, ca, current.storage, try checkpointExtents(ca, &checkpoint, ""), try checkpointExtents(ca, &current, ""), paged, &store, .none, &budget);
    var delta_reader: Reader = .{ .root = delta, .prefix = "/delta-publication", .store = store, .context = .{}, .scratch = a };
    defer delta_reader.deinit();
    var delta_index = try sparse.SparseIndex.open(a, "/delta-publication", .{ .lsm_storage = delta_reader.storage(), .lsm_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
    defer delta_index.close();
    const delta_hits = try delta_index.search(a, &query, 10);
    defer {
        for (delta_hits) |hit| a.free(hit.doc_id);
        a.free(delta_hits);
    }
    try std.testing.expectEqual(@as(usize, 2), delta_hits.len);
    try std.testing.expectEqualStrings("physical-c", delta_hits[0].doc_id);
    try std.testing.expectEqualStrings("physical-b", delta_hits[1].doc_id);
    try std.testing.expectApproxEqAbs(@as(f32, 20), delta_hits[0].score, 0.001);

    // A tiny delta over a wide native cut must not point-look up every
    // retained descriptor. None of these retained physical files exist.
    const sample = try publishBytes(ca, "runs/000000.tbl", "retained", &store, .none, &budget, null);
    const wide = try ca.alloc(File, 1024);
    const before = try ca.alloc(Extent, wide.len);
    for (wide, before, 0..) |*file, *extent, i| {
        file.* = sample;
        file.path = try std.fmt.allocPrint(ca, "runs/{d:0>6}.tbl", .{i});
        extent.* = .{ .path = file.path, .bytes = sample.bytes };
    }
    const wide_root = try native_directory.persist(a, ca, &store, .{ .domain = root.domain, .files = wide }, null, .none, null);
    before[1].data = "retained";
    const after = try ca.dupe(Extent, before[1..]);
    after[0] = .{ .path = before[1].path, .bytes = 7, .data = "changed" };
    const Counter = struct {
        var underlying: stores.ArtifactStore = undefined;
        var gets: usize = 0;
        fn read(_: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, token: Cancellation) ![]u8 {
            gets += 1;
            return underlying.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, length, token);
        }
    };
    Counter.underlying = store;
    var counted = store;
    var counted_vtable = store.vtable.*;
    counted_vtable.get_range_alloc_with_cancellation = Counter.read;
    counted.vtable = &counted_vtable;
    var retained_budget: u64 = 1024 * 1024;
    var no_work: u64 = 0;
    const unchanged = try publishExtentDelta(a, ca, checkpoint.storage, before, before, wide_root, &counted, .none, OutputBudget{ .generation = &retained_budget, .work = &no_work });
    try std.testing.expectEqual(@as(usize, 0), Counter.gets);
    try std.testing.expect(unchanged.directory.?.eql(wide_root.directory.?));
    var delta_budget: u64 = 1024 * 1024;
    const changed_root = try publishExtentDelta(a, ca, checkpoint.storage, before, after, wide_root, &counted, .none, &delta_budget);
    try std.testing.expect(Counter.gets < 64);
    try std.testing.expectEqual(@as(u32, 1023), changed_root.file_count);
    try std.testing.expect((try native_directory.lookup(changed_root, store, before[0].path, .none, null)) == null);
    try std.testing.expectEqual(@as(u64, 7), (try native_directory.lookup(changed_root, store, before[1].path, .none, null)).?.bytes);
    try std.testing.expect((try native_directory.lookup(wide_root, store, before[0].path, .none, null)) != null);
    const resized = try ca.dupe(Extent, before);
    resized[2].bytes -= 1;
    try std.testing.expectError(error.InvalidNativeLakeFiles, publishExtentDelta(a, ca, checkpoint.storage, before, resized, wide_root, &store, .none, &delta_budget));
    reader.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, reader.storage().fileSize("/immutable-publication/manifest.bin"));
}

test "external lake immutable native files reopen dense posting authority without compatibility indexes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("lake-native-dense-files");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const path = try std.fmt.allocPrintSentinel(a, "{s}/index", .{directory.path()}, 0);
    defer a.free(path);
    const hbc = local.storage_hbc_adapter;
    const config: hbc.HBCConfig = .{ .dims = 2, .leaf_size = 2, .branching_factor = 2, .search_width = 8 };
    const native = @import("lake_index_native_dense.zig");
    const vector_path = try std.fmt.allocPrint(ca, "{s}/vectors", .{path});
    var vectors = try local.storage_lsm_backend.Backend.open(a, vector_path, .{});
    defer vectors.close();
    var write = try vectors.beginBatch();
    errdefer write.abort();
    for ([_][]const u8{ "physical-a", "physical-b", "physical-c" }, [_]f32{ 1, 5, 9 }) |key, value| {
        const bytes = try @import("antfly_vector").codec.encodePackedF32BytesAlloc(ca, &.{ value, 0 });
        try write.put(.{ .name = "exact_vectors" }, key, bytes);
    }
    try write.commit();
    var index = try hbc.HBCIndex.open(a, path, config);
    defer index.close();
    index.setIo(std.testing.io);
    var loader: native.VectorLoader = .{ .backend = &vectors, .dims = 2 };
    index.setExternalVectorLoader(&loader, native.VectorLoader.load);
    index.setExperimentalPostingAuthorityTransitionPermitted(true);
    try index.beginBulkIngestSession();
    try index.batchInsertWithMetadataOptions(&.{
        .{ .vector_id = 1, .vector = &.{ 1, 0 }, .metadata = "physical-a" },
        .{ .vector_id = 2, .vector = &.{ 5, 0 }, .metadata = "physical-b" },
        .{ .vector_id = 3, .vector = &.{ 9, 0 }, .metadata = "physical-c" },
    }, .{ .assume_absent_ids = true, .bulk_ingest = true });
    try index.finishBulkIngestSessionWithOptions(.{});
    try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = true, .make_authoritative = true });
    var checkpoint = (try index.nativeBackupGeneration(a, 0)).?;
    defer checkpoint.deinit();
    var budget: u64 = 64 * 1024 * 1024;
    const postings = try native.publishGeneration(a, ca, &checkpoint, &store, .none, &budget, null);
    var vector_checkpoint = try vectors.pinNativeCheckpoint();
    defer vector_checkpoint.deinit();
    const vector_files = try publishCheckpoint(a, ca, &vector_checkpoint, &store, .none, &budget, null);
    const root = try native.combineVectors(ca, postings, vector_files);
    var reader: Reader = .{ .root = root, .prefix = "/immutable-dense", .store = store, .context = .{}, .scratch = a };
    var reopened_vectors = try local.storage_lsm_backend.Backend.open(a, "/immutable-dense/vectors", .{ .storage = reader.storage(), .backend = .{ .read_only = true, .create_if_missing = false } });
    defer reopened_vectors.close();
    var reopened_loader: native.VectorLoader = .{ .backend = &reopened_vectors, .dims = 2 };
    var reopened = try hbc.HBCIndex.openWithLsmOptions(a, "/immutable-dense", config, .{ .storage = reader.storage(), .backend_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
    defer reopened.close();
    reopened.setIo(std.testing.io);
    reopened.setExternalVectorLoader(&reopened_loader, native.VectorLoader.load);
    reopened.setExternalVectorScratchLoader(&reopened_loader, native.VectorLoader.loadInto);
    reopened.setExternalVectorBatchScratchLoader(&reopened_loader, native.VectorLoader.loadMany);
    try reopened.activateExperimentalPostingReads(0);
    try std.testing.expect(reopened.experimentalPostingWalAuthoritative());
    var hits = try reopened.search(&.{ 5, 0 }, 3);
    defer hits.deinit();
    try std.testing.expectEqual(@as(usize, 3), hits.items.items.len);
    try std.testing.expectEqual(@as(u64, 2), hits.items.items[0].vector_id);
    try std.testing.expectApproxEqAbs(@as(f32, 0), hits.items.items[0].distance, 0.001);
    const metadata = (try reopened.getMetadata(2)).?;
    defer a.free(metadata);
    try std.testing.expectEqualStrings("physical-b", metadata);
}

/// A private writable candidate over an immutable checkpoint. Unchanged runs
/// stay remote; only a file opened for append/rename is copied into the private
/// namespace. Every base read still checks the current producer's proof.
pub const Overlay = struct {
    allocator: A,
    native: local.storage_lsm_backend.NativeStorage,
    base: Reader,
    mutex: std.atomic.Mutex = .unlocked,
    references: std.atomic.Value(usize) = .init(1),
    deleted: std.StringHashMapUnmanaged(void) = .empty,
    materialized_bytes: u64 = 1024 * 1024 * 1024,

    pub fn create(a: A, root: Root, store: stores.ArtifactStore, path: []const u8, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) !*Overlay {
        try root.validate();
        const self = try a.create(Overlay);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .native = try .init(a, .threaded), .base = .{ .root = root, .prefix = path, .store = store, .context = context, .cancellation = cancellation, .scratch = a, .navigation = .{ .allocator = a }, .max_block_bytes = 8 * 1024 * 1024 } };
        return self;
    }
    pub fn release(self: *Overlay) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        self.base.deinit();
        var keys = self.deleted.keyIterator();
        while (keys.next()) |key| self.allocator.free(key.*);
        self.deleted.deinit(self.allocator);
        self.native.deinit();
        self.allocator.destroy(self);
    }
    pub fn storage(self: *Overlay) Storage {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn from(raw: *anyopaque) *Overlay {
        return @ptrCast(@alignCast(raw));
    }
    fn acquire(raw: *anyopaque) !Storage.Lease {
        const self = from(raw);
        try self.base.check();
        _ = self.references.fetchAdd(1, .monotonic);
        return .{ .view = self.storage(), .release = releaseLease };
    }
    fn releaseLease(raw: *anyopaque) void {
        from(raw).release();
    }
    fn check(self: *Overlay, path: []const u8) !void {
        try self.base.check();
        if (!std.mem.startsWith(u8, path, self.base.prefix) or (path.len != self.base.prefix.len and path[self.base.prefix.len] != '/') or std.mem.indexOf(u8, path, "/../") != null) return error.InvalidNativeLakeFileRoot;
    }
    fn hidden(self: *Overlay, path: []const u8) bool {
        var iterator = self.deleted.keyIterator();
        while (iterator.next()) |key| if (std.mem.eql(u8, path, key.*) or (std.mem.startsWith(u8, path, key.*) and path.len > key.len and path[key.len] == '/')) return true;
        return false;
    }
    fn selected(self: *Overlay, path: []const u8) !Storage {
        try self.check(path);
        _ = self.native.storage().fileSize(path) catch |err| switch (err) {
            error.FileNotFound => {
                if (self.hidden(path)) return error.FileNotFound;
                return self.base.storage();
            },
            else => return err,
        };
        return self.native.storage();
    }
    fn tombstone(self: *Overlay, path: []const u8) !void {
        if (self.deleted.contains(path)) return;
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        try self.deleted.put(self.allocator, owned, {});
    }
    // The caller serializes mutations. Copy in bounded chunks, and remove an
    // incomplete local file on failure so it can never shadow the base.
    fn copy(self: *Overlay, path: []const u8) !void {
        const source = try self.selected(path);
        if (source.ptr != @as(*anyopaque, @ptrCast(&self.base))) return;
        const len = try source.fileSize(path);
        try stores.chargeReadBudget(&self.materialized_bytes, len);
        if (std.fs.path.dirname(path)) |parent| try self.native.storage().createDirPath(parent);
        try self.native.storage().writeFileAbsolute(path, "");
        errdefer self.native.storage().deleteFileAbsolute(path) catch {};
        const buffer = try self.allocator.alloc(u8, chunk_bytes);
        defer self.allocator.free(buffer);
        var offset: u64 = 0;
        while (offset < len) {
            const bytes = buffer[0..@min(buffer.len, len - offset)];
            try source.readFileRangeInto(self.allocator, path, offset, bytes);
            try self.native.storage().appendFileAbsolute(self.allocator, path, bytes, false);
            offset += bytes.len;
        }
    }
    fn createDir(raw: *anyopaque, path: []const u8) !void {
        const self = from(raw);
        try self.check(path);
        try self.native.storage().createDirPath(path);
    }
    fn readAll(raw: *anyopaque, a: A, path: []const u8, limit: usize) ![]u8 {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return (try self.selected(path)).readFileAlloc(a, path, limit);
    }
    fn readRange(raw: *anyopaque, a: A, path: []const u8, offset: u64, len: usize) ![]u8 {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return (try self.selected(path)).readFileRangeAlloc(a, path, offset, len);
    }
    fn readInto(raw: *anyopaque, path: []const u8, offset: u64, bytes: []u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try (try self.selected(path)).readFileRangeInto(self.allocator, path, offset, bytes);
    }
    fn size(raw: *anyopaque, path: []const u8) !u64 {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return (try self.selected(path)).fileSize(path);
    }
    fn write(raw: *anyopaque, path: []const u8, bytes: []const u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.check(path);
        try self.native.storage().writeFileAbsolute(path, bytes);
    }
    fn append(raw: *anyopaque, path: []const u8, bytes: []const u8, sync: bool) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.copy(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try self.native.storage().appendFileAbsolute(self.allocator, path, bytes, sync);
    }
    fn rename(raw: *anyopaque, old: []const u8, new: []const u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.check(new);
        try self.copy(old);
        // Reserve the tombstone before changing the physical namespace.
        try self.tombstone(old);
        try self.native.storage().renameAbsolute(old, new);
    }
    fn erase(raw: *anyopaque, path: []const u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.check(path);
        try self.tombstone(path);
        self.native.storage().deleteFileAbsolute(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
    fn eraseTree(raw: *anyopaque, path: []const u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.check(path);
        try self.tombstone(path);
        self.native.storage().deleteTree(path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
    fn syncContents(raw: *anyopaque, path: []const u8) !void {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const source = try self.selected(path);
        if (source.ptr != @as(*anyopaque, @ptrCast(&self.base))) try source.syncFileContentsAbsolute(path);
    }
    fn syncParent(raw: *anyopaque, path: []const u8) !void {
        const self = from(raw);
        try self.check(path);
        try self.native.storage().syncParentAbsolute(path);
    }
    fn identity(raw: *anyopaque, a: A, path: []const u8) ![]u8 {
        const self = from(raw);
        try self.check(path);
        return self.native.storage().rootIdentityAlloc(a, path);
    }
    fn atomicWrite(raw: *anyopaque, a: A, path: []const u8) !local.storage_lsm_backend_storage_io.AtomicWriteSink {
        const self = from(raw);
        try self.check(path);
        return self.native.storage().beginAtomicWrite(a, path);
    }
    fn list(raw: *anyopaque, a: A, path: []const u8) ![][]u8 {
        return listKind(raw, a, path, false);
    }
    fn directories(raw: *anyopaque, a: A, path: []const u8) ![][]u8 {
        return listKind(raw, a, path, true);
    }
    fn listKind(raw: *anyopaque, a: A, path: []const u8, dirs: bool) ![][]u8 {
        const self = from(raw);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        try self.check(path);
        const local_names = (if (dirs) self.native.storage().listDirectoryNamesAlloc(a, path) else self.native.storage().listFileNamesAlloc(a, path)) catch |err| switch (err) {
            error.FileNotFound => try a.alloc([]u8, 0),
            else => return err,
        };
        defer Storage.freeFileNames(a, local_names);
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        var result: std.ArrayList([]u8) = .empty;
        errdefer {
            for (result.items) |name| a.free(name);
            result.deinit(a);
        }
        for (local_names) |name| {
            const copy_name = try a.dupe(u8, name);
            result.append(a, copy_name) catch |err| {
                a.free(copy_name);
                return err;
            };
            try names.put(a, copy_name, {});
        }
        const base_names = try Reader.listKind(&self.base, a, path, dirs);
        defer Storage.freeFileNames(a, base_names);
        for (base_names) |name| {
            if (names.contains(name)) continue;
            const full = try std.fmt.allocPrint(a, "{s}/{s}", .{ path, name });
            defer a.free(full);
            if (self.hidden(full)) continue;
            const copy_name = try a.dupe(u8, name);
            result.append(a, copy_name) catch |err| {
                a.free(copy_name);
                return err;
            };
            try names.put(a, copy_name, {});
        }
        std.mem.sort([]u8, result.items, {}, struct {
            fn less(_: void, l: []u8, r: []u8) bool {
                return std.mem.order(u8, l, r) == .lt;
            }
        }.less);
        return result.toOwnedSlice(a);
    }
    fn now(raw: *anyopaque) u64 {
        return from(raw).native.storage().nowNs();
    }
    const vtable: Storage.VTable = .{ .begin_atomic_write = atomicWrite, .list_file_names_alloc = list, .list_directory_names_alloc = directories, .acquire_lease = acquire, .create_dir_path = createDir, .read_file_alloc = readAll, .read_file_range_alloc = readRange, .read_file_range_into = readInto, .file_size = size, .write_file_absolute = write, .append_file_absolute = append, .rename_absolute = rename, .delete_file_absolute = erase, .delete_tree = eraseTree, .sync_contents_absolute = syncContents, .sync_parent_absolute = syncParent, .root_identity_alloc = identity, .now_ns = now, .rename_is_atomic = true, .supports_native_path_locks = true };
};

test "external lake native chunk flights overlap distinct loads and share identical loads" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try local.common_test_directory.TestDirectory.init("native-chunk-flights");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backing = fs.artifactStore();
    backing.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, io);
    var one = try backing.put("a");
    defer one.deinit(a);
    var two = try backing.put("b");
    defer two.deinit(a);
    const Spy = struct {
        backing: stores.ArtifactStore,
        loads: std.atomic.Value(usize) = .init(0),
        both: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        fn stat(raw: *anyopaque, alloc: A, id: []const u8) !stores.ArtifactMetadata {
            const spy: *@This() = @ptrCast(@alignCast(raw));
            if (spy.loads.fetchAdd(1, .acq_rel) + 1 == 2) spy.both.set(std.testing.io);
            try spy.gate.wait(std.testing.io);
            return spy.backing.statWithCancellationUsingAllocator(alloc, id, .none);
        }
        fn range(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, len: usize) ![]u8 {
            const spy: *@This() = @ptrCast(@alignCast(raw));
            return spy.backing.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, len, .none);
        }
    };
    var spy: Spy = .{ .backing = backing };
    var vtable = backing.vtable.*;
    vtable.stat = Spy.stat;
    vtable.stat_with_cancellation = null;
    vtable.get_range_alloc = Spy.range;
    vtable.get_range_alloc_with_cancellation = null;
    var reader: Reader = .{ .root = .{ .domain = @splat(7), .files = &.{} }, .prefix = "/flights", .store = .{ .allocator = a, .ptr = &spy, .vtable = &vtable }, .context = .{ .io = io }, .scratch = a, .max_block_bytes = 1024 };
    defer reader.deinit();
    const Worker = struct {
        fn run(owner: *Reader, ref: artifacts.ChunkRef) anyerror!u8 {
            const block = try owner.acquireBlock(ref);
            defer block.release();
            return block.data.bytes()[0];
        }
    };
    const first: artifacts.ChunkRef = .{ .artifact_id = one.artifact_id, .checksum = one.checksum, .byte_len = 1 };
    const second: artifacts.ChunkRef = .{ .artifact_id = two.artifact_id, .checksum = two.checksum, .byte_len = 1 };
    var tasks: [3]?std.Io.Future(anyerror!u8) = @splat(null);
    defer for (&tasks) |*task| if (task.*) |*future| {
        _ = future.cancel(io) catch {};
    };
    defer spy.gate.set(io);
    tasks[0] = try io.concurrent(Worker.run, .{ &reader, first });
    tasks[1] = try io.concurrent(Worker.run, .{ &reader, second });
    tasks[2] = try io.concurrent(Worker.run, .{ &reader, first });
    try spy.both.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } });
    spy.gate.set(io);
    for (&tasks, [_]u8{ 'a', 'b', 'a' }) |*task, expected| {
        const result = task.*.?.await(io);
        task.* = null;
        try std.testing.expectEqual(expected, try result);
    }
    try std.testing.expectEqual(@as(usize, 2), spy.loads.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), reader.block_bytes);
}

test "external lake chunk wait observes its own deadline" {
    const io = std.testing.io;
    const block = try std.heap.page_allocator.create(Reader.Block);
    const id = std.heap.page_allocator.dupe(u8, "pending") catch |err| {
        std.heap.page_allocator.destroy(block);
        return err;
    };
    block.* = .{ .id = id, .size = 0, .used = 0, .refs = .init(2) };
    defer block.release();
    var reader: Reader = .{ .root = undefined, .prefix = "", .store = undefined, .scratch = std.testing.allocator, .context = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 100 * std.time.ns_per_ms } };
    const Worker = struct {
        reader: *Reader,
        block: *Reader.Block,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            const result = self.reader.awaitBlock(self.block) catch {
                self.done.store(true, .release);
                return;
            };
            result.release();
            self.done.store(true, .release);
        }
    };
    var worker: Worker = .{ .reader = &reader, .block = block };
    var task = io.concurrent(Worker.run, .{&worker}) catch |err| {
        block.release();
        return err;
    };
    var joined = false;
    defer if (!joined) task.cancel(io);
    try io.sleep(.fromMilliseconds(250), .awake);
    const honored_deadline = worker.done.load(.acquire);
    // Always unblock and join before reporting the assertion.
    if (!honored_deadline) {
        block.failure = error.TestLeaderFailed;
        block.state.store(.failed, .release);
        block.ready.set(io);
    }
    task.await(io);
    joined = true;
    try std.testing.expect(honored_deadline);
}

test "external lake cumulative output charges new chunks while generation limits fence reuse" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-output-work");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const payload = "immutable native run";
    var work: u64 = payload.len;
    var generation: u64 = payload.len;
    const budget: OutputBudget = .{ .generation = &generation, .work = &work };
    const first = try publishBytes(out, "runs/1.tbl", payload, &store, .none, budget, null);
    try std.testing.expectEqual(@as(u64, 0), work);
    const reuse: Reuse = .{ .root = .{ .domain = @splat(7), .files = &.{first} } };
    for (0..100) |_| {
        generation = payload.len;
        // No source read or put is possible with an exhausted work budget.
        const next = try publishPrefix(a, out, undefined, "unused", "runs/1.tbl", payload.len, &store, .none, budget, reuse);
        try std.testing.expectEqualStrings(first.chunks[0].artifact_id, next.chunks[0].artifact_id);
        try std.testing.expectEqual(@as(u64, 0), generation);
        generation = payload.len;
        _ = try publishBytes(out, "runs/1.tbl", payload, &store, .none, budget, reuse);
        try std.testing.expectEqual(@as(u64, 0), work);
    }
    generation = payload.len - 1;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, publishPrefix(a, out, undefined, "unused", "runs/1.tbl", payload.len, &store, .none, budget, reuse));
    generation = payload.len;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, publishBytes(out, "runs/2.tbl", payload, &store, .none, budget, reuse));
}

test "external lake native publication chunk waves overlap preserve order reuse and drain failures" {
    const scheduler_api = local.sql_parallel_scheduler;
    const Fixture = struct {
        var active: *@This() = undefined;
        original: stores.ArtifactStore,
        execution: scheduler_api.Execution,
        pending: ?scheduler_api.Execution.Io = null,
        current: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        puts: std.atomic.Value(usize) = .init(0),
        fail: bool = false,
        begins: usize = 0,
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pending = try self.execution.beginIo(std.testing.io, count);
            self.begins += 1;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 0), self.current.load(.acquire));
            var pending = self.pending.?;
            self.pending = null;
            try pending.finish();
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.pending) |*pending| pending.abort();
            self.pending = null;
        }
        fn put(_: *anyopaque, _: A, scope: stores.UploadScope, bytes: []const u8, token: Cancellation) !stores.ArtifactMetadata {
            const self = active;
            try std.testing.expect(!self.execution.active);
            const current = self.current.fetchAdd(1, .acq_rel) + 1;
            _ = self.peak.fetchMax(current, .acq_rel);
            _ = self.puts.fetchAdd(1, .acq_rel);
            defer _ = self.current.fetchSub(1, .acq_rel);
            try std.testing.io.sleep(.fromMilliseconds(3), .awake);
            if (self.fail and bytes.len != 0 and bytes[0] == 3) return error.InjectedUploadFailure;
            return self.original.putScoped(scope, bytes, token);
        }
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("native-chunk-upload-waves");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var original = fs.artifactStore();
    original.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var scheduler: scheduler_api.Scheduler = .{ .max_workers = 1, .max_io_workers = 3 };
    var fixture: Fixture = .{ .original = original, .execution = .{ .scheduler = &scheduler, .bytes = 1024 } };
    Fixture.active = &fixture;
    try fixture.execution.enter(std.testing.io);
    defer fixture.execution.pause();
    var vtable = original.vtable.*;
    vtable.put_scoped = Fixture.put;
    var store = original;
    store.vtable = &vtable;
    const capability: stores.IoPhase = .{ .io = std.testing.io, .ptr = &fixture, .max_parallel = 3, .begin = Fixture.begin, .finish = Fixture.finish, .abort = Fixture.abort };
    store.io_phase = &capability;
    const bytes = try a.alloc(u8, 17 * chunk_bytes - 7);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, index| byte.* = @intCast(index / chunk_bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var generation: u64 = bytes.len;
    var work: u64 = bytes.len;
    const file = try publishBytes(out, "manifest.bin", bytes, &store, .none, OutputBudget{ .generation = &generation, .work = &work }, null);
    try std.testing.expectEqual(@as(u64, 0), generation);
    try std.testing.expectEqual(@as(u64, 0), work);
    try std.testing.expectEqual(@as(usize, 17), fixture.puts.load(.acquire));
    try std.testing.expect(fixture.peak.load(.acquire) > 1 and fixture.peak.load(.acquire) <= 3);
    for (file.chunks, 0..) |ref, ordinal| {
        const value = try original.getAlloc(ref.artifact_id);
        defer a.free(value);
        try std.testing.expectEqualSlices(u8, bytes[ordinal * chunk_bytes .. @min((ordinal + 1) * chunk_bytes, bytes.len)], value);
    }
    generation = bytes.len;
    const before = fixture.puts.load(.acquire);
    const reused = try publishBytes(out, "manifest.bin", bytes, &store, .none, OutputBudget{ .generation = &generation, .work = &work }, .{ .root = .{ .domain = @splat(7), .files = &.{file} } });
    try std.testing.expectEqual(before, fixture.puts.load(.acquire));
    for (file.chunks, reused.chunks) |old, current| try std.testing.expectEqualStrings(old.artifact_id, current.artifact_id);
    fixture.fail = true;
    generation = bytes.len;
    work = bytes.len;
    try std.testing.expectError(error.InjectedUploadFailure, publishBytes(out, "manifest.bin", bytes, &store, .none, OutputBudget{ .generation = &generation, .work = &work }, null));
    try std.testing.expectEqual(@as(usize, 0), fixture.current.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expect(fixture.execution.active);
    fixture.fail = false;
    // The same bounded writer backs paged physical-file publication. A retained
    // source reader supplies real range reads, then a no-op refresh must touch
    // neither source payloads nor new objects.
    var source: Reader = .{ .root = .{ .domain = @splat(7), .files = &.{file} }, .prefix = "/candidate", .store = original, .context = .{}, .scratch = a };
    defer source.deinit();
    generation = bytes.len;
    work = 2 * bytes.len;
    var capture_budget = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{ .max_working_set_bytes = 8 * 1024 * 1024 });
    var capture_allocations: scheduler_api.LockedAllocator = .{ .backing = capture_budget.allocator() };
    try std.testing.expectEqual(@as(usize, 3), captureWidth(capture_allocations.allocator(), &store));
    const held = try capture_allocations.allocator().alloc(u8, 7 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 2), captureWidth(capture_allocations.allocator(), &store));
    capture_allocations.allocator().free(held);
    fixture.peak.store(0, .release);
    const prefix = try publishPrefix(capture_allocations.allocator(), out, source.storage(), "/candidate/manifest.bin", "manifest.bin", bytes.len, &store, .none, OutputBudget{ .generation = &generation, .work = &work }, null);
    try std.testing.expect(fixture.peak.load(.acquire) > 1 and fixture.peak.load(.acquire) <= 3);
    const paged = try native_directory.persist(a, out, &store, .{ .domain = @splat(7), .files = &.{prefix} }, null, .none, &work);
    try std.testing.expectEqual(@as(u64, 17), prefix.chunk_root.?.records);
    try std.testing.expectEqual(@as(usize, 0), capture_budget.live_bytes);
    try std.testing.expect(!capture_budget.limit_exceeded);
    const before_noop = fixture.puts.load(.acquire);
    generation = bytes.len;
    work = 0;
    const unchanged = try publishPrefix(a, out, undefined, "unread", "manifest.bin", bytes.len, &store, .none, OutputBudget{ .generation = &generation, .work = &work }, .{ .root = paged });
    try std.testing.expect(prefix.chunk_root.?.eql(unchanged.chunk_root.?));
    try std.testing.expectEqual(before_noop, fixture.puts.load(.acquire));
    // Required inline publication retains the same chunk identities and order.
    var inline_store = original;
    generation = bytes.len;
    work = bytes.len;
    const inline_file = try publishBytes(out, "manifest.bin", bytes, &inline_store, .none, OutputBudget{ .generation = &generation, .work = &work }, null);
    for (file.chunks, inline_file.chunks) |parallel, inline_ref| try std.testing.expectEqualStrings(parallel.artifact_id, inline_ref.artifact_id);
    std.debug.print("native chunk publication: chunks=17 peak_uploads={} io_phases={}\n", .{ fixture.peak.load(.acquire), fixture.begins });
}

test "external lake chunk upload metadata transfer unwinds every output allocation failure" {
    const Fixture = struct {
        fn run(a: A, store: *stores.ArtifactStore) !void {
            var generation: u64 = 3;
            var work: u64 = 3;
            const contents = [_][]const u8{ "a", "bc" };
            var refs: [2]artifacts.ChunkRef = undefined;
            try uploadChunks(a, store, .none, .{ .generation = &generation, .work = &work }, &contents, &refs);
            defer for (refs) |ref| {
                a.free(ref.artifact_id);
                a.free(ref.checksum);
            };
            try std.testing.expectEqual(@as(u64, 0), work);
        }
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("native-chunk-output-faults");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{&store});
}
