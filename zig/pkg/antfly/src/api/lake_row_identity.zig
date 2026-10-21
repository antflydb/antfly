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

//! Authenticated, bounded logical-row state and flattened relocation proofs.
//! The current and index coordinate spaces are explicit and share one resolver.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const logical = local.storage_rowsource_identity.logical;
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_api = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Coordinate = logical.Coordinate;
pub const metadata_version: u16 = 1;
pub const max_root_bytes = 64 * 1024;

/// Only derived map navigation uses this normalization. Source reads retain
/// their original errors, so a missing source never retries as reusable state.
pub fn derivedReadFailure(err: anyerror) anyerror {
    return switch (err) {
        error.FileNotFound, error.NotFound, error.ObjectNotFound, error.CloudGenerationMapUnavailable, error.ArtifactIntegrityMismatch, error.InvalidGraphPage => error.LakeIndexArtifactUnavailable,
        else => err,
    };
}

pub const Space = struct {
    source: []const u8,
    snapshot: []const u8,
    rows: u64,
    file_count: u32,
    files: ?tree.Ref,
    navigation: ?Ref = null,
    by_id: ?tree.Ref,
    by_location: ?tree.Ref,
    fn validate(self: Space) !void {
        if (self.source.len == 0 or self.source.len > 4096 or self.snapshot.len == 0 or self.snapshot.len > 4096) return error.InvalidLogicalRowIdentity;
        inline for (.{ .{ self.files, self.file_count }, .{ self.by_id, self.rows }, .{ self.by_location, self.rows } }) |entry| {
            if (entry[0]) |ref| {
                try ref.validate();
                if (ref.records != entry[1]) return error.InvalidLogicalRowIdentity;
            } else if (entry[1] != 0) return error.InvalidLogicalRowIdentity;
        }
    }
};
pub const Root = struct {
    version: u16 = metadata_version,
    domain: logical.Digest,
    namespace: logical.Digest,
    values_recipe: logical.Digest,
    /// Fresh provider-qualified source cut and exact reused index closure.
    cut: logical.Digest,
    artifacts: logical.Digest = @splat(0),
    current: Space,
    /// Flattened original coordinate space of the reused index artifacts.
    /// No predecessor root or chain is followed by a query or by GC.
    indexes: ?Space = null,
    certificate: ?logical.Digest = null,
    pub fn validate(self: Root, a: A) !void {
        if (self.version != metadata_version or std.mem.allEqual(u8, &self.domain, 0) or std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.values_recipe, 0) or std.mem.allEqual(u8, &self.cut, 0)) return error.InvalidLogicalRowIdentity;
        try self.current.validate();
        if (self.indexes) |index| {
            try index.validate();
            if (std.mem.allEqual(u8, &self.artifacts, 0)) return error.InvalidLogicalRowIdentity;
            if (index.rows != self.current.rows or !std.mem.eql(u8, index.source, self.current.source) or self.certificate == null or !std.mem.eql(u8, &self.certificate.?, &try self.seal(a))) return error.InvalidLogicalRowIdentity;
        } else if (self.certificate != null) return error.InvalidLogicalRowIdentity;
    }
    fn seal(self: Root, a: A) !logical.Digest {
        var copy = self;
        copy.certificate = null;
        const encoded = try std.json.Stringify.valueAlloc(a, copy, .{});
        defer a.free(encoded);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.logical-row.reuse-certificate.v1");
        hash.update(encoded);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn indexSpace(self: Root) Space {
        return self.indexes orelse self.current;
    }
};

pub fn declarationDigest(a: A, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !logical.Digest {
    const sorted = try a.dupe(local.serverless_segment_sidecar_manifest.DeclaredArtifact, declarations);
    defer a.free(sorted);
    std.mem.sort(local.serverless_segment_sidecar_manifest.DeclaredArtifact, sorted, {}, struct {
        fn less(_: void, left: local.serverless_segment_sidecar_manifest.DeclaredArtifact, right: local.serverless_segment_sidecar_manifest.DeclaredArtifact) bool {
            return std.mem.order(u8, left.name, right.name) == .lt;
        }
    }.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.index-closure.v1");
    for (sorted) |declaration| {
        const bytes = try std.json.Stringify.valueAlloc(a, .{ declaration.name, declaration.artifact, declaration.binding.index_config_hash, declaration.binding.column_bindings }, .{});
        defer a.free(bytes);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, bytes.len, .big);
        hash.update(&size);
        hash.update(bytes);
    }
    var digest: logical.Digest = undefined;
    hash.final(&digest);
    return digest;
}

pub fn navigationDependencies(root: Root) [2]?Ref {
    return .{ root.current.navigation, if (root.indexes) |index| index.navigation else null };
}
pub fn dependencies(root: Root) [6]?tree.Ref {
    const index = root.indexes;
    return .{ root.current.files, root.current.by_id, root.current.by_location, if (index) |space| space.files else null, if (index) |space| space.by_id else null, if (index) |space| space.by_location else null };
}
pub fn locationKey(file: []const u8, group: u32, row: u64) [44]u8 {
    var result: [44]u8 = undefined;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.file-address.v1");
    hash.update(file);
    hash.final(result[0..32]);
    std.mem.writeInt(u32, result[32..36], group, .big);
    std.mem.writeInt(u64, result[36..44], row, .big);
    return result;
}

pub const Builder = struct {
    a: A,
    by_id: local.sql_spill.Sort,
    by_location: local.sql_spill.Sort,
    source_order: local.sql_spill.Sort,
    file_digests: [][32]u8,
    files: []const []const u8,
    count: u64 = 0,
    pub fn init(a: A, manager: *local.sql_spill.Manager, files: []const []const u8, workspace_bytes: usize, context: struct { source: []const u8, snapshot: []const u8, version: u8 = 2 }) !Builder {
        if (files.len > std.math.maxInt(u32) or workspace_bytes < 4096) return error.InvalidLogicalRowIdentity;
        for (files) |file| if (file.len == 0 or file.len > 4096) return error.InvalidLogicalRowIdentity;
        if (context.version != 1 and context.version != 2) return error.InvalidLogicalRowIdentity;
        const digests = try a.alloc([32]u8, files.len);
        // Match native text's canonical source order across pack layouts:
        // SHA-256(file ID), then physical group/row. This is independent of the
        // public snapshot alias and does not make packing define score ties.
        for (files, digests) |file, *digest| std.crypto.hash.sha2.Sha256.hash(file, digest, .{});
        return .{ .a = a, .files = files, .file_digests = digests, .by_id = .init(a, manager, &.{.{}}, workspace_bytes / 3), .by_location = .init(a, manager, &.{.{}}, workspace_bytes / 3), .source_order = .init(a, manager, &.{.{}}, workspace_bytes / 3) };
    }
    pub fn deinit(self: *Builder) void {
        self.by_id.deinit();
        self.by_location.deinit();
        self.source_order.deinit();
        self.a.free(self.file_digests);
    }
    pub fn add(self: *Builder, state: logical.State, coordinate: Coordinate) !void {
        try state.validate();
        if (coordinate.file >= self.files.len or self.count == std.math.maxInt(u64)) return error.InvalidLogicalRowIdentity;
        var order_key: [44]u8 = undefined;
        order_key[0..32].* = self.file_digests[coordinate.file];
        std.mem.writeInt(u32, order_key[32..36], coordinate.group, .big);
        std.mem.writeInt(u64, order_key[36..44], coordinate.row, .big);
        var forward: [80]u8 = undefined;
        forward[0..32].* = state.id;
        forward[32..48].* = coordinate.encode();
        forward[48..80].* = state.values;
        const key = locationKey(self.files[coordinate.file], coordinate.group, coordinate.row);
        var reverse: [64]u8 = undefined;
        reverse[0..32].* = state.id;
        reverse[32..64].* = state.values;
        try self.source_order.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &order_key })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = &forward })}, .ordinal = self.count });
        try self.by_location.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = &reverse })}, .ordinal = self.count });
        self.count += 1;
    }
    const Sorted = struct {
        sort: *local.sql_spill.Sort,
        scratch: std.heap.ArenaAllocator,
        last: [44]u8 = undefined,
        have_last: bool = false,
        width: usize,
        value_width: usize,
        count: u64 = 0,
        pub fn next(self: *Sorted) !?tree.Cursor.Record {
            _ = self.scratch.reset(.retain_capacity);
            const row = (try self.sort.next(self.scratch.allocator())) orelse return null;
            if (row.keys.len != 1 or row.values.len != 1 or row.keys[0].value != .string or row.values[0].value != .string) return error.InvalidLogicalRowIdentity;
            const key = row.keys[0].value.string;
            const value = row.values[0].value.string;
            if (key.len != self.width or value.len != self.value_width) return error.InvalidLogicalRowIdentity;
            if (self.have_last and std.mem.order(u8, self.last[0..self.width], key) != .lt) {
                if (self.width == 32 and std.mem.eql(u8, self.last[0..self.width], key)) return error.DuplicateLogicalRowKey;
                return error.InvalidLogicalRowIdentity;
            }
            @memcpy(self.last[0..self.width], key);
            self.have_last = true;
            self.count += 1;
            return .{ .key = key, .value = value };
        }
    };
    pub fn finish(self: *Builder, pages: *pages_api.PageStore, source: []const u8, snapshot: []const u8, namespace: logical.Digest, values_recipe: logical.Digest, cut: logical.Digest) !Root {
        const Files = struct {
            values: []const []const u8,
            index: u32 = 0,
            key: [4]u8 = undefined,
            pub fn next(current: *@This()) !?tree.Cursor.Record {
                if (current.index == current.values.len) return null;
                std.mem.writeInt(u32, &current.key, current.index, .big);
                const value = current.values[current.index];
                current.index += 1;
                return .{ .key = &current.key, .value = value };
            }
        };
        var files: Files = .{ .values = self.files };
        const file_root = try tree.buildSortedWithOptions(self.a, pages.store(), &files, .{ .target_bytes = tree.bulk_target_page_bytes });
        var ordered_rows: Sorted = .{ .sort = &self.source_order, .scratch = .init(self.a), .width = 44, .value_width = 80 };
        defer ordered_rows.scratch.deinit();
        while (try ordered_rows.next()) |record| {
            var value: [56]u8 = undefined;
            value[0..48].* = record.value[32..80].*;
            std.mem.writeInt(u64, value[48..56], ordered_rows.count - 1, .big);
            try self.by_id.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = record.value[0..32] })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = &value })}, .ordinal = ordered_rows.count - 1 });
        }
        var ids: Sorted = .{ .sort = &self.by_id, .scratch = .init(self.a), .width = 32, .value_width = 56 };
        defer ids.scratch.deinit();
        const id_root = try tree.buildSortedWithOptions(self.a, pages.store(), &ids, .{ .target_bytes = tree.bulk_target_page_bytes });
        var locations: Sorted = .{ .sort = &self.by_location, .scratch = .init(self.a), .width = 44, .value_width = 64 };
        defer locations.scratch.deinit();
        const location_root = try tree.buildSortedWithOptions(self.a, pages.store(), &locations, .{ .target_bytes = tree.bulk_target_page_bytes });
        if (ids.count != self.count or locations.count != self.count) return error.InvalidLogicalRowIdentity;
        const root: Root = .{ .domain = pages.domain, .namespace = namespace, .values_recipe = values_recipe, .cut = cut, .current = .{ .source = source, .snapshot = snapshot, .rows = self.count, .file_count = @intCast(self.files.len), .files = file_root, .by_id = id_root, .by_location = location_root } };
        try root.validate(self.a);
        return root;
    }
};

/// Proof construction is bounded and compares logical IDs and indexed values,
/// independent of file names, physical ordering and source snapshot labels.
/// The original index space is flattened into the next root on success.
pub fn proveReuse(a: A, pages: *pages_api.PageStore, previous: Root, next: *Root) !bool {
    try previous.validate(a);
    if (std.mem.allEqual(u8, &previous.artifacts, 0)) return error.InvalidLogicalRowIdentity;
    try next.validate(a);
    if (!std.mem.eql(u8, &previous.domain, &next.domain) or !std.mem.eql(u8, &previous.namespace, &next.namespace) or !std.mem.eql(u8, &previous.values_recipe, &next.values_recipe) or previous.current.rows != next.current.rows or !std.mem.eql(u8, previous.current.source, next.current.source)) return false;
    var old = tree.Cursor.init(a, pages.store(), previous.current.by_id, "", null) catch |err| return derivedReadFailure(err);
    defer old.deinit();
    var current = tree.Cursor.init(a, pages.store(), next.current.by_id, "", null) catch |err| return derivedReadFailure(err);
    defer current.deinit();
    var seen: u64 = 0;
    while (old.next() catch |err| return derivedReadFailure(err)) |left| {
        const right = (current.next() catch |err| return derivedReadFailure(err)) orelse return error.InvalidLogicalRowIdentity;
        if (left.key.len != 32 or right.key.len != 32 or left.value.len != 56 or right.value.len != 56) return error.InvalidLogicalRowIdentity;
        if (!std.mem.eql(u8, left.key, right.key) or !std.mem.eql(u8, left.value[16..48], right.value[16..48])) return false;
        seen += 1;
    }
    if ((current.next() catch |err| return derivedReadFailure(err)) != null or seen != next.current.rows) return error.InvalidLogicalRowIdentity;
    next.indexes = previous.indexSpace();
    next.artifacts = previous.artifacts;
    next.certificate = try next.seal(a);
    try next.validate(a);
    return true;
}

pub fn publish(a: A, store: *stores.ArtifactStore, root: Root, token: Token) !Ref {
    try root.validate(a);
    if (std.mem.allEqual(u8, &root.artifacts, 0)) return error.InvalidLogicalRowIdentity;
    if (store.upload_scope == null or !std.mem.eql(u8, &store.upload_scope.?.domain, &root.domain)) return error.InvalidLogicalRowIdentity;
    const encoded = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(encoded);
    if (encoded.len > max_root_bytes) return error.InvalidLogicalRowIdentity;
    var upload = try store.putWithCancellation(encoded, token);
    defer upload.deinit(store.allocator);
    const id = try a.dupe(u8, upload.artifact_id);
    errdefer a.free(id);
    const checksum = try a.dupe(u8, upload.checksum);
    return .{ .kind = .doc_values, .metadata_version = metadata_version, .artifact_id = id, .checksum = checksum, .byte_len = upload.byte_len };
}
pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, token: Token, cached: ?artifacts.CachedRead) !Root {
    if (ref.kind != .doc_values or ref.metadata_version != metadata_version or ref.byte_len > max_root_bytes) return error.InvalidLogicalRowIdentity;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidLogicalRowIdentity;
    if (!std.mem.eql(u8, &scope.domain, &root.domain)) return error.InvalidLogicalRowIdentity;
    try root.validate(a);
    if (std.mem.allEqual(u8, &root.artifacts, 0)) return error.InvalidLogicalRowIdentity;
    return root;
}

pub const Resolved = struct { state: logical.State, coordinate: Coordinate, tie_rank: u64 };
pub fn decodeState(space: Space, key: []const u8, value: []const u8) !Resolved {
    if (key.len != 32 or value.len != 56) return error.InvalidLogicalRowIdentity;
    const resolved: Resolved = .{ .state = .{ .id = key[0..32].*, .values = value[16..48].* }, .coordinate = try Coordinate.decode(value[0..16]), .tie_rank = std.mem.readInt(u64, value[48..56], .big) };
    try resolved.state.validate();
    if (resolved.coordinate.file >= space.file_count or resolved.tie_rank >= space.rows) return error.InvalidLogicalRowIdentity;
    return resolved;
}

pub const Reader = struct {
    root: Root,
    pages: *pages_api.PageStore,
    cache: ?*tree.Cache = null,
    fn storage(self: *Reader) tree.Store {
        return if (self.cache) |cache| cache.store() else self.pages.store();
    }
    fn lookup(self: *Reader, a: A, root: ?tree.Ref, key: []const u8, width: usize) !?[]const u8 {
        var cursor = tree.Cursor.init(a, self.storage(), root, key, null) catch |err| return derivedReadFailure(err);
        defer cursor.deinit();
        const record = (cursor.next() catch |err| return derivedReadFailure(err)) orelse return null;
        if (!std.mem.eql(u8, record.key, key)) return null;
        if (record.value.len != width) return error.InvalidLogicalRowIdentity;
        return try a.dupe(u8, record.value);
    }
    pub fn file(self: *Reader, a: A, space: Space, slot: u32) ![]const u8 {
        if (slot >= space.file_count) return error.InvalidLogicalRowIdentity;
        var key: [4]u8 = undefined;
        std.mem.writeInt(u32, &key, slot, .big);
        var cursor = tree.Cursor.init(a, self.storage(), space.files, &key, null) catch |err| return derivedReadFailure(err);
        defer cursor.deinit();
        const record = (cursor.next() catch |err| return derivedReadFailure(err)) orelse return error.InvalidLogicalRowIdentity;
        if (!std.mem.eql(u8, record.key, &key) or record.value.len == 0 or record.value.len > 4096) return error.InvalidLogicalRowIdentity;
        return a.dupe(u8, record.value);
    }
    pub fn byId(self: *Reader, a: A, space: Space, id: logical.Id) !?Resolved {
        const value = (try self.lookup(a, space.by_id, &id, 56)) orelse return null;
        defer a.free(value);
        return try decodeState(space, &id, value);
    }
    pub fn byLocation(self: *Reader, a: A, space: Space, file_id: []const u8, group: u32, row: u64) !?logical.State {
        const key = locationKey(file_id, group, row);
        const value = (try self.lookup(a, space.by_location, &key, 64)) orelse return null;
        defer a.free(value);
        const state: logical.State = .{ .id = value[0..32].*, .values = value[32..64].* };
        try state.validate();
        return state;
    }
    pub fn fromIndex(self: *Reader, a: A, file_id: []const u8, group: u32, row: u64) !?Resolved {
        const state = (try self.byLocation(a, self.root.indexSpace(), file_id, group, row)) orelse return null;
        const current = (try self.byId(a, self.root.current, state.id)) orelse return null;
        if (!state.unchanged(current.state)) return null;
        return current;
    }
    pub fn toIndex(self: *Reader, a: A, file_id: []const u8, group: u32, row: u64) !?Resolved {
        const state = (try self.byLocation(a, self.root.current, file_id, group, row)) orelse return null;
        const index = (try self.byId(a, self.root.indexSpace(), state.id)) orelse return null;
        if (!state.unchanged(index.state)) return null;
        return index;
    }
    pub fn publicReference(self: *Reader, a: A, id: []const u8) !?local.storage_rowsource_types.RowRef {
        const parsed = try logical.parsePublic(id);
        if (!std.mem.eql(u8, &parsed.namespace, &self.root.namespace)) return error.ExternalLakeSnapshotMismatch;
        const current = (try self.byId(a, self.root.current, parsed.id)) orelse return null;
        return try self.reference(a, self.root.current, current);
    }
    pub fn reference(self: *Reader, a: A, space: Space, resolved: Resolved) !local.storage_rowsource_types.RowRef {
        const source = try a.dupe(u8, space.source);
        errdefer a.free(source);
        const snapshot = try a.dupe(u8, space.snapshot);
        errdefer a.free(snapshot);
        const file_id = try self.file(a, space, resolved.coordinate.file);
        return .{ .external = .{ .identity_version = 2, .source_id = source, .snapshot_id = snapshot, .file_id = file_id, .row_group_ordinal = resolved.coordinate.group, .row_ordinal = resolved.coordinate.row } };
    }
};

const TestRows = struct {
    const namespace: logical.Namespace = .{ .incarnation = @splat(1), .adapter = .stable_key, .recipe = @splat(2) };
    const old_files = [_][]const u8{ "old-a", "old-b" };
    const new_files = [_][]const u8{ "new-a", "new-b", "new-c" };
    fn check(_: *anyopaque) !void {}
    fn state(i: usize, changed: bool) !logical.State {
        var key: [32]u8 = undefined;
        const canonical = try std.fmt.bufPrint(&key, "typed-key:{d}", .{i});
        var value: [64]u8 = undefined;
        const values = try std.fmt.bufPrint(&value, "body:{d}:{d}", .{ i, @intFromBool(changed) });
        return .{ .id = try logical.keyId(namespace, canonical), .values = logical.contentVersion(@splat(3), values) };
    }
    fn coordinate(i: usize, rewrite: bool, total: usize) Coordinate {
        if (!rewrite) return .{ .file = @intCast(i % 2), .group = 0, .row = i / 2 };
        const ordinal = (i * 37) % total;
        return .{ .file = @intCast(ordinal % 3), .group = @intCast((ordinal / 3) % 2), .row = ordinal / 6 };
    }
    fn build(a: A, manager: *local.sql_spill.Manager, pages: *pages_api.PageStore, total: usize, rewrite: bool, changed: bool, snapshot: []const u8) !Root {
        var builder = try Builder.init(a, manager, if (rewrite) &new_files else &old_files, 64 * 1024, .{ .source = "lake", .snapshot = snapshot });
        defer builder.deinit();
        for (0..total) |i| try builder.add(try state(i, changed and i == total / 2), coordinate(i, rewrite, total));
        var root = try builder.finish(pages, "lake", snapshot, namespace.digest(), @splat(3), @splat(4));
        root.artifacts = @splat(5);
        return root;
    }
};

test "external lake logical row maps spill verify value equivalence and flatten arbitrary rewrites" {
    const a = std.testing.allocator;
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = 4 * 1024 * 1024 };
    const ma = memory.allocator();
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = ma, .io = std.testing.io, .context = &marker, .checkpoint = TestRows.check, .max_bytes = 32 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("lake-logical-row-maps");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 256 * 1024 * 1024;
    var pages: pages_api.PageStore = .{ .domain = @splat(7), .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const count = 2049;
    const old = try TestRows.build(ma, &manager, &pages, count, false, false, "first");
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 2, std.testing.io);
    pages.attempt = store.upload_scope.?.attempt;
    var next = try TestRows.build(ma, &manager, &pages, count, true, false, "second");
    try std.testing.expect(try proveReuse(ma, &pages, old, &next));
    try std.testing.expect(manager.written_bytes > 0);
    const artifact = try publish(a, &store, next, .none);
    defer a.free(artifact.artifact_id);
    defer a.free(artifact.checksum);
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    const oa = output.allocator();
    const loaded = try load(oa, store, artifact, .none, null);
    var reader: Reader = .{ .root = loaded, .pages = &pages };
    for ([_]usize{ 0, 1, 17, 1024, count - 1 }) |i| {
        const before = TestRows.coordinate(i, false, count);
        const expected = TestRows.coordinate(i, true, count);
        const moved = (try reader.fromIndex(oa, TestRows.old_files[before.file], before.group, before.row)).?;
        try std.testing.expectEqual(expected, moved.coordinate);
        try std.testing.expectEqual((try TestRows.state(i, false)).id, moved.state.id);
        const reversed = (try reader.toIndex(oa, TestRows.new_files[expected.file], expected.group, expected.row)).?;
        try std.testing.expectEqual(before, reversed.coordinate);
        const public = try logical.publicId(oa, loaded.namespace, moved.state.id);
        const ref = (try reader.publicReference(oa, public)).?.external;
        try std.testing.expectEqualStrings("second", ref.snapshot_id);
        try std.testing.expectEqualStrings(TestRows.new_files[expected.file], ref.file_id);
        try std.testing.expectEqual(expected.row, ref.row_ordinal);
    }
    var third = try TestRows.build(ma, &manager, &pages, count, false, false, "third");
    try std.testing.expect(try proveReuse(ma, &pages, loaded, &third));
    try std.testing.expectEqualStrings("first", third.indexes.?.snapshot);
    try std.testing.expectEqual(old.current.by_location, third.indexes.?.by_location);
    var changed = try TestRows.build(ma, &manager, &pages, count, true, true, "changed");
    try std.testing.expect(!try proveReuse(ma, &pages, old, &changed));
    try std.testing.expect(changed.indexes == null and changed.certificate == null);
    var deleted = try TestRows.build(ma, &manager, &pages, count - 1, true, false, "deleted");
    try std.testing.expect(!try proveReuse(ma, &pages, old, &deleted));
    var corrupt = next;
    corrupt.current.snapshot = "foreign-cut";
    try std.testing.expectError(error.InvalidLogicalRowIdentity, corrupt.validate(ma));
    corrupt = next;
    corrupt.namespace = @splat(9);
    try std.testing.expectError(error.InvalidLogicalRowIdentity, corrupt.validate(ma));
    // Point resolution owns no retained scratch allocations. Exercise every
    // allocation failure in forward/reverse and public-reference hydration.
    const Failure = struct {
        fn run(allocator: A, storage: *pages_api.PageStore, root: Root) !void {
            var resolver: Reader = .{ .root = root, .pages = storage };
            const before = TestRows.coordinate(17, false, count);
            const moved = (try resolver.fromIndex(allocator, TestRows.old_files[before.file], before.group, before.row)).?;
            const reverse = (try resolver.toIndex(allocator, TestRows.new_files[moved.coordinate.file], moved.coordinate.group, moved.coordinate.row)).?;
            try std.testing.expectEqual(before, reverse.coordinate);
            const ref = try resolver.reference(allocator, root.current, moved);
            defer allocator.free(ref.external.source_id);
            defer allocator.free(ref.external.snapshot_id);
            defer allocator.free(ref.external.file_id);
        }
    };
    // Failure injection performs repeated reads; give each invocation its own
    // bounded-operation budget rather than exhausting the fixture's lifetime.
    pages.streaming = true;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Failure.run, .{ &pages, loaded });
    var canceled = std.atomic.Value(bool).init(true);
    pages.cancellation = Token.fromAtomic(&canceled);
    try std.testing.expectError(error.Canceled, reader.fromIndex(a, TestRows.old_files[0], 0, 0));
    pages.cancellation = .none;
    const missing = try pages_api.PageStore.identity(old.domain, old.current.by_id.?);
    try store.delete(&missing);
    var unproven = next;
    unproven.indexes = null;
    unproven.certificate = null;
    try std.testing.expectError(error.LakeIndexArtifactUnavailable, proveReuse(a, &pages, old, &unproven));
    try std.testing.expect(unproven.indexes == null and unproven.certificate == null);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
}

test "external lake logical row maps reject duplicate logical IDs and duplicate physical addresses" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = TestRows.check, .async_writes = false };
    defer manager.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("lake-logical-row-duplicates");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var reads: u64 = 4 * 1024 * 1024;
    var writes: u64 = 4 * 1024 * 1024;
    var pages: pages_api.PageStore = .{ .domain = @splat(7), .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    for ([_]bool{ false, true }) |same_id| {
        var builder = try Builder.init(a, &manager, &TestRows.old_files, 4096, .{ .source = "lake", .snapshot = "snapshot" });
        defer builder.deinit();
        try builder.add(try TestRows.state(0, false), .{ .file = 0, .group = 0, .row = 0 });
        try builder.add(try TestRows.state(if (same_id) 0 else 1, false), .{ .file = 0, .group = 0, .row = if (same_id) 1 else 0 });
        try std.testing.expectError(if (same_id) error.DuplicateLogicalRowKey else error.InvalidLogicalRowIdentity, builder.finish(&pages, "lake", "one", TestRows.namespace.digest(), @splat(3), @splat(4)));
    }
}
