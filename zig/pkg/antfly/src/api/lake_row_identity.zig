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
pub const metadata_version: u16 = 2;
pub const cohort_values_format: u16 = 3;
pub const max_root_bytes = 1024 * 1024;

/// Only derived map navigation uses this normalization. Source reads retain
/// their original errors, so a missing source never retries as reusable state.
pub fn derivedReadFailure(err: anyerror) anyerror {
    return switch (err) {
        error.FileNotFound, error.NotFound, error.ObjectNotFound, error.CloudGenerationMapUnavailable, error.ArtifactIntegrityMismatch, error.InvalidGraphPage => error.LakeIndexArtifactUnavailable,
        else => err,
    };
}

pub const Space = struct {
    /// Coordinates only; value evidence lives once in the input-cohort tree.
    compact: bool = false,
    source: []const u8,
    snapshot: []const u8,
    rows: u64,
    file_count: u32,
    files: ?tree.Ref,
    navigation: ?Ref = null,
    by_id: ?tree.Ref,
    by_location: ?tree.Ref,
    /// Incremental maps use stable, possibly sparse file slots. File-local
    /// membership permits deleting/replacing files without replaying all rows.
    file_slots: ?tree.Ref = null,
    by_file: ?tree.Ref = null,
    by_order: ?tree.Ref = null,
    input_versions: ?tree.Ref = null,
    /// Canonical bucket commitments for native fragment input versions.
    input_commitments: ?tree.Ref = null,
    input_recipes: []const @import("lake_row_input_recipes.zig").Recipe = &.{},
    pub fn idWidth(self: Space) usize {
        return if (self.compact) 16 else 56;
    }
    pub fn memberWidth(self: Space) usize {
        return if (self.compact) 32 else 64;
    }
    pub fn liveFiles(self: Space) u64 {
        return if (self.files) |ref| ref.records else 0;
    }
    pub fn jsonStringify(self: Space, writer: anytype) !void {
        if (self.input_commitments != null) {
            try writer.write(.{ .compact = self.compact, .source = self.source, .snapshot = self.snapshot, .rows = self.rows, .file_count = self.file_count, .files = self.files, .navigation = self.navigation, .by_id = self.by_id, .by_location = self.by_location, .file_slots = self.file_slots, .by_file = self.by_file, .by_order = self.by_order, .input_versions = self.input_versions, .input_recipes = self.input_recipes, .input_commitments = self.input_commitments });
        } else if (self.compact) {
            try writer.write(.{ .compact = true, .source = self.source, .snapshot = self.snapshot, .rows = self.rows, .file_count = self.file_count, .files = self.files, .navigation = self.navigation, .by_id = self.by_id, .by_location = self.by_location, .file_slots = self.file_slots, .by_file = self.by_file, .by_order = self.by_order, .input_versions = self.input_versions, .input_recipes = self.input_recipes });
        } else if (self.file_slots == null) {
            try writer.write(.{ .source = self.source, .snapshot = self.snapshot, .rows = self.rows, .file_count = self.file_count, .files = self.files, .navigation = self.navigation, .by_id = self.by_id, .by_location = self.by_location });
        } else if (self.input_versions == null and self.input_recipes.len == 0) {
            try writer.write(.{ .source = self.source, .snapshot = self.snapshot, .rows = self.rows, .file_count = self.file_count, .files = self.files, .navigation = self.navigation, .by_id = self.by_id, .by_location = self.by_location, .file_slots = self.file_slots, .by_file = self.by_file, .by_order = self.by_order });
        } else {
            try writer.write(.{ .source = self.source, .snapshot = self.snapshot, .rows = self.rows, .file_count = self.file_count, .files = self.files, .navigation = self.navigation, .by_id = self.by_id, .by_location = self.by_location, .file_slots = self.file_slots, .by_file = self.by_file, .by_order = self.by_order, .input_versions = self.input_versions, .input_recipes = self.input_recipes });
        }
    }
    fn validate(self: Space) !void {
        if (self.compact and ((self.file_slots == null and self.file_count != 0) or !std.meta.eql(self.by_location, self.by_file))) return error.InvalidLogicalRowIdentity;
        if (self.source.len == 0 or self.source.len > 4096 or self.snapshot.len == 0 or self.snapshot.len > 4096) return error.InvalidLogicalRowIdentity;
        inline for (.{ .{ self.files, if (self.file_slots != null) self.liveFiles() else self.file_count }, .{ self.by_id, self.rows }, .{ self.by_location, self.rows } }) |entry| {
            if (entry[0]) |ref| {
                try ref.validate();
                if (ref.records != entry[1]) return error.InvalidLogicalRowIdentity;
            } else if (entry[1] != 0) return error.InvalidLogicalRowIdentity;
        }
        if (self.file_slots) |slots| {
            try slots.validate();
            if (slots.records != self.liveFiles() or self.liveFiles() > self.file_count) return error.InvalidLogicalRowIdentity;
            inline for (.{ self.by_file, self.by_order }) |root| {
                if (root) |ref| {
                    try ref.validate();
                    if (ref.records != self.rows) return error.InvalidLogicalRowIdentity;
                } else if (self.rows != 0) return error.InvalidLogicalRowIdentity;
            }
        } else if (self.by_file != null or self.by_order != null) return error.InvalidLogicalRowIdentity;
        if (self.input_commitments) |ref| try ref.validate();
        if (self.input_recipes.len > local.metadata_lake_index_catalog.max_directory_artifacts) return error.InvalidLogicalRowIdentity;
        const inputs = try std.math.mul(u64, self.rows, @import("lake_row_input_recipes.zig").count(self.input_recipes));
        if (self.input_versions) |ref| {
            try ref.validate();
            if (ref.records != inputs or self.file_slots == null) return error.InvalidLogicalRowIdentity;
        } else if (inputs != 0) return error.InvalidLogicalRowIdentity;
    }
};
pub const Translation = struct {
    forward: tree.Ref,
    reverse: tree.Ref,
    /// Historical maps embed rank. New maps share current.by_order so a
    /// coordinate change cannot rewrite ranks for every unaffected row.
    ranked: bool = true,
    pub fn jsonStringify(self: Translation, writer: anytype) !void {
        if (self.ranked) try writer.write(.{ .forward = self.forward, .reverse = self.reverse }) else try writer.write(.{ .forward = self.forward, .reverse = self.reverse, .ranked = false });
    }
};
pub const ReusedIndex = struct {
    name: []const u8,
    space: Space,
    translation: ?Translation = null,
    pub fn jsonStringify(self: ReusedIndex, writer: anytype) !void {
        if (self.translation) |translation| try writer.write(.{ .name = self.name, .space = self.space, .translation = translation }) else try writer.write(.{ .name = self.name, .space = self.space });
    }
};
const CoordinateSpace = struct { id: u32, space: Space, translation: ?Translation = null };
const IndexBinding = struct { name: []const u8, space: u32 };
fn descriptorEqual(left: anytype, right: @TypeOf(left)) bool {
    switch (@typeInfo(@TypeOf(left))) {
        .@"struct" => |info| {
            inline for (info.field_names) |name| if (!descriptorEqual(@field(left, name), @field(right, name))) return false;
            return true;
        },
        .optional => {
            const value = left orelse return right == null;
            return descriptorEqual(value, right orelse return false);
        },
        .pointer => |info| if (info.size == .slice) {
            if (left.len != right.len) return false;
            if (left.ptr == right.ptr) return true;
            for (left, right) |l, r| if (!descriptorEqual(l, r)) return false;
            return true;
        },
        else => {},
    }
    return std.meta.eql(left, right);
}
fn sameView(left: ReusedIndex, right: ReusedIndex) bool {
    return descriptorEqual(left.space, right.space) and descriptorEqual(left.translation, right.translation);
}
const CompactSpaces = struct {
    entries: []const ReusedIndex,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginArray();
        for (self.entries, 0..) |entry, i| {
            const duplicate = for (self.entries[0..i]) |prior| {
                if (sameView(prior, entry)) break true;
            } else false;
            if (!duplicate) try writer.write(CoordinateSpace{ .id = @intCast(i), .space = entry.space, .translation = entry.translation });
        }
        try writer.endArray();
    }
};
const CompactBindings = struct {
    entries: []const ReusedIndex,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginArray();
        for (self.entries, 0..) |entry, i| {
            var first = i;
            for (self.entries[0..i], 0..) |prior, j| if (sameView(prior, entry)) {
                first = j;
                break;
            };
            try writer.write(IndexBinding{ .name = entry.name, .space = @intCast(first) });
        }
        try writer.endArray();
    }
};
/// Expand the bounded v2 wire directory once; every binding borrows the same
/// immutable space/map descriptors. V1 certificate bytes remain unchanged.
pub fn decode(a: A, encoded: []const u8) !Root {
    var root = try std.json.parseFromSliceLeaky(Root, a, encoded, .{ .allocate = .alloc_always });
    if (root.version == 1) {
        if (root.coordinate_spaces.len != 0 or root.index_bindings.len != 0) return error.InvalidLogicalRowIdentity;
        return root;
    }
    if (root.version != metadata_version or root.reused.len != 0 or root.index_bindings.len > local.metadata_lake_index_catalog.max_directory_artifacts or root.coordinate_spaces.len > root.index_bindings.len) return error.InvalidLogicalRowIdentity;
    const bindings = try a.alloc(ReusedIndex, root.index_bindings.len);
    var positions: std.AutoHashMapUnmanaged(u32, usize) = .empty;
    defer positions.deinit(a);
    for (root.coordinate_spaces, 0..) |view, i| {
        const position = try positions.getOrPut(a, view.id);
        if (position.found_existing) return error.InvalidLogicalRowIdentity;
        position.value_ptr.* = i;
    }
    for (bindings, root.index_bindings) |*binding, wire| {
        const view = root.coordinate_spaces[positions.get(wire.space) orelse return error.InvalidLogicalRowIdentity];
        binding.* = .{ .name = wire.name, .space = view.space, .translation = view.translation };
    }
    root.reused = bindings;
    return root;
}
pub const Root = struct {
    version: u16 = metadata_version,
    domain: logical.Digest,
    namespace: logical.Digest,
    values_recipe: logical.Digest,
    /// Definition signature is independent of the row-value projection recipe.
    definition: ?logical.Digest = null,
    values_format: u16 = 1,
    /// Fresh provider-qualified source cut and exact reused index closure.
    cut: logical.Digest,
    artifacts: logical.Digest = @splat(0),
    current: Space,
    /// Flattened original coordinate space of the reused index artifacts.
    /// No predecessor root or chain is followed by a query or by GC.
    indexes: ?Space = null,
    certificate: ?logical.Digest = null,
    reused: []const ReusedIndex = &.{},
    native_space: ?Space = null,
    coordinate_spaces: []const CoordinateSpace = &.{},
    index_bindings: []const IndexBinding = &.{},
    pub fn jsonStringify(self: Root, writer: anytype) !void {
        if (self.version == 2) {
            if (self.definition) |definition| {
                try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .definition = definition, .values_format = self.values_format, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate, .coordinate_spaces = CompactSpaces{ .entries = self.reused }, .index_bindings = CompactBindings{ .entries = self.reused }, .native_space = self.native_space });
                return;
            }
            try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .values_format = self.values_format, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate, .coordinate_spaces = CompactSpaces{ .entries = self.reused }, .index_bindings = CompactBindings{ .entries = self.reused }, .native_space = self.native_space });
            return;
        }
        if (self.native_space) |space| {
            try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .values_format = self.values_format, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate, .reused = self.reused, .native_space = space });
            return;
        }
        // Keep the original certificate payload for maps predating typed hashes.
        if (self.values_format == 1) {
            try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate });
        } else if (self.reused.len == 0) {
            try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .values_format = self.values_format, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate });
        } else {
            try writer.write(.{ .version = self.version, .domain = self.domain, .namespace = self.namespace, .values_recipe = self.values_recipe, .values_format = self.values_format, .cut = self.cut, .artifacts = self.artifacts, .current = self.current, .indexes = self.indexes, .certificate = self.certificate, .reused = self.reused });
        }
    }
    pub fn validate(self: Root, a: A) !void {
        if ((self.values_format != 1 and self.values_format != 2 and self.values_format != cohort_values_format) or (self.version != 1 and self.version != metadata_version) or std.mem.allEqual(u8, &self.domain, 0) or std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.values_recipe, 0) or std.mem.allEqual(u8, &self.cut, 0)) return error.InvalidLogicalRowIdentity;
        if (self.definition) |definition| if (self.version != 2 or std.mem.allEqual(u8, &definition, 0)) return error.InvalidLogicalRowIdentity;
        try self.current.validate();
        if ((self.values_format == cohort_values_format) != self.current.compact) return error.InvalidLogicalRowIdentity;
        if ((self.reused.len != 0 and self.values_format < 2) or self.reused.len > local.metadata_lake_index_catalog.max_directory_artifacts) return error.InvalidLogicalRowIdentity;
        if (self.indexes) |index| try self.validateIndexSpace(index);
        if (self.native_space) |space| {
            if (self.values_format < 2) return error.InvalidLogicalRowIdentity;
            try self.validateIndexSpace(space);
        }
        for (self.reused, 0..) |entry, i| {
            if (entry.name.len == 0) return error.InvalidLogicalRowIdentity;
            for (self.reused[0..i]) |prior| if (std.mem.eql(u8, prior.name, entry.name)) return error.InvalidLogicalRowIdentity;
            try self.validateIndexSpace(entry.space);
            if (entry.translation) |translation| {
                try translation.forward.validate();
                try translation.reverse.validate();
                if (translation.forward.records != self.current.rows or translation.reverse.records != self.current.rows or (!translation.ranked and self.current.by_order == null) or entry.space.file_slots == null or self.current.file_slots == null) return error.InvalidLogicalRowIdentity;
            }
        }
        if (self.hasRelocation()) {
            if (std.mem.allEqual(u8, &self.artifacts, 0) or self.certificate == null or !std.mem.eql(u8, &self.certificate.?, &try self.seal(a))) return error.InvalidLogicalRowIdentity;
        } else if (self.certificate != null) return error.InvalidLogicalRowIdentity;
    }
    fn validateIndexSpace(self: Root, space: Space) !void {
        try space.validate();
        if (space.compact != self.current.compact) return error.InvalidLogicalRowIdentity;
        if (space.rows != self.current.rows or !std.mem.eql(u8, space.source, self.current.source)) return error.InvalidLogicalRowIdentity;
    }
    pub fn definitionFingerprint(self: Root) logical.Digest {
        return self.definition orelse self.values_recipe;
    }
    pub fn hasRelocation(self: Root) bool {
        return self.indexes != null or self.reused.len != 0 or self.native_space != null;
    }
    pub fn reseal(self: *Root, a: A) !void {
        self.certificate = if (self.hasRelocation()) try self.seal(a) else null;
    }
    pub fn spaceForIndex(self: Root, name: []const u8) Space {
        for (self.reused) |entry| if (std.mem.eql(u8, entry.name, name)) return entry.space;
        return self.indexes orelse self.native_space orelse self.current;
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
        return self.indexes orelse (if (self.reused.len != 0) self.reused[0].space else self.native_space orelse self.current);
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

pub const Spaces = struct {
    root: Root,
    position: usize = 0,
    translation: ?Translation = null,
    pub fn next(self: *Spaces) ?Space {
        while (true) {
            const position = self.position;
            self.position += 1;
            self.translation = null;
            if (position == 0) return self.root.current;
            if (position == 1) {
                if (self.root.indexes) |space| if (!descriptorEqual(space, self.root.current)) return space;
                continue;
            }
            if (position == 2) {
                if (self.root.native_space) |space| {
                    if (descriptorEqual(space, self.root.current)) continue;
                    if (self.root.indexes) |indexes| if (descriptorEqual(space, indexes)) continue;
                    return space;
                }
                continue;
            }
            if (position - 3 < self.root.reused.len) {
                const entry = self.root.reused[position - 3];
                const duplicate = for (self.root.reused[0 .. position - 3]) |prior| {
                    if (sameView(prior, entry)) break true;
                } else false;
                if (duplicate) continue;
                if (entry.translation == null) {
                    if (descriptorEqual(entry.space, self.root.current)) continue;
                    if (self.root.indexes) |space| if (descriptorEqual(entry.space, space)) continue;
                    if (self.root.native_space) |space| if (descriptorEqual(entry.space, space)) continue;
                }
                self.translation = entry.translation;
                return entry.space;
            }
            return null;
        }
    }
};
pub const PageDependencies = struct {
    spaces: Spaces,
    current: ?Space = null,
    translation: ?Translation = null,
    position: usize = 0,
    pub fn next(self: *PageDependencies) ?tree.Ref {
        while (true) {
            if (self.current == null) {
                self.current = self.spaces.next() orelse return null;
                self.translation = self.spaces.translation;
                self.position = 0;
            }
            const space = self.current.?;
            const roots = [_]?tree.Ref{ space.files, space.by_id, space.by_location, space.file_slots, space.by_file, space.by_order, space.input_versions, space.input_commitments, if (self.translation) |value| value.forward else null, if (self.translation) |value| value.reverse else null };
            while (self.position < roots.len) {
                const position = self.position;
                self.position += 1;
                if (roots[position]) |ref| return ref;
            }
            self.current = null;
        }
    }
};
pub const NavigationDependencies = struct {
    spaces: Spaces,
    pub fn next(self: *NavigationDependencies) ?Ref {
        while (self.spaces.next()) |space| if (space.navigation) |ref| return ref;
        return null;
    }
};
pub fn dependencies(root: Root) PageDependencies {
    return .{ .spaces = .{ .root = root } };
}
pub fn navigationDependencies(root: Root) NavigationDependencies {
    return .{ .spaces = .{ .root = root } };
}

/// Probe only the bounded navigation roots. Query failures on deeper pages
/// request repair for the exact pinned generation instead of scanning all rows
/// on every reconciliation or readiness poll.
pub fn validateRoots(backing: A, store: *stores.ArtifactStore, root: Root, token: Token) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: pages_api.PageStore = .{ .domain = root.domain, .artifacts = store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .cancellation = token };
    var seen: std.AutoHashMapUnmanaged(tree.Ref, void) = .empty;
    var scratch = std.heap.ArenaAllocator.init(backing);
    defer scratch.deinit();
    var page_dependencies = dependencies(root);
    while (page_dependencies.next()) |value| {
        if (seen.contains(value)) continue;
        try seen.put(a, value, {});
        // Further roots are checked by their actual readers, which report a
        // generation repair hint. Health polling never performs an archive walk.
        if (value.bytes > reads) return;
        _ = scratch.reset(.retain_capacity);
        var page = tree.readPage(scratch.allocator(), pages.store(), value) catch |err| return derivedReadFailure(err);
        page.deinit(scratch.allocator());
    }
    var navigation_seen: std.StringHashMapUnmanaged(void) = .empty;
    var navigation_dependencies = navigationDependencies(root);
    while (navigation_dependencies.next()) |value| {
        if (navigation_seen.contains(value.artifact_id)) continue;
        try navigation_seen.put(a, value.artifact_id, {});
        if (value.byte_len > reads) return;
        reads -= value.byte_len;
        _ = scratch.reset(.retain_capacity);
        _ = @import("lake_source_navigation.zig").load(scratch.allocator(), store.*, value, token, null) catch |err| return derivedReadFailure(err);
    }
}

pub fn orderKey(file: []const u8, group: u32, row: u64) [44]u8 {
    var result: [44]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(file, result[0..32], .{});
    std.mem.writeInt(u32, result[32..36], group, .big);
    std.mem.writeInt(u64, result[36..44], row, .big);
    return result;
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
    if (previous.values_format != next.values_format or !std.mem.eql(u8, &previous.definitionFingerprint(), &next.definitionFingerprint()) or !std.mem.eql(u8, &previous.domain, &next.domain) or !std.mem.eql(u8, &previous.namespace, &next.namespace) or !std.mem.eql(u8, &previous.values_recipe, &next.values_recipe) or previous.current.rows != next.current.rows or !std.mem.eql(u8, previous.current.source, next.current.source)) return false;
    if (next.values_format == cohort_values_format) {
        for (next.current.input_recipes) |input| if (!input.reusable) return false;
        if (!try tree.equivalent(a, pages.store(), previous.current.input_versions, next.current.input_versions, .{ .key_bytes = 64, .value_bytes = 32 })) return false;
    }
    var old = tree.Cursor.init(a, pages.store(), previous.current.by_id, "", null) catch |err| return derivedReadFailure(err);
    defer old.deinit();
    var current = tree.Cursor.init(a, pages.store(), next.current.by_id, "", null) catch |err| return derivedReadFailure(err);
    defer current.deinit();
    var seen: u64 = 0;
    while (old.next() catch |err| return derivedReadFailure(err)) |left| {
        const right = (current.next() catch |err| return derivedReadFailure(err)) orelse return error.InvalidLogicalRowIdentity;
        const before = try decodeState(previous.current, left.key, left.value);
        const after = try decodeState(next.current, right.key, right.value);
        if (!before.state.unchanged(after.state)) return false;
        seen += 1;
    }
    if ((current.next() catch |err| return derivedReadFailure(err)) != null or seen != next.current.rows) return error.InvalidLogicalRowIdentity;
    next.indexes = previous.indexSpace();
    next.artifacts = previous.artifacts;
    next.certificate = try next.seal(a);
    try next.validate(a);
    return true;
}

/// Exact ID-set proof is shared by every index-specific value proof. A
/// changed row set still selects complete physical construction.
pub fn proveIndexReuse(a: A, out: A, pages: *pages_api.PageStore, previous: Root, next: Root, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) ![]ReusedIndex {
    try previous.validate(a);
    try next.validate(a);
    if (previous.values_format != next.values_format or !std.mem.eql(u8, &previous.domain, &next.domain) or !std.mem.eql(u8, &previous.namespace, &next.namespace) or previous.current.rows != next.current.rows or !std.mem.eql(u8, previous.current.source, next.current.source)) return &.{};
    if (!(tree.equivalent(a, pages.store(), previous.current.by_id, next.current.by_id, .{ .compare_values = false, .key_bytes = 32, .value_bytes = next.current.idWidth() }) catch |err| return derivedReadFailure(err))) return &.{};
    var reusable: std.ArrayList(ReusedIndex) = .empty;
    for (next.current.input_recipes) |recipe| {
        if (!recipe.reusable) continue;
        const prior_recipe = for (previous.current.input_recipes) |candidate| {
            if (candidate.reusable and std.mem.eql(u8, candidate.name, recipe.name) and std.mem.eql(u8, &candidate.digest, &recipe.digest) and std.meta.eql(candidate.binding, recipe.binding)) break candidate;
        } else continue;
        _ = prior_recipe;
        const declaration = for (declarations) |entry| {
            if (std.mem.eql(u8, entry.name, recipe.name)) break entry;
        } else continue;
        if (!try equivalentInputs(a, pages, previous.current, next.current, recipe.digest)) continue;
        var space = previous.spaceForIndex(declaration.name);
        if (!std.mem.eql(u8, space.source, declaration.binding.source_id) or !std.mem.eql(u8, space.snapshot, declaration.binding.snapshot_id)) return error.InvalidLogicalRowIdentity;
        space.input_versions = null;
        space.input_commitments = null;
        space.input_recipes = &.{};
        try reusable.append(out, .{ .name = try out.dupe(u8, declaration.name), .space = space });
    }
    return reusable.toOwnedSlice(out);
}
fn equivalentInputs(a: A, pages: *pages_api.PageStore, previous: Space, next: Space, recipe: logical.Digest) !bool {
    var upper = recipe;
    var i: usize = upper.len;
    var have_upper = false;
    while (i != 0) {
        i -= 1;
        if (upper[i] != 255) {
            upper[i] += 1;
            have_upper = true;
            break;
        }
        upper[i] = 0;
    }
    return tree.equivalent(a, pages.store(), previous.input_versions, next.input_versions, .{ .lower = &recipe, .upper = if (have_upper) &upper else null, .key_bytes = 64, .value_bytes = 32 }) catch |err| return derivedReadFailure(err);
}

pub fn publish(a: A, store: *stores.ArtifactStore, root: Root, token: Token) !Ref {
    try root.validate(a);
    if (std.mem.allEqual(u8, &root.artifacts, 0)) return error.InvalidLogicalRowIdentity;
    if (store.upload_scope == null or !std.mem.eql(u8, &store.upload_scope.?.domain, &root.domain)) return error.InvalidLogicalRowIdentity;
    const encoded = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(encoded);
    if (encoded.len > max_root_bytes) return error.LogicalRowIdentityAdmissionDeclined;
    var upload = try store.putWithCancellation(encoded, token);
    defer upload.deinit(store.allocator);
    const id = try a.dupe(u8, upload.artifact_id);
    errdefer a.free(id);
    const checksum = try a.dupe(u8, upload.checksum);
    return .{ .kind = .doc_values, .metadata_version = root.version, .artifact_id = id, .checksum = checksum, .byte_len = upload.byte_len };
}
pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, token: Token, cached: ?artifacts.CachedRead) !Root {
    if (ref.kind != .doc_values or (ref.metadata_version != 1 and ref.metadata_version != metadata_version) or ref.byte_len > max_root_bytes) return error.InvalidLogicalRowIdentity;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
    defer a.free(bytes);
    const root = try decode(a, bytes);
    if (root.version != ref.metadata_version) return error.InvalidLogicalRowIdentity;
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidLogicalRowIdentity;
    if (!std.mem.eql(u8, &scope.domain, &root.domain)) return error.InvalidLogicalRowIdentity;
    try root.validate(a);
    if (std.mem.allEqual(u8, &root.artifacts, 0)) return error.InvalidLogicalRowIdentity;
    return root;
}

pub const Resolved = struct { state: logical.State, coordinate: Coordinate, tie_rank: u64 };
pub fn decodeState(space: Space, key: []const u8, value: []const u8) !Resolved {
    if (key.len != 32 or value.len != space.idWidth()) return error.InvalidLogicalRowIdentity;
    const resolved: Resolved = .{ .state = .{ .id = key[0..32].*, .values = if (space.compact) key[0..32].* else value[16..48].* }, .coordinate = try Coordinate.decode(value[0..16]), .tie_rank = if (space.compact) 0 else std.mem.readInt(u64, value[48..56], .big) };
    try resolved.state.validate();
    if (resolved.coordinate.file >= space.file_count or resolved.tie_rank >= space.rows) return error.InvalidLogicalRowIdentity;
    return resolved;
}

pub const Reader = struct {
    root: Root,
    pages: *pages_api.PageStore,
    cache: ?*tree.Cache = null,
    verify_values: bool = true,
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
        const value = (try self.lookup(a, space.by_id, &id, space.idWidth())) orelse return null;
        defer a.free(value);
        return try decodeState(space, &id, value);
    }
    pub fn byLocation(self: *Reader, a: A, space: Space, file_id: []const u8, group: u32, row: u64) !?logical.State {
        const coordinate: Coordinate = .{ .file = if (space.compact) try self.fileSlot(a, space, file_id) else 0, .group = group, .row = row };
        const key = locationKey(file_id, group, row);
        const compact_key = coordinate.encode();
        const value = (try self.lookup(a, space.by_location, if (space.compact) &compact_key else &key, space.memberWidth())) orelse return null;
        defer a.free(value);
        const state: logical.State = .{ .id = value[0..32].*, .values = if (space.compact) value[0..32].* else value[32..64].* };
        try state.validate();
        return state;
    }
    pub fn fileSlot(self: *Reader, a: A, space: Space, file_id: []const u8) !u32 {
        const key = @import("lake_row_identity_delta.zig").fileKey(file_id);
        const value = (try self.lookup(a, space.file_slots, &key, 4)) orelse return error.InvalidLogicalRowIdentity;
        defer a.free(value);
        const slot = std.mem.readInt(u32, value[0..4], .big);
        const name = try self.file(a, space, slot);
        defer a.free(name);
        if (!std.mem.eql(u8, name, file_id)) return error.InvalidLogicalRowIdentity;
        return slot;
    }
    /// Batch both halves of the dictionary proof: digest -> slot -> exact name.
    /// Shared ancestor pages are read once per bounded multi-seek wave.
    pub fn fileSlotsMany(self: *Reader, a: A, space: Space, names: []const []const u8, output: []u32) !void {
        if (names.len != output.len or names.len > tree.max_lookup_batch) return error.InvalidLogicalRowIdentity;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const keys = try sa.alloc([32]u8, names.len);
        const pointers = try sa.alloc([]const u8, names.len);
        for (names, keys, pointers) |name, *key, *pointer| {
            key.* = @import("lake_row_identity_delta.zig").fileKey(name);
            pointer.* = key;
        }
        const values = try self.lookupMany(sa, space.file_slots, pointers, 4);
        for (values, output) |value, *slot| slot.* = std.mem.readInt(u32, value[0..4], .big);
        const verified = try self.filesMany(sa, space, output);
        for (names, verified) |name, found| if (!std.mem.eql(u8, name, found)) return error.InvalidLogicalRowIdentity;
    }
    /// Caller owns each returned filename and the result slice.
    pub fn filesMany(self: *Reader, a: A, space: Space, slots: []const u32) ![][]const u8 {
        if (slots.len > tree.max_lookup_batch) return error.InvalidLogicalRowIdentity;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const keys = try sa.alloc([4]u8, slots.len);
        const pointers = try sa.alloc([]const u8, slots.len);
        for (slots, keys, pointers) |slot, *key, *pointer| {
            if (slot >= space.file_count) return error.InvalidLogicalRowIdentity;
            std.mem.writeInt(u32, key, slot, .big);
            pointer.* = key;
        }
        const values = tree.lookupMany(sa, self.storage(), space.files, pointers) catch |err| return derivedReadFailure(err);
        const result = try a.alloc([]const u8, slots.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |name| a.free(name);
            a.free(result);
        }
        for (values, result) |value, *name| {
            const record = value orelse return error.InvalidLogicalRowIdentity;
            if (record.value.len == 0 or record.value.len > 4096) return error.InvalidLogicalRowIdentity;
            name.* = try a.dupe(u8, record.value);
            initialized += 1;
        }
        return result;
    }
    pub const Translated = struct { coordinate: Coordinate, rank: u64 };
    pub fn translateAt(self: *Reader, a: A, translation: Translation, coordinate: Coordinate, forward: bool) !Translated {
        const key = coordinate.encode();
        const bytes = (try self.lookup(a, if (forward) translation.forward else translation.reverse, &key, if (forward and translation.ranked) 24 else 16)) orelse return error.InvalidLogicalRowIdentity;
        defer a.free(bytes);
        return .{ .coordinate = try Coordinate.decode(bytes[0..16]), .rank = if (forward and translation.ranked) std.mem.readInt(u64, bytes[16..24], .big) else std.math.maxInt(u64) };
    }
    pub fn translateDenseMany(self: *Reader, a: A, translation: Translation, coordinates: []const Coordinate, forward: bool) ![]Translated {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        const keys = try sa.alloc([16]u8, coordinates.len);
        const pointers = try sa.alloc([]const u8, coordinates.len);
        for (coordinates, keys, pointers) |coordinate, *key, *pointer| {
            key.* = coordinate.encode();
            pointer.* = key;
        }
        const values = try self.lookupMany(sa, if (forward) translation.forward else translation.reverse, pointers, if (forward and translation.ranked) 24 else 16);
        const result = try a.alloc(Translated, coordinates.len);
        errdefer a.free(result);
        for (values, result) |bytes, *value| value.* = .{ .coordinate = try Coordinate.decode(bytes[0..16]), .rank = if (forward and translation.ranked) std.mem.readInt(u64, bytes[16..24], .big) else std.math.maxInt(u64) };
        return result;
    }
    pub fn canonicalRank(self: *Reader, a: A, space: Space, resolved: Resolved) !u64 {
        const order = space.by_order orelse return resolved.tie_rank;
        const name = try self.file(a, space, resolved.coordinate.file);
        defer a.free(name);
        const key = orderKey(name, resolved.coordinate.group, resolved.coordinate.row);
        return tree.rankOf(a, self.storage(), order, &key) catch |err| return derivedReadFailure(err);
    }
    pub fn fromIndex(self: *Reader, a: A, file_id: []const u8, group: u32, row: u64) !?Resolved {
        const state = (try self.byLocation(a, self.root.indexSpace(), file_id, group, row)) orelse return null;
        const current = (try self.byId(a, self.root.current, state.id)) orelse return null;
        if (self.verify_values and !state.unchanged(current.state)) return null;
        return current;
    }
    pub fn toIndex(self: *Reader, a: A, file_id: []const u8, group: u32, row: u64) !?Resolved {
        const state = (try self.byLocation(a, self.root.current, file_id, group, row)) orelse return null;
        const index = (try self.byId(a, self.root.indexSpace(), state.id)) orelse return null;
        if (self.verify_values and !state.unchanged(index.state)) return null;
        return index;
    }
    /// Two ordered multi-seeks retain ancestor/leaf paths across the whole
    /// batch. Duplicate inputs share navigation and preserve caller order.
    pub fn translateMany(self: *Reader, backing: A, refs: []const local.storage_rowsource_types.RowRef, forward: bool) ![]Resolved {
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const from = if (forward) self.root.indexSpace() else self.root.current;
        const to = if (forward) self.root.current else self.root.indexSpace();
        const keys = try a.alloc([44]u8, refs.len);
        const pointers = try a.alloc([]const u8, refs.len);
        const file_names = try a.alloc([]const u8, refs.len);
        for (refs, file_names) |ref, *name| {
            if (ref != .external or !std.mem.eql(u8, ref.external.source_id, from.source) or !std.mem.eql(u8, ref.external.snapshot_id, from.snapshot)) return error.ExternalLakeSnapshotMismatch;
            name.* = ref.external.file_id;
        }
        const slots = try a.alloc(u32, refs.len);
        if (from.compact) try self.fileSlotsMany(a, from, file_names, slots);
        for (refs, keys, pointers, 0..) |ref, *key, *pointer, i| {
            if (from.compact) {
                key[0..16].* = (Coordinate{ .file = slots[i], .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }).encode();
                pointer.* = key[0..16];
            } else {
                key.* = locationKey(ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal);
                pointer.* = key;
            }
        }
        const states = try self.lookupMany(a, from.by_location, pointers, from.memberWidth());
        for (pointers, states) |*key, state| key.* = state[0..32];
        const locations = try self.lookupMany(a, to.by_id, pointers, to.idWidth());
        const result = try backing.alloc(Resolved, refs.len);
        errdefer backing.free(result);
        for (result, states, locations) |*target, state, location| {
            target.* = try decodeState(to, state[0..32], location);
            if (self.verify_values and !std.mem.eql(u8, if (from.compact) state[0..32] else state[32..64], &target.state.values)) return error.InvalidLogicalRowIdentity;
        }
        return result;
    }
    fn lookupMany(self: *Reader, a: A, root: ?tree.Ref, keys: []const []const u8, width: usize) ![][]const u8 {
        const result = try a.alloc([]const u8, keys.len);
        errdefer a.free(result);
        const found = tree.lookupMany(a, self.storage(), root, keys) catch |err| return derivedReadFailure(err);
        defer {
            for (found) |entry| if (entry) |value| value.deinit(a);
            a.free(found);
        }
        var initialized: usize = 0;
        errdefer for (result[0..initialized]) |value| a.free(value);
        for (found, result) |entry, *value| {
            const record = entry orelse return error.InvalidLogicalRowIdentity;
            if (record.value.len != width) return error.InvalidLogicalRowIdentity;
            value.* = try a.dupe(u8, record.value);
            initialized += 1;
        }
        return result;
    }

    pub fn publicReference(self: *Reader, a: A, id: []const u8) !?local.storage_rowsource_types.RowRef {
        const parsed = try logical.parsePublic(id);
        if (!std.mem.eql(u8, &parsed.namespace, &self.root.namespace)) return error.ExternalLakeSnapshotMismatch;
        const current = (try self.byId(a, self.root.current, parsed.id)) orelse return null;
        return try self.reference(a, self.root.current, current);
    }
    pub fn reference(self: *Reader, a: A, space: Space, resolved: Resolved) !local.storage_rowsource_types.RowRef {
        return self.coordinateReference(a, space, resolved.coordinate);
    }
    pub fn coordinateReference(self: *Reader, a: A, space: Space, coordinate: Coordinate) !local.storage_rowsource_types.RowRef {
        const source = try a.dupe(u8, space.source);
        errdefer a.free(source);
        const snapshot = try a.dupe(u8, space.snapshot);
        errdefer a.free(snapshot);
        const file_id = try self.file(a, space, coordinate.file);
        return .{ .external = .{ .identity_version = 2, .source_id = source, .snapshot_id = snapshot, .file_id = file_id, .row_group_ordinal = coordinate.group, .row_ordinal = coordinate.row } };
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
            const before = TestRows.coordinate(1, false, 4);
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
    // Exhaustive failure enumeration is quadratic in decoded leaf allocations.
    // The large fixture above owns spill/multi-page coverage; a small map has
    // the same cursor and hydration ownership boundaries for this sweep.
    const fault_old = try TestRows.build(ma, &manager, &pages, 4, false, false, "fault-old");
    var fault_next = try TestRows.build(ma, &manager, &pages, 4, true, false, "fault-new");
    try std.testing.expect(try proveReuse(ma, &pages, fault_old, &fault_next));
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Failure.run, .{ &pages, fault_next });
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

test "external lake batched relocation preserves shuffled duplicates and reverse coordinates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-row-batch");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = TestRows.check };
    defer manager.deinit();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: pages_api.PageStore = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const old = try TestRows.build(a, &manager, &pages, 257, false, false, "before");
    var current = try TestRows.build(a, &manager, &pages, 257, true, false, "after");
    try std.testing.expect(try proveReuse(a, &pages, old, &current));
    var reader: Reader = .{ .root = current, .pages = &pages };
    var refs: [7]local.storage_rowsource_types.RowRef = undefined;
    const positions = [_]usize{ 256, 0, 17, 0, 71, 256, 1 };
    for (&refs, positions) |*ref, i| {
        const c = TestRows.coordinate(i, false, 257);
        ref.* = .{ .external = .{ .source_id = old.current.source, .snapshot_id = old.current.snapshot, .file_id = TestRows.old_files[c.file], .row_group_ordinal = c.group, .row_ordinal = c.row } };
    }
    const mapped = try reader.translateMany(a, &refs, true);
    defer a.free(mapped);
    for (mapped, positions) |row, i| try std.testing.expectEqual(TestRows.coordinate(i, true, 257), row.coordinate);
}

test "external lake shared identity spaces keep thousands of declarations inside root admission" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const ref: tree.Ref = .{ .digest = @splat(128), .attempt = @splat(128), .bytes = 256, .height = 0, .records = 1 };
    const space: Space = .{ .source = "source", .snapshot = "snapshot", .rows = 1, .file_count = 1, .files = ref, .by_id = ref, .by_location = ref, .file_slots = ref, .by_file = ref, .by_order = ref };
    const bindings = try out.alloc(ReusedIndex, 1024);
    for (bindings, 0..) |*binding, i| binding.* = .{ .name = try std.fmt.allocPrint(out, "aggregate:stats{d}:rows{d}", .{ i / 64, i % 64 }), .space = space, .translation = .{ .forward = ref, .reverse = ref, .ranked = false } };
    // Independently decoded descriptors must coalesce by content as well.
    for (bindings) |*binding| {
        binding.space.source = try out.dupe(u8, space.source);
        binding.space.snapshot = try out.dupe(u8, space.snapshot);
    }
    var root: Root = .{ .domain = @splat(1), .namespace = @splat(2), .values_recipe = @splat(3), .cut = @splat(4), .artifacts = @splat(5), .values_format = 2, .current = space, .native_space = space, .reused = bindings };
    try root.reseal(a);
    try root.validate(a);
    const encoded = try std.json.Stringify.valueAlloc(out, root, .{});
    try std.testing.expect(encoded.len < 128 * 1024);
    const reopened = try decode(out, encoded);
    try reopened.validate(a);
    try std.testing.expectEqual(@as(usize, 1), reopened.coordinate_spaces.len);
    try std.testing.expectEqual(@as(usize, 1024), reopened.reused.len);
    var closure = dependencies(reopened);
    var roots: usize = 0;
    while (closure.next() != null) roots += 1;
    try std.testing.expectEqual(@as(usize, 14), roots);
    try std.testing.expect(reopened.reused[0].space.source.ptr == reopened.reused[1023].space.source.ptr);
    try std.testing.expectEqualStrings(encoded, try std.json.Stringify.valueAlloc(out, reopened, .{}));
    root.version = 1;
    try root.reseal(a);
    try root.validate(a);
    const historical = try std.json.Stringify.valueAlloc(out, root, .{});
    try std.testing.expect(historical.len > max_root_bytes);
    const old = try decode(out, historical);
    try old.validate(a);
}

test "external lake batched file dictionaries share navigation beyond the scalar cache" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("lake-batch-dictionary");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var reads: u64 = 16 * 1024 * 1024;
    var writes = reads;
    var pages: pages_api.PageStore = .{ .domain = @splat(7), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const Item = struct { digest: [32]u8, slot: u32 };
    var items: [257]Item = undefined;
    var names: [257][]const u8 = undefined;
    var slots: [257]u32 = undefined;
    var files: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
    defer files.deinit();
    for (&names, &items, &slots, 0..) |*name, *item, *slot, i| {
        name.* = try std.fmt.allocPrint(sa, "archive-file-{d:0>6}", .{i});
        item.* = .{ .digest = @import("lake_row_identity_delta.zig").fileKey(name.*), .slot = @intCast(i) };
        slot.* = @intCast(i);
        var key: [4]u8 = undefined;
        std.mem.writeInt(u32, &key, @intCast(i), .big);
        try files.add(&key, name.*);
    }
    std.mem.sort(Item, &items, {}, struct {
        fn less(_: void, l: Item, r: Item) bool {
            return std.mem.lessThan(u8, &l.digest, &r.digest);
        }
    }.less);
    var by_name: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
    defer by_name.deinit();
    for (items) |item| {
        var value: [4]u8 = undefined;
        std.mem.writeInt(u32, &value, item.slot, .big);
        try by_name.add(&item.digest, &value);
    }
    const space: Space = .{ .source = "lake", .snapshot = "cut", .rows = names.len, .file_count = names.len, .files = try files.finish(), .file_slots = try by_name.finish(), .by_id = null, .by_location = null };
    var reader: Reader = .{ .root = undefined, .pages = &pages };
    var shuffled: [256][]const u8 = undefined;
    for (&shuffled, 0..) |*name, i| name.* = names[(i * 73) % names.len];
    var output: [256]u32 = undefined;
    const before = reads;
    try reader.fileSlotsMany(a, space, &shuffled, &output);
    // Each dictionary fits one page. A scalar loop would charge 512 pages.
    try std.testing.expectEqual(before - space.files.?.bytes - space.file_slots.?.bytes, reads);
    for (output, shuffled) |slot, name| try std.testing.expectEqualStrings(names[slot], name);
    const roundtrip = try reader.filesMany(a, space, &output);
    defer {
        for (roundtrip) |name| a.free(name);
        a.free(roundtrip);
    }
    for (roundtrip, shuffled) |actual, expected| try std.testing.expectEqualStrings(expected, actual);
    try std.testing.expectError(error.InvalidLogicalRowIdentity, reader.fileSlotsMany(a, space, &.{"foreign"}, output[0..1]));
    const Fault = struct {
        fn run(out: A, resolver: *Reader, from: Space, input: []const []const u8) !void {
            var values: [3]u32 = undefined;
            try resolver.fileSlotsMany(out, from, input, &values);
            const result = try resolver.filesMany(out, from, &values);
            defer {
                for (result) |name| out.free(name);
                out.free(result);
            }
        }
    };
    pages.streaming = true;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fault.run, .{ &reader, space, shuffled[0..3] });
}
