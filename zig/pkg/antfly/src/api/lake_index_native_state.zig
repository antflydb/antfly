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

//! Snapshot-independent private row identities and authenticated per-file build
//! state. Public row IDs remain fenced to the current serving snapshot.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Provider = @import("lake_index_row_source.zig").Provider;
const A = std.mem.Allocator;
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const File = struct {
    id: []const u8,
    digest: [32]u8,
    key_digest: ?[32]u8 = null,
    aggregate_digest: ?[32]u8 = null,
    docs: []const artifacts.ChunkRef = &.{},
    count: u64 = 0,
    key_width: u16 = 96,
};
fn part(hash: *std.crypto.hash.Blake3, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}
pub fn recipe(a: A, table: local.common_topology_records.TableRecord, config: []const u8, columns: ?[]const []const u8) ![32]u8 {
    const contract = try @import("lake_index_dependencies.zig").project(a, table.schema_json, columns);
    defer a.free(contract);
    var hash = std.crypto.hash.Blake3.init(.{});
    part(&hash, "native-lake-producer-schema-v4-dependencies-signed-datetime");
    part(&hash, contract);
    part(&hash, config);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn identity(a: A, provider: *Provider, input_file: local.serverless_external_source_types.FileEntry) ![32]u8 {
    if (provider.native_delta) |delta| {
        if (!std.mem.eql(u8, delta.source, provider.source.inventory.source_id) or !std.mem.eql(u8, delta.schema, provider.source.inventory.schema_fingerprint)) return error.ExternalLakeSnapshotMismatch;
        const index = delta.by_id.get(input_file.file_id) orelse return error.ExternalLakeSnapshotMismatch;
        return (try delta.file(index)).digest;
    }
    // Warm immutable plans retain an authenticated publication instead of
    // materializing every provider version into the fresh manifest inventory.
    // Private row keys must use those same versions as the index builder.
    var file = input_file;
    if (provider.source.verified_files) |files| {
        const verified = files.get(input_file.file_id) orelse return error.ExternalLakeSnapshotMismatch;
        if (!std.mem.eql(u8, verified.object_uri, input_file.object_uri) or verified.byte_len != input_file.byte_len) return error.ExternalLakeSnapshotMismatch;
        // Selectors come from coverage evidence; sequence/partition metadata
        // belongs to this immutable source snapshot, not a prior inventory.
        file.etag = verified.etag;
        file.version_id = verified.version_id;
    }
    if (provider.source.scanner.iceberg_delete_plan != null and provider.source.prepared_deletes == null) {
        var stream = try local.serverless_query_lake_stream.Stream.init(a, provider.source, &.{}, &.{}, provider.context, provider.limits);
        defer stream.deinit();
        try stream.prepareDeletes();
    }
    const deletes: [32]u8 = fingerprint: {
        if (provider.source.prepared_deletes) |prepared| {
            if (provider.expected_delete_objects) |expected| if (!std.mem.eql(u8, &expected, &prepared.object_versions)) return error.ExternalLakeIndexSourceChanged;
            break :fingerprint try prepared.fileFingerprint(file.file_id);
        } else {
            break :fingerprint local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint();
        }
    };
    return inventoryIdentity(a, provider.source.inventory, file, deletes);
}
/// The authenticated inventory and per-file delete proof fully determine the
/// native identity. Inventory delta planning uses the same function as scans.
pub fn inventoryIdentity(a: A, inventory: local.serverless_external_source_types.Inventory, file: local.serverless_external_source_types.FileEntry, deletes: [32]u8) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    part(&hash, "native-lake-private-file-v3");
    for ([_][]const u8{ inventory.source_id, inventory.schema_fingerprint, file.file_id, file.object_uri, file.etag, file.version_id }) |bytes| part(&hash, bytes);
    // Inventory deletes are sorted by file ID. Hash only this file's slice;
    // hashing the complete delete inventory per file would be quadratic.
    const groups = inventory.deleted_row_groups;
    var lower: usize = 0;
    var upper = groups.len;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        if (std.mem.order(u8, groups[middle].file_id, file.file_id) == .lt) lower = middle + 1 else upper = middle;
    }
    var end = lower;
    while (end < groups.len and std.mem.eql(u8, groups[end].file_id, file.file_id)) end += 1;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .bytes = file.byte_len, .data_sequence = file.data_sequence_number, .partition = file.partition_spec_id, .values = file.partition_values, .deletes = groups[lower..end] }, .{});
    defer a.free(encoded);
    part(&hash, encoded);
    hash.update(&deletes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn validate(files: []const File, domain: [32]u8) !void {
    if (files.len > 16384) return error.InvalidNativeLakeFileState;
    var names: std.StringHashMapUnmanaged(void) = .empty;
    defer names.deinit(std.heap.page_allocator);
    for (files) |file| {
        if (file.key_digest) |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidNativeLakeFileState;
        if (file.key_width != 96 and file.key_width != 108) return error.InvalidNativeLakeFileState;
        if (file.id.len == 0 or std.mem.allEqual(u8, &file.digest, 0) or (try names.getOrPut(std.heap.page_allocator, file.id)).found_existing) return error.InvalidNativeLakeFileState;
        var count: u64 = 0;
        for (file.docs) |ref| {
            if (ref.byte_len == 0 or ref.byte_len % file.key_width != 0 or ref.byte_len > 256 * 1024) return error.InvalidNativeLakeFileState;
            try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeFileState;
            if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeFileState;
            count = std.math.add(u64, count, ref.byte_len / file.key_width) catch return error.InvalidNativeLakeFileState;
        }
        if (count != file.count) return error.InvalidNativeLakeFileState;
    }
}
pub fn coordinates(input_key: []const u8) !struct { group: u32, row: u64 } {
    const key = try @import("lake_enrichment_units.zig").parent(input_key);
    const parts = try local.storage_rowsource_identity.parse(key);
    return .{ .group = parts.group, .row = parts.row };
}
/// A build borrows the persistent source census and records unchanged hash
/// ranges, rather than one mutable descriptor and flag per archive file.
pub const Selection = struct {
    const Census = local.serverless_query_lake_file_census.Census;
    pub const Range = struct { first: [32]u8, last: [32]u8 };
    files: []const local.serverless_external_source_types.FileEntry,
    census: *Census,
    positions: local.serverless_query_lake_inventory_catalog.Positions,
    unchanged: std.ArrayList(Range) = .empty,
    pub fn idKey(id: []const u8) [32]u8 {
        var value: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(id, &value, .{});
        return value;
    }
    pub fn file(self: Selection, index: usize) !File {
        if (index >= self.files.len) return error.InvalidNativeLakeFileState;
        const id = self.files[index].file_id;
        const entry = self.census.find(idKey(id)) orelse return error.InvalidNativeLakeFileState;
        if (!std.mem.eql(u8, id, entry.id)) return error.InvalidNativeLakeFileState;
        return .{ .id = id, .digest = entry.digest };
    }
    pub fn finish(self: *Selection) void {
        std.mem.sort(Range, self.unchanged.items, {}, struct {
            fn less(_: void, l: Range, r: Range) bool {
                return std.mem.lessThan(u8, &l.first, &r.first);
            }
        }.less);
    }
    fn containing(self: Selection, value: [32]u8) ?Range {
        var lo: usize = 0;
        var hi = self.unchanged.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, &self.unchanged.items[mid].first, &value) != .gt) lo = mid + 1 else hi = mid;
        }
        if (lo == 0) return null;
        const range = self.unchanged.items[lo - 1];
        return if (std.mem.order(u8, &value, &range.last) != .gt) range else null;
    }
    pub fn changed(self: Selection, index: usize) bool {
        return self.containing(idKey(self.files[index].file_id)) == null;
    }
    pub const Item = struct { ordinal: usize, file: File };
    pub const Iterator = struct {
        selection: Selection,
        current: Census.Iterator,
        pub fn next(self: *Iterator) !?Item {
            while (try self.current.next()) |entry| {
                if (self.selection.containing(entry.key)) |range| {
                    self.current = self.selection.census.iteratorFrom(range.last);
                    _ = try self.current.next();
                    continue;
                }
                return .{ .ordinal = self.selection.positions.get(entry.id) orelse return error.InvalidNativeLakeFileState, .file = .{ .id = entry.id, .digest = entry.digest } };
            }
            return null;
        }
    };
    pub fn iterator(self: Selection) Iterator {
        return .{ .selection = self, .current = self.census.iterator() };
    }
};
pub fn directoryFiles(a: A, store: stores.ArtifactStore, root: @import("../serverless/graph_segment/page_tree.zig").Ref, domain: [32]u8, token: Cancellation) ![]const File {
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    var artifacts_ = store;
    var reads: u64 = 512 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = domain, .artifacts = &artifacts_, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var cursor = try tree.Cursor.init(a, pages.store(), root, "", null);
    defer cursor.deinit();
    var result: std.ArrayList(File) = .empty;
    while (try cursor.next()) |entry| {
        const file = try decodeDirectoryFile(a, entry.key, entry.value, domain);
        try result.append(a, file);
    }
    return result.toOwnedSlice(a);
}
pub fn decodeDirectoryFile(a: A, key: []const u8, bytes: []const u8, domain: [32]u8) !File {
    const file = try std.json.parseFromSliceLeaky(File, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, key, &Selection.idKey(file.id))) return error.InvalidNativeLakeFileState;
    try validate(&.{file}, domain);
    return file;
}
pub fn canUseDelta(provider: *Provider, snapshot: []const u8) bool {
    const delta = provider.native_delta orelse return false;
    if (delta.selection == null) return false;
    return if (delta.prior_snapshot) |prior| std.mem.eql(u8, snapshot, prior) else false;
}
pub const ScanOrder = struct {
    sparse: ?Selection.Iterator = null,
    count: usize,
    position: usize = 0,
    pub fn init(provider: *Provider) ScanOrder {
        var order: ScanOrder = .{ .count = provider.source.inventory.files.len };
        if (provider.native_delta) |delta| if (delta.selection) |selected| {
            var all = selected;
            all.unchanged = .empty;
            order.sparse = all.iterator();
        };
        return order;
    }
    pub fn next(self: *ScanOrder) !?usize {
        if (self.sparse) |*iterator_| return if (try iterator_.next()) |item| item.ordinal else null;
        if (self.position == self.count) return null;
        const ordinal = self.position;
        self.position += 1;
        return ordinal;
    }
};
pub const Plan = struct {
    a: A,
    files: []File = &.{},
    changed: []bool = &.{},
    selection: ?Selection = null,
    append_file: ?Selection.Item = null,
    append_count: usize = 0,
    append_provider: ?*Provider = null,
    force_all: bool = false,
    borrowed_positions: bool = false,
    key_digests: []const [32]u8 = &.{},
    row_changes: []const ?[]const u64 = &.{},
    partial_files: std.AutoHashMapUnmanaged(usize, void) = .empty,
    updates: std.AutoHashMapUnmanaged(usize, File) = .empty,
    prior_directory: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
    prior_files: []const File = &.{},
    source_delta: ?@import("lake_source_navigation.zig").Delta = null,
    by_id: local.serverless_query_lake_inventory_catalog.Positions = .empty,
    pub fn init(a: A, out: A, provider: *Provider, previous: []const File) !Plan {
        return initPaged(a, out, provider, previous, null, previous.len != 0, false);
    }
    pub fn initPaged(a: A, out: A, provider: *Provider, previous: []const File, directory: ?@import("../serverless/graph_segment/page_tree.zig").Ref, has_seed: bool, delta_compatible: bool) !Plan {
        if (provider.resume_work) |work| if (work.file) |ordinal| {
            const end = work.end orelse ordinal + 1;
            if (end <= ordinal or end != provider.source.inventory.files.len or previous.len != 0 or (if (directory) |root| root.records else 0) != ordinal) return error.InvalidNativeLakeFileState;
            const input = provider.source.inventory.files[ordinal];
            var self: Plan = .{ .a = a, .key_digests = provider.key_digests, .append_provider = provider, .by_id = work.positions orelse return error.InvalidNativeLakeFileState, .borrowed_positions = true, .append_file = .{ .ordinal = ordinal, .file = .{ .id = input.file_id, .digest = try identity(a, provider, input) } }, .append_count = end, .prior_directory = directory };
            errdefer self.deinit();
            if (self.by_id.get(input.file_id) != ordinal) return error.InvalidNativeLakeFileState;
            for (provider.source.inventory.files[ordinal..end], ordinal..) |entry, index| {
                try self.updates.put(a, index, if (index == ordinal) self.append_file.?.file else .{ .id = entry.file_id, .digest = try identity(a, provider, entry) });
            }
            return self;
        };
        if (!has_seed or delta_compatible or previous.len == 0 and directory == null) if (provider.native_delta) |delta| if (delta.selection) |selection| {
            const inventory = provider.source.inventory;
            if (!std.mem.eql(u8, delta.source, inventory.source_id) or !std.mem.eql(u8, delta.schema, inventory.schema_fingerprint) or delta.count() != inventory.files.len) return error.ExternalLakeSnapshotMismatch;
            return .{ .a = a, .key_digests = provider.key_digests, .row_changes = if (has_seed and delta_compatible) provider.changed_rows else &.{}, .selection = selection, .borrowed_positions = true, .force_all = !has_seed or directory == null and previous.len == 0, .by_id = delta.by_id, .prior_directory = directory, .prior_files = previous, .source_delta = delta };
        };
        var self: Plan = .{ .a = a, .key_digests = provider.key_digests, .row_changes = if (has_seed and delta_compatible) provider.changed_rows else &.{}, .by_id = .{ .catalog = provider.source.catalog() }, .files = try out.alloc(File, provider.source.inventory.files.len), .changed = try a.alloc(bool, provider.source.inventory.files.len) };
        errdefer self.deinit();
        var old: std.StringHashMapUnmanaged(*const File) = .empty;
        defer old.deinit(a);
        for (previous) |*file| try old.put(a, file.id, file);
        for (provider.source.inventory.files, self.files, self.changed, 0..) |file, *state, *changed, index| {
            state.* = .{ .id = file.file_id, .digest = try identity(a, provider, file), .key_digest = if (index < provider.key_digests.len) provider.key_digests[index] else null };
            changed.* = true;
            if (old.get(file.file_id)) |prior| if (std.mem.eql(u8, &prior.digest, &state.digest)) {
                state.docs = prior.docs;
                state.count = prior.count;
                state.key_width = prior.key_width;
                changed.* = false;
            };
            try self.by_id.put(a, file.file_id, index);
        }
        return self;
    }
    pub fn fromDelta(a: A, out: A, delta: @import("lake_source_navigation.zig").Delta) !Plan {
        if (delta.selection) |selection| return .{ .a = a, .selection = selection, .borrowed_positions = true, .by_id = delta.by_id };
        var self: Plan = .{ .a = a, .by_id = .{ .catalog = delta.by_id.catalog }, .files = try out.dupe(File, delta.files), .changed = try a.dupe(bool, delta.changed) };
        errdefer self.deinit();
        if (self.by_id.catalog == null) for (self.files, 0..) |file, i| try self.by_id.put(a, file.id, i);
        return self;
    }
    pub fn fromInventory(a: A, out: A, provider: *Provider, prior: local.serverless_external_source_types.Inventory) !Plan {
        const previous = try a.alloc(File, prior.files.len);
        defer a.free(previous);
        for (prior.files, previous) |file, *owned| {
            try provider.context.ensureActive();
            owned.* = .{ .id = file.file_id, .digest = try inventoryIdentity(a, prior, file, local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint()) };
        }
        return init(a, out, provider, previous);
    }
    pub fn isPaged(self: *const Plan) bool {
        return self.selection != null or self.append_file != null;
    }
    pub fn count(self: *const Plan) usize {
        if (self.append_file != null) return self.append_count;
        return if (self.selection) |selection| selection.files.len else self.files.len;
    }
    pub fn stateAt(self: *const Plan, index: usize) !File {
        var file: File = if (self.updates.get(index)) |value| value else if (self.append_file) |item| append: {
            if (index >= self.append_count) return error.InvalidNativeLakeFileState;
            if (index == item.ordinal) break :append item.file;
            const provider = self.append_provider.?;
            const input = provider.source.inventory.files[index];
            break :append .{ .id = input.file_id, .digest = try identity(self.a, provider, input) };
        } else if (self.selection) |selection| try selection.file(index) else self.files[index];
        if (index < self.key_digests.len) file.key_digest = self.key_digests[index];
        return file;
    }
    pub fn mutable(self: *Plan, index: usize) !*File {
        if (!self.isPaged()) return &self.files[index];
        const value = try self.stateAt(index);
        const slot = try self.updates.getOrPut(self.a, index);
        if (!slot.found_existing) slot.value_ptr.* = value;
        return slot.value_ptr;
    }
    pub fn isChanged(self: *const Plan, index: usize) bool {
        if (self.append_file) |item| return index >= item.ordinal and index < self.append_count;
        return self.force_all or if (self.selection) |selection| selection.changed(index) else self.changed[index];
    }
    pub const Iterator = struct {
        plan: *const Plan,
        changed_only: bool,
        position: usize = 0,
        sparse: ?Selection.Iterator = null,
        pub fn next(self: *Iterator) !?Selection.Item {
            if (self.plan.append_file) |item| {
                const ordinal = item.ordinal + self.position;
                if (ordinal == self.plan.append_count) return null;
                self.position += 1;
                return .{ .ordinal = ordinal, .file = try self.plan.stateAt(ordinal) };
            }
            if (self.sparse) |*iterator_| {
                const item = (try iterator_.next()) orelse return null;
                return .{ .ordinal = item.ordinal, .file = try self.plan.stateAt(item.ordinal) };
            }
            while (self.position < self.plan.count()) {
                const index = self.position;
                self.position += 1;
                if (self.changed_only and !self.plan.isChanged(index)) continue;
                return .{ .ordinal = index, .file = try self.plan.stateAt(index) };
            }
            return null;
        }
    };
    pub fn iterator(self: *const Plan, changed_only: bool) Iterator {
        var selection = self.selection;
        if (self.force_all and selection != null) selection.?.unchanged = .empty;
        return .{ .plan = self, .changed_only = changed_only, .sparse = if (changed_only and selection != null) selection.?.iterator() else null };
    }
    pub fn scanIterator(self: *const Plan, changed_only: bool) Iterator {
        var selected = self.selection orelse return self.iterator(changed_only);
        if (!changed_only or self.force_all) selected.unchanged = .empty;
        return .{ .plan = self, .changed_only = changed_only, .sparse = selected.iterator() };
    }
    pub fn publishDirectory(self: *Plan, a: A, store: *stores.ArtifactStore, domain: [32]u8, token: Cancellation) !?@import("../serverless/graph_segment/page_tree.zig").Ref {
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        var read_budget: u64 = 512 * 1024 * 1024;
        var write_budget: u64 = 512 * 1024 * 1024;
        var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = domain, .attempt = store.upload_scope.?.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &read_budget, .remaining_write_bytes = &write_budget, .streaming = true };
        var root = self.prior_directory;
        // Census order is the directory's hash-key order, so first publication
        // streams bottom-up instead of repeatedly rewriting the growing root.
        if (root == null and self.selection != null and self.force_all) {
            var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
            defer builder.deinit();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var it = self.iterator(true);
            while (try it.next()) |item| {
                try token.check();
                _ = scratch.reset(.retain_capacity);
                try builder.add(&Selection.idKey(item.file.id), try std.json.Stringify.valueAlloc(scratch.allocator(), item.file, .{}));
            }
            root = try builder.finish();
            if ((if (root) |ref| ref.records else 0) != self.count()) return error.InvalidNativeLakeFileState;
            return root;
        }
        var batch: @import("lake_source_navigation.zig").Batch = .{ .arena = .init(a) };
        defer batch.arena.deinit();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        // Legacy flat states migrate once; current roots path-copy only the delta.
        if (root == null) for (self.prior_files) |prior| if (self.unchanged(prior)) {
            _ = scratch.reset(.retain_capacity);
            try batch.add(&Selection.idKey(prior.id), try std.json.Stringify.valueAlloc(scratch.allocator(), prior, .{}));
            if (batch.changes.items.len >= 64) try batch.flush(pages.store(), &root);
        };
        if (self.source_delta) |delta| for (delta.prior_changed) |prior| if (!self.by_id.contains(prior.id)) {
            try batch.add(&Selection.idKey(prior.id), null);
            if (batch.changes.items.len >= 64) try batch.flush(pages.store(), &root);
        };
        try batch.flush(pages.store(), &root);
        var it = self.iterator(true);
        while (try it.next()) |item| {
            try token.check();
            _ = scratch.reset(.retain_capacity);
            try batch.add(&Selection.idKey(item.file.id), try std.json.Stringify.valueAlloc(scratch.allocator(), item.file, .{}));
            if (batch.changes.items.len >= 64) try batch.flush(pages.store(), &root);
        }
        try batch.flush(pages.store(), &root);
        if (root) |ref| if (ref.records != self.count()) return error.InvalidNativeLakeFileState;
        return root;
    }
    pub fn deinit(self: *Plan) void {
        self.a.free(self.changed);
        self.updates.deinit(self.a);
        self.partial_files.deinit(self.a);
        if (!self.borrowed_positions) self.by_id.deinit(self.a);
    }
    pub fn privateKey(self: *const Plan, a: A, ref: local.storage_rowsource_types.RowRef) ![]u8 {
        const row = switch (ref) {
            .external => |row| row,
            else => return error.SidecarSourceBindingMismatch,
        };
        const file = self.by_id.get(row.file_id) orelse return error.SidecarSourceBindingMismatch;
        const state = try self.stateAt(file);
        return std.fmt.allocPrint(a, "lake2:{s}:{x:0>8}:{x:0>16}", .{ std.fmt.bytesToHex(&(state.key_digest orelse state.digest), .lower), row.row_group_ordinal, row.row_ordinal });
    }
    pub fn enableRowDeltas(self: *Plan, provider: *Provider, snapshot: []const u8) void {
        if (provider.resume_work != null or provider.key_digests.len != self.count()) return;
        const delta = provider.native_delta orelse return;
        const prior = delta.prior_snapshot orelse return;
        if (std.mem.eql(u8, prior, snapshot)) self.row_changes = provider.changed_rows;
    }
    pub fn patchRows(self: *const Plan, index: usize) ?[]const u64 {
        if (!self.partial_files.contains(index) or index >= self.row_changes.len) return null;
        return self.row_changes[index];
    }
    fn canPatch(self: *const Plan, file: File) bool {
        const index = self.by_id.get(file.id) orelse return false;
        if (self.force_all or index >= self.row_changes.len or self.row_changes[index] == null or index >= self.key_digests.len) return false;
        const prior_key = file.key_digest orelse return false;
        return std.mem.eql(u8, &prior_key, &self.key_digests[index]);
    }
    pub fn unchanged(self: *const Plan, file: File) bool {
        if (self.append_file) |item| return (self.by_id.get(file.id) orelse return false) < item.ordinal;
        const ordinal = self.by_id.get(file.id) orelse return false;
        return !self.isChanged(ordinal) and std.mem.eql(u8, &file.digest, &(self.stateAt(ordinal) catch return false).digest);
    }
};
pub const Tracker = struct {
    plan: *Plan,
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    cancellation: Cancellation,
    refs: std.AutoHashMapUnmanaged(usize, std.ArrayList(artifacts.ChunkRef)) = .empty,
    pending: std.ArrayList(u8) = .empty,
    active: ?usize = null,
    output_bytes: u64 = 256 * 1024 * 1024,
    cumulative_bytes: ?*u64 = null,
    pub fn init(a: A, out: A, plan: *Plan, store: *stores.ArtifactStore, cancellation: Cancellation) !Tracker {
        return .{ .plan = plan, .a = a, .out = out, .store = store, .cancellation = cancellation };
    }
    pub fn deinit(self: *Tracker) void {
        var it = self.refs.valueIterator();
        while (it.next()) |refs| refs.deinit(self.a);
        self.refs.deinit(self.a);
        self.pending.deinit(self.a);
    }
    pub fn append(self: *Tracker, ref: local.storage_rowsource_types.RowRef, key: []const u8) !void {
        if (ref != .external) return error.SidecarSourceBindingMismatch;
        const file = self.plan.by_id.get(ref.external.file_id) orelse return error.SidecarSourceBindingMismatch;
        return self.appendFile(file, key);
    }
    fn appendFile(self: *Tracker, file: usize, key: []const u8) !void {
        if ((key.len != 96 and key.len != 108) or !std.mem.startsWith(u8, key, "lake2:")) return error.InvalidNativeLakeFileState;
        const state = try self.plan.mutable(file);
        if (state.count == 0) state.key_width = @intCast(key.len);
        if (state.key_width != key.len) return error.InvalidNativeLakeFileState;
        if (self.active != null and self.active.? != file) try self.flush();
        self.active = file;
        try self.pending.appendSlice(self.a, key);
        state.count += 1;
        if (self.pending.items.len >= 96 * 2048) try self.flush();
    }
    fn keepChunk(self: *Tracker, file_index: usize, reference: artifacts.ChunkRef, width: u16) !void {
        try self.flush();
        const file = try self.plan.mutable(file_index);
        if (file.count != 0 and file.key_width != width) return error.InvalidNativeLakeFileState;
        file.key_width = width;
        const slot = try self.refs.getOrPut(self.a, file_index);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        try slot.value_ptr.append(self.a, .{ .artifact_id = try self.out.dupe(u8, reference.artifact_id), .checksum = try self.out.dupe(u8, reference.checksum), .byte_len = reference.byte_len });
        file.count += reference.byte_len / width;
    }
    fn keepKey(self: *Tracker, file: File, key: []const u8) !void {
        try self.appendFile(self.plan.by_id.get(file.id) orelse return error.SidecarSourceBindingMismatch, key);
    }
    fn flush(self: *Tracker) !void {
        if (self.pending.items.len == 0) return;
        try stores.chargeReadBudget(self.cumulative_bytes orelse &self.output_bytes, self.pending.items.len);
        var store = self.store.*;
        store.allocator = self.out;
        const ref = try store.putWithCancellation(self.pending.items, self.cancellation);
        defer if (ref.provider_version) |version| store.allocator.free(version);
        const slot = try self.refs.getOrPut(self.a, self.active.?);
        if (!slot.found_existing) slot.value_ptr.* = .empty;
        try slot.value_ptr.append(self.a, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
        self.pending.clearRetainingCapacity();
    }
    pub fn finish(self: *Tracker) !void {
        try self.flush();
        var it = self.refs.iterator();
        while (it.next()) |slot| {
            const file = try self.plan.mutable(slot.key_ptr.*);
            file.docs = try self.out.dupe(artifacts.ChunkRef, slot.value_ptr.items);
        }
    }
};
/// Stream IDs for removed/replaced files; callers apply bounded native deletes.
pub const Deletes = struct {
    files: []const File,
    directory: ?@import("../serverless/graph_segment/page_tree.zig").Ref = null,
    domain: [32]u8 = @splat(0),
    record_arena: ?std.heap.ArenaAllocator = null,
    loaded: ?File = null,
    current: *Plan,
    tracker: ?*Tracker = null,
    file: usize = 0,
    chunk: usize = 0,
    budget: u64 = 256 * 1024 * 1024,
    key_width: usize = 96,
    pub fn deinit(self: *Deletes) void {
        if (self.record_arena) |*arena| arena.deinit();
    }
    pub fn next(self: *Deletes, a: A, store: stores.ArtifactStore, cancellation: Cancellation) !?[]const u8 {
        while (self.file < self.files.len) {
            if (self.directory) |root| if (self.loaded == null) {
                const tree = @import("../serverless/graph_segment/page_tree.zig");
                if (self.record_arena == null) self.record_arena = .init(a);
                _ = self.record_arena.?.reset(.retain_capacity);
                const owned = self.record_arena.?.allocator();
                var artifacts_ = store;
                var writes: u64 = 0;
                var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = self.domain, .artifacts = &artifacts_, .cancellation = cancellation, .remaining_read_bytes = &self.budget, .remaining_write_bytes = &writes };
                const key = Selection.idKey(self.files[self.file].id);
                var cursor = try tree.Cursor.init(owned, pages.store(), root, &key, null);
                defer cursor.deinit();
                const entry = (try cursor.next()) orelse return error.InvalidNativeLakeFileState;
                if (!std.mem.eql(u8, entry.key, &key)) return error.InvalidNativeLakeFileState;
                self.loaded = try decodeDirectoryFile(owned, entry.key, entry.value, self.domain);
            };
            const file = self.loaded orelse self.files[self.file];
            // A previously all-null vector fragment has no key chunks. Its
            // proven delta can still gather just the rows becoming non-null.
            if (self.tracker != null and !self.current.unchanged(file) and self.current.canPatch(file)) try self.current.partial_files.put(self.current.a, self.current.by_id.get(file.id).?, {});
            if (self.current.unchanged(file) or self.chunk == file.docs.len) {
                self.file += 1;
                self.chunk = 0;
                self.loaded = null;
                continue;
            }
            const ref = file.docs[self.chunk];
            self.chunk += 1;
            try stores.chargeReadBudget(&self.budget, ref.byte_len);
            const bytes = try artifacts.readArtifact(a, store, ref, cancellation, null);
            errdefer a.free(bytes);
            self.key_width = file.key_width;
            if (bytes.len % self.key_width != 0) return error.InvalidNativeLakeFileState;
            for (0..bytes.len / self.key_width) |row| {
                const key = bytes[row * self.key_width ..][0..self.key_width];
                const expected = std.fmt.bytesToHex(&(file.key_digest orelse file.digest), .lower);
                if (!std.mem.startsWith(u8, key, "lake2:") or !std.mem.eql(u8, key[6..70], &expected)) return error.InvalidNativeLakeFileState;
                _ = try coordinates(key);
            }
            if (self.tracker != null and self.current.canPatch(file)) {
                const file_index = self.current.by_id.get(file.id).?;
                try self.current.partial_files.put(self.current.a, file_index, {});
                const selected = self.current.row_changes[file_index].?;
                var retained: usize = 0;
                for (0..bytes.len / self.key_width) |row| {
                    const key = bytes[row * self.key_width ..][0..self.key_width];
                    const coordinate = try coordinates(key);
                    if (coordinate.group != 0) return error.InvalidNativeLakeFileState;
                    if (std.sort.binarySearch(u64, selected, coordinate.row, struct {
                        fn order(left: u64, right: u64) std.math.Order {
                            return std.math.order(left, right);
                        }
                    }.order) != null) continue;
                    retained += 1;
                }
                if (retained == bytes.len / self.key_width) {
                    try self.tracker.?.keepChunk(file_index, ref, file.key_width);
                    a.free(bytes);
                    continue;
                }
                var count: usize = 0;
                for (0..bytes.len / self.key_width) |row| {
                    const key = bytes[row * self.key_width ..][0..self.key_width];
                    const coordinate = try coordinates(key);
                    if (coordinate.group != 0) return error.InvalidNativeLakeFileState;
                    if (std.sort.binarySearch(u64, selected, coordinate.row, struct {
                        fn order(left: u64, right: u64) std.math.Order {
                            return std.math.order(left, right);
                        }
                    }.order) != null) {
                        std.mem.copyForwards(u8, bytes[count * self.key_width ..][0..self.key_width], key);
                        count += 1;
                    } else try self.tracker.?.keepKey(file, key);
                }
                return try a.realloc(bytes, count * self.key_width);
            }
            return bytes;
        }
        return null;
    }
};

test "external lake native row deltas retain untouched vector keys and immutable chunks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-row-delta");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var current = [_]File{.{ .id = "fragment", .digest = @splat(2), .key_digest = @splat(9) }};
    var changed = [_]bool{true};
    var plan: Plan = .{ .a = a, .files = &current, .changed = &changed, .key_digests = &.{@splat(9)}, .row_changes = &.{&.{3}} };
    defer plan.deinit();
    // changed is borrowed for this scenario, unlike a normal initialized plan.
    defer plan.changed = &.{};
    try plan.by_id.put(a, "fragment", 0);
    var docs: [8][]const u8 = undefined;
    for (&docs, 0..) |*key, row| key.* = try plan.privateKey(ca, .{ .external = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "fragment", .row_group_ordinal = 0, .row_ordinal = row } });
    const first_bytes = try std.mem.concat(ca, u8, docs[0..4]);
    const last_bytes = try std.mem.concat(ca, u8, docs[4..8]);
    const first = try store.putWithCancellation(first_bytes, .none);
    defer {
        a.free(first.artifact_id);
        a.free(first.checksum);
        if (first.provider_version) |value| a.free(value);
    }
    const last = try store.putWithCancellation(last_bytes, .none);
    defer {
        a.free(last.artifact_id);
        a.free(last.checksum);
        if (last.provider_version) |value| a.free(value);
    }
    const prior: File = .{ .id = "fragment", .digest = @splat(1), .key_digest = @splat(9), .count = 8, .docs = &.{
        .{ .artifact_id = first.artifact_id, .checksum = first.checksum, .byte_len = first.byte_len },
        .{ .artifact_id = last.artifact_id, .checksum = last.checksum, .byte_len = last.byte_len },
    } };
    var tracker = try Tracker.init(a, ca, &plan, &store, .none);
    defer tracker.deinit();
    var deletes: Deletes = .{ .files = &.{prior}, .current = &plan, .tracker = &tracker };
    defer deletes.deinit();
    const removed = (try deletes.next(a, store, .none)).?;
    defer a.free(removed);
    try std.testing.expectEqualStrings(docs[3], removed);
    try std.testing.expect((try deletes.next(a, store, .none)) == null);
    try std.testing.expectEqualSlices(u64, &.{3}, plan.patchRows(0).?);
    try tracker.append(.{ .external = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "fragment", .row_group_ordinal = 0, .row_ordinal = 3 } }, docs[3]);
    try tracker.finish();
    try std.testing.expectEqual(@as(u64, 8), current[0].count);
    try validate(&current, store.upload_scope.?.domain);
    var retained = false;
    for (current[0].docs) |reference| if (std.mem.eql(u8, reference.artifact_id, last.artifact_id)) {
        retained = true;
    };
    try std.testing.expect(retained);
    var incompatible = prior;
    incompatible.key_digest = @splat(8);
    try std.testing.expect(!plan.canPatch(incompatible));
}

test "external lake incremental native file identity ignores snapshot labels and binds deletion evidence" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-file-identity");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "n", .values = &.{1} }});
    defer a.free(data);
    var uploaded = try client.putObject("antfly", "part.parquet", data, .{});
    uploaded.deinit(a);
    const schema_json = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\"}}}}", .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .expected_delete_objects = @splat(1) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var first = try Plan.init(a, ca, &provider, &.{});
    defer first.deinit();
    try std.testing.expect(first.changed[0]);
    const authenticated_file = source.inventory.files[0];
    var unresolved_file = authenticated_file;
    unresolved_file.etag = "";
    unresolved_file.version_id = @constCast("iceberg:v1:unresolved");
    try std.testing.expect(!std.mem.eql(u8, &first.files[0].digest, &try identity(a, &provider, unresolved_file)));
    var verified_files: std.StringHashMapUnmanaged(*const local.serverless_external_source_types.FileEntry) = .empty;
    defer verified_files.deinit(a);
    try verified_files.put(a, authenticated_file.file_id, &authenticated_file);
    source.verified_files = &verified_files;
    try std.testing.expectEqual(first.files[0].digest, try identity(a, &provider, unresolved_file));
    unresolved_file.object_uri = @constCast("object://antfly/conflicting.parquet");
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, identity(a, &provider, unresolved_file));
    source.verified_files = null;
    const snapshot = source.inventory.snapshot_id;
    source.inventory.snapshot_id = try a.dupe(u8, "next-snapshot");
    defer {
        a.free(source.inventory.snapshot_id);
        source.inventory.snapshot_id = snapshot;
    }
    var retained = try Plan.init(a, ca, &provider, first.files);
    defer retained.deinit();
    try std.testing.expect(!retained.changed[0]);
    var metadata_cache: @import("lake_index_search_metadata.zig").Cache = .{ .max_entries = 1 };
    defer metadata_cache.deinit();
    const metadata = try metadata_cache.acquire(@splat(1), &provider);
    defer metadata.release();
    const reused_metadata = try metadata_cache.acquire(@splat(1), &provider);
    defer reused_metadata.release();
    try std.testing.expect(metadata == reused_metadata);
    const next_metadata = try metadata_cache.acquire(@splat(2), &provider);
    defer next_metadata.release();
    try std.testing.expect(next_metadata != metadata);
    try std.testing.expectEqual(@as(usize, 1), metadata_cache.entries.count());
    provider.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, metadata_cache.acquire(@splat(1), &provider));
    provider.context.deadline_ns = null;
    provider.expected_delete_objects = @splat(2);
    var deleted = try Plan.init(a, ca, &provider, first.files);
    defer deleted.deinit();
    // A global delete-object version alone must not invalidate unrelated files.
    try std.testing.expect(!deleted.changed[0]);
    const previous_groups = source.inventory.deleted_row_groups;
    var deleted_rows = [_]u64{0};
    var deleted_groups = [_]local.serverless_external_source_types.DeletedRowGroup{.{ .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 0, .row_ordinals = &deleted_rows }};
    source.inventory.deleted_row_groups = &deleted_groups;
    defer source.inventory.deleted_row_groups = previous_groups;
    var localized = try Plan.init(a, ca, &provider, first.files);
    defer localized.deinit();
    try std.testing.expect(localized.changed[0]);
    const private = try retained.privateKey(ca, .{ .external = .{ .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .file_id = source.inventory.files[0].file_id, .row_group_ordinal = 3, .row_ordinal = 9007199254740993 } });
    const position = try coordinates(private);
    try std.testing.expectEqual(@as(u32, 3), position.group);
    try std.testing.expectEqual(@as(u64, 9007199254740993), position.row);
}

test "external lake private append directory passes the legacy inline file ceiling" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const inventory_api = local.serverless_external_source_types;
    const entries = try out.alloc(inventory_api.FileEntry, 16385);
    var positions: local.serverless_query_lake_inventory_catalog.Positions = .empty;
    defer positions.deinit(a);
    for (entries, 0..) |*entry, ordinal| {
        entry.* = .{ .file_id = try std.fmt.allocPrint(out, "file-{d}", .{ordinal}), .object_uri = @constCast("file://review/part.parquet"), .etag = @constCast("v1"), .byte_len = 1, .row_count = 0, .row_groups = &.{} };
        try positions.put(a, entry.file_id, ordinal);
    }
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.selected_catalog_view = true;
    source.inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file://review"), .snapshot_id = @constCast("cut"), .schema_fingerprint = @constCast("schema"), .files = entries };
    try source.inventory.validateAlloc(a);
    source.verified_files = null;
    source.prepared_deletes = null;
    source.scanner.iceberg_delete_plan = null;
    var work: Provider.ResumeWork = .{ .file = entries.len - 1, .positions = positions };
    var provider: Provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .resume_work = &work };
    var directory = try local.common_test_directory.TestDirectory.init("append-file-directory");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = @splat(7), .attempt = @splat(3), .artifacts = &store, .cancellation = .none, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    const Key = struct { digest: [32]u8, file: File };
    const keys = try out.alloc(Key, entries.len - 1);
    for (keys, entries[0..keys.len]) |*key, entry| key.* = .{ .digest = Selection.idKey(entry.file_id), .file = .{ .id = entry.file_id, .digest = try identity(a, &provider, entry) } };
    std.mem.sort(Key, keys, {}, struct {
        fn less(_: void, left: Key, right: Key) bool {
            return std.mem.lessThan(u8, &left.digest, &right.digest);
        }
    }.less);
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
    defer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (keys) |key| {
        _ = scratch.reset(.retain_capacity);
        try builder.add(&key.digest, try std.json.Stringify.valueAlloc(scratch.allocator(), key.file, .{}));
    }
    const prior = (try builder.finish()).?;
    var plan = try Plan.initPaged(a, out, &provider, &.{}, prior, true, false);
    defer plan.deinit();
    try std.testing.expect(plan.isPaged());
    try std.testing.expectEqual(@as(usize, 0), plan.files.len);
    var it = plan.iterator(true);
    try std.testing.expectEqual(entries.len - 1, (try it.next()).?.ordinal);
    try std.testing.expect((try it.next()) == null);
    const next = (try plan.publishDirectory(a, &store, @splat(7), .none)).?;
    try std.testing.expectEqual(@as(u64, entries.len), next.records);
    const key = Selection.idKey(entries[entries.len - 1].file_id);
    var cursor = try tree.Cursor.init(a, pages.store(), next, &key, null);
    defer cursor.deinit();
    const record = (try cursor.next()).?;
    const decoded = try decodeDirectoryFile(out, record.key, record.value, @splat(7));
    try std.testing.expectEqualStrings(entries[entries.len - 1].file_id, decoded.id);
}
