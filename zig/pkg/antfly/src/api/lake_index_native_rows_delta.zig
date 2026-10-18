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

//! Authenticated sparse slot planning for completed native ordered indexes.
const std = @import("std");
const local = @import("antfly_local_sources");
const parent = @import("lake_index_native_rows.zig");
const ordered = @import("lake_index_ordered_rows.zig");
const dictionaries = ordered.file_dictionary;
const state = @import("lake_index_native_state.zig");
const stores = @import("../serverless/artifacts/store.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const spill = local.sql_spill;
const tuples = local.storage_db_relational_index_keys;
const predicates = local.storage_db_relational_index_predicate;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Provider = @import("lake_index_row_source.zig").Provider;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Entry = struct {
    tuple: tuples.TuplePlan,
    predicate: ?predicates.Plan,
    binding: local.serverless_segment_source_binding.Binding,
    name: []const u8,
    cover: []const []const u8,
    root: ordered.Root,
    sort: spill.Sort,
    reverse: ?*ordered.ReverseBuilder = null,
    slots: std.AutoHashMapUnmanaged(usize, dictionaries.Edit) = .empty,
    edits: []dictionaries.Edit = &.{},
    retired: []const u32 = &.{},
    published: ?local.serverless_manifest_artifact_ref.ArtifactRef = null,
    arena: std.heap.ArenaAllocator,
    fn deinit(self: *Entry, a: A) void {
        self.slots.deinit(a);
        self.sort.deinit();
        self.arena.deinit();
        if (self.predicate) |*p| p.deinit();
        self.tuple.deinit();
    }
};
fn seed(a: A, scratch: A, store: *stores.ArtifactStore, binding: local.serverless_segment_source_binding.Binding, name: []const u8, digest: [32]u8, candidates: []const Declared, provider: *Provider, token: Cancellation) !?ordered.Root {
    const delta = provider.native_delta.?;
    for (candidates) |decl| {
        if (decl.artifact.kind != .ordered_row_index or !ordered.supportsBuildMetadata(decl.artifact.metadata_version) or !std.mem.eql(u8, decl.name, name)) continue;
        var previous_binding = decl.binding;
        previous_binding.snapshot_id = binding.snapshot_id;
        if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(binding, previous_binding)) continue;
        const root = try ordered.loadRootLazy(a, store.*, decl.artifact, token, null);
        const dictionary = root.file_dictionary orelse continue;
        if (root.building or !dictionary.public_names or dictionary.public_count != delta.prior_count or
            !std.mem.eql(u8, root.source, delta.source) or !std.mem.eql(u8, root.snapshot, delta.prior_snapshot.?) or
            !std.mem.eql(u8, root.snapshot, decl.binding.snapshot_id) or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain) or !std.mem.eql(u8, &root.fingerprint, &digest)) continue;
        try ordered.validateReuse(scratch, store.*, root, provider.context, token);
        return root;
    }
    return null;
}
fn planSlots(a: A, out: A, entry: *Entry, provider: *Provider, store: *stores.ArtifactStore, token: Cancellation) !void {
    const delta = provider.native_delta.?;
    const roots = entry.root.file_dictionary.?;
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 0;
    var bridge: pages.PageStore = .{ .domain = entry.root.domain, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var edits: std.AutoHashMapUnmanaged(u32, dictionaries.Edit) = .empty;
    defer edits.deinit(a);
    var retired: std.AutoHashMapUnmanaged(u32, void) = .empty;
    defer retired.deinit(a);
    var vacant: std.ArrayList(u32) = .empty;
    defer vacant.deinit(a);
    for (delta.prior_changed) |old| {
        try token.check();
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const slot = (try dictionaries.named(sa, bridge.store(), roots, old.id)) orelse return error.InvalidNativeLakeRowIndex;
        const record = try dictionaries.file(sa, bridge.store(), roots, slot);
        if (!std.mem.eql(u8, &record.fingerprint, &old.digest)) return error.InvalidNativeLakeRowIndex;
        try retired.put(a, slot, {});
        try edits.put(a, slot, .{ .slot = slot, .file = try out.dupe(u8, old.id), .fingerprint = @splat(0) });
        if (!delta.by_id.contains(old.id)) try vacant.append(a, slot);
    }
    var count = roots.count;
    var plan = try state.Plan.fromDelta(a, out, delta);
    defer plan.deinit();
    var iterator = plan.iterator(true);
    while (try iterator.next()) |item| {
        try token.check();
        _ = scratch.reset(.retain_capacity);
        const existing = try dictionaries.named(scratch.allocator(), bridge.store(), roots, item.file.id);
        const slot = existing orelse if (vacant.pop()) |available| available else allocate: {
            const next = count;
            count = std.math.add(u32, count, 1) catch return error.LakeSidecarBuildLimitExceeded;
            break :allocate next;
        };
        if (slot < roots.count) try retired.put(a, slot, {});
        const edit: dictionaries.Edit = .{ .slot = slot, .file = try out.dupe(u8, item.file.id), .fingerprint = item.file.digest };
        try entry.slots.put(a, item.ordinal, edit);
        var pending = edit;
        pending.fingerprint = @splat(0);
        try edits.put(a, slot, pending);
    }
    entry.edits = try out.alloc(dictionaries.Edit, edits.count());
    var values = edits.valueIterator();
    for (entry.edits) |*edit| edit.* = values.next().?.*;
    std.mem.sort(dictionaries.Edit, entry.edits, {}, struct {
        fn less(_: void, l: dictionaries.Edit, r: dictionaries.Edit) bool {
            return l.slot < r.slot;
        }
    }.less);
    const selected = try out.alloc(u32, retired.count());
    var slots = retired.keyIterator();
    for (selected) |*slot| slot.* = slots.next().?.*;
    std.mem.sort(u32, selected, {}, std.sort.asc(u32));
    entry.retired = selected;
}
pub fn build(a: A, out: A, provider: *Provider, store: *stores.ArtifactStore, schema: anytype, layout: anytype, definitions: anytype, reusable: []const Declared, candidates: []const Declared, token: Cancellation, manager: *spill.Manager) !?[]const Declared {
    const delta = provider.native_delta orelse return null;
    if (delta.prior_snapshot == null or delta.public_navigation == null or delta.count() != provider.source.inventory.files.len or
        !std.mem.eql(u8, delta.source, provider.source.inventory.source_id) or !std.mem.eql(u8, delta.schema, provider.source.inventory.schema_fingerprint)) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var entries: std.ArrayList(Entry) = .empty;
    defer {
        for (entries.items) |*entry| entry.deinit(a);
        entries.deinit(a);
    }
    var result: std.ArrayList(Declared) = .empty;
    // Preflight every seed before uploading or checkpointing anything. New
    // recipes and incomplete/legacy cuts keep the established reconciliation.
    for (definitions) |definition| {
        var tuple = try tuples.TuplePlan.init(a, schema, layout, definition.keys);
        var predicate: ?predicates.Plan = if (definition.where.len != 0) try predicates.Plan.init(a, schema, layout, definition.where) else null;
        var transferred = false;
        defer if (!transferred) {
            if (predicate) |*p| p.deinit();
            tuple.deinit();
        };
        const cover = try parent.coverColumns(ca, definition);
        const name = try parent.logicalName(ca, definition.name);
        const binding = try parent.bindingFor(ca, provider.source, tuple, predicate, cover);
        const reused = for (reusable) |decl| {
            if (decl.artifact.kind == .ordered_row_index and decl.artifact.metadata_version == ordered.metadata_version and std.mem.eql(u8, decl.name, name) and std.mem.eql(u8, decl.binding.index_config_hash, binding.index_config_hash)) break decl;
        } else null;
        if (reused) |decl| {
            const root = try ordered.loadRootLazy(ca, store.*, decl.artifact, token, null);
            if (!std.mem.eql(u8, root.source, binding.source_id) or !std.mem.eql(u8, root.snapshot, binding.snapshot_id) or !std.mem.eql(u8, &root.fingerprint, &parent.fingerprint(tuple, predicate, cover))) return error.InvalidNativeLakeRowIndex;
            try ordered.validateReuse(a, store.*, root, provider.context, token);
            try result.append(ca, decl);
            continue;
        }
        const root = (try seed(ca, a, store, binding, name, parent.fingerprint(tuple, predicate, cover), candidates, provider, token)) orelse return null;
        var entry: Entry = .{ .tuple = tuple, .predicate = predicate, .binding = binding, .name = name, .cover = cover, .root = root, .sort = spill.Sort.init(a, manager, &.{.{}}, 512 * 1024), .arena = .init(a) };
        transferred = true;
        errdefer entry.deinit(a);
        try planSlots(a, ca, &entry, provider, store, token);
        entry.sort.shared_runs = true;
        entry.sort.encoded_bytes = true;
        try entries.append(a, entry);
        var changed_rows: u64 = 0;
        var unknown_files: u64 = 0;
        var changed_files = entry.slots.keyIterator();
        while (changed_files.next()) |ordinal| {
            const file = provider.source.inventory.files[ordinal.*];
            changed_rows +|= file.row_count;
            if (file.row_count == 0) unknown_files += 1;
        }
        const prior_rows = if (root.page) |page| page.records else 0;
        changed_rows +|= unknown_files *| @max(1, prior_rows / @max(1, delta.prior_count));
        if (entry.slots.count() > 1 and changed_rows >= @max(1, prior_rows / 4)) return null;
    }
    var first: usize = 0;
    while (first < entries.items.len) {
        const end = @min(entries.items.len, first + 8);
        const cohort = entries.items[first..end];
        var columns: std.StringHashMapUnmanaged(void) = .empty;
        defer columns.deinit(a);
        for (cohort) |*entry| {
            entry.sort.memory_bytes = 4 * 1024 * 1024 / cohort.len;
            for (try entry.tuple.columnBindings(ca)) |column| try columns.put(a, column, {});
            for (entry.cover) |column| try columns.put(a, column, {});
            if (entry.predicate) |p| for (p.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |column| try columns.put(a, column, {});
        }
        const names = try ca.alloc([]const u8, columns.count());
        var keys = columns.keyIterator();
        for (names) |*name| name.* = keys.next().?.*;
        var binding = cohort[0].binding;
        binding.column_bindings = names;
        var plan = try state.Plan.fromDelta(a, ca, delta);
        defer plan.deinit();
        var iterator = plan.iterator(true);
        var ordinal: u64 = 0;
        while (try iterator.next()) |item| {
            try token.check();
            var input = provider.*;
            input.only_files = null;
            input.only_file = item.ordinal;
            const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, token);
            defer rows.deinit(a);
            while (try rows.next(a)) |batch| for (cohort) |*entry| {
                const edit = entry.slots.get(item.ordinal) orelse return error.InvalidNativeLakeRowIndex;
                try parent.consumeBatch(a, entry, batch, item.file.id, edit.slot, &ordinal, provider.context, token);
            };
            for (cohort) |*entry| {
                const edit = entry.slots.get(item.ordinal).?;
                const first_checkpoint = entry.published == null;
                if (first_checkpoint) for (entry.edits) |*pending| {
                    if (pending.slot == edit.slot) pending.fingerprint = edit.fingerprint;
                };
                const edits: []const dictionaries.Edit = if (first_checkpoint) entry.edits else &.{edit};
                const ref = try ordered.publishIncremental(a, ca, store, &entry.sort, entry.name, parent.fingerprint(entry.tuple, entry.predicate, entry.cover), provider.source.inventory, entry.cover, token, .{
                    .previous = entry.root,
                    .sparse_slots = true,
                    .retired_slots = if (first_checkpoint) entry.retired else &.{},
                    .dictionary_edits = edits,
                    .public_navigation = delta.public_navigation,
                });
                entry.published = ref;
                try provider.checkpoint(.{ .name = entry.name, .binding = entry.binding, .artifact = ref });
                _ = entry.arena.reset(.free_all);
                entry.root = try ordered.loadRootLazy(entry.arena.allocator(), store.*, ref, token, null);
                entry.sort.deinit();
                entry.sort = spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024 / cohort.len);
                entry.sort.shared_runs = true;
                entry.sort.encoded_bytes = true;
            }
        }
        for (cohort) |*entry| {
            const ref = entry.published orelse try ordered.publishIncremental(a, ca, store, &entry.sort, entry.name, parent.fingerprint(entry.tuple, entry.predicate, entry.cover), provider.source.inventory, entry.cover, token, .{
                .previous = entry.root,
                .sparse_slots = true,
                .retired_slots = entry.retired,
                .dictionary_edits = entry.edits,
                .public_navigation = delta.public_navigation,
            });
            try result.append(ca, .{ .name = entry.name, .binding = entry.binding, .artifact = ref });
        }
        first = end;
    }
    // Transfer only owned public declarations out of the planning arena.
    return try std.json.parseFromSliceLeaky([]const Declared, out, try std.json.Stringify.valueAlloc(ca, result.items, .{}), .{ .allocate = .alloc_always });
}
