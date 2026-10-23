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
    resuming: bool = false,
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
fn currentSeed(a: A, store: *stores.ArtifactStore, root: ordered.Root, provider: *Provider, token: Cancellation) !bool {
    const delta = provider.native_delta.?;
    const dictionary = root.file_dictionary.?;
    if (root.source_cut == null or root.checkpoint_base == null or delta.source_cut == null or delta.prior_cut == null or
        !std.mem.eql(u8, &root.source_cut.?, &delta.source_cut.?) or !std.mem.eql(u8, &root.checkpoint_base.?, &delta.prior_cut.?)) return false;
    const shared = delta.public_navigation.?;
    if (!std.mem.eql(u8, root.snapshot, provider.source.inventory.snapshot_id) or dictionary.public_count != shared.count or dictionary.public_identity_version != shared.identity_version) return false;
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 0;
    var bridge: pages.PageStore = .{ .domain = root.domain, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var planning = std.heap.ArenaAllocator.init(a);
    defer planning.deinit();
    var plan = try state.Plan.fromDelta(a, planning.allocator(), delta);
    defer plan.deinit();
    var iterator = plan.iterator(true);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var pending: u32 = 0;
    while (try iterator.next()) |item| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const slot = (try dictionaries.named(sa, bridge.store(), dictionary, item.file.id)) orelse return false;
        const record = try dictionaries.file(sa, bridge.store(), dictionary, slot);
        // A completed file is exact; a pending file has no old rows and a
        // zero fingerprint. Same-label predecessor cuts are not checkpoints.
        if (std.mem.allEqual(u8, &record.fingerprint, 0)) pending += 1 else if (!std.mem.eql(u8, &record.fingerprint, &item.file.digest)) return false;
    }
    return pending == root.pending_files;
}
const Seed = struct { root: ordered.Root, resuming: bool };
fn seed(a: A, scratch: A, store: *stores.ArtifactStore, binding: local.serverless_segment_source_binding.Binding, name: []const u8, digest: [32]u8, candidates: []const Declared, provider: *Provider, token: Cancellation) !?Seed {
    const delta = provider.native_delta.?;
    // Prefer authenticated work from this cut even when the published
    // predecessor appears first in the candidate list.
    for ([_]bool{ true, false }) |is_resume| for (candidates) |decl| {
        if (decl.artifact.kind != .ordered_row_index or !ordered.supportsBuildMetadata(decl.artifact.metadata_version) or !std.mem.eql(u8, decl.name, name)) continue;
        var previous_binding = decl.binding;
        previous_binding.snapshot_id = binding.snapshot_id;
        if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(binding, previous_binding)) continue;
        const root = try ordered.loadRootLazy(a, store.*, decl.artifact, token, null);
        const dictionary = root.file_dictionary orelse continue;
        if (root.building or !dictionary.public_names or !std.mem.eql(u8, root.source, delta.source) or
            !std.mem.eql(u8, root.snapshot, decl.binding.snapshot_id) or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain) or !std.mem.eql(u8, &root.fingerprint, &digest)) continue;
        if (is_resume) {
            if (!try currentSeed(scratch, store, root, provider, token)) continue;
        } else {
            if (root.pending_files != 0 or root.source_cut == null or delta.prior_cut == null or dictionary.public_count != delta.prior_count or !std.mem.eql(u8, root.snapshot, delta.prior_snapshot.?)) continue;
            if (!std.mem.eql(u8, &root.source_cut.?, &delta.prior_cut.?)) continue;
        }
        try ordered.validateReuse(scratch, store.*, root, provider.context, token);
        return .{ .root = root, .resuming = is_resume };
    };
    return null;
}

fn planSlots(a: A, out: A, entry: *Entry, provider: *Provider, store: *stores.ArtifactStore, token: Cancellation) !void {
    const delta = provider.native_delta.?;
    var roots = entry.root.file_dictionary.?;
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 256 * 1024 * 1024;
    var bridge: pages.PageStore = .{ .domain = entry.root.domain, .attempt = store.upload_scope.?.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    roots = try dictionaries.ensureFree(a, bridge.store(), roots, .{ .identity_version = roots.public_identity_version, .root = roots.public, .count = roots.public_count, .source = entry.root.source, .snapshot = entry.root.snapshot });
    entry.root.file_dictionary = roots;
    var holes = try @import("../serverless/graph_segment/page_tree.zig").Cursor.init(a, bridge.store(), roots.free, "", null);
    defer holes.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var edits: std.AutoHashMapUnmanaged(u32, dictionaries.Edit) = .empty;
    defer edits.deinit(a);
    var retired: std.AutoHashMapUnmanaged(u32, void) = .empty;
    defer retired.deinit(a);
    var vacant: std.ArrayList(u32) = .empty;
    defer vacant.deinit(a);
    for (if (entry.resuming) @as([]const state.File, &.{}) else delta.prior_changed) |old| {
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
        if (entry.resuming) {
            const slot = existing orelse return error.InvalidNativeLakeRowIndex;
            const record = try dictionaries.file(scratch.allocator(), bridge.store(), roots, slot);
            if (std.mem.eql(u8, &record.fingerprint, &item.file.digest)) continue;
            if (!std.mem.allEqual(u8, &record.fingerprint, 0)) return error.InvalidNativeLakeRowIndex;
        }
        const slot = existing orelse if (vacant.pop()) |available| available else allocate: {
            while (try holes.next()) |hole| {
                if (hole.key.len != 4) return error.InvalidNativeLakeRowIndex;
                const candidate = std.mem.readInt(u32, hole.key[0..4], .big);
                const record = try dictionaries.file(scratch.allocator(), bridge.store(), roots, candidate);
                // A resurrected name reserves its original slot regardless of
                // the order in which this refresh visits its changed files.
                if (!delta.by_id.contains(record.file)) break :allocate candidate;
            }
            const next = count;
            count = std.math.add(u32, count, 1) catch return error.LakeSidecarBuildLimitExceeded;
            break :allocate next;
        };
        if (!entry.resuming and slot < roots.count) try retired.put(a, slot, {});
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
/// Private append checkpoints already own their exact immutable predecessor.
/// Extend the slot/name/public trees and ordered pages for one reached file,
/// without flattening the prior dictionary or rebuilding a prefix-sized plan.
pub fn buildAppend(a: A, out: A, provider: *Provider, store: *stores.ArtifactStore, schema: anytype, layout: anytype, definitions: anytype, candidates: []const Declared, token: Cancellation, manager: *spill.Manager) !?[]const Declared {
    const work = provider.resume_work orelse return null;
    const ordinal = work.file orelse return null;
    if (ordinal == 0) return null;
    const navigation = work.navigation orelse return error.InvalidNativeLakeFileState;
    const end = work.end orelse ordinal + 1;
    if (end <= ordinal or end != provider.source.inventory.files.len or navigation.count != end) return error.InvalidNativeLakeFileState;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const result = try ca.alloc(Declared, definitions.len);
    for (definitions, result) |definition, *declaration| {
        var tuple = try tuples.TuplePlan.init(a, schema, layout, definition.keys);
        defer tuple.deinit();
        var predicate: ?predicates.Plan = if (definition.where.len != 0) try predicates.Plan.init(a, schema, layout, definition.where) else null;
        defer if (predicate) |*value| value.deinit();
        const cover = try parent.coverColumns(ca, definition);
        const name = try parent.logicalName(ca, definition.name);
        const binding = try parent.bindingFor(ca, provider.source, tuple, predicate, cover);
        const digest = parent.fingerprint(tuple, predicate, cover);
        const previous = for (candidates) |candidate| {
            if (candidate.artifact.kind != .ordered_row_index or !std.mem.eql(u8, candidate.name, name) or !work.trusts(store.*, candidate.artifact)) continue;
            const root = try ordered.loadRootLazy(ca, store.*, candidate.artifact, token, null);
            if (root.building or root.file_dictionary == null or !root.file_dictionary.?.public_names or root.file_dictionary.?.public_count != ordinal or !std.mem.eql(u8, root.source, binding.source_id) or !std.mem.eql(u8, root.snapshot, binding.snapshot_id) or !std.mem.eql(u8, &root.fingerprint, &digest)) return error.InvalidNativeLakeRowIndex;
            break root;
        } else return error.InvalidNativeLakeRowIndex;
        var entry: Entry = .{ .tuple = tuple, .predicate = predicate, .binding = binding, .name = name, .cover = cover, .root = previous, .sort = spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024), .arena = .init(a) };
        // The tuple/predicate are borrowed above; only these two owners retire here.
        defer entry.sort.deinit();
        defer entry.arena.deinit();
        entry.sort.shared_runs = true;
        entry.sort.encoded_bytes = true;
        var columns: std.StringHashMapUnmanaged(void) = .empty;
        defer columns.deinit(a);
        for (try tuple.columnBindings(ca)) |column| try columns.put(a, column, {});
        for (cover) |column| try columns.put(a, column, {});
        if (predicate) |plan| for (plan.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |column| try columns.put(a, column, {});
        var projected = binding;
        const names = try ca.alloc([]const u8, columns.count());
        var keys = columns.keyIterator();
        for (names) |*column| column.* = keys.next().?.*;
        projected.column_bindings = names;
        const edits = try ca.alloc(dictionaries.Edit, end - ordinal);
        for (edits, ordinal..) |*edit, file_ordinal| {
            var input = provider.*;
            input.only_file = file_ordinal;
            input.only_files = null;
            input.only_plan = null;
            const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, projected, token);
            defer rows.deinit(a);
            var row_ordinal: u64 = 0;
            const file = provider.source.inventory.files[file_ordinal];
            while (try rows.next(a)) |batch| try parent.consumeBatch(a, &entry, batch, file.file_id, @intCast(file_ordinal), &row_ordinal, provider.context, token);
            edit.* = .{ .slot = @intCast(file_ordinal), .file = file.file_id, .fingerprint = try state.identity(a, provider, file) };
        }
        const ref = try ordered.publishIncremental(a, ca, store, &entry.sort, name, digest, provider.source.inventory, cover, token, .{ .previous = previous, .sparse_slots = true, .retired_slots = &.{}, .dictionary_edits = edits, .public_navigation = navigation, .source_cut = work.source_cut });
        declaration.* = .{ .name = name, .binding = binding, .artifact = ref };
    }
    // Public declarations have bounded metadata; no archive-sized dictionary is owned.
    return try @import("lake_index_metadata_ownership.zig").clone([]const Declared, out, result);
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
        var predicate: ?predicates.Plan = null;
        var transferred = false;
        defer if (!transferred) {
            if (predicate) |*p| p.deinit();
            tuple.deinit();
        };
        if (definition.where.len != 0) predicate = try predicates.Plan.init(a, schema, layout, definition.where);
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
        const selected = (try seed(ca, a, store, binding, name, parent.fingerprint(tuple, predicate, cover), candidates, provider, token)) orelse return null;
        const root = selected.root;
        var entry: Entry = .{ .tuple = tuple, .predicate = predicate, .binding = binding, .name = name, .cover = cover, .root = root, .resuming = selected.resuming, .sort = spill.Sort.init(a, manager, &.{.{}}, 512 * 1024), .arena = .init(a) };
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
            const needed = for (cohort) |entry| {
                if (entry.slots.contains(item.ordinal)) break true;
            } else false;
            if (!needed) continue;
            var input = provider.*;
            input.only_files = null;
            input.only_file = item.ordinal;
            const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, token);
            defer rows.deinit(a);
            while (try rows.next(a)) |batch| for (cohort) |*entry| {
                const edit = entry.slots.get(item.ordinal) orelse continue;
                try parent.consumeBatch(a, entry, batch, item.file.id, edit.slot, &ordinal, provider.context, token);
            };
            for (cohort) |*entry| {
                const edit = entry.slots.get(item.ordinal) orelse continue;
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
                    .source_cut = delta.source_cut,
                    .checkpoint_base = delta.prior_cut,
                    .pending_files = @intCast(entry.slots.count() - 1),
                });
                entry.published = ref;
                try provider.checkpoint(.{ .name = entry.name, .binding = entry.binding, .artifact = ref });
                _ = entry.slots.remove(item.ordinal);
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
                .source_cut = delta.source_cut,
                .checkpoint_base = delta.prior_cut,
                .pending_files = 0,
            });
            try result.append(ca, .{ .name = entry.name, .binding = entry.binding, .artifact = ref });
        }
        first = end;
    }
    // Transfer only owned public declarations out of the planning arena.
    return try std.json.parseFromSliceLeaky([]const Declared, out, try std.json.Stringify.valueAlloc(ca, result.items, .{}), .{ .allocate = .alloc_always });
}
