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

//! Cohort construction of remote native ordered indexes. Bounded native spill
//! runs share one delete-aware physical scan and the common worker scheduler.
const std = @import("std");
const local = @import("antfly_local_sources");
const ordered = @import("lake_index_ordered_rows.zig");
const stores = @import("../serverless/artifacts/store.zig");
const state = @import("lake_index_native_state.zig");
const tuples = local.storage_db_relational_index_keys;
const predicates = local.storage_db_relational_index_predicate;
const spill = local.sql_spill;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const cohort_files = @import("lake_index_cohort_files.zig");
// Share the existing 4 MiB workspace across active builders. Reusable spill
// extents decouple logical merge levels from the 64-file descriptor limit.
// Small cohorts receive larger runs; fused cohorts avoid extra replay passes.
const cohort_size = 8;
const sort_workspace_bytes = 4 * 1024 * 1024;

pub fn logicalName(a: A, name: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
    return std.fmt.allocPrint(a, "sql-rows:{s}", .{std.fmt.bytesToHex(&digest, .lower)});
}
pub fn fingerprint(tuple: tuples.TuplePlan, predicate: ?predicates.Plan, cover: []const []const u8) [32]u8 {
    var result = tuple.fingerprint;
    if (predicate) |plan| plan.bindFingerprint(&result);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.native-row-index.cover.v2");
    hash.update(&result);
    for (cover) |column| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, column.len, .little);
        hash.update(&length);
        hash.update(column);
    }
    hash.final(&result);
    return result;
}
const Entry = struct {
    tuple: tuples.TuplePlan,
    predicate: ?predicates.Plan,
    binding: local.serverless_segment_source_binding.Binding,
    name: []const u8,
    cover: []const []const u8,
    sort: spill.Sort,
    changed: []const bool,
    slots: []const u32,
    delta: ordered.Delta,
    initial_build: bool = false,
    reverse: ?*ordered.ReverseBuilder = null,
    built_hashes: [][32]u8,
    retained: []bool,
    published: ?local.serverless_manifest_artifact_ref.ArtifactRef = null,
    root_arena: std.heap.ArenaAllocator,
    fn deinit(self: *Entry, a: A) void {
        a.free(self.retained);
        self.sort.deinit();
        if (self.reverse) |reverse| reverse.deinit();
        self.root_arena.deinit();
        if (self.predicate) |*predicate| predicate.deinit();
        self.tuple.deinit();
    }
};
/// The same seed proof is used by shared replay planning and cohort execution.
fn priorRoot(a: A, scratch: A, store: *stores.ArtifactStore, binding: local.serverless_segment_source_binding.Binding, name: []const u8, digest: [32]u8, candidates: []const Declared, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) !?ordered.Root {
    for (candidates) |decl| {
        if (decl.artifact.kind != .ordered_row_index or !ordered.supportsBuildMetadata(decl.artifact.metadata_version) or !std.mem.eql(u8, decl.name, name)) continue;
        var previous_binding = decl.binding;
        previous_binding.snapshot_id = binding.snapshot_id;
        if (!@import("../serverless/build/lake_rebuild.zig").bindingsEqual(binding, previous_binding)) continue;
        const root = try ordered.loadRoot(a, store.*, decl.artifact, cancellation, null);
        if (!std.mem.eql(u8, root.source, binding.source_id) or !std.mem.eql(u8, root.snapshot, decl.binding.snapshot_id) or root.file_fingerprints.len != root.files.len or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain) or !std.mem.eql(u8, &root.fingerprint, &digest)) continue;
        try ordered.validateReuse(scratch, store.*, root, context, cancellation);
        return root;
    }
    return null;
}
pub fn bindingFor(a: A, source: *local.serverless_query_lake_serving.ServingSource, tuple: tuples.TuplePlan, predicate: ?predicates.Plan, cover: []const []const u8) !local.serverless_segment_source_binding.Binding {
    return .{ .sidecar_kind = .ordered_rows, .source_kind = switch (source.inventory.format) {
        .parquet => .external_parquet,
        .iceberg => .external_iceberg,
        .lance => return error.UnsupportedExternalLakeIndex,
    }, .row_ref_kind = .external, .source_id = source.inventory.source_id, .snapshot_id = source.inventory.snapshot_id, .schema_fingerprint = source.inventory.schema_fingerprint, .index_config_hash = try std.fmt.allocPrint(a, "native-ordered-rows-v6:{s}", .{std.fmt.bytesToHex(&fingerprint(tuple, predicate, cover), .lower)}), .column_bindings = try tuple.columnBindings(a) };
}
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared) ![]const Declared {
    return buildIncremental(a, out, table, source, store, provider, cancellation, reusable, &.{});
}
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared, candidates: []const Declared) ![]const Declared {
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer parsed.deinit(a);
    const declarations = parsed.relational_indexes orelse return &.{};
    if (declarations.value.len == 0) return &.{};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const all_definitions = (try parsed.relationalIndexDefinitions(ca)).?;
    var selected_definitions: std.ArrayList(local.storage_relational_index.RelationalIndexDefinition) = .empty;
    for (all_definitions) |definition| if (provider.wantsIndex(try logicalName(ca, definition.name))) try selected_definitions.append(ca, definition);
    const definitions = selected_definitions.items;
    if (definitions.len == 0) return &.{};
    // Input cohorts share coordinates and replay, but own distinct authenticated
    // file versions. Keep the ordered fast paths within one such census.
    if (provider.fragment_views.len != 0) {
        var built: std.ArrayList(Declared) = .empty;
        errdefer built.deinit(out);
        var handled: std.StringHashMapUnmanaged(void) = .empty;
        defer handled.deinit(a);
        for (definitions) |definition| {
            const name = try logicalName(ca, definition.name);
            if (handled.contains(name)) continue;
            var selected_source = source.*;
            var selected = provider.forIndex(&selected_source, name);
            const selected_files = selected_source.inventory.files.ptr;
            var skips: std.ArrayList([]const u8) = .empty;
            try skips.appendSlice(ca, provider.skip_indexes);
            for (definitions) |candidate| {
                const candidate_name = try logicalName(ca, candidate.name);
                var candidate_source = source.*;
                _ = provider.forIndex(&candidate_source, candidate_name);
                if (candidate_source.inventory.files.ptr == selected_files) {
                    try handled.put(a, candidate_name, {});
                } else try skips.append(ca, candidate_name);
            }
            selected.skip_indexes = skips.items;
            selected.fragment_views = &.{};
            const original_cut = if (selected.resume_work) |work| work.source_cut else null;
            defer if (selected.resume_work) |work| {
                work.source_cut = original_cut;
            };
            if (original_cut != null) if (selected.resume_work) |work| for (provider.fragment_views) |view| {
                if (std.mem.eql(u8, view.name, name)) {
                    work.source_cut = view.delta.source_cut;
                    break;
                }
            };
            const cohort = try buildIncremental(a, out, table, &selected_source, store, &selected, cancellation, reusable, candidates);
            defer out.free(cohort);
            try built.appendSlice(out, cohort);
            provider.ordered_delta_plans = selected.ordered_delta_plans;
        }
        return built.toOwnedSlice(out);
    }
    const schema = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, schema);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, schema);
    defer layout.deinit();
    const io = provider.context.io orelse return error.UnsupportedSqlExecution;
    var manager: spill.Manager = .{ .alloc = a, .io = io, .context = provider, .checkpoint = check };
    defer manager.deinit();
    var schemas = @import("sql_schema_cache.zig").Cache.init(a);
    defer schemas.deinit();
    const sql_table = try schemas.resolve(io, ca, table.schema_json, table.table_id, table.name);
    const contract = try ca.alloc(local.serverless_query_lake_schema.Column, sql_table.columns.len);
    for (contract, sql_table.columns) |*column, definition| column.* = .{ .name = definition.path, .kind = if (definition.element_type == .numeric) "numeric" else @tagName(definition.type), .required = !definition.nullable };
    var input = provider.*;
    input.schema_contract = if (source.iceberg_schema) |selected| selected.columns else contract;
    if (try @import("lake_index_native_rows_delta.zig").buildAppend(a, out, &input, store, schema, &layout, definitions, candidates, cancellation, &manager)) |built| return built;
    if (try @import("lake_index_native_rows_delta.zig").build(a, out, &input, store, schema, &layout, definitions, reusable, candidates, cancellation, &manager)) |built| {
        provider.ordered_delta_plans += 1;
        return built;
    }
    // Publication normally supplies a replay shared with text/vector builders.
    // Direct callers also need one decode pipeline across ordered cohorts.
    var replay_columns: std.StringHashMapUnmanaged(void) = .empty;
    defer replay_columns.deinit(a);
    var planned_roots: ?[]?ordered.Root = null;
    var planned_files: ?[]const cohort_files.File = null;
    var replay_files: ?[]const bool = null;
    if (input.replay == null and definitions.len > cohort_size) {
        const roots = try ca.alloc(?ordered.Root, definitions.len);
        @memset(roots, null);
        const requirements = try ca.alloc(cohort_files.Requirement, definitions.len);
        const current_files = try ca.alloc(cohort_files.File, source.inventory.files.len);
        for (current_files, source.inventory.files) |*current, file| current.* = .{ .id = file.file_id, .digest = try state.identity(a, &input, file) };
        for (definitions, 0..) |definition, index| {
            var tuple = try tuples.TuplePlan.init(a, schema, &layout, definition.keys);
            defer tuple.deinit();
            var predicate: ?predicates.Plan = if (definition.where.len != 0) try predicates.Plan.init(a, schema, &layout, definition.where) else null;
            defer if (predicate) |*plan| plan.deinit();
            const cover = try coverColumns(ca, definition);
            for (try tuple.columnBindings(ca)) |column| try replay_columns.put(a, column, {});
            for (cover) |column| try replay_columns.put(a, column, {});
            if (predicate) |plan| for (plan.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |column| try replay_columns.put(a, column, {});
            const name = try logicalName(ca, definition.name);
            const binding = try bindingFor(ca, source, tuple, predicate, cover);
            const reused = for (reusable) |decl| {
                if (decl.artifact.kind == .ordered_row_index and decl.artifact.metadata_version == ordered.metadata_version and std.mem.eql(u8, decl.name, name) and std.mem.eql(u8, decl.binding.index_config_hash, binding.index_config_hash)) break true;
            } else false;
            if (!reused) roots[index] = try priorRoot(ca, a, store, binding, name, fingerprint(tuple, predicate, cover), candidates, provider.context, cancellation);
            requirements[index] = .{ .reused = reused, .previous = if (roots[index]) |root| .{ .files = root.files, .fingerprints = root.file_fingerprints } else null };
        }
        replay_files = try cohort_files.requiredFiles(ca, current_files, requirements);
        planned_roots = roots;
        planned_files = current_files;
    }
    const shared_columns = try ca.alloc([]const u8, replay_columns.count());
    var replay_keys = replay_columns.keyIterator();
    for (shared_columns) |*column| column.* = replay_keys.next().?.*;
    var replay = @import("lake_index_build_replay.zig").Replay.init(a, &input, shared_columns);
    defer replay.deinit();
    replay.only_files = if (replay_files) |mask| try @import("lake_index_build_replay.zig").Needed.fromMask(a, mask) else null;
    if (shared_columns.len != 0 and input.replay == null) input.replay = &replay;
    var files: std.StringHashMapUnmanaged(u32) = .empty;
    defer files.deinit(a);
    for (source.inventory.files, 0..) |file, index| try files.put(a, file.file_id, std.math.cast(u32, index) orelse return error.LakeSidecarBuildLimitExceeded);
    var result: std.ArrayList(Declared) = .empty;
    errdefer result.deinit(out);
    const orders = [_]local.sql_operators.Order{.{}};
    var first: usize = 0;
    while (first < definitions.len) {
        // Retained runs share one anonymous extent container per sort.
        // Pending runs and bounded merges remain within the 64-file manager.
        const end = @min(first + cohort_size, definitions.len);
        defer first = end;
        var entries: std.ArrayList(Entry) = .empty;
        defer {
            for (entries.items) |*entry| entry.deinit(a);
            entries.deinit(a);
        }
        var columns: std.StringHashMapUnmanaged(void) = .empty;
        defer columns.deinit(a);
        for (definitions[first..end], first..) |definition, definition_index| {
            var tuple = try tuples.TuplePlan.init(a, schema, &layout, definition.keys);
            errdefer tuple.deinit();
            var predicate: ?predicates.Plan = if (definition.where.len != 0) try predicates.Plan.init(a, schema, &layout, definition.where) else null;
            errdefer if (predicate) |*plan| plan.deinit();
            const paths = try tuple.columnBindings(ca);
            const cover = try coverColumns(ca, definition);
            for (cover) |column| try columns.put(a, column, {});
            for (paths) |path| try columns.put(a, path, {});
            if (predicate) |plan| for (plan.conditions) |condition| for (try condition.tuple.columnBindings(ca)) |path| try columns.put(a, path, {});
            const name = try logicalName(ca, definition.name);
            const binding = try bindingFor(ca, source, tuple, predicate, cover);
            const hash = binding.index_config_hash;
            const previous = for (reusable) |decl| {
                if (decl.artifact.kind == .ordered_row_index and decl.artifact.metadata_version == ordered.metadata_version and std.mem.eql(u8, decl.name, name) and std.mem.eql(u8, decl.binding.index_config_hash, hash)) break decl;
            } else null;
            if (previous) |decl| {
                const root = try ordered.loadRoot(ca, store.*, decl.artifact, cancellation, null);
                try ordered.validateReuse(a, store.*, root, provider.context, cancellation);
                try result.append(out, decl);
                if (predicate) |*plan| plan.deinit();
                tuple.deinit();
                continue;
            }
            const prior = if (planned_roots) |roots| roots[definition_index] else try priorRoot(ca, a, store, binding, name, fingerprint(tuple, predicate, cover), candidates, provider.context, cancellation);
            const changed = try ca.alloc(bool, source.inventory.files.len);
            const slots = try ca.alloc(u32, source.inventory.files.len);
            var slot_names: std.ArrayList([]const u8) = .empty;
            var slot_hashes: std.ArrayList([32]u8) = .empty;
            var slot_map: std.StringHashMapUnmanaged(u32) = .empty;
            defer slot_map.deinit(a);
            const keep = try ca.alloc(bool, if (prior) |root| root.files.len else 0);
            @memset(keep, false);
            if (prior) |root| {
                try slot_names.appendSlice(ca, root.files);
                try slot_hashes.appendSlice(ca, root.file_fingerprints);
                for (root.files, 0..) |file, slot| try slot_map.put(a, file, @intCast(slot));
            }
            var vacant: std.ArrayList(u32) = .empty;
            if (prior) |root| for (root.files, 0..) |file, slot| {
                if (!files.contains(file)) try vacant.append(ca, @intCast(slot));
            };
            for (source.inventory.files, changed, slots, 0..) |file, *replace, *slot, file_index| {
                const digest = if (planned_files) |planned| planned[file_index].digest else try state.identity(a, &input, file);
                const found = try slot_map.getOrPut(a, file.file_id);
                if (!found.found_existing) {
                    found.value_ptr.* = try claimSlot(ca, &slot_names, &slot_hashes, &vacant, file.file_id, digest);
                    replace.* = true;
                } else {
                    slot.* = found.value_ptr.*;
                    replace.* = !std.mem.eql(u8, &slot_hashes.items[slot.*], &digest);
                    if (!replace.*) keep[slot.*] = true;
                    slot_hashes.items[slot.*] = digest;
                }
                slot.* = found.value_ptr.*;
            }
            for (slot_names.items, slot_hashes.items) |file, *digest| if (!files.contains(file)) {
                digest.* = @splat(0);
            };
            var retired: std.ArrayList(u32) = .empty;
            for (keep, 0..) |retained_slot, slot| if (!retained_slot) try retired.append(ca, @intCast(slot));
            const built_hashes = try ca.alloc([32]u8, slot_names.items.len);
            @memset(built_hashes, @splat(0));
            if (prior) |root| for (root.file_fingerprints, 0..) |digest, slot| {
                if (keep[slot]) built_hashes[slot] = digest;
            };
            const retained = try a.alloc(bool, slot_names.items.len);
            errdefer a.free(retained);
            try entries.append(a, .{ .tuple = tuple, .predicate = predicate, .binding = binding, .name = name, .cover = cover, .sort = spill.Sort.init(a, &manager, &orders, sort_workspace_bytes / (end - first)), .changed = changed, .slots = slots, .built_hashes = built_hashes, .retained = retained, .root_arena = .init(a), .initial_build = prior == null or prior.?.building, .delta = .{ .previous = prior, .keep = keep, .retired_slots = retired.items, .files = slot_names.items, .fingerprints = slot_hashes.items, .public_navigation = if (input.resume_work) |work| work.navigation else if (input.native_delta) |delta| delta.public_navigation else null, .source_cut = if (input.resume_work) |work| work.source_cut else if (input.native_delta) |delta| delta.source_cut else null, .checkpoint_base = if (input.native_delta) |delta| delta.prior_cut else null, .pending_files = if (input.native_delta != null or (if (input.resume_work) |work| work.source_cut != null else false)) @intCast(std.mem.count(bool, changed, &.{true})) else 0 } });
            entries.items[entries.items.len - 1].sort.shared_runs = true;
            entries.items[entries.items.len - 1].sort.encoded_bytes = true;
        }
        if (entries.items.len == 0) continue;
        for (entries.items) |*entry| entry.sort.memory_bytes = sort_workspace_bytes / entries.items.len;
        for (entries.items) |*entry| {
            const prior = entry.delta.previous orelse continue;
            if (entry.initial_build) continue;
            var changed_files: usize = 0;
            var changed_rows: u64 = 0;
            var unknown_files: u64 = 0;
            for (entry.changed, source.inventory.files) |changed, file| if (changed) {
                changed_files += 1;
                changed_rows +|= file.row_count;
                if (file.row_count == 0) unknown_files += 1;
            };
            const prior_rows = if (prior.page) |page| page.records else 0;
            // Lightweight Parquet discovery can leave row counts unknown until
            // decode. Use the retained index's average instead of treating those
            // files as zero-cost changes and rebuilding once per file.
            changed_rows +|= unknown_files *| @max(1, prior_rows / @max(1, prior.files.len));
            if (!bulkRefresh(changed_files, changed_rows, prior_rows)) continue;
            // Retire changed coordinates once, then checkpoint fresh files in
            // the same bounded merge forest used by initial builds. Avoid N
            // complete forward/reverse/tie/predicate rebuilds for a dense refresh.
            var base_sort = spill.Sort.init(a, &manager, &orders, entry.sort.memory_bytes);
            defer base_sort.deinit();
            base_sort.encoded_bytes = true;
            base_sort.shared_runs = true;
            const ref = try ordered.publishIncremental(a, ca, store, &base_sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, .{ .source_cut = entry.delta.source_cut, .checkpoint_base = entry.delta.checkpoint_base, .pending_files = entry.delta.pending_files, .public_navigation = entry.delta.public_navigation, .run_only = true, .previous = prior, .keep = entry.delta.keep, .retired_slots = entry.delta.retired_slots, .files = entry.delta.files, .fingerprints = entry.built_hashes });
            entry.published = ref;
            try input.checkpoint(.{ .name = entry.name, .binding = entry.binding, .artifact = ref });
            entry.delta.previous = try ordered.loadRootLazy(entry.root_arena.allocator(), store.*, ref, cancellation, null);
            entry.initial_build = true;
            @memset(entry.retained, true);
            entry.delta.keep = entry.retained;
        }
        for (entries.items) |*entry| if (entry.delta.previous == null) {
            entry.reverse = try ordered.ReverseBuilder.create(a, store, cancellation);
        };
        var names: std.ArrayList([]const u8) = .empty;
        defer names.deinit(a);
        var keys = columns.keyIterator();
        while (keys.next()) |path| try names.append(a, path.*);
        var binding = entries.items[0].binding;
        binding.column_bindings = names.items;
        const needed = try ca.alloc(bool, source.inventory.files.len);
        @memset(needed, false);
        for (entries.items) |entry| for (needed, entry.changed) |*need, changed| {
            need.* = need.* or changed;
        };
        var ordinal: u64 = 0;
        // Checkpoint one source file at a time into durable merge levels.
        // Fused sorts and replay never retain archive-sized local runs.
        var scan_order = state.ScanOrder.init(&input);
        while (try scan_order.next()) |file_index| {
            if (!needed[file_index]) continue;
            input.only_files = null;
            input.only_file = file_index;
            const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, cancellation);
            defer rows.deinit(a);
            while (try rows.next(a)) |batch| {
                try cancellation.check();
                for (entries.items) |*entry| {
                    if (!entry.changed[file_index]) continue;
                    try consumeBatch(a, entry, batch, source.inventory.files[file_index].file_id, entry.slots[file_index], &ordinal, provider.context, cancellation);
                }
            }
            for (entries.items) |*entry| {
                if (!entry.changed[file_index]) continue;
                const slot = entry.slots[file_index];
                entry.built_hashes[slot] = entry.delta.fingerprints[slot];
                // The first merge uses the prior layout's length (new slots
                // may extend it). Subsequent checkpoints share a fixed census.
                const first_checkpoint = entry.published == null;
                const keep = if (first_checkpoint) try a.dupe(bool, entry.delta.keep) else entry.retained;
                defer if (first_checkpoint) a.free(keep);
                if (slot < keep.len) keep[slot] = false;
                var delta: ordered.Delta = .{ .source_cut = entry.delta.source_cut, .checkpoint_base = entry.delta.checkpoint_base, .pending_files = if (entry.delta.source_cut != null) entry.delta.pending_files - 1 else 0, .previous = entry.delta.previous, .keep = keep, .retired_slots = if (first_checkpoint) entry.delta.retired_slots else &.{slot}, .files = entry.delta.files, .fingerprints = entry.built_hashes, .public_navigation = entry.delta.public_navigation, .dictionary_slots = if (entry.published != null) &.{slot} else null };
                if (entry.reverse) |reverse| {
                    delta.built_reverse = try reverse.finish();
                    delta.reverse_built = true;
                }
                const ref = (if (entry.initial_build) ordered.publishRunCheckpoint(a, ca, store, &entry.sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, delta) else ordered.publishIncremental(a, ca, store, &entry.sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, delta)) catch |err| {
                    if (err == error.SqlProgramLimitExceeded) {
                        const admission = manager.admissionStats();
                        std.log.warn("native ordered lake admission exhausted file_index={d} total_files={d} peak_spill_files={d} peak_spill_bytes={d} spill_byte_limit={d}", .{ file_index, needed.len, admission.peak_files, admission.peak_bytes, admission.max_bytes });
                    }
                    return err;
                };
                if (entry.reverse) |reverse| {
                    reverse.deinit();
                    entry.reverse = null;
                }
                entry.published = ref;
                try input.checkpoint(.{ .name = entry.name, .binding = entry.binding, .artifact = ref });
                // Only the latest root directory is needed for the next file.
                // Retaining every prefix here would grow metadata quadratically.
                _ = entry.root_arena.reset(.free_all);
                entry.delta.previous = try ordered.loadRootLazy(entry.root_arena.allocator(), store.*, ref, cancellation, null);
                entry.initial_build = entry.delta.previous.?.building;
                if (first_checkpoint) @memset(entry.retained, true) else entry.retained[slot] = true;
                entry.delta.keep = entry.retained;
                entry.delta.dictionary_slots = &.{};
                entry.delta.retired_slots = &.{};
                entry.delta.pending_files = delta.pending_files;
                entry.sort.deinit();
                entry.sort = spill.Sort.init(a, &manager, &orders, sort_workspace_bytes / entries.items.len);
                entry.sort.shared_runs = true;
                entry.sort.encoded_bytes = true;
            }
        }
        for (entries.items) |*entry| {
            if (entry.reverse) |reverse| {
                entry.delta.built_reverse = try reverse.finish();
                entry.delta.reverse_built = true;
            }
            // With no changed file, an empty incremental merge still retires
            // deleted source slots. Otherwise the last per-file root is final.
            const root = if (entry.initial_build) try ordered.finalizeRunCheckpoint(a, out, store, &entry.sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, entry.delta) else entry.published orelse try ordered.publishIncremental(a, out, store, &entry.sort, entry.name, fingerprint(entry.tuple, entry.predicate, entry.cover), source.inventory, entry.cover, cancellation, entry.delta);
            var owned_binding = entry.binding;
            owned_binding.source_id = try out.dupe(u8, entry.binding.source_id);
            owned_binding.snapshot_id = try out.dupe(u8, entry.binding.snapshot_id);
            owned_binding.schema_fingerprint = try out.dupe(u8, entry.binding.schema_fingerprint);
            owned_binding.index_config_hash = try out.dupe(u8, entry.binding.index_config_hash);
            const paths = try out.alloc([]const u8, entry.binding.column_bindings.len);
            for (paths, entry.binding.column_bindings) |*path, old| path.* = try out.dupe(u8, old);
            owned_binding.column_bindings = paths;
            const encoded = try std.json.Stringify.valueAlloc(a, Declared{ .name = root.name, .artifact = root, .binding = owned_binding }, .{});
            defer a.free(encoded);
            try result.append(out, try std.json.parseFromSliceLeaky(Declared, out, encoded, .{ .allocate = .alloc_always }));
        }
    }
    return result.toOwnedSlice(out);
}
fn bulkRefresh(changed_files: usize, changed_rows: u64, previous_rows: u64) bool {
    // Keep selective copy-on-write updates for tiny deltas. Dense multi-file
    // refreshes amortize the single base pass and final assembly.
    return changed_files > 1 and changed_rows >= @max(1, previous_rows / 4);
}
fn claimSlot(a: A, names: *std.ArrayList([]const u8), hashes: *std.ArrayList([32]u8), vacant: *std.ArrayList(u32), file: []const u8, digest: [32]u8) !u32 {
    if (vacant.pop()) |slot| {
        names.items[slot] = file;
        hashes.items[slot] = digest;
        return slot;
    }
    if (names.items.len >= std.math.maxInt(u32)) return error.LakeSidecarBuildLimitExceeded;
    const slot: u32 = @intCast(names.items.len);
    try names.append(a, file);
    try hashes.append(a, digest);
    return slot;
}

test "external lake ordered slot admission remains bounded under lifetime file churn" {
    const a = std.testing.allocator;
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    var hashes: std.ArrayList([32]u8) = .empty;
    defer hashes.deinit(a);
    var vacant: std.ArrayList(u32) = .empty;
    defer vacant.deinit(a);
    try names.resize(a, 16384);
    @memset(names.items, "retained");
    try hashes.resize(a, 16384);
    @memset(hashes.items, @splat(1));
    for (0..20000) |_| {
        try vacant.append(a, 41);
        try std.testing.expectEqual(@as(u32, 41), try claimSlot(a, &names, &hashes, &vacant, "replacement", @splat(2)));
    }
    try std.testing.expectEqual(@as(usize, 16384), names.items.len);
    try std.testing.expectEqualStrings("replacement", names.items[41]);
    try std.testing.expectEqual(@as(u32, 16384), try claimSlot(a, &names, &hashes, &vacant, "beyond-legacy-cap", @splat(3)));
    try std.testing.expectEqual(@as(usize, 16385), names.items.len);
}

fn check(raw: *anyopaque) !void {
    const provider: *@import("lake_index_row_source.zig").Provider = @ptrCast(@alignCast(raw));
    try provider.context.ensureActive();
}

test "external lake native ordered indexes spill real Parquet and seek exact expression keys" {
    // Preserve checked ownership without unwinding every successful allocation.
    var fixture_allocator = try @import("antfly_platform").allocator.CheckedTestAllocator.init();
    defer fixture_allocator.deinit();
    const a = fixture_allocator.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-row-index-parquet");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const values = try a.alloc(i64, 4096);
    defer a.free(values);
    for (values, 0..) |*value, row| value.* = 9007199254740993 + @as(i64, @intCast(values.len - row));
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = values, .field_id = 1 }});
    defer a.free(bytes);
    var object = try client.putObject("antfly", "part.parquet", bytes, .{});
    object.deinit(a);
    const base_schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}]}},{{"name":"expression_idx","keys":[{{"expression":{{"op":"negate","args":[{{"op":"column","column":"amount"}}]}},"result_type":"integer","direction":"desc"}}],"include_columns":["amount"],"where":[{{"column":"amount","op":"gt","value":0}}]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(base_schema_json);
    var schema_arena = std.heap.ArenaAllocator.init(a);
    defer schema_arena.deinit();
    const sa = schema_arena.allocator();
    var schema_value = try std.json.parseFromSliceLeaky(std.json.Value, sa, base_schema_json, .{});
    const indexes = schema_value.object.getPtr("relational_indexes").?;
    for (0..8) |index| {
        const json = try std.fmt.allocPrint(sa, "{{\"name\":\"extra_{d}\",\"keys\":[{{\"column\":\"amount\"}}]}}", .{index});
        try indexes.array.append(try std.json.parseFromSliceLeaky(std.json.Value, sa, json, .{}));
    }
    const schema_json = try std.json.Stringify.valueAlloc(a, schema_value, .{});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(4), 1, std.testing.io);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    const declarations = try build(a, ca, table, &source, &store, &provider, .none, &.{});
    try std.testing.expectEqual(@as(usize, 10), declarations.len);
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
    defer parsed.deinit(a);
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, runtime);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
    defer layout.deinit();
    const definitions = (try parsed.relationalIndexDefinitions(ca)).?;
    for (declarations, definitions, 0..) |declaration, definition, index| {
        var tuple = try tuples.TuplePlan.init(a, runtime, &layout, definition.keys);
        defer tuple.deinit();
        var predicate: ?predicates.Plan = if (definition.where.len == 0) null else try predicates.Plan.init(a, runtime, &layout, definition.where);
        defer if (predicate) |*plan| plan.deinit();
        const target = values[17];
        var lower: std.ArrayList(u8) = .empty;
        defer lower.deinit(a);
        _ = try tuple.appendValues(a, &lower, &.{.{ .integer = if (index == 1) -target else target }});
        const upper = try a.dupe(u8, lower.items);
        defer a.free(upper);
        upper[upper.len - 1] += 1;
        const root = try ordered.loadRoot(ca, store, declaration.artifact, .none, null);
        var reader: ordered.Reader = undefined;
        try reader.init(a, &store, root, fingerprint(tuple, predicate, try coverColumns(ca, definition)), lower.items, upper, .none);
        defer reader.deinit();
        const refs = try reader.next(ca, 64);
        try std.testing.expectEqual(@as(usize, 1), refs.len);
        try std.testing.expectEqual(@as(u64, 17), refs[0].external.row_ordinal);
        try std.testing.expect(reader.exhausted);
        var covered_reader: ordered.Reader = undefined;
        try covered_reader.init(a, &store, root, fingerprint(tuple, predicate, try coverColumns(ca, definition)), lower.items, upper, .none);
        defer covered_reader.deinit();
        const entries = try covered_reader.nextEntries(ca, 64);
        const cover = entries[0].cover orelse return error.TestUnexpectedResult;
        var loaded = try readTestCover(a, store, root, entries[0]);
        defer loaded.deinit();
        const block = loaded.block;
        try std.testing.expectEqual(@as(usize, 1), root.cover.len);
        try std.testing.expectEqualStrings("amount", root.cover[0]);
        try std.testing.expectEqual(target, (try block.cell(cover.row, 0)).value.integer);
        // readTestCover validates the physical coordinate and full-key witness.
    }
    const incremental = try buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, declarations);
    try std.testing.expectEqual(declarations.len, incremental.len);
    for (incremental) |declaration| {
        const root = try ordered.loadRoot(ca, store, declaration.artifact, .none, null);
        try std.testing.expectEqual(@as(u64, 4096), root.page.?.records);
    }
    const reused = try build(a, ca, table, &source, &store, &provider, .none, declarations);
    for (declarations, reused) |old, new| try std.testing.expectEqualStrings(old.artifact.artifact_id, new.artifact.artifact_id);
}

pub fn coverColumns(a: A, definition: local.storage_relational_index.RelationalIndexDefinition) ![]const []const u8 {
    var columns: std.ArrayList([]const u8) = .empty;
    for (definition.keys) |key| if (key.expression_json == null) {
        const exists = for (columns.items) |column| {
            if (std.mem.eql(u8, column, key.column)) break true;
        } else false;
        if (!exists) try columns.append(a, key.column);
    };
    for (definition.include_columns) |column| {
        const exists = for (columns.items) |old| {
            if (std.mem.eql(u8, old, column)) break true;
        } else false;
        if (!exists) try columns.append(a, column);
    }
    return columns.items;
}

test "external lake dense ordered refresh checkpoints durable deltas and resumes without stale coordinates" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-dense-refresh");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const old_bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ -4, 9, 1, 5 }, .field_id = 1 }});
    defer a.free(old_bytes);
    const new_bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 100, 100, 101, 102 }, .field_id = 1 }});
    defer a.free(new_bytes);
    for ([_][]const u8{ "a.parquet", "b.parquet", "c.parquet" }) |name| {
        var uploaded = try client.putObject("antfly", name, old_bytes, .{});
        uploaded.deinit(a);
    }
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    var original = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer original.deinit();
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifacts.deinit();
    var store = artifacts.artifactStore();
    store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(1) };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &original, .context = .{ .io = std.testing.io } };
    const prior = try build(a, ca, table, &original, &store, &provider, .none, &.{});
    for ([_][]const u8{ "a.parquet", "b.parquet" }) |name| {
        var uploaded = try client.putObject("antfly", name, new_bytes, .{});
        uploaded.deinit(a);
    }
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const Progress = struct {
        store: *stores.ArtifactStore,
        a: A,
        saved: ?Declared = null,
        count: usize = 0,
        fn save(raw: *anyopaque, declaration: Declared) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const root = try ordered.loadRoot(self.a, self.store.*, declaration.artifact, .none, null);
            try std.testing.expect(root.building);
            // Progress arguments belong to the active build's scratch arena.
            // A simulated durable checkpoint must own its metadata after the
            // injected error unwinds that build.
            const bytes = try std.json.Stringify.valueAlloc(self.a, declaration, .{});
            self.saved = try std.json.parseFromSliceLeaky(Declared, self.a, bytes, .{ .allocate = .alloc_always });
            self.count += 1;
            if (self.count == 2) return error.InjectedCheckpointStop;
        }
    };
    var progress: Progress = .{ .store = &store, .a = ca };
    store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(2) };
    provider = .{ .source = &source, .context = .{ .io = std.testing.io }, .progress = .{ .ptr = &progress, .save = Progress.save } };
    try std.testing.expectError(error.InjectedCheckpointStop, buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, prior));
    try std.testing.expectEqual(@as(usize, 2), progress.count);
    store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(3) };
    provider.progress = null;
    const resumed = try buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, &.{progress.saved.?});
    const root = try ordered.loadRoot(ca, store, resumed[0].artifact, .none, null);
    try std.testing.expect(!root.building and root.build_runs.len == 0);
    try std.testing.expectEqual(@as(u64, 12), root.page.?.records);
    try std.testing.expectEqual(root.page.?.records, root.reverse.?.records);
    var reader: ordered.Reader = undefined;
    try reader.init(a, &store, root, root.fingerprint, "", null, .none);
    defer reader.deinit();
    var old_count: usize = 0;
    var new_count: usize = 0;
    for (try reader.nextEntries(ca, 100)) |entry| {
        const cover = entry.cover.?;
        var loaded = try readTestCover(a, store, root, entry);
        defer loaded.deinit();
        const block = loaded.block;
        if ((try block.cell(cover.row, 0)).value.integer < 100) old_count += 1 else new_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), old_count);
    try std.testing.expectEqual(@as(usize, 8), new_count);
}

/// Shared typed ingestion boundary for full reconciliations and sparse edits.
pub fn consumeBatch(a: A, entry: anytype, batch: anytype, file_id: []const u8, slot: u32, ordinal: *u64, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) !void {
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(a);
    var condition_key: std.ArrayList(u8) = .empty;
    defer condition_key.deinit(a);
    var bound = try entry.tuple.bindExternalBatch(a, batch);
    defer bound.deinit();
    var condition_batches: std.ArrayList(tuples.BatchKeys) = .empty;
    defer {
        for (condition_batches.items) |*condition| condition.deinit();
        condition_batches.deinit(a);
    }
    if (entry.predicate) |plan| for (plan.conditions) |condition| {
        var selected = try condition.tuple.bindExternalBatch(a, batch);
        errdefer selected.deinit();
        try condition_batches.append(a, selected);
    };
    var begin: usize = 0;
    while (begin < batch.rowCount()) {
        try cancellation.check();
        try context.ensureActive();
        var window = std.heap.ArenaAllocator.init(a);
        defer window.deinit();
        const pa = window.allocator();
        var positions: [64]usize = undefined;
        var key_values: [64][1]local.sql_scalar.Datum = undefined;
        var key_rows: [64][]const local.sql_scalar.Datum = undefined;
        var ordinals: [64]u64 = undefined;
        var count: usize = 0;
        const end = @min(begin + 64, batch.rowCount());
        for (begin..end) |row| {
            var live = true;
            if (entry.predicate) |plan| for (plan.conditions, condition_batches.items) |condition, *selected| {
                condition_key.clearRetainingCapacity();
                const is_null = try selected.append(a, &condition_key, row);
                if (!condition.evaluateEncoded(condition_key.items, is_null).matches()) {
                    live = false;
                    break;
                }
            };
            if (!live) continue;
            const ref = batch.rowRef(row);
            if (ref != .external or !std.mem.eql(u8, ref.external.file_id, file_id)) return error.InvalidNativeLakeRowIndex;
            key.clearRetainingCapacity();
            _ = try bound.append(a, &key, row);
            try key.appendSlice(a, &ordered.coordinate(slot, ref.external));
            if (entry.reverse) |reverse| try reverse.add(key.items);
            positions[count] = row;
            key_values[count][0] = local.sql_scalar.Datum.json(.{ .string = try pa.dupe(u8, key.items) });
            key_rows[count] = &key_values[count];
            ordinals[count] = std.math.add(u64, ordinal.*, count) catch return error.LakeSidecarBuildLimitExceeded;
            count += 1;
        }
        const next_ordinal = std.math.add(u64, ordinal.*, count) catch return error.LakeSidecarBuildLimitExceeded;
        if (count != 0) {
            var cover: CoverBatch = .{ .page = .{ .batch = batch, .selection = positions[0..count] }, .names = entry.cover };
            const borrowed: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &cover, .read = CoverBatch.cell, .read_physical = CoverBatch.physical, .read_frozen = CoverBatch.frozen, .read_json = CoverBatch.json, .count = count, .width = entry.cover.len } };
            const values = try borrowed.preparePhysical(pa);
            try entry.sort.addBatch(values, key_rows[0..count], ordinals[0..count]);
        }
        ordinal.* = next_ordinal;
        begin = end;
    }
}

/// Preserve the raw covering-cell contract while forwarding native scalar and
/// JSON payloads directly into the sort's typed store. Decimal text and encoded
/// interpretations use the same lazy cell accessor as ordinary row ingestion.
const CoverBatch = struct {
    page: local.sql_catalog.ColumnPage,
    names: []const []const u8,
    fn cell(raw: *anyopaque, a: A, row: usize, column: usize) !local.sql_scalar.Datum {
        const self: *CoverBatch = @ptrCast(@alignCast(raw));
        return self.page.cell(a, row, self.names[column]);
    }
    fn physical(raw: *anyopaque, _: A, column: usize) !?local.sql_execution_batch.PhysicalColumn {
        const self: *CoverBatch = @ptrCast(@alignCast(raw));
        const vector = self.page.batch.findColumn(self.names[column]) orelse return null;
        const kind: local.sql_ast.ColumnType = switch (vector.values) {
            .i64, .dictionary_i64 => .integer,
            .f64, .dictionary_f64 => .number,
            .bool => .boolean,
            .bytes, .dictionary_bytes => .string,
            else => return null,
        };
        return .{ .vector = vector, .selection = self.page.selection, .kind = kind, .element = if (kind == .number) .float64 else null };
    }
    fn frozen(raw: *anyopaque, row: usize, column: usize) !?local.sql_execution_batch.FrozenJson {
        const self: *CoverBatch = @ptrCast(@alignCast(raw));
        return self.page.frozenJson(row, self.names[column]);
    }
    fn json(raw: *anyopaque, row: usize, column: usize) !?[]const u8 {
        const self: *CoverBatch = @ptrCast(@alignCast(raw));
        return self.page.encodedJson(row, self.names[column]);
    }
};

test "external lake ordered sparse publication preserves covering rows across authenticated file churn" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-sparse-refresh");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 1, 2, 3, 4 }, .field_id = 1 }});
    defer a.free(bytes);
    for (0..12) |index| {
        const name = try std.fmt.allocPrint(a, "part-{d}.parquet", .{index});
        defer a.free(name);
        var uploaded = try client.putObject("antfly", name, bytes, .{});
        uploaded.deinit(a);
    }
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"relational_indexes":[{{"name":"amount_idx","keys":[{{"column":"amount"}}]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"amount":{{"type":"integer"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    const path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(path);
    var artifacts = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, path);
    defer artifacts.deinit();
    var store = artifacts.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const navigation_api = @import("lake_source_navigation.zig");
    var previous_navigation: ?navigation_api.Root = null;
    var previous: []const Declared = &.{};
    const Denied = struct {
        var base: local.storage_object_storage.ObjectStorage = undefined;
        var allowed: []const u8 = "";
        fn get(_: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: local.storage_object_storage.GetOptions) !local.storage_object_storage.GetResult {
            if (!std.mem.eql(u8, key, allowed) and !(std.mem.eql(u8, allowed, "new-*") and std.mem.startsWith(u8, key, "new-"))) return error.UnexpectedUnchangedParquetRead;
            var copy = base;
            copy.allocator = alloc;
            return copy.getObject(bucket, key, options);
        }
    };
    const table: local.common_topology_records.TableRecord = .{ .table_id = 1, .name = "lake", .schema_json = schema_json, .indexes_json = "{}" };
    for (0..7) |phase| {
        if (phase == 1 or phase == 4 or phase == 5) {
            if (phase == 4) try client.deleteObject("antfly", "part-2.parquet", .{});
            var uploaded = try client.putObject("antfly", if (phase == 1) "new-a.parquet" else if (phase == 5) "later.parquet" else "renamed.parquet", bytes, .{});
            uploaded.deinit(a);
            if (phase == 1) {
                var other = try client.putObject("antfly", "new-b.parquet", bytes, .{});
                other.deinit(a);
            }
        } else if (phase == 2) {
            try client.deleteObject("antfly", "part-1.parquet", .{});
            const changed = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 100, 101, 102, 103 }, .field_id = 1 }});
            defer a.free(changed);
            var uploaded = try client.putObject("antfly", "part-0.parquet", changed, .{});
            uploaded.deinit(a);
        }
        var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        var denied = source.scanner.object_reader.client.vtable.*;
        Denied.base = source.scanner.object_reader.client;
        Denied.allowed = if (phase == 1) "new-*" else if (phase == 2) "part-0.parquet" else if (phase == 4) "renamed.parquet" else if (phase == 5) "later.parquet" else "";
        denied.get_object = Denied.get;
        if (phase != 0) source.scanner.object_reader.client.vtable = &denied;
        store.upload_scope = .{ .domain = @splat(9), .attempt = @splat(@as(u8, @intCast(phase + 1))) };
        if (phase == 3) {
            // Upgrade a dictionary that has a valid census commitment but
            // predates the persistent free-slot directory. Migration must own
            // new pages under this publication attempt, including on quiet cuts.
            var legacy = try ordered.loadRootLazy(ca, store, previous[0].artifact, .none, null);
            legacy.file_dictionary.?.free = null;
            legacy.file_dictionary.?.free_complete = false;
            var upload = store;
            upload.allocator = ca;
            const migrated = try upload.put(try std.json.Stringify.valueAlloc(ca, legacy, .{}));
            const copy = try ca.dupe(Declared, previous);
            copy[0].artifact.artifact_id = migrated.artifact_id;
            copy[0].artifact.checksum = migrated.checksum;
            copy[0].artifact.byte_len = migrated.byte_len;
            previous = copy;
        }
        var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
        var navigation = try navigation_api.build(a, ca, &store, &provider, previous_navigation, .none);
        defer navigation.deinit();
        provider.native_delta = navigation.delta;
        if (phase == 1) {
            var parsed_failure = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
            defer parsed_failure.deinit(a);
            const runtime_failure = try local.schema_mod.deriveRuntimeTableSchema(a, parsed_failure);
            defer local.storage_schema.freeSchema(a, runtime_failure);
            var layout_failure = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime_failure);
            defer layout_failure.deinit();
            const definitions_failure = (try parsed_failure.relationalIndexDefinitions(ca)).?;
            var partial = definitions_failure[0];
            partial.where = &.{.{ .field = "amount", .op = .gt, .value_json = "0" }};
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
            var unopened: spill.Manager = undefined;
            const selected: []const @TypeOf(partial) = &.{partial};
            // The tuple allocation succeeds; the first predicate allocation
            // fails before the builder can transfer either owner into Entry.
            try std.testing.expectError(error.OutOfMemory, @import("lake_index_native_rows_delta.zig").build(failing.allocator(), ca, &provider, &store, runtime_failure, &layout_failure, selected, &.{}, previous, .none, &unopened));
        }
        var candidates = previous;
        if (phase == 1) {
            const Interrupted = struct {
                out: A,
                declaration: ?Declared = null,
                fn save(raw: *anyopaque, declaration: Declared) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.declaration = try std.json.parseFromSliceLeaky(Declared, self.out, try std.json.Stringify.valueAlloc(self.out, declaration, .{}), .{ .allocate = .alloc_always });
                    return error.TestPublicationInterrupted;
                }
            };
            var interrupted: Interrupted = .{ .out = ca };
            provider.progress = .{ .ptr = &interrupted, .save = Interrupted.save };
            try std.testing.expectError(error.TestPublicationInterrupted, buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, previous));
            provider.progress = null;
            const checkpoint = interrupted.declaration orelse return error.TestUnexpectedResult;
            const checkpoint_root = try ordered.loadRootLazy(ca, store, checkpoint.artifact, .none, null);
            try std.testing.expectEqual(@as(u32, 1), checkpoint_root.pending_files);
            try std.testing.expectEqualDeep(navigation.delta.source_cut, checkpoint_root.source_cut);
            try std.testing.expectEqualDeep(navigation.delta.prior_cut, checkpoint_root.checkpoint_base);
            // One changed file completed, one still pending. Probe the exact
            // fingerprint to forbid reads from the completed file on resume.
            var reads: u64 = 16 * 1024 * 1024;
            var writes: u64 = 0;
            var bridge: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = checkpoint_root.domain, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
            var completed_a = false;
            for (source.inventory.files) |file| if (std.mem.endsWith(u8, file.file_id, "new-a.parquet")) {
                const slot = (try ordered.file_dictionary.named(ca, bridge.store(), checkpoint_root.file_dictionary.?, file.file_id)).?;
                const record = try ordered.file_dictionary.file(ca, bridge.store(), checkpoint_root.file_dictionary.?, slot);
                completed_a = !std.mem.allEqual(u8, &record.fingerprint, 0);
            };
            Denied.allowed = if (completed_a) "new-b.parquet" else "new-a.parquet";
            candidates = try std.mem.concat(ca, Declared, &.{ previous, &.{checkpoint} });
            const good_cut = provider.native_delta.?.source_cut;
            provider.native_delta.?.source_cut.?[0] ^= 1;
            // Identical public names cannot authorize a checkpoint from a
            // different logical file-content cut. It must reread changed files.
            try std.testing.expectError(error.UnexpectedUnchangedParquetRead, buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, candidates));
            provider.native_delta.?.source_cut = good_cut;
            provider.ordered_delta_plans = 0;
            // A durable retry owns a new upload attempt and physical directory,
            // while both authenticated logical census commitments stay exact.
            store.upload_scope.?.attempt = @splat(77);
            var retried_navigation = try navigation_api.build(a, ca, &store, &provider, previous_navigation, .none);
            defer retried_navigation.deinit();
            try std.testing.expectEqualDeep(navigation.delta.source_cut, retried_navigation.delta.source_cut);
            provider.native_delta = retried_navigation.delta;
            candidates = &.{checkpoint};
        }
        const built = try buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, candidates);
        if (phase != 0) try std.testing.expectEqual(@as(usize, 1), provider.ordered_delta_plans);
        if (phase == 3) {
            const reused = try build(a, ca, table, &source, &store, &provider, .none, built);
            try std.testing.expectEqualStrings(built[0].artifact.artifact_id, reused[0].artifact.artifact_id);
        }
        if (phase == 1) {
            const complete_candidates = try std.mem.concat(ca, Declared, &.{ previous, built });
            Denied.allowed = "";
            const resumed = try buildIncremental(a, ca, table, &source, &store, &provider, .none, &.{}, complete_candidates);
            const resumed_root = try ordered.loadRootLazy(ca, store, resumed[0].artifact, .none, null);
            try std.testing.expectEqual(@as(u64, 56), resumed_root.page.?.records);
        }
        const root = try ordered.loadRootLazy(ca, store, built[0].artifact, .none, null);
        try std.testing.expectEqual(@as(u32, 0), root.pending_files);
        if (phase == 3) {
            try std.testing.expect(root.file_dictionary.?.free_complete);
            try std.testing.expectEqualDeep(store.upload_scope.?.attempt, root.file_dictionary.?.free.?.attempt);
        }
        try std.testing.expectEqual(@as(usize, if (phase == 0) 12 else 14), root.fileCount());
        try std.testing.expectEqual(@as(u64, if (phase == 1 or phase >= 5) 56 else if (phase == 0) 48 else 52), root.page.?.records);
        var reader: ordered.Reader = undefined;
        try reader.init(a, &store, root, root.fingerprint, "", null, .none);
        defer reader.deinit();
        var rows: usize = 0;
        var fresh: usize = 0;
        while (!reader.exhausted) for (try reader.nextEntries(ca, 16)) |entry| {
            const ref = entry.ref;
            try std.testing.expect(navigation.delta.by_id.contains(ref.external.file_id));
            const cover = entry.cover.?;
            var loaded = try readTestCover(a, store, root, entry);
            defer loaded.deinit();
            const block = loaded.block;
            if ((try block.cell(cover.row, 0)).value.integer >= 100) fresh += 1;
            rows += 1;
        };
        try std.testing.expectEqual(@as(usize, if (phase == 1 or phase >= 5) 56 else if (phase == 0) 48 else 52), rows);
        try std.testing.expectEqual(@as(usize, if (phase >= 2) 4 else 0), fresh);
        previous_navigation = try navigation_api.load(ca, store, navigation.ref, .none, null);
        previous = built;
    }
}

test "external lake ordered batches preserve covering nulls dictionaries JSON decimals and spill ownership" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    var integers = [_]i64{ 9007199254740993, -9007199254740993, 0, 4 };
    var floating = [_]f64{ -0.0, std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64) };
    var coefficients = [_]i128{ 123, -999, 0, 42 };
    var dictionary = [_][]const u8{ "alpha", "beta" };
    const ids = [_]u32{ 1, 999999, 0, 1 };
    const batch: local.storage_rowsource_types.ColumnBatch = .{ .snapshot = .{ .table_id = "lake", .snapshot_id = "snapshot" }, .external_span = .{ .first = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = 4 }, .columns = &.{
        .{ .name = "n", .values = .{ .i64 = &integers } },
        .{ .name = "f", .values = .{ .f64 = &floating } },
        .{ .name = "label", .values = .{ .dictionary_bytes = .{ .values = &dictionary, .indices = &ids } }, .nulls = .{ .bytes = &.{ 0, 1, 0, 0 } } },
        .{ .name = "exact", .values = .{ .decimal128 = .{ .values = &coefficients, .precision = 12, .scale = 2 } } },
        .{ .name = "doc", .values = .{ .json = &.{ "{ \"a\": 1 }", "null", "[1,2]", "\"text\"" } } },
    } };
    try batch.validate();
    const selection = [_]usize{ 3, 1, 0, 2 };
    const names = &.{ "n", "f", "label", "exact", "doc", "_id" };
    var cover: CoverBatch = .{ .page = .{ .batch = batch, .selection = &selection }, .names = names };
    const input: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &cover, .read = CoverBatch.cell, .read_physical = CoverBatch.physical, .read_frozen = CoverBatch.frozen, .read_json = CoverBatch.json, .count = 4, .width = 6 } };
    const expected = try pa.alloc([]const local.sql_scalar.Datum, 4);
    for (expected, 0..) |*row, index| row.* = try input.row(pa, index);
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check, .async_writes = false };
    defer manager.deinit();
    var sort = spill.Sort.init(a, &manager, &.{.{}}, 1024);
    defer sort.deinit();
    sort.shared_runs = true;
    const keys = [_][]const local.sql_scalar.Datum{ &.{local.sql_scalar.Datum.json(.{ .integer = 3 })}, &.{local.sql_scalar.Datum.json(.{ .integer = 2 })}, &.{local.sql_scalar.Datum.json(.{ .integer = 1 })}, &.{local.sql_scalar.Datum.json(.{ .integer = 0 })} };
    try sort.addBatch(try input.preparePhysical(pa), &keys, &.{ 0, 1, 2, 3 });
    // The producer page may disappear as soon as addBatch returns.
    @memset(&integers, 0);
    @memset(&floating, 0);
    @memset(&coefficients, 0);
    @memset(&dictionary, "changed");
    var count: usize = 0;
    while (try sort.next(pa)) |row| {
        try std.testing.expectEqual(@as(i64, @intCast(count)), row.keys[0].value.integer);
        for (row.values, expected[@intCast(row.ordinal)]) |actual, wanted| {
            try std.testing.expectEqual(wanted.sql_null, actual.sql_null);
            if (!wanted.sql_null) {
                try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(actual, wanted));
                if (wanted.value == .float) try std.testing.expectEqual(@as(u64, @bitCast(wanted.value.float)), @as(u64, @bitCast(actual.value.float)));
            }
        }
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expect(manager.written_bytes > 0);
}

fn readTestCover(a: A, store: stores.ArtifactStore, root: ordered.Root, entry: ordered.Reader.Entry) !@import("lake_index_cover_columns.zig").Loaded {
    const columns = @import("lake_index_cover_columns.zig");
    const cover = entry.cover.?;
    var reads: u64 = 16 * 1024 * 1024;
    var loaded = try columns.load(a, store, cover.block, cover.striped, &.{0}, root.cover.len, .none, null, 16 * 1024 * 1024, &reads, null);
    errdefer loaded.deinit();
    try loaded.verify(root.domain, cover.row, entry.key[entry.key.len - 16 ..][0..16].*, columns.witness(root.domain, entry.key));
    return loaded;
}
