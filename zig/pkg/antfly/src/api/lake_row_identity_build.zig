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

//! Native typed stable-key verification over delete-aware external batches.
//! Discovery reads source values; verification never assumes the declared key.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const rows = local.storage_rowsource_types;
const tuples = local.storage_db_relational_index_keys;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;

fn bytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, value.len, .big);
    hash.update(&size);
    hash.update(value);
}
const fingerprints = @import("lake_row_fingerprints.zig");

pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cut: identity.logical.Digest, base: local.serverless_manifest_base_source.BaseSourceDescriptor, navigation: local.serverless_manifest_artifact_ref.ArtifactRef, previous: ?identity.Root, token: Token, capture_out: *?*@import("lake_row_payloads.zig").Capture) !?identity.Root {
    if (provider.identity_spill_bytes == 0) return error.LogicalRowIdentityAdmissionDeclined;
    const io = provider.context.io orelse return error.InvalidLogicalRowIdentity;
    const scheduler = local.sql_parallel_scheduler.global();
    const retained = scheduler.reservePullUpTo(1, @min(provider.identity_working_set_bytes, 1024 * 1024), provider.identity_working_set_bytes) orelse return error.LogicalRowIdentityAdmissionDeclined;
    defer scheduler.releasePull(1, retained);
    const allowance = scheduler.cpuWorkspaceBudget(@min(retained, 8 * 1024 * 1024));
    if (allowance < 1024 * 1024) return error.LogicalRowIdentityAdmissionDeclined;
    var execution: local.sql_parallel_scheduler.Execution = .{ .scheduler = scheduler, .bytes = allowance, .checkpoint = .{ .ptr = provider, .check = checkpoint } };
    defer execution.pause();
    try execution.enter(io);
    var phase: @import("../serverless/artifacts/store.zig").ExecutionIo = .{ .execution = &execution, .io = io };
    var scoped = store.*;
    scoped.io_phase = phase.capability();
    var input = provider.*;
    input.io_phase = scoped.io_phase;
    var workspace: local.sql_memory_budget = .{ .backing = a, .limit = retained };
    errdefer if (capture_out.*) |capture| {
        capture.deinit();
        capture_out.* = null;
    };
    return buildInner(workspace.allocator(), out, table, &scoped, &input, cut, base, navigation, previous, token, a, capture_out) catch |err| {
        if (err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and workspace.isLocallyExhausted())) return error.LogicalRowIdentityAdmissionDeclined;
        return err;
    };
}
fn buildInner(a: A, out: A, table: local.common_topology_records.TableRecord, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cut: identity.logical.Digest, base: local.serverless_manifest_base_source.BaseSourceDescriptor, navigation: local.serverless_manifest_artifact_ref.ArtifactRef, previous: ?identity.Root, token: Token, capture_allocator: A, capture_out: *?*@import("lake_row_payloads.zig").Capture) !?identity.Root {
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer parsed.deinit(a);
    const binding = (parsed.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    const adapter = binding.row_identity orelse return null;
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, runtime);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
    defer layout.deinit();
    const definitions = try a.alloc(local.storage_relational_index.RelationalIndexKey, adapter.key_columns.len);
    defer a.free(definitions);
    for (adapter.key_columns, definitions) |column, *definition| definition.* = .{ .column = column };
    var tuple = try tuples.TuplePlan.init(a, runtime, &layout, definitions);
    defer tuple.deinit();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.table-incarnation.v1");
    var table_id: [16]u8 = undefined;
    std.mem.writeInt(u64, table_id[0..8], table.table_id, .big);
    std.mem.writeInt(u64, table_id[8..16], table.object_storage_generation, .big);
    hash.update(&table_id);
    bytes(&hash, binding.table_id);
    var incarnation: identity.logical.Digest = undefined;
    hash.final(&incarnation);
    const namespace: identity.logical.Namespace = .{ .incarnation = incarnation, .adapter = .stable_key, .recipe = tuple.fingerprint };
    const recipe = local.metadata_lake_index_catalog.desiredFingerprint(table);
    var metadata = std.heap.ArenaAllocator.init(a);
    defer metadata.deinit();
    const input_recipes = try @import("lake_row_input_recipes.zig").plan(a, metadata.allocator(), table, provider, base);
    var needed: std.StringHashMapUnmanaged(void) = .empty;
    defer needed.deinit(a);
    for (input_recipes) |input_recipe| for (input_recipe.columns) |column| try needed.put(a, column, {});
    var key_arena = std.heap.ArenaAllocator.init(a);
    defer key_arena.deinit();
    for (try tuple.columnBindings(key_arena.allocator())) |column| try needed.put(a, column, {});
    const column_storage = try a.alloc([]const u8, runtime.relational_columns.len);
    defer a.free(column_storage);
    var column_count: usize = 0;
    const contract = try a.alloc(local.serverless_query_lake_schema.Column, runtime.relational_columns.len);
    defer a.free(contract);
    for (runtime.relational_columns, contract) |column, *item| {
        item.* = .{ .name = column.path, .kind = @tagName(column.column_type), .required = column.required and !column.allows_null };
        if (needed.contains(column.path) or needed.contains(column.name)) {
            column_storage[column_count] = column.path;
            column_count += 1;
        }
    }
    // Identity and index input evidence need no unrelated payload columns.
    const columns = column_storage[0..column_count];
    const inventory = provider.source.inventory;
    const files = try a.alloc([]const u8, inventory.files.len);
    defer a.free(files);
    for (inventory.files, files) |file, *name| name.* = file.file_id;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = provider.context.io orelse return error.InvalidLogicalRowIdentity, .context = provider, .checkpoint = checkpoint, .max_bytes = provider.identity_spill_bytes };
    defer manager.deinit();
    var read_bytes: u64 = 0;
    var write_bytes: u64 = 0;
    const scope = store.upload_scope orelse return error.InvalidLogicalRowIdentity;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes, .cancellation = token, .streaming = true };
    pages.admitPublication();
    pages.read_phase = store.io_phase;
    const prior: ?identity.Root = compatible: {
        const old = previous orelse break :compatible null;
        const delta = provider.native_delta orelse break :compatible null;
        if (old.current.input_recipes.len != input_recipes.len or old.current.file_slots == null or old.values_format != fingerprints.format or delta.prior_snapshot == null or delta.count() != inventory.files.len or delta.prior_count != old.current.liveFiles() or
            !std.mem.eql(u8, &old.domain, &scope.domain) or !std.mem.eql(u8, &old.namespace, &namespace.digest()) or
            !std.mem.eql(u8, &old.values_recipe, &recipe) or !std.mem.eql(u8, old.current.source, inventory.source_id) or !std.mem.eql(u8, delta.prior_snapshot.?, old.current.snapshot)) break :compatible null;
        for (old.current.input_recipes, input_recipes) |left, right| if (!std.mem.eql(u8, &left.digest, &right.digest)) break :compatible null;
        break :compatible old;
    };
    const builder = try @import("lake_row_identity_delta.zig").Builder.create(a, &manager, &pages, files, prior, if (prior != null) provider.native_delta else null, input_recipes, inventory.source_id, inventory.snapshot_id);
    defer builder.deinit();
    var by_file: std.StringHashMapUnmanaged(u32) = .empty;
    defer by_file.deinit(a);
    for (files, builder.slots) |name, slot| try by_file.put(a, name, slot);
    // A full verification already reads the entire typed input union. Retain
    // its payload once; physical coordinates gain no serving authority here.
    const full_scan = for (builder.scan) |scan| {
        if (!scan) break false;
    } else true;
    var estimated_rows: u64 = 0;
    for (inventory.files) |file| estimated_rows +|= file.row_count;
    const capture_eligible = @import("lake_row_input_recipes.zig").nativeFragmentsEligible(try std.json.parseFromSliceLeaky(std.json.Value, metadata.allocator(), table.indexes_json, .{}));
    if (full_scan and capture_eligible and input_recipes.len != 0 and estimated_rows >= 128) capture_out.* = try @import("lake_row_payloads.zig").Capture.start(capture_allocator, provider, store, columns);
    if (capture_out.*) |capture| manager.max_bytes = provider.identity_spill_bytes - capture.manager.max_bytes;
    var input = provider.*;
    input.only_file = null;
    input.only_files = builder.scan;
    input.only_plan = null;
    input.replay = null;
    input.replay_consumer = null;
    input.enrichment_columns = false;
    input.schema_contract = contract;
    input.statistics = null;
    var opening = try store.beginIo(1);
    defer opening.abort();
    var source = input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, .{ .sidecar_kind = .text, .source_kind = if (inventory.format == .iceberg) .external_iceberg else .external_parquet, .row_ref_kind = .external, .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .schema_fingerprint = inventory.schema_fingerprint, .index_config_hash = "native-logical-row-state-v1", .column_bindings = columns }, token) catch |err| return opening.failed(err);
    defer {
        // Joining speculative source work must not retain the CPU admission
        // that its required progress can need.
        if (store.io_phase) |phase| if (phase.park) |park| park(phase.ptr);
        source.deinit(a);
    }
    try opening.finish();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var key: std.ArrayList(u8) = .empty;
    defer key.deinit(a);
    while (true) {
        var phase = try store.beginIo(1);
        defer phase.abort();
        const next = source.next(a) catch |err| return phase.failed(err);
        try phase.finish();
        const batch = next orelse break;
        if (capture_out.*) |capture| capture.add(batch, &by_file, store) catch |err| {
            if (!capture.optionalFailure(err)) return err;
            capture.deinit();
            capture_out.* = null;
            manager.max_bytes = provider.identity_spill_bytes;
        };
        var bound = try tuple.bindExternalBatch(a, batch);
        defer bound.deinit();
        const input_plans = try a.alloc(fingerprints.Plan, builder.cohorts.len + 1);
        var bound_inputs: usize = 0;
        defer {
            for (input_plans[0..bound_inputs]) |*plan| plan.deinit();
            a.free(input_plans);
        }
        input_plans[0] = try fingerprints.Plan.bind(a, recipe, batch, columns);
        bound_inputs += 1;
        for (builder.cohorts, input_plans[1..]) |recipe_, *plan| {
            plan.* = try fingerprints.Plan.bind(a, recipe_.digest, batch, recipe_.columns);
            bound_inputs += 1;
        }
        const input_tiles = try a.alloc([64]identity.logical.Digest, input_plans.len);
        defer a.free(input_tiles);
        const row_inputs = try a.alloc(identity.logical.Digest, builder.cohorts.len);
        defer a.free(row_inputs);
        var kernels = try fingerprints.Kernels.init(a, input_plans);
        defer kernels.deinit();
        const tile_rows = 64;
        for (0..batch.rowCount()) |row| {
            if (row % tile_rows == 0) {
                const count = @min(tile_rows, batch.rowCount() - row);
                try kernels.fill(a, row, count, input_tiles);
            }
            for (row_inputs, input_tiles[1..]) |*value, tile| value.* = tile[row % tile_rows];
            try token.check();
            try provider.context.ensureActive();
            _ = scratch.reset(.retain_capacity);
            key.clearRetainingCapacity();
            if (try bound.append(a, &key, row)) return error.NullLogicalRowKey;
            const ref = batch.rowRef(row);
            if (ref != .external) return error.InvalidLogicalRowIdentity;
            const slot = by_file.get(ref.external.file_id) orelse return error.InvalidLogicalRowIdentity;
            try builder.addWithInputs(.{ .id = try identity.logical.keyId(namespace, key.items), .values = input_tiles[0][row % tile_rows] }, .{ .file = slot, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }, row_inputs);
        }
    }
    var root = try builder.finish(namespace.digest(), recipe, cut);
    root.values_format = fingerprints.format;
    root.current.navigation = navigation;
    const encoded = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(encoded);
    if (encoded.len > identity.max_root_bytes) return error.LogicalRowIdentityAdmissionDeclined;
    if (capture_out.*) |capture| capture.finish(root.current.rows) catch |err| {
        if (!capture.optionalFailure(err)) return err;
        capture.deinit();
        capture_out.* = null;
    };
    // Only the small admitted root escapes the optional workspace. Temporary
    // file dictionaries and recipe planning cannot consume publication memory.
    return try identity.decode(out, encoded);
}
fn checkpoint(raw: *anyopaque) !void {
    const provider: *@import("lake_index_row_source.zig").Provider = @ptrCast(@alignCast(raw));
    try provider.context.ensureActive();
}
