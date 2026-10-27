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
    const definition = local.metadata_lake_index_catalog.desiredFingerprint(table);
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
    // Membership is independent of index projections. Each input cohort owns
    // exact typed value evidence under its schema/configuration binding.
    const recipe = namespace.digest();
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
        if (old.definition == null or old.current.file_slots == null or old.values_format != identity.cohort_values_format or !old.current.compact or delta.prior_snapshot == null or delta.count() != inventory.files.len or delta.prior_count != old.current.liveFiles() or
            !std.mem.eql(u8, &old.domain, &scope.domain) or !std.mem.eql(u8, &old.namespace, &namespace.digest()) or
            !std.mem.eql(u8, &old.values_recipe, &recipe) or !std.mem.eql(u8, old.current.source, inventory.source_id) or !std.mem.eql(u8, delta.prior_snapshot.?, old.current.snapshot)) break :compatible null;
        break :compatible old;
    };
    const builder = try @import("lake_row_identity_delta.zig").Builder.createWithOptions(a, &manager, &pages, files, prior, if (prior != null) provider.native_delta else null, input_recipes, inventory.source_id, inventory.snapshot_id, true);
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
    if (capture_out.*) |capture| capture.configureOrdering(runtime, store) catch |err| {
        if (!capture.optionalFailure(err)) return err;
        capture.deinit();
        capture_out.* = null;
    };
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
    while (true) {
        var phase = try store.beginIo(1);
        defer phase.abort();
        const next = source.next(a) catch |err| return phase.failed(err);
        try phase.finish();
        const batch = next orelse break;
        const proof = try @import("lake_row_verification.zig").compute(a, tuple, namespace, batch, builder.cohorts, token, provider.context, store.io_phase);
        defer proof.deinit();
        const ids = proof.ids;
        if (capture_out.*) |capture| capture.add(batch, ids, store) catch |err| {
            if (!capture.optionalFailure(err)) return err;
            capture.deinit();
            capture_out.* = null;
            manager.max_bytes = provider.identity_spill_bytes;
        };

        for (0..batch.rowCount()) |row| {
            try token.check();
            try provider.context.ensureActive();
            _ = scratch.reset(.retain_capacity);
            const ref = batch.rowRef(row);
            if (ref != .external) return error.InvalidLogicalRowIdentity;
            const slot = by_file.get(ref.external.file_id) orelse return error.InvalidLogicalRowIdentity;
            try builder.addWithInputs(.{ .id = ids[row], .values = ids[row] }, .{ .file = slot, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }, proof.row(row));
        }
    }
    try extendInputs(a, builder, provider, store, &tuple, namespace, contract, by_file, token);
    var root = try builder.finish(namespace.digest(), recipe, cut);
    root.definition = definition;
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
/// Only missing cohorts are read for retained immutable files. The ordered
/// physical membership join rejects omissions, duplicates and mismatched keys.
fn extendInputs(a: A, builder: *@import("lake_row_identity_delta.zig").Builder, provider: *@import("lake_index_row_source.zig").Provider, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, tuple: *tuples.TuplePlan, namespace: identity.logical.Namespace, contract: []const local.serverless_query_lake_schema.Column, slots: std.StringHashMapUnmanaged(u32), token: Token) !void {
    if (builder.added_cohorts.len == 0) return;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var needed: std.StringHashMapUnmanaged(void) = .empty;
    for (builder.added_cohorts) |cohort| for (cohort.columns) |column| try needed.put(ca, column, {});
    for (try tuple.columnBindings(ca)) |column| try needed.put(ca, column, {});
    var columns: std.ArrayList([]const u8) = .empty;
    for (contract) |column| if (needed.contains(column.name)) try columns.append(ca, column.name);
    const scan = try ca.alloc(bool, builder.scan.len);
    for (scan, builder.scan) |*value, changed| value.* = !changed;
    var input = provider.*;
    input.only_file = null;
    input.only_files = scan;
    input.only_plan = null;
    input.replay = null;
    input.replay_consumer = null;
    input.enrichment_columns = false;
    input.schema_contract = contract;
    input.statistics = null;
    const inventory = provider.source.inventory;
    var opening = try store.beginIo(1);
    defer opening.abort();
    const source = input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, .{ .sidecar_kind = .text, .source_kind = if (inventory.format == .iceberg) .external_iceberg else .external_parquet, .row_ref_kind = .external, .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .schema_fingerprint = inventory.schema_fingerprint, .index_config_hash = "native-logical-input-extension-v1", .column_bindings = columns.items }, token) catch |err| return opening.failed(err);
    var source_open = true;
    defer if (source_open) {
        if (store.io_phase) |phase| if (phase.park) |park| park(phase.ptr);
        source.deinit(a);
    };
    try opening.finish();
    while (true) {
        var phase = try store.beginIo(1);
        defer phase.abort();
        const next = source.next(a) catch |err| return phase.failed(err);
        try phase.finish();
        const batch = next orelse break;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        var bound = try tuple.bindExternalBatch(sa, batch);
        defer bound.deinit();
        const plans = try sa.alloc(fingerprints.Plan, builder.added_cohorts.len);
        for (plans, builder.added_cohorts) |*plan, cohort| plan.* = try fingerprints.Plan.bind(sa, cohort.digest, batch, cohort.columns);
        var kernels = try fingerprints.Kernels.init(sa, plans);
        defer kernels.deinit();
        const tiles = try sa.alloc([64]identity.logical.Digest, plans.len);
        const values = try sa.alloc(identity.logical.Digest, plans.len);
        var key: std.ArrayList(u8) = .empty;
        for (0..batch.rowCount()) |row| {
            try token.check();
            try provider.context.ensureActive();
            if (row % 64 == 0) try kernels.fill(sa, row, @min(64, batch.rowCount() - row), tiles);
            for (tiles, values) |tile, *value| value.* = tile[row % 64];
            key.clearRetainingCapacity();
            if (try bound.append(sa, &key, row)) return error.NullLogicalRowKey;
            const id = try identity.logical.keyId(namespace, key.items);
            const ref = batch.rowRef(row);
            if (ref != .external) return error.InvalidLogicalRowIdentity;
            const slot = slots.get(ref.external.file_id) orelse return error.InvalidLogicalRowIdentity;
            try builder.addExtension(id, .{ .file = slot, .group = ref.external.row_group_ordinal, .row = ref.external.row_ordinal }, values);
        }
    }
    if (store.io_phase) |phase| if (phase.park) |park| park(phase.ptr);
    source.deinit(a);
    source_open = false;
    if (store.io_phase) |phase| if (phase.resume_cpu) |resume_| try resume_(phase.ptr);
}

fn checkpoint(raw: *anyopaque) !void {
    const provider: *@import("lake_index_row_source.zig").Provider = @ptrCast(@alignCast(raw));
    try provider.context.ensureActive();
}

test "external lake new input cohorts verify only new columns and retain compact membership" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-input-extension");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const wide = try a.alloc(u8, 192 * 1024);
    defer a.free(wide);
    @memset(wide, 'x');
    const parquet = local.serverless_query_lake_parquet_rowgroup;
    const data = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "id", .field_id = 1, .values = &.{ 1, 2, 3 } }}, &.{
        .{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = &.{ wide, wide, wide } },
        .{ .column_id = "label", .field_id = 3, .converted_type = 0, .values = &.{ "red", "green", "blue" } },
    });
    defer a.free(data);
    var uploaded = try client.putObject("antfly", "part.parquet", data, .{});
    uploaded.deinit(a);
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"integer"}},"body":{{"type":"string"}},"label":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "indexes" });
    defer a.free(artifact_path);
    var artifacts_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifacts_fs.deinit();
    var store = artifacts_fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(1) };
    var provider: @import("lake_index_row_source.zig").Provider = .{ .source = &source, .context = .{ .io = std.testing.io } };
    const nav = @import("lake_source_navigation.zig");
    var roots = std.heap.ArenaAllocator.init(a);
    defer roots.deinit();
    var navigation = try nav.build(a, roots.allocator(), &store, &provider, null, .none);
    defer navigation.deinit();
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema, .indexes_json = "{\"text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":false}}" };
    const base = try binding.binding.toManifestBaseSource(source.inventory.snapshot_id, "inventory");
    var captured: ?*@import("lake_row_payloads.zig").Capture = null;
    defer if (captured) |capture| capture.deinit();
    const first = (try build(a, roots.allocator(), table, &store, &provider, @splat(3), base, navigation.ref, null, .none, &captured)).?;
    try std.testing.expect(first.current.compact);
    try std.testing.expectEqual(first.current.by_file, first.current.by_location);
    var footer = try local.serverless_query_lake_schema.readFooter(a, source.scanner.reader(), source.inventory.files[0]);
    defer footer.deinit(a);
    const body = for (footer.row_groups[0].column_chunks) |chunk| {
        if (std.mem.eql(u8, chunk.column_id, "body")) break chunk;
    } else return error.TestUnexpectedResult;
    const Storage = local.storage_object_storage;
    const Probe = struct {
        var active: *@This() = undefined;
        original: Storage.ObjectStorage,
        start: u64,
        end: u64,
        tail: u64,
        reads: std.atomic.Value(usize) = .init(0),
        forbid_all: bool = false,
        fn get(raw: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: Storage.GetOptions) !Storage.GetResult {
            const self = active;
            if (self.forbid_all) return error.UnexpectedRetiredCohortRead;
            const range = options.range orelse return error.UnexpectedWholeSourceRead;
            // Footer discovery may overlap the last physical column. Ordinary
            // projected reads must never fetch the retained body payload.
            if (range.offset != self.tail and range.offset < self.end and range.offset + (range.length orelse return error.UnexpectedWholeSourceRead) > self.start) return error.UnexpectedRetainedColumnRead;
            _ = self.reads.fetchAdd(1, .monotonic);
            return self.original.vtable.get_object(raw, alloc, bucket, key, options);
        }
    };
    var probe: Probe = .{ .original = source.scanner.object_reader.client, .start = body.file_offset, .end = body.file_offset + body.compressed_len, .tail = data.len - 64 * 1024 };
    Probe.active = &probe;
    var vtable = source.scanner.object_reader.client.vtable.*;
    vtable.get_object = Probe.get;
    source.scanner.object_reader.client.vtable = &vtable;
    defer source.scanner.object_reader.client = probe.original;
    var delta = try nav.build(a, roots.allocator(), &store, &provider, navigation.root, .none);
    defer delta.deinit();
    provider.native_delta = delta.delta;
    table.indexes_json = "{\"text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":false},\"labels\":{\"type\":\"full_text\",\"field\":\"label\",\"store_source\":false}}";
    const second = (try build(a, roots.allocator(), table, &store, &provider, @splat(4), base, delta.ref, first, .none, &captured)).?;
    try std.testing.expect(probe.reads.load(.monotonic) > 0);
    try std.testing.expectEqual(first.current.by_id, second.current.by_id);
    try std.testing.expectEqual(first.current.by_file, second.current.by_file);
    try std.testing.expectEqual(first.current.by_order, second.current.by_order);
    try std.testing.expectEqual(@as(u64, 6), second.current.input_versions.?.records);
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = first.domain, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    var before = try tree.Cursor.init(a, pages.store(), first.current.input_versions, "", null);
    defer before.deinit();
    while (try before.next()) |row| {
        var after = try tree.Cursor.init(a, pages.store(), second.current.input_versions, row.key, null);
        defer after.deinit();
        const retained = (try after.next()).?;
        try std.testing.expectEqualSlices(u8, row.key, retained.key);
        try std.testing.expectEqualSlices(u8, row.value, retained.value);
    }
    probe.forbid_all = true;
    table.indexes_json = "{\"labels\":{\"type\":\"full_text\",\"field\":\"label\",\"store_source\":false}}";
    const third = (try build(a, roots.allocator(), table, &store, &provider, @splat(5), base, delta.ref, second, .none, &captured)).?;
    try std.testing.expectEqual(first.current.by_id, third.current.by_id);
    try std.testing.expectEqual(@as(u64, 3), third.current.input_versions.?.records);
    try std.testing.expect(captured == null);
}
