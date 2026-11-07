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

//! Read-only distributed coverage collection. No readiness is synthesized
//! from a declaration or a partial set of owners.
const std = @import("std");
const wire = @import("antfly_metadata_openapi").types;
const tables = @import("tables.zig");
const metadata = @import("../metadata/table_manager.zig");
const schema = @import("antfly_local_sources").schema_mod;
const reads = @import("antfly_local_sources").api_table_read_source;
const operation = @import("antfly_local_sources").api_operation;
const catalog_mod = @import("antfly_local_sources").storage_db_relational_integrity_catalog;
const activation = @import("antfly_local_sources").storage_db_relational_integrity_activation_contract;
const types = @import("antfly_local_sources").storage_db_types;

const NativeStatusStage = enum(u4) { snapshot, definition, boot_read, boot_ready, initialized_proof, descriptor_match, actual_binding, current_schema, integrity_catalog, coverage };
var native_status_pending_sites: std.atomic.Value(u16) = .init(0);
var native_status_failure_sites: std.atomic.Value(u16) = .init(0);
fn diagnoseNativeStatus(stage: NativeStatusStage, err: anyerror, table: u64, boot_generation: u64, registered: bool, initialized: bool) void {
    if (comptime @import("builtin").mode == .debug) {
        const mask: u16 = @as(u16, 1) << @intCast(@backingInt(stage));
        const seen = if (err == error.ConstraintActivationPending or err == error.SqlRelationRecoveryIncomplete or err == error.SqlRelationOwnerUnavailable) &native_status_pending_sites else &native_status_failure_sites;
        if (seen.fetchOr(mask, .monotonic) & mask != 0) return;
        std.debug.print("native constraint status stage={s} err={s} table={} boot={} registered={} initialized={}\n", .{ @tagName(stage), @errorName(err), table, boot_generation, registered, initialized });
    }
}

/// Owner-bound native controls use the exact status catalog cut. This is a
/// private process-local capability; the hosted client signs its actual body
/// before remote forwarding. Permanent lookups retain their ordinary path.
fn lookupOwner(alloc: std.mem.Allocator, reader: reads.TableReadSource, snapshot: @import("../metadata/api.zig").AdminSnapshot, table: metadata.TableRecord, name: []const u8, key: []const u8, opts: types.LookupOptions, request: operation.RequestContext) !?reads.LookupResponse {
    const descriptor = table.sql_persistence orelse return reader.lookup(alloc, name, key, opts, .read_index);
    const normalized = try request.platformDeadline();
    try descriptor.requireConnectionOwner(request.sql_relation_owner);
    const catalog = @import("table_catalog.zig");
    const routing: @import("../metadata/api.zig").CatalogRoutingSnapshot = .{ .metadata_group_id = snapshot.status.metadata_group_id, .metadata_incarnation = snapshot.status.metadata_incarnation, .tables = snapshot.tables, .ranges = snapshot.ranges };
    var plan = (try catalog.routePlanFromSnapshotUntil(alloc, routing, name, .{ .key = key }, normalized.deadline_ns)) orelse return error.ConstraintActivationPending;
    defer plan.deinit(alloc);
    if (plan.table_id != table.table_id or plan.groups.len != 1 or plan.groups[0].execution != .native_local or !plan.groups[0].execution.native_local.eql(descriptor)) return error.SqlRelationOwnerChanged;
    var fence: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = plan.metadata_group_id, .metadata_incarnation = plan.metadata_incarnation, .catalog_revision = plan.catalog_revision, .table_id = plan.table_id, .topology_epoch = plan.topology_epoch, .route = plan.groups[0] };
    fence.admission_sql_relation_owner = request.sql_relation_owner;
    fence.admission_relation_minter = request.sql_relation_owner_minter;
    if (descriptor.kind == .temporary) {
        const minter = request.sql_relation_owner_minter orelse return error.SqlTemporaryRelationNotOwned;
        try descriptor.requireConnectionOwner(minter.owner);
    }
    fence.admission_deadline_ns = normalized.deadline_ns;
    fence.admission_deadline_io = normalized.deadline_io;
    fence.admission_cancellation = request.cancellation;
    var routed = reader;
    routed.route_fence = fence;
    var scoped_opts = opts;
    scoped_opts.execution_deadline_ns = normalized.deadline_ns;
    scoped_opts.execution_io = normalized.deadline_io;
    scoped_opts.cancellation = normalized.cancellation;
    return routed.lookupGroupLocal(alloc, plan.groups[0].group_id, name, key, scoped_opts, .read_index);
}

const NativeStatusDiagnostic = struct { stage: NativeStatusStage = .snapshot, table: u64 = 0, native: bool = false, boot: u64 = 0, registered: bool = false, initialized: bool = false };
pub fn collect(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, request: operation.RequestContext) ![]u8 {
    var diagnostic: NativeStatusDiagnostic = .{};
    return collectObserved(alloc, source, reader, name, request, &diagnostic) catch |err| {
        diagnoseNativeStatus(diagnostic.stage, err, diagnostic.table, diagnostic.boot, diagnostic.registered, diagnostic.initialized);
        return err;
    };
}

fn collectObserved(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, request: operation.RequestContext, diagnostic: *NativeStatusDiagnostic) ![]u8 {
    try request.ensureActive();
    var snapshot = (try source.adminSnapshot()) orelse return error.ConstraintActivationPending;
    defer source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, name) orelse return error.TableNotFound;
    diagnostic.table = table.table_id;
    diagnostic.native = table.sql_persistence != null;
    diagnostic.stage = .definition;
    var validator = try schema.CompiledTableValidator.init(alloc, table.schema_json);
    defer validator.deinit(alloc);
    const parsed = validator.schema;
    if (parsed.storage_mode != .relational) return error.RelationalTableRequired;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    const has_constraints = (if (parsed.unique_constraints) |list| list.value.len else 0) != 0 or (if (parsed.foreign_keys) |list| list.value.len else 0) != 0 or (if (parsed.checks) |list| list.value.len else 0) != 0;
    var result = wire.RelationalConstraintStatus{ .schema_version = parsed.version, .coverage_kind = "unique_foreign_key_and_check", .state = if (table.read_schema_json.len == 0) .enforced else .validating, .ranges = &.{} };
    if (table.relational_retirement_json.len != 0) {
        var job = try @import("../metadata/relational_retirement.zig").parse(temporary, table.relational_retirement_json);
        defer job.deinit();
        result.retirement = .{
            .id = try temporary.dupe(u8, &std.fmt.bytesToHex(job.value.id, .lower)),
            .phase = switch (job.value.phase) {
                .fencing => "fencing",
                .foreign_keys => "foreign_keys",
                .unique => "unique",
                .ready => if (job.value.drop) "ready_to_drop" else "publishing",
                .published => "published",
            },
            .drop = job.value.drop,
            .target_schema_version = try tables.schemaVersion(job.value.target_schema_json),
            .failure = if (job.value.failure.len > 0) try temporary.dupe(u8, job.value.failure) else null,
        };
    }
    if (table.sql_persistence) |descriptor| {
        // No-constraint status used to report enforced from catalog presence
        // alone. Rejoin the current registered Boot and the actual signed
        // initialized image before declaring this native owner usable.
        const Source = switch (@typeInfo(@TypeOf(source))) {
            .pointer => |ptr| ptr.child,
            else => @TypeOf(source),
        };
        if (comptime @hasDecl(Source, "systemCatalog")) {
            var private = request;
            private.setting_admin = true;
            diagnostic.stage = .boot_read;
            const bytes = try source.systemCatalog(temporary, private, .{ .sql_relation_boot_read = .{ .node = descriptor.owner_node_id, .binding_table = table.table_id } });
            if (bytes.len > 64 * 1024) return error.InvalidSqlBackendResponse;
            const observed = try std.json.parseFromSliceLeaky(@import("../metadata/sql_relation_boots.zig").Observation, temporary, bytes, .{});
            diagnostic.registered = observed.registration_current;
            diagnostic.initialized = observed.initialized != null;
            diagnostic.boot = if (observed.record) |record| record.generation else 0;
            diagnostic.stage = .boot_ready;
            const boot = observed.requireReady() catch |err| switch (err) {
                error.SqlRelationOwnerUnavailable, error.SqlRelationRecoveryIncomplete => return error.ConstraintActivationPending,
                else => return err,
            };
            diagnostic.stage = .initialized_proof;
            const initialized = observed.initialized orelse return error.ConstraintActivationPending;
            try initialized.verify();
            diagnostic.stage = .descriptor_match;
            const binding = initialized.binding;
            var owner: ?metadata.RangeRecord = null;
            for (snapshot.ranges) |range| if (range.table_id == table.table_id) {
                if (owner != null) return error.TopologyChanged;
                owner = range;
            };
            const range = owner orelse return error.ConstraintActivationPending;
            const incarnation = snapshot.status.metadata_incarnation orelse return error.ConstraintActivationPending;
            if (!binding.descriptor.eql(descriptor) or binding.table_id != table.table_id or
                binding.shard_id != metadata.rangeDocIdentityShardId(range) or binding.range_id != metadata.rangeDocIdentityRangeId(range) or
                initialized.metadata_group_id != snapshot.status.metadata_group_id or !std.mem.eql(u8, &initialized.identity.metadata_incarnation, &incarnation) or
                initialized.boot_generation != boot.generation or
                initialized.reporter_incarnation != boot.reporter_incarnation or !std.mem.eql(u8, &binding.native_instance, &boot.instance) or
                !std.mem.eql(u8, &try initialized.identity.digest(), &try boot.identity.digest())) return error.SqlRelationOwnerChanged;
            // Reopen only through ordinary authenticated owner routing, then
            // compare the real primary catalog's immutable image and current
            // schema. Initial schema bytes in the first receipt legitimately
            // differ after later DDL; never equate those mutable bytes forever.
            const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
            const binding_opts: types.LookupOptions = .{
                .relational_topology_json = "{\"mode\":\"sql_relation_binding\"}",
                .execution_deadline_ns = request.deadline_ns,
                .execution_io = request.deadline_io,
                .cancellation = request.cancellation,
            };
            diagnostic.stage = .actual_binding;
            var actual_value = (lookupOwner(temporary, reader, snapshot, table.*, name, range.start_key, binding_opts, request) catch |err| switch (err) {
                error.SqlRelationRecoveryIncomplete, error.SqlRelationOwnerUnavailable => return error.ConstraintActivationPending,
                else => return err,
            }) orelse return error.ConstraintActivationPending;
            defer actual_value.deinit(temporary);
            const actual = try std.json.parseFromSliceLeaky(persistence.NativeBinding, temporary, actual_value.json, .{});
            try actual.validate();
            if (!(try persistence.PhysicalPin.fromBinding(actual)).eql(try persistence.PhysicalPin.fromBinding(binding)) or
                !actual.descriptor.eql(descriptor) or !std.mem.eql(u8, &actual.native_instance, &boot.instance)) return error.SqlRelationOwnerChanged;
            diagnostic.stage = .current_schema;
            const native_schema = @import("antfly_local_sources").storage_schema;
            const expected_schema = try schema.deriveRuntimeTableSchema(temporary, parsed);
            const expected_bytes = try native_schema.serializeSchema(temporary, expected_schema);
            if (actual.schema_version != parsed.version or !std.mem.eql(u8, &actual.schema_digest, &@import("antfly_local_sources").storage_db_restore_staging_contract.digest(expected_bytes))) return error.PreparedGenerationChanged;
        } else return error.ConstraintActivationPending;
    }
    if (!has_constraints) return std.json.Stringify.valueAlloc(alloc, result, .{});
    var opts: types.LookupOptions = .{ .relational_integrity_catalog = true, .execution_deadline_ns = request.deadline_ns, .execution_io = request.deadline_io, .cancellation = request.cancellation };
    diagnostic.stage = .integrity_catalog;
    var envelope = (try lookupOwner(temporary, reader, snapshot, table.*, name, "", opts, request)) orelse return error.ConstraintActivationPending;
    defer envelope.deinit(temporary);
    var catalog_response = try std.json.parseFromSlice(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, temporary, envelope.json, .{});
    defer catalog_response.deinit();
    const decoded_id = std.fmt.parseInt(u64, catalog_response.value.table_id, 10) catch return error.PreparedGenerationChanged;
    if (decoded_id != table.table_id or catalog_response.value.schema_version != parsed.version) return error.PreparedGenerationChanged;
    const encoded = catalog_response.value.catalog;
    const raw = try temporary.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
    try std.base64.standard.Decoder.decode(raw, encoded);
    var catalog = try catalog_mod.decode(temporary, raw);
    defer catalog.deinit();
    const checks_digest: [32]u8 = if (validator.execution.checks) |checks| checks.validationFingerprint() else @splat(0);
    if (catalog.schema_version != parsed.version or !std.mem.eql(u8, &catalog.incarnation, &(try catalog_mod.incarnationFromTableId(table.table_id))) or
        !std.mem.eql(u8, &catalog.checks_digest, &checks_digest)) return error.PreparedGenerationChanged;
    var owners: std.ArrayList(*const metadata.RangeRecord) = .empty;
    var owner_by_group: std.AutoHashMapUnmanaged(u64, *const metadata.RangeRecord) = .empty;
    for (snapshot.ranges) |*owner| if (owner.table_id == table.table_id) {
        const entry = try owner_by_group.getOrPut(temporary, owner.group_id);
        if (entry.found_existing) return error.TopologyChanged;
        entry.value_ptr.* = owner;
        try owners.append(temporary, owner);
    };
    if (owners.items.len == 0 or owners.items.len > 4096) return error.ConstraintActivationPending;
    std.mem.sort(*const metadata.RangeRecord, owners.items, {}, struct {
        fn less(_: void, left: *const metadata.RangeRecord, right: *const metadata.RangeRecord) bool {
            return std.mem.order(u8, left.start_key, right.start_key) == .lt;
        }
    }.less);
    const ranges = try temporary.alloc(wire.RelationalConstraintRangeStatus, owners.items.len);
    var expected_start: []const u8 = "";
    opts.relational_integrity_catalog = false;
    opts.relational_activation_json = "{\"mode\":\"status\"}";
    diagnostic.stage = .coverage;
    for (owners.items, ranges, 0..) |owner, *range, i| {
        try request.ensureActive();
        if (owner.restore_backup_id.len != 0 or !std.mem.eql(u8, owner.start_key, expected_start) or ((i + 1 == owners.items.len) != (owner.end_key == null))) return error.TopologyChanged;
        var response = (try lookupOwner(temporary, reader, snapshot, table.*, name, owner.start_key, opts, request)) orelse return error.ConstraintActivationPending;
        defer response.deinit(temporary);
        const Status = struct {
            schema_version: u32,
            schema_digest: [32]u8,
            generation_set: [32]u8,
            owner: [32]u8,
            range_start: []const u8,
            range_end: []const u8,
            unique_covered: bool,
            state: activation.State,
            phase: activation.Phase,
            rows_scanned: u64,
            failure: []const u8,
        };
        var status = try std.json.parseFromSlice(Status, temporary, response.json, .{});
        defer status.deinit();
        const value = status.value;
        if (value.schema_version != parsed.version or !std.mem.eql(u8, &value.schema_digest, &catalog.schema_digest) or
            !std.mem.eql(u8, &value.generation_set, &activation.generationSet(catalog)) or
            !std.mem.eql(u8, value.range_start, owner.start_key) or !std.mem.eql(u8, value.range_end, owner.end_key orelse "")) return error.PreparedGenerationChanged;
        const owner_hex = std.fmt.bytesToHex(value.owner, .lower);
        range.* = .{
            .group_id = try std.fmt.allocPrint(temporary, "{d}", .{owner.group_id}),
            .state = switch (value.state) {
                .enforced => .enforced,
                .validating => .validating,
                .invalid => .invalid,
            },
            .phase = switch (value.phase) {
                .unique => .unique,
                .foreign_key => .foreign_key,
                .check => .check,
            },
            .rows_scanned = try std.fmt.allocPrint(temporary, "{d}", .{value.rows_scanned}),
            .owner = try temporary.dupe(u8, &owner_hex),
            .failure = if (value.failure.len != 0) try temporary.dupe(u8, value.failure) else null,
        };
        if (value.state == .invalid) result.state = .invalid else if (value.state == .validating and result.state != .invalid) result.state = .validating;
        expected_start = owner.end_key orelse "";
    }
    // Reject a topology/DDL change that raced the fanout, even if every old
    // owner individually reported internally consistent coverage.
    var current = (try source.adminSnapshot()) orelse return error.ConstraintActivationPending;
    defer source.freeAdminSnapshot(&current);
    const current_table = tables.findTableByName(&current, name) orelse return error.TopologyChanged;
    if (!metadata.tableDefinitionsEqual(table.*, current_table.*)) return error.TopologyChanged;
    var found: usize = 0;
    for (current.ranges) |owner| if (owner.table_id == table.table_id) {
        found += 1;
        const prior = owner_by_group.fetchRemove(owner.group_id) orelse return error.TopologyChanged;
        if (!metadata.rangeRecordsEqual(prior.value.*, owner)) return error.TopologyChanged;
    };
    if (found != owners.items.len) return error.TopologyChanged;
    result.ranges = ranges;
    return std.json.Stringify.valueAlloc(alloc, result, .{});
}

test "relational declarations status requires complete matching owner coverage" {
    const alloc = std.testing.allocator;
    const Fixture = struct {
        update: *catalog_mod.Update,
        stale: bool = false,
        calls: usize = 0,
        changed_identity: bool = false,
        check_only: bool = false,
        snapshots: usize = 0,
        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const checked_schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        pub fn adminSnapshot(self: *@This()) !?@import("../metadata/api.zig").AdminSnapshot {
            self.snapshots += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = if (self.check_only) @constCast(&[_]metadata.TableRecord{.{ .table_id = 7, .name = "rows", .schema_json = checked_schema_json }}) else @constCast(&[_]metadata.TableRecord{.{ .table_id = 7, .name = "rows", .schema_json = schema_json }}),
                .ranges = if (self.changed_identity and self.snapshots > 1) @constCast(&[_]metadata.RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .range_id = 999, .start_key = "", .end_key = "\x00" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00", .end_key = null },
                }) else @constCast(&[_]metadata.RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .start_key = "", .end_key = "\x00" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00", .end_key = null },
                }),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *@import("../metadata/api.zig").AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (opts.relational_integrity_catalog) {
                try std.testing.expectEqualStrings("", key);
                const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(self.update.value.len));
                defer allocator.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, self.update.value);
                return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, .{ .catalog = encoded, .schema_version = @as(u32, 1), .table_id = "7" }, .{}) };
            }
            self.calls += 1;
            try std.testing.expectEqualStrings("{\"mode\":\"status\"}", opts.relational_activation_json);
            const first = key.len == 0;
            if (!first) try std.testing.expectEqualStrings("\x00", key);
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, .{
                .schema_version = @as(u32, if (self.stale) 2 else 1),
                .schema_digest = self.update.catalog.schema_digest,
                .generation_set = activation.generationSet(self.update.catalog),
                .owner = @as([32]u8, @splat(1)),
                .range_start = key,
                .range_end = if (first) "\x00" else "",
                .unique_covered = true,
                .state = "enforced",
                .phase = if (self.check_only) "check" else "foreign_key",
                .rows_scanned = @as(u64, 9007199254740993),
                .failure = "",
            }, .{}) };
        }
    };
    var update = try catalog_mod.prepare(alloc, null, try catalog_mod.incarnationFromTableId(7), 1, @splat(9), &.{.{ .kind = .unique, .name = "pk", .fingerprint = @splat(1) }});
    defer update.deinit();
    var fixture = Fixture{ .update = &update };
    const reader = reads.TableReadSource{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
    const body = try collect(alloc, &fixture, reader, "rows", .{});
    defer alloc.free(body);
    var result = try std.json.parseFromSlice(wire.RelationalConstraintStatus, alloc, body, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.value.ranges.len);
    try std.testing.expectEqualStrings("9007199254740993", result.value.ranges[0].rows_scanned);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    fixture.stale = true;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(alloc, &fixture, reader, "rows", .{}));
    fixture.stale = false;
    fixture.changed_identity = true;
    fixture.snapshots = 0;
    try std.testing.expectError(error.TopologyChanged, collect(alloc, &fixture, reader, "rows", .{}));
    var checked_validator = try schema.CompiledTableValidator.init(alloc, Fixture.checked_schema_json);
    defer checked_validator.deinit(alloc);
    var checked_update = try catalog_mod.prepareWithChecks(alloc, null, try catalog_mod.incarnationFromTableId(7), 1, @splat(9), &.{}, checked_validator.execution.checks.?.fingerprint());
    defer checked_update.deinit();
    fixture = .{ .update = &checked_update, .check_only = true };
    const checked_body = try collect(alloc, &fixture, reader, "rows", .{});
    defer alloc.free(checked_body);
    var checked_result = try std.json.parseFromSlice(wire.RelationalConstraintStatus, alloc, checked_body, .{});
    defer checked_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), checked_result.value.ranges.len);
    try std.testing.expectEqual(wire.RelationalConstraintActivationPhase.check, checked_result.value.ranges[0].phase);
    try std.testing.expectEqualStrings("unique_foreign_key_and_check", checked_result.value.coverage_kind);
    // Matching schema versions do not disguise a different CHECK definition.
    fixture.update = &update;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(alloc, &fixture, reader, "rows", .{}));
}

test "SQL relation lifetime unconstrained readiness requires actual initialized image and accepts current schema evolution" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const persistence = local.common_topology_records.RelationPersistence;
    const attest = @import("../metadata/sql_relation_attestation.zig");
    const boots = @import("../metadata/sql_relation_boots.zig");
    const client = @import("../storage/kernel_owner_client.zig");
    const abi = @import("kernel_owner_abi");
    const services = @import("../storage/kernel_runtime_services.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    var io = std.testing.io;
    const borrow = services.executor.Borrow.init(&io);
    var owner_context: client.Context = .{};
    try owner_context.ensureWithRuntime(.{ .io = &borrow });
    defer owner_context.deinit();
    const instance = (try owner_context.metrics()).execution_instance;
    const descriptor: persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = instance, .data_generation = 1 };
    const Fixture = struct {
        db: *client.Owner,
        declaration: []const u8,
        descriptor: persistence.Descriptor,
        observed: boots.Observation,
        actual_reads: usize = 0,
        const initial =
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const evolved =
            \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"payload":{"type":"keyword"}},"additionalProperties":false}}}}
        ;
        pub fn adminSnapshot(self: *@This()) !?@import("../metadata/api.zig").AdminSnapshot {
            const table = try a.create(metadata.TableRecord);
            table.* = .{ .table_id = 7, .name = "rows", .schema_json = self.declaration, .sql_persistence = self.descriptor };
            return .{ .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('a'), .metrics = .{} }, .tables = @as([*]metadata.TableRecord, @ptrCast(table))[0..1], .ranges = @constCast(&[_]metadata.RangeRecord{.{ .table_id = 7, .group_id = 11, .range_id = 11, .start_key = "", .end_key = null }}), .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        pub fn freeAdminSnapshot(_: *@This(), snapshot: *@import("../metadata/api.zig").AdminSnapshot) void {
            a.destroy(&snapshot.tables[0]);
        }
        pub fn systemCatalog(self: *@This(), allocator: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin);
            try std.testing.expect(call == .sql_relation_boot_read);
            try std.testing.expectEqual(@as(?u64, 7), call.sql_relation_boot_read.binding_table);
            return std.json.Stringify.valueAlloc(allocator, self.observed, .{});
        }
        fn lookupRouted(raw: *anyopaque, allocator: std.mem.Allocator, fence: @import("../metadata/api.zig").CatalogRouteFence, group: u64, name: []const u8, key: []const u8, opts: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 11), group);
            try std.testing.expectEqual(@as(u64, 7), fence.table_id);
            try std.testing.expect(fence.route.execution == .native_local);
            try std.testing.expect(fence.route.execution.native_local.eql(self.descriptor));
            return lookup(raw, allocator, name, key, opts, consistency);
        }
        fn lookup(raw: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.actual_reads += 1;
            try std.testing.expectEqualStrings("", key);
            try std.testing.expect(consistency == .read_index);
            try std.testing.expectEqualStrings("{\"mode\":\"sql_relation_binding\"}", opts.relational_topology_json);
            const body = try @import("table_reads.zig").encodeStorageKernelLookupRequest(allocator, key, opts);
            defer allocator.free(body);
            var actual = try self.db.lookupJson("rows", body);
            defer actual.deinit();
            return .{ .version = actual.version(), .json = try allocator.dupe(u8, actual.bytes()) };
        }
    };
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/native-ready", .{temporary.sub_path}, 0);
    defer a.free(path);
    const descriptor_json = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(descriptor_json);
    const host_json = try std.json.Stringify.valueAlloc(a, persistence.Context{ .node_id = 9, .store_id = 11, .execution_instance = instance, .generation_instance = instance, .data_generation = 1 }, .{});
    defer a.free(host_json);
    const boot_json = try std.json.Stringify.valueAlloc(a, persistence.Boot{ .instance = instance, .unclean_restart = false }, .{});
    defer a.free(boot_json);
    var db = try client.Owner.open(abi.OpenRequest{ .context = owner_context.handle, .path = .fromSlice(path), .table_name = .fromSlice("rows"), .group_id = 11, .has_identity_namespace = 1, .identity_table_id = 7, .identity_shard_id = 11, .identity_range_id = 11, .online_source_authority = 2, .sql_persistence_json = .fromSlice(descriptor_json), .sql_persistence_context_json = .fromSlice(host_json), .persistence_boot_json = .fromSlice(boot_json), .schema_json = .fromSlice(Fixture.initial), .indexes_json = .fromSlice("{}") });
    defer db.deinit();
    const captured = try db.nativeRelationBinding(a, "rows");
    const seed: [32]u8 = @splat(7);
    const keys = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 19, .seed = seed, .public_key = keys.public_key.toBytes() };
    const identity: @import("../metadata/store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key };
    const before = try attest.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .identity = identity, .reporter_incarnation = 1, .boot_generation = 1, .binding = captured }, root);
    var fixture: Fixture = .{ .db = &db, .declaration = Fixture.initial, .descriptor = descriptor, .observed = .{ .registration_current = true, .record = .{ .node_id = 9, .identity = identity, .reporter_incarnation = 1, .generation = 1, .instance = instance, .claim_digest = @splat(3), .phase = .ready, .unclean_restart = false } } };
    const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .lookup_group_local_routed = Fixture.lookupRouted, .scan = undefined, .query = undefined } };
    try std.testing.expectError(error.ConstraintActivationPending, collect(a, &fixture, reader, "rows", .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.actual_reads);
    fixture.observed.initialized = before;
    fixture.observed.registration_current = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, collect(a, &fixture, reader, "rows", .{}));
    fixture.observed.registration_current = true;
    const ready = try collect(a, &fixture, reader, "rows", .{});
    a.free(ready);
    fixture.observed.record.?.instance = @splat(2);
    try std.testing.expectError(error.SqlRelationOwnerChanged, collect(a, &fixture, reader, "rows", .{}));
    fixture.observed.record.?.instance = instance;
    fixture.observed.record.?.generation = 2;
    try std.testing.expectError(error.SqlRelationOwnerChanged, collect(a, &fixture, reader, "rows", .{}));
    fixture.observed.record.?.generation = 1;
    var wrong_image = before;
    wrong_image.binding.physical_root_incarnation += 1;
    fixture.observed.initialized = try attest.SignedBinding.sign(wrong_image, root);
    try std.testing.expectError(error.SqlRelationOwnerChanged, collect(a, &fixture, reader, "rows", .{}));
    fixture.observed.initialized = before;
    fixture.declaration = Fixture.evolved;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(a, &fixture, reader, "rows", .{}));
    try db.configure("rows", Fixture.evolved, "{}");
    // The initialized receipt still pins schema1. Real current schema2 on the
    // same actual primary image must become ready without replacing it.
    const evolved = try collect(a, &fixture, reader, "rows", .{});
    defer a.free(evolved);
    var status = try std.json.parseFromSlice(wire.RelationalConstraintStatus, a, evolved, .{});
    defer status.deinit();
    try std.testing.expectEqual(@as(u32, 2), status.value.schema_version);
    try std.testing.expect(status.value.state == .enforced);
}
