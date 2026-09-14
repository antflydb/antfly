// Copyright 2026 Antfly, Inc.
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

//! Bounded all-owner retirement. Metadata phase barriers separate admission
//! fencing, outgoing-reference drain, unique-claim drain, and publication.
//! Each row page and its checkpoint share the ordinary durable 2PC decision.
const std = @import("std");
const metadata = @import("../metadata/relational_retirement.zig");
const records = @import("../common/topology_records.zig");
const native = @import("../storage/db/relational_integrity_retirement.zig");
const catalog_mod = @import("../storage/db/relational_integrity_catalog.zig");
const activation = @import("../storage/db/relational_integrity_activation.zig");
const planner = @import("relational_integrity_commit.zig");
const reads = @import("table_read_source.zig");
const writes = @import("table_writes.zig");
const contract = @import("distributed_txn_contract.zig");
const schema_api = @import("../schema/mod.zig");
const schema = @import("../storage/schema.zig");
const Allocator = std.mem.Allocator;
const Control = @import("operation.zig").RequestContext;

fn equivalent(a: std.json.Value, b: std.json.Value, depth: usize) bool {
    if (depth > 128 or std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => |value| value == b.bool,
        .integer => |value| value == b.integer,
        .float => |value| value == b.float,
        .number_string => |value| std.mem.eql(u8, value, b.number_string),
        .string => |value| std.mem.eql(u8, value, b.string),
        .array => |value| blk: {
            if (value.items.len != b.array.items.len) break :blk false;
            for (value.items, b.array.items) |left, right| if (!equivalent(left, right, depth + 1)) break :blk false;
            break :blk true;
        },
        .object => |value| blk: {
            if (value.count() != b.object.count()) break :blk false;
            var iterator = value.iterator();
            while (iterator.next()) |entry| if (!equivalent(entry.value_ptr.*, b.object.get(entry.key_ptr.*) orelse break :blk false, depth + 1)) break :blk false;
            break :blk true;
        },
    };
}

const Status = struct { catalog: []const u8, progress: ?[]const u8, owner: [32]u8, range_start: []const u8, range_end: []const u8 };
fn readStatus(alloc: Allocator, reader: reads.TableReadSource, table: []const u8, start: []const u8, control: Control, include_catalog: bool) !std.json.Parsed(Status) {
    try control.ensureActive();
    const request_json = if (include_catalog) "{\"kind\":\"retirement\",\"mode\":\"status\"}" else "{\"kind\":\"retirement\",\"mode\":\"status\",\"include_catalog\":false}";
    var response = (try reader.lookup(alloc, table, start, .{ .relational_integrity_jobs_json = request_json, .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation }, .read_index)) orelse return error.IntegrityCatalogUnavailable;
    defer response.deinit(alloc);
    return std.json.parseFromSlice(Status, alloc, response.json, .{ .allocate = .alloc_always });
}

pub const Replacement = struct {
    arena: std.heap.ArenaAllocator,
    table: records.TableRecord,
    pub fn deinit(self: *Replacement) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Administrative caller must authorize source ADMIN and (for DROP) confirm
/// intent. The returned private record is admitted with the ordinary metadata
/// exact-definition CAS, never accepted through public TableSchema fields.
pub fn begin(alloc: Allocator, reader: reads.TableReadSource, tables: []const records.TableRecord, ranges: []const records.RangeRecord, table_name: []const u8, target_input: []const u8, drop: bool) !Replacement {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const table = for (tables) |table| {
        if (std.mem.eql(u8, table.name, table_name)) break table;
    } else return error.TableNotFound;
    if (table.relational_retirement_json.len != 0 or table.restore_backup_id.len != 0) return error.ConstraintRetirementInProgress;
    if (table.read_schema_json.len != 0) return error.TableTransitionActive;
    const normalized = try @import("tables.zig").applySchemaUpdateRecord(owned, &table, target_input);
    const target_json = normalized.schema_json;
    const target = try schema_api.parseValidatedTableSchema(owned, target_json);
    var source_value = try std.json.parseFromSlice(std.json.Value, owned, table.schema_json, .{ .allocate = .alloc_always });
    var target_value = try std.json.parseFromSlice(std.json.Value, owned, target_json, .{ .allocate = .alloc_always });
    for ([_][]const u8{ "version", "unique_constraints", "foreign_keys" }) |field| {
        _ = source_value.value.object.swapRemove(field);
        _ = target_value.value.object.swapRemove(field);
    }
    if (!equivalent(source_value.value, target_value.value, 0)) return error.InvalidConstraintRetirement;
    const target_runtime = try schema_api.deriveRuntimeTableSchema(owned, target);
    const target_bytes = try schema.serializeSchema(owned, target_runtime);
    const target_digest = metadata.digest(target_bytes);
    const definitions = try @import("../schema/relational_declarations.zig").definitionFingerprints(owned, target, target_runtime);
    var owners = std.ArrayList(metadata.Job.Owner).empty;
    for (ranges) |range| if (range.table_id == table.table_id) {
        if (range.restore_backup_id.len != 0) return error.TableTransitionActive;
        try owners.append(owned, .{ .group_id = range.group_id, .range_id = range.range_id, .start = range.start_key, .end = range.end_key orelse "" });
    };
    std.mem.sort(metadata.Job.Owner, owners.items, {}, struct {
        fn less(_: void, a: metadata.Job.Owner, b: metadata.Job.Owner) bool {
            return std.mem.order(u8, a.start, b.start) == .lt;
        }
    }.less);
    if (owners.items.len == 0 or owners.items.len > 4096) return error.TopologyChanged;
    const first = try readStatus(owned, reader, table.name, owners.items[0].start, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s }, true);
    const catalog = try catalog_mod.decode(owned, first.value.catalog);
    if (!std.mem.eql(u8, &catalog.incarnation, &(try catalog_mod.incarnationFromTableId(table.table_id))) or target_runtime.version != std.math.add(u32, catalog.schema_version, 1) catch return error.PreparedGenerationChanged) return error.PreparedGenerationChanged;
    for (definitions) |definition| {
        const existing = catalog.find(definition.kind, definition.name) orelse return error.InvalidConstraintRetirement;
        if (!std.mem.eql(u8, &existing.definition.fingerprint, &definition.fingerprint)) return error.InvalidConstraintRetirement;
    }
    var selected = std.ArrayList([16]u8).empty;
    for (catalog.bindings) |binding| {
        if (binding.retired) continue;
        const retained = for (definitions) |definition| {
            if (binding.definition.kind == definition.kind and std.mem.eql(u8, binding.definition.name, definition.name) and std.mem.eql(u8, &binding.definition.fingerprint, &definition.fingerprint)) break true;
        } else false;
        if (!retained) try selected.append(owned, binding.generation);
    }
    if (selected.items.len == 0) return error.ConstraintNotFound;
    if (drop and definitions.len != 0) return error.InvalidConstraintRetirement;
    const previous = try schema_api.parseValidatedTableSchema(owned, table.schema_json);
    // A retained FK owns references under one concrete UNIQUE generation;
    // an equivalent second UNIQUE does not transfer those references.
    for (tables) |candidate| {
        if (candidate.schema_json.len == 0) continue;
        const child = if (candidate.table_id == table.table_id) target else try schema_api.parseValidatedTableSchema(owned, candidate.schema_json);
        if (child.foreign_keys) |foreign| for (foreign.value) |fk| {
            if (!std.mem.eql(u8, fk.parent_table, table.name)) continue;
            if (previous.unique_constraints) |unique| for (unique.value) |definition| {
                const binding = catalog.find(.unique, definition.name) orelse return error.IntegrityCatalogChanged;
                const retiring = for (selected.items) |generation| {
                    if (std.mem.eql(u8, &generation, &binding.generation)) break true;
                } else false;
                if (!retiring or definition.columns.len != fk.parent_columns.len) continue;
                const matches = for (definition.columns, fk.parent_columns) |left, right| {
                    if (!std.mem.eql(u8, left, right)) break false;
                } else true;
                if (matches) return error.ForeignKeyReferenced;
            };
        };
    }
    // Admission is RESTRICT for external declarations. Removing the source's
    // outgoing FKs is safe; deleting another table's policy is never implicit.
    for (tables) |candidate| {
        if (candidate.table_id == table.table_id) continue;
        if (candidate.schema_json.len == 0) continue;
        const child = try schema_api.parseValidatedTableSchema(owned, candidate.schema_json);
        if (child.foreign_keys) |foreign| for (foreign.value) |fk| if (std.mem.eql(u8, fk.parent_table, table.name)) {
            if (drop) return error.ForeignKeyReferenced;
            try @import("../schema/relational_foreign_key_target.zig").validate(owned, candidate.schema_json, table.name, target_json);
        };
    }
    const generation_set = activation.generationSet(catalog);
    var identity: [64]u8 = undefined;
    @memcpy(identity[0..32], &generation_set);
    @memcpy(identity[32..64], &target_digest);
    const id = metadata.digest(&identity)[0..16].*;
    const job: metadata.Job = .{ .id = id, .source_schema_digest = metadata.digest(table.schema_json), .target_schema_digest = target_digest, .generation_set = generation_set, .generations = selected.items, .target_schema_json = target_json, .drop = drop, .owners = owners.items };
    try job.validate();
    var replacement = table;
    replacement.relational_retirement_json = try std.json.Stringify.valueAlloc(owned, job, .{});
    return .{ .arena = arena, .table = replacement };
}

pub fn finalize(alloc: Allocator, table: records.TableRecord) !Replacement {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var job = try metadata.parse(arena.allocator(), table.relational_retirement_json);
    if (job.value.failure.len != 0 or job.value.drop) return error.ConstraintRetirementInProgress;
    if (job.value.phase == .published) {
        if (table.read_schema_json.len != 0 or !std.mem.eql(u8, table.schema_json, job.value.target_schema_json)) return error.ConstraintRetirementInProgress;
        var replacement = table;
        replacement.relational_retirement_json = "";
        return .{ .arena = arena, .table = replacement };
    }
    if (job.value.phase != .ready) return error.ConstraintRetirementInProgress;
    var replacement = try @import("tables.zig").applySchemaUpdateRecord(arena.allocator(), &table, job.value.target_schema_json);
    if (!std.mem.eql(u8, replacement.schema_json, job.value.target_schema_json)) return error.ConstraintRetirementChanged;
    job.value.phase = .published;
    replacement.relational_retirement_json = try std.json.Stringify.valueAlloc(arena.allocator(), job.value, .{});
    return .{ .arena = arena, .table = replacement };
}

fn commit(alloc: Allocator, writer: writes.TableWriteSource, requests: []const contract.TableCommitRequest, control: Control) !void {
    try control.ensureActive();
    const outcome = (try writer.commitBatchWithCancellation(alloc, requests, .write, control.cancellation)) orelse return error.UnsupportedOperation;
    switch (outcome) {
        .committed => {},
        .conflict => return error.ConstraintRetirementChanged,
    }
}

/// At most one 128-row native page or one metadata phase transition per call.
/// A returned replacement must be durably CAS-published before later work.
pub fn retry(alloc: Allocator, table: records.TableRecord) !Replacement {
    return setFailure(alloc, table, "");
}

fn setFailure(alloc: Allocator, table: records.TableRecord, failure: []const u8) !Replacement {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var job = try metadata.parse(arena.allocator(), table.relational_retirement_json);
    if (failure.len == 0 and job.value.failure.len == 0) return error.InvalidConstraintRetirement;
    job.value.failure = failure;
    var replacement = table;
    replacement.relational_retirement_json = try std.json.Stringify.valueAlloc(arena.allocator(), job.value, .{});
    return .{ .arena = arena, .table = replacement };
}

pub fn runPage(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, tables: []const records.TableRecord, table: records.TableRecord, owner_start: []const u8) !?Replacement {
    return runPageAttempt(alloc, reader, writer, tables, table, owner_start) catch |err| switch (err) {
        error.TransactionTooLarge, error.RelationalRowResultTooLarge, error.ForeignKeyReferenced, error.InvalidIntegrityBudget => try setFailure(alloc, table, @errorName(err)),
        error.SchemaInUse, error.ConstraintRetirementChanged, error.PreparedGenerationChanged, error.IntegrityCatalogChanged => null,
        else => return err,
    };
}

fn runPageAttempt(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, tables: []const records.TableRecord, table: records.TableRecord, owner_start: []const u8) !?Replacement {
    if (table.relational_retirement_json.len == 0) return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const parsed_job = try metadata.parse(owned, table.relational_retirement_json);
    var job = parsed_job.value;
    if (job.phase == .ready or job.phase == .published or job.failure.len != 0) {
        arena.deinit();
        return null;
    }
    const control: Control = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s };
    const state = try readStatus(owned, reader, table.name, owner_start, control, true);
    const catalog = try catalog_mod.decode(owned, state.value.catalog);
    if (!std.mem.eql(u8, &job.generation_set, &activation.generationSet(catalog))) return error.ConstraintRetirementChanged;
    const expected_phase = metadata.phaseForOwner(job.phase);
    var progress: native.Progress = if (state.value.progress) |bytes| try native.Progress.decode(bytes) else .{
        .job_id = job.id,
        .generation_set = job.generation_set,
        .target_schema_digest = job.target_schema_digest,
        .owner = state.value.owner,
        .schema_version = catalog.schema_version,
        .generations = job.generations,
    };
    if (!std.mem.eql(u8, &progress.job_id, &job.id) or !std.mem.eql(u8, &progress.owner, &state.value.owner)) return error.ConstraintRetirementChanged;
    if (state.value.progress == null or @intFromEnum(progress.phase) < @intFromEnum(expected_phase)) {
        if (state.value.progress == null and expected_phase != .fenced) return error.ConstraintRetirementChanged;
        progress.phase = expected_phase;
        const command: native.Command = .{ .routing_key = owner_start, .expected = state.value.progress, .next = try progress.encode(owned) };
        try commit(owned, writer, &.{.{ .table_name = table.name, .relational_schema_version = catalog.schema_version, .relational_integrity_generation_set = job.generation_set, .relational_retirement = command }}, control);
        arena.deinit();
        return null;
    }
    if (progress.phase == expected_phase and expected_phase != .fenced) {
        var max_rows: u32 = 128;
        for (0..8) |_| {
            const request = try std.fmt.allocPrint(owned, "{{\"kind\":\"retirement\",\"mode\":\"page\",\"max_rows\":{d}}}", .{max_rows});
            const response = (try reader.lookup(owned, table.name, owner_start, .{ .relational_integrity_jobs_json = request, .execution_deadline_ns = control.deadline_ns }, .read_index)) orelse return error.ConstraintRetirementChanged;
            const page = try std.json.parseFromSlice(struct { rows: []const planner.BackfillRow, command: native.Command, phase: native.Phase }, owned, response.json, .{ .allocate = .alloc_always });
            const prepared = planner.prepareRetirementPage(owned, reader, tables, table.name, page.value.rows, try native.Progress.decode(page.value.command.expected.?), control) catch |err| {
                if (err == error.TransactionTooLarge and max_rows > 1) {
                    max_rows /= 2;
                    continue;
                }
                return err;
            };
            const requests = try owned.dupe(contract.TableCommitRequest, prepared.tables);
            const source = for (requests) |*request_table| {
                if (std.mem.eql(u8, request_table.table_name, table.name)) break request_table;
            } else return error.InvalidConstraintRetirement;
            source.relational_retirement = page.value.command;
            try commit(owned, writer, requests, control);
            arena.deinit();
            return null;
        }
        return error.TransactionTooLarge;
    }
    // Every phase boundary is an all-owner linearizable proof. Positive
    // evidence lives in metadata; no volatile across-request readiness cache.
    for (job.owners) |owner| {
        const peer = try readStatus(owned, reader, table.name, owner.start, control, false);
        if (!std.mem.eql(u8, peer.value.range_start, owner.start) or !std.mem.eql(u8, peer.value.range_end, owner.end)) return error.TopologyChanged;
        const proof = try native.Progress.decode(peer.value.progress orelse {
            arena.deinit();
            return null;
        });
        if (!std.mem.eql(u8, &proof.job_id, &job.id) or !std.mem.eql(u8, &proof.generation_set, &job.generation_set) or
            !std.mem.eql(u8, &proof.owner, &peer.value.owner) or proof.schema_version != catalog.schema_version or
            !std.mem.eql(u8, &proof.target_schema_digest, &job.target_schema_digest) or
            !std.mem.eql(u8, std.mem.sliceAsBytes(proof.generations), std.mem.sliceAsBytes(job.generations)) or
            (job.phase != .fencing and @intFromEnum(proof.phase) <= @intFromEnum(expected_phase)))
        {
            arena.deinit();
            return null;
        }
    }
    job.phase = switch (job.phase) {
        .fencing => .foreign_keys,
        .foreign_keys => .unique,
        .unique => .ready,
        .ready, .published => unreachable,
    };
    var replacement = table;
    replacement.relational_retirement_json = try std.json.Stringify.valueAlloc(owned, job, .{});
    return .{ .arena = arena, .table = replacement };
}

test "distributed txn retirement drains self foreign keys before unique claims with durable checkpoints" {
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const read_gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/retirement", .{tmp.sub_path});
    var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 400, .shard_id = 401, .range_id = 401 }, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const target =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, declaration);
    const Fixture = struct {
        db: *db_mod.DB,
        attempts: u8 = 0,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, options: types.LookupOptions, _: read_gate.ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const result = (try self.db.lookup(allocator, key, options)) orelse return null;
            return .{ .json = result.json, .version = try self.db.getTimestamp(allocator, key) };
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: read_gate.ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: read_gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
        fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: types.BatchRequest) !?void {
            return error.UnexpectedCall;
        }
        fn commitBatch(ptr: *anyopaque, _: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, _: @import("../common/cancellation.zig").CancellationToken) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), requests.len);
            const request = requests[0];
            self.attempts += 1;
            const timestamp = @as(u64, self.attempts) * 100;
            const transaction = try self.db.beginTransactionWithId(@splat(self.attempts), timestamp);
            self.db.writeTransaction(transaction, .{
                .writes = request.writes,
                .deletes = request.deletes,
                .predicates = request.predicates,
                .relational_schema_version = request.relational_schema_version,
                .relational_integrity_generation_set = request.relational_integrity_generation_set,
                .integrity_commands = request.integrity_commands,
                .relational_retirement = request.relational_retirement,
            }) catch |err| {
                try self.db.abortTransaction(transaction, timestamp + 1);
                return err;
            };
            try self.db.commitTransaction(transaction, timestamp + 1);
            return .{ .committed = .{ .participant_count = 1 } };
        }
    };
    var fixture: Fixture = .{ .db = &db };
    const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
    const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_batch_with_cancellation = Fixture.commitBatch } };
    var tables = [_]records.TableRecord{.{ .table_id = 400, .name = "rows", .schema_json = declaration }};
    const ranges = [_]records.RangeRecord{.{ .table_id = 400, .group_id = 401, .range_id = 401, .start_key = "" }};
    var initial = try planner.prepareWithCoverage(alloc, reader, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{ .{ .key = "a", .value = "{\"id\":1}" }, .{ .key = "b", .value = "{\"id\":2,\"parent\":1}" } } }});
    defer initial.deinit();
    try commit(alloc, writer, initial.tables, .{});
    const address = initial.tables[0].integrity_commands[0].address;
    try std.testing.expectError(error.ConstraintRetirementRequired, db.setSchemaJson(alloc, target));
    var job = try begin(alloc, reader, &tables, &ranges, "rows", target, false);
    defer job.deinit();
    tables[0] = job.table;
    var owned_updates = std.ArrayList(Replacement).empty;
    defer {
        for (owned_updates.items) |*update| update.deinit();
        owned_updates.deinit(alloc);
    }
    for (0..24) |_| {
        var state = try metadata.parse(alloc, tables[0].relational_retirement_json);
        defer state.deinit();
        if (state.value.phase == .ready) break;
        if (try runPage(alloc, reader, writer, &tables, tables[0], "")) |update| {
            try std.testing.expect(try metadata.transitionAllowed(alloc, tables[0], update.table));
            tables[0] = update.table;
            try owned_updates.append(alloc, update);
        }
    } else return error.RetirementDidNotConverge;
    const raw_claim = try db.core.getStoreValue(alloc, &address.claimKey());
    defer if (raw_claim) |bytes| alloc.free(bytes);
    try std.testing.expect(raw_claim == null);
    const row = (try db.lookup(alloc, "b", .{})).?;
    defer alloc.free(row.json);
    try std.testing.expect(std.mem.indexOf(u8, row.json, "parent") != null);
    try std.testing.expectError(error.ConstraintRetirementInProgress, commit(alloc, writer, initial.tables, .{}));
    var finalized = try finalize(alloc, tables[0]);
    defer finalized.deinit();
    try std.testing.expect(finalized.table.read_schema_json.len != 0);
    try std.testing.expect(finalized.table.relational_retirement_json.len != 0);
    try std.testing.expect(try metadata.transitionAllowed(alloc, tables[0], finalized.table));
    try db.setSchemaJson(alloc, finalized.table.schema_json);
    const proof = try db.core.getStoreValue(alloc, native.key);
    defer if (proof) |bytes| alloc.free(bytes);
    try std.testing.expect(proof == null);
    var migrated = finalized.table;
    migrated.read_schema_json = "";
    try std.testing.expect(try metadata.permitsMigrationCleanup(alloc, finalized.table, migrated));
    var completed = try finalize(alloc, migrated);
    defer completed.deinit();
    try std.testing.expectEqualStrings("", completed.table.relational_retirement_json);
    try std.testing.expect(try metadata.transitionAllowed(alloc, migrated, completed.table));
}
