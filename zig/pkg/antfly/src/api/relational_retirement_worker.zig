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
const native = @import("../storage/db/relational_integrity_retirement_contract.zig");
const catalog_mod = @import("../storage/db/relational_integrity_catalog.zig");
const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
const planner = @import("relational_integrity_commit.zig");
const reads = @import("table_read_source.zig");
const writes = @import("table_writes.zig");
const contract = @import("distributed_txn_contract.zig");
const schema_api = @import("../schema/mod.zig");
const schema = @import("../storage/schema.zig");
const Allocator = std.mem.Allocator;
const Control = @import("operation.zig").RequestContext;

test "SQL catalog retirement permits only deletion of ordered indexes" {
    const alloc = std.testing.allocator;
    const before = "{\"version\":1,\"storage_mode\":\"relational\",\"relational_indexes\":[{\"name\":\"drop\",\"keys\":[{\"column\":\"id\"}]},{\"name\":\"retain\",\"keys\":[{\"column\":\"label\"}]}],\"unique_constraints\":[{\"name\":\"drop\",\"columns\":[\"id\"]}]}";
    try std.testing.expect(try retainedSchemaEqual(alloc, before, "{\"version\":2,\"storage_mode\":\"relational\",\"relational_indexes\":[{\"keys\":[{\"column\":\"label\"}],\"name\":\"retain\"}],\"unique_constraints\":[]}"));
    try std.testing.expect(!try retainedSchemaEqual(alloc, before, "{\"version\":2,\"storage_mode\":\"relational\",\"relational_indexes\":[{\"name\":\"retain\",\"keys\":[{\"column\":\"id\"}]}],\"unique_constraints\":[]}"));
    try std.testing.expect(!try retainedSchemaEqual(alloc, before, "{\"version\":2,\"storage_mode\":\"relational\",\"relational_indexes\":[{\"name\":\"new\",\"keys\":[{\"column\":\"label\"}]}],\"unique_constraints\":[]}"));
    try std.testing.expect(!try retainedSchemaEqual(alloc, before, "{\"version\":2,\"storage_mode\":\"document\",\"relational_indexes\":[],\"unique_constraints\":[]}"));
}

fn retainedSchemaEqual(alloc: Allocator, source: []const u8, target: []const u8) !bool {
    var before = try std.json.parseFromSlice(std.json.Value, alloc, source, .{ .parse_numbers = false });
    defer before.deinit();
    var after = try std.json.parseFromSlice(std.json.Value, alloc, target, .{ .parse_numbers = false });
    defer after.deinit();
    if (before.value != .object or after.value != .object) return false;
    // Dropping a SQL-owned UNIQUE index retires its claims and removes the
    // corresponding ordered index in the same publication. Permit deletion
    // only: additions or changed retained indexes need their normal lifecycle.
    var prior_indexes: std.StringHashMapUnmanaged(std.json.Value) = .empty;
    defer prior_indexes.deinit(alloc);
    if (before.value.object.get("relational_indexes")) |indexes| if (indexes == .array) {
        for (indexes.array.items) |index| {
            const name = index.object.get("name") orelse return false;
            try prior_indexes.put(alloc, name.string, index);
        }
    };
    if (after.value.object.get("relational_indexes")) |indexes| if (indexes == .array) {
        for (indexes.array.items) |index| {
            const name = index.object.get("name") orelse return false;
            const prior = prior_indexes.get(name.string) orelse return false;
            const prior_json = try @import("../storage/db/document_content_hash.zig").canonicalJsonValueAlloc(alloc, prior);
            defer alloc.free(prior_json);
            const current_json = try @import("../storage/db/document_content_hash.zig").canonicalJsonValueAlloc(alloc, index);
            defer alloc.free(current_json);
            if (!std.mem.eql(u8, prior_json, current_json)) return false;
        }
    };
    _ = before.value.object.swapRemove("relational_indexes");
    _ = after.value.object.swapRemove("relational_indexes");
    for ([_][]const u8{ "version", "unique_constraints", "foreign_keys" }) |field| {
        _ = before.value.object.swapRemove(field);
        _ = after.value.object.swapRemove(field);
    }
    // Exact decimal canonicalization accepts equivalent numeric spellings
    // without rounding large integers/decimals or losing small nonzero values.
    const canonical = @import("../storage/db/document_content_hash.zig").canonicalJsonValueAlloc;
    const left = try canonical(alloc, before.value);
    defer alloc.free(left);
    const right = try canonical(alloc, after.value);
    defer alloc.free(right);
    return std.mem.eql(u8, left, right);
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
    return beginControlled(alloc, reader, tables, ranges, table_name, target_input, drop, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s });
}

pub fn beginControlled(alloc: Allocator, reader: reads.TableReadSource, tables: []const records.TableRecord, ranges: []const records.RangeRecord, table_name: []const u8, target_input: []const u8, drop: bool, control: Control) !Replacement {
    try control.ensureActive();
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
    if (!try retainedSchemaEqual(owned, table.schema_json, target_json)) return error.InvalidConstraintRetirement;
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
    const first = try readStatus(owned, reader, table.name, owners.items[0].start, control, true);
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
        try control.ensureActive();
        if (candidate.schema_json.len == 0) continue;
        const child = if (candidate.table_id == table.table_id) target else try schema_api.parseValidatedTableSchema(owned, candidate.schema_json);
        if (child.foreign_keys) |foreign| for (foreign.value) |fk| {
            if (!std.mem.eql(u8, fk.parent_table, table.name)) continue;
            if (previous.unique_constraints) |unique| for (unique.value) |definition| {
                const binding = catalog.find(.unique, definition.name) orelse return error.IntegrityCatalogChanged;
                const retiring = for (selected.items) |generation| {
                    if (std.mem.eql(u8, &generation, &binding.generation)) break true;
                } else false;
                const columns = definition.columns orelse continue;
                if (!retiring or columns.len != fk.parent_columns.len) continue;
                if (definition.where) |conditions| if (conditions.len != 0) continue;
                const matches = for (columns, fk.parent_columns) |left, right| {
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

const DrainBudget = struct {
    rows: u32 = 128,

    fn shrink(self: *DrainBudget, observed: usize) bool {
        // Native pages can stop early for their byte/CPU budget. Halving the
        // requested cap alone would repeatedly prepare the same actual page.
        if (observed <= 1 or self.rows <= 1) return false;
        self.rows = @intCast(@max(@as(usize, 1), @min(self.rows / 2, observed / 2)));
        return true;
    }
};

fn drainPage(alloc: Allocator, reader: reads.TableReadSource, writer: writes.TableWriteSource, tables: []const records.TableRecord, table: records.TableRecord, owner_start: []const u8, expected: []const u8, control: Control) !void {
    var budget: DrainBudget = .{};
    for (0..8) |_| {
        try control.ensureActive();
        // Each failed admission releases its projections, parsed rows, and
        // derived intents before another attempt, bounding peak memory to one
        // page instead of retaining up to eight independent prepared arenas.
        var attempt = std.heap.ArenaAllocator.init(alloc);
        defer attempt.deinit();
        const owned = attempt.allocator();
        const request = try std.fmt.allocPrint(owned, "{{\"kind\":\"retirement\",\"mode\":\"page\",\"max_rows\":{d}}}", .{budget.rows});
        var response = (try reader.lookup(owned, table.name, owner_start, .{
            .relational_integrity_jobs_json = request,
            .execution_deadline_ns = control.deadline_ns,
            .execution_io = control.deadline_io,
            .cancellation = control.cancellation,
        }, .read_index)) orelse return error.ConstraintRetirementChanged;
        defer response.deinit(owned);
        var page = try std.json.parseFromSlice(struct { rows: []const planner.BackfillRow, command: native.Command, phase: native.Phase }, owned, response.json, .{ .allocate = .alloc_always });
        defer page.deinit();
        if (!std.mem.eql(u8, page.value.command.expected orelse return error.ConstraintRetirementChanged, expected) or
            !std.mem.eql(u8, page.value.command.routing_key, owner_start)) return error.ConstraintRetirementChanged;
        const progress = try native.Progress.decode(expected);
        if (page.value.phase != progress.phase) return error.ConstraintRetirementChanged;
        var prepared = planner.prepareRetirementPage(owned, reader, tables, table.name, page.value.rows, progress, control) catch |err| {
            if (err == error.TransactionTooLarge and budget.shrink(page.value.rows.len)) continue;
            return err;
        };
        defer prepared.deinit();
        const requests = try owned.dupe(contract.TableCommitRequest, prepared.tables);
        const source = for (requests) |*request_table| {
            if (std.mem.eql(u8, request_table.table_name, table.name)) break request_table;
        } else return error.InvalidConstraintRetirement;
        source.relational_retirement = page.value.command;
        commit(owned, writer, requests, control) catch |err| {
            // Participant fanout and encoded intent overhead are only known
            // at distributed admission. Treat this pre-decision rejection
            // just like planner pressure; never retry ambiguous outcomes or
            // advance the owner checkpoint independently of its detachments.
            if (err == error.TransactionTooLarge and budget.shrink(page.value.rows.len)) continue;
            return err;
        };
        return;
    }
    return error.TransactionTooLarge;
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
    const deadline = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
    const control: Control = .{ .deadline_ns = deadline, .cancellation = .{ .ptr = &deadline, .is_cancelled_fn = struct {
        fn expired(ptr: *const anyopaque) bool {
            const value: *const u64 = @ptrCast(@alignCast(ptr));
            return @import("antfly_platform").time.monotonicNs() >= value.*;
        }
    }.expired } };
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
        try drainPage(alloc, reader, writer, tables, table, owner_start, state.value.progress orelse return error.ConstraintRetirementChanged, control);
        arena.deinit();
        return null;
    }
    // Verify at most one owner per tick. The topology/job is frozen and native
    // progress is monotonic, making this durable prefix valid across process
    // restarts and leadership changes. Rescanning all owners under a fresh
    // deadline would never finish for sufficiently large tables.
    {
        const owner = job.owners[job.verified_owners];
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
    job.verified_owners += 1;
    if (job.verified_owners == job.owners.len) {
        job.phase = switch (job.phase) {
            .fencing => .foreign_keys,
            .foreign_keys => .unique,
            .unique => .ready,
            .ready, .published => unreachable,
        };
        job.verified_owners = 0;
    }
    var replacement = table;
    replacement.relational_retirement_json = try std.json.Stringify.valueAlloc(owned, job, .{});
    return .{ .arena = arena, .table = replacement };
}

test "distributed txn retirement compares retained schema numbers without rounding" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try retainedSchemaEqual(alloc,
        \\{"version":0,"unique_constraints":[{}],"default":1.0,"nested":{"x":9007199254740993.0}}
    ,
        \\{"nested":{"x":90071992547409930e-1},"default":1,"version":1}
    ));
    try std.testing.expect(!try retainedSchemaEqual(alloc,
        \\{"default":9007199254740993.0}
    ,
        \\{"default":9007199254740992}
    ));
    try std.testing.expect(!try retainedSchemaEqual(alloc, "{\"default\":1e-999}", "{\"default\":0}"));
    try std.testing.expect(!try retainedSchemaEqual(alloc, "{\"checks\":[{}]}", "{\"checks\":[]}"));
}

test "distributed txn retirement drains unique claims with durable checkpoints" {
    try testRetirementDrain(.none);
}

test "distributed txn retirement adapts commit headroom without advancing rejected checkpoints" {
    try testRetirementDrain(.page);
}

test "distributed txn retirement retains singleton failure and resumes the same proof after explicit retry" {
    try testRetirementDrain(.singleton);
}

test "distributed txn retirement budget uses observed page size" {
    var budget: DrainBudget = .{};
    try std.testing.expect(budget.shrink(3));
    try std.testing.expectEqual(@as(u32, 1), budget.rows);
    try std.testing.expect(!budget.shrink(1));
}

const RetirementPressure = enum { none, page, singleton };

test "distributed txn retirement verifies large owner barriers in bounded restartable slices" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var update = try catalog_mod.prepare(alloc, null, try catalog_mod.incarnationFromTableId(501), 1, @splat(1), &.{.{ .kind = .unique, .name = "pk", .fingerprint = @splat(2) }});
    defer update.deinit();
    const owners = try owned.alloc(metadata.Job.Owner, 129);
    for (owners, 0..) |*owner, i| owner.* = .{
        .group_id = i + 1,
        .range_id = i + 1,
        .start = if (i == 0) "" else try std.fmt.allocPrint(owned, "{d:0>4}", .{i}),
        .end = if (i + 1 == owners.len) "" else try std.fmt.allocPrint(owned, "{d:0>4}", .{i + 1}),
    };
    const job: metadata.Job = .{
        .id = @splat(3),
        .source_schema_digest = metadata.digest("{}"),
        .target_schema_digest = @splat(4),
        .generation_set = activation.generationSet(update.catalog),
        .generations = &.{update.catalog.bindings[0].generation},
        .target_schema_json = "{\"version\":2}",
        .owners = owners,
    };
    const Fixture = struct {
        job: metadata.Job,
        catalog: []const u8,
        reads_this_tick: usize = 0,
        expected_owner: usize = 0,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, options: @import("../storage/db/types.zig").LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (options.cancellation) |cancellation| try cancellation.check();
            self.reads_this_tick += 1;
            try std.testing.expect(self.reads_this_tick <= 2);
            const owner = if (self.reads_this_tick == 1) self.job.owners[0] else self.job.owners[self.expected_owner];
            try std.testing.expectEqualStrings(owner.start, key);
            const proof: native.Progress = .{
                .job_id = self.job.id,
                .generation_set = self.job.generation_set,
                .owner = metadata.digest(owner.start),
                .target_schema_digest = self.job.target_schema_digest,
                .schema_version = 1,
                .phase = .ready,
                .generations = self.job.generations,
            };
            const progress = try proof.encode(allocator);
            defer allocator.free(progress);
            // Binary owner/range/checkpoint fields retain the same codec as
            // the native endpoint; no live native state is cached by worker.
            var output: std.Io.Writer.Allocating = .init(allocator);
            defer output.deinit();
            var json: std.json.Stringify = .{ .writer = &output.writer };
            try @import("../storage/db/relational_integrity_json.zig").write(Status{ .catalog = self.catalog, .progress = progress, .owner = proof.owner, .range_start = owner.start, .range_end = owner.end }, &json);
            return .{ .json = try allocator.dupe(u8, output.written()), .version = 0 };
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: @import("../storage/db/types.zig").ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: @import("../storage/db/types.zig").SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
        fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: @import("../storage/db/types.zig").BatchRequest) !?void {
            return error.UnexpectedCall;
        }
    };
    var fixture: Fixture = .{ .job = job, .catalog = update.value };
    const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
    const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch } };
    var table: records.TableRecord = .{ .table_id = 501, .name = "rows", .schema_json = "{}", .relational_retirement_json = try std.json.Stringify.valueAlloc(alloc, job, .{}) };
    defer alloc.free(table.relational_retirement_json);
    for (0..owners.len * 3) |tick| {
        fixture.reads_this_tick = 0;
        fixture.expected_owner = tick % owners.len;
        var replacement = (try runPage(alloc, reader, writer, &.{table}, table, "")) orelse return error.MissingBarrierContinuation;
        defer replacement.deinit();
        try std.testing.expectEqual(@as(usize, 2), fixture.reads_this_tick);
        try std.testing.expect(try metadata.transitionAllowed(alloc, table, replacement.table));
        // Retain only a serialized metadata checkpoint between ticks, just as
        // a replacement supervisor does after process restart or failover.
        const persisted = try alloc.dupe(u8, replacement.table.relational_retirement_json);
        alloc.free(table.relational_retirement_json);
        table.relational_retirement_json = persisted;
    }
    var result = try metadata.parse(alloc, table.relational_retirement_json);
    defer result.deinit();
    try std.testing.expectEqual(metadata.Phase.ready, result.value.phase);
    try std.testing.expectEqual(@as(u32, 0), result.value.verified_owners);
}

fn testRetirementDrain(pressure: RetirementPressure) !void {
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const read_gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/retirement", .{tmp.sub_path});
    const open_options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 400, .shard_id = 401, .range_id = 401 }, .primary_backend = .{ .lsm = .{} } };
    var db = try db_mod.DB.open(alloc, path, open_options);
    var db_open = true;
    defer if (db_open) db.close();
    const declaration =
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"pk","keys":[{"column":"id"}],"description":"SQL UNIQUE INDEX"},{"name":"retained","keys":[{"column":"parent"}]}],"unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const target =
        \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"retained","keys":[{"column":"parent"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, declaration);
    const Fixture = struct {
        db: *db_mod.DB,
        attempts: u8 = 0,
        pressure: RetirementPressure,
        rejections: usize = 0,
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
        fn commitBatch(ptr: *anyopaque, allocator: Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel, cancellation: @import("../common/cancellation.zig").CancellationToken) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try cancellation.check();
            try std.testing.expectEqual(@as(usize, 1), requests.len);
            const request = requests[0];
            if (request.relational_retirement) |command| {
                if (request.predicates.len != 0 and (self.pressure == .singleton or (self.pressure == .page and request.predicates.len > 1))) {
                    // Distributed wire/participant headroom may reject an
                    // otherwise valid prepared page before the decision.
                    const durable = (try self.db.core.getStoreValue(allocator, native.key)).?;
                    defer allocator.free(durable);
                    try std.testing.expectEqualStrings(command.expected.?, durable);
                    self.rejections += 1;
                    return error.TransactionTooLarge;
                }
            }
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
    var fixture: Fixture = .{ .db = &db, .pressure = pressure };
    const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
    const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_batch_with_cancellation = Fixture.commitBatch } };
    var tables = [_]records.TableRecord{.{ .table_id = 400, .name = "rows", .schema_json = declaration }};
    const ranges = [_]records.RangeRecord{.{ .table_id = 400, .group_id = 401, .range_id = 401, .start_key = "" }};
    if (pressure == .none) {
        const guarded_declaration =
            \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"pk","keys":[{"column":"id"}],"description":"SQL UNIQUE INDEX"},{"name":"retained","keys":[{"column":"parent"}]}],"unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const guarded_tables = [_]records.TableRecord{.{ .table_id = 400, .name = "rows", .schema_json = guarded_declaration }};
        try std.testing.expectError(error.ForeignKeyGenerationPublicationRequired, begin(alloc, reader, &guarded_tables, &ranges, "rows", target, false));
    }
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
    var failure_retried = false;
    var reopened = false;
    for (0..32) |_| {
        var state = try metadata.parse(alloc, tables[0].relational_retirement_json);
        defer state.deinit();
        if (state.value.phase == .foreign_keys and !reopened) {
            const before = (try db.core.getStoreValue(alloc, native.key)).?;
            defer alloc.free(before);
            db.close();
            db_open = false;
            db = try db_mod.DB.open(alloc, path, open_options);
            db_open = true;
            try db.setSchemaJson(alloc, declaration);
            const after = (try db.core.getStoreValue(alloc, native.key)).?;
            defer alloc.free(after);
            try std.testing.expectEqualStrings(before, after);
            reopened = true;
        }
        if (state.value.failure.len != 0) {
            try std.testing.expectEqual(RetirementPressure.singleton, pressure);
            try std.testing.expect(!failure_retried);
            try std.testing.expectEqualStrings("TransactionTooLarge", state.value.failure);
            const before = (try db.core.getStoreValue(alloc, native.key)).?;
            defer alloc.free(before);
            // A paused job does no storage work. Administrative retry only
            // clears the diagnostic, retaining its exact owner continuation.
            try std.testing.expect(try runPage(alloc, reader, writer, &tables, tables[0], "") == null);
            const update = try retry(alloc, tables[0]);
            try std.testing.expect(try metadata.transitionAllowed(alloc, tables[0], update.table));
            tables[0] = update.table;
            try owned_updates.append(alloc, update);
            const after = (try db.core.getStoreValue(alloc, native.key)).?;
            defer alloc.free(after);
            try std.testing.expectEqualStrings(before, after);
            fixture.pressure = .none;
            failure_retried = true;
            continue;
        }
        if (state.value.phase == .ready) break;
        if (try runPage(alloc, reader, writer, &tables, tables[0], "")) |update| {
            try std.testing.expect(try metadata.transitionAllowed(alloc, tables[0], update.table));
            tables[0] = update.table;
            try owned_updates.append(alloc, update);
        }
    } else return error.RetirementDidNotConverge;
    if (pressure != .none) try std.testing.expect(fixture.rejections > 0);
    try std.testing.expectEqual(pressure == .singleton, failure_retried);
    const raw_claim = try db.core.getStoreValue(alloc, &address.claimKey());
    defer if (raw_claim) |bytes| alloc.free(bytes);
    try std.testing.expect(raw_claim == null);
    const row = (try db.lookup(alloc, "b", .{})).?;
    defer alloc.free(row.json);
    try std.testing.expect(std.mem.indexOf(u8, row.json, "parent") != null);
    try std.testing.expectError(error.ConstraintRetirementInProgress, commit(alloc, writer, initial.tables, .{}));
    try std.testing.expect(reopened);
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
