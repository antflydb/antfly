// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Public mutation adapter. Read versions become durable 2PC predicates, and
//! semantic claim/reference commands execute on their independently routed
//! owners. No preflight-only parent lookup can authorize a child write.
const std = @import("std");
const contract = @import("distributed_txn_contract.zig");
const reads = @import("table_read_source.zig");
const schema_api = @import("../schema/mod.zig");
const declarations = @import("../schema/relational_declarations.zig");
const schema = @import("../storage/schema.zig");
const registry = @import("../storage/db/schema_registry.zig");
const mapper = @import("../storage/db/document_mapper.zig");
const catalog = @import("../storage/db/relational_integrity_catalog.zig");
const planner = @import("relational_integrity.zig");
const types = @import("../storage/db/types.zig");
const native = @import("../storage/relational_index.zig");
const TableRecord = @import("../metadata/table_manager.zig").TableRecord;
const RangeRecord = @import("../metadata/table_manager.zig").RangeRecord;
const Allocator = std.mem.Allocator;
const RequestContext = @import("operation.zig").RequestContext;
var preparation_diagnostic_gate: @import("bounded_diagnostic_gate.zig").Gate = .{};

/// ScanOptions has a platform-monotonic deadline but no borrowed clock. Move
/// the remaining budget across that boundary; never copy an Io-awake epoch.
fn partialWitnessScanOptions(control: RequestContext, query: []const u8) !types.ScanOptions {
    const scan_control = try control.platformDeadline();
    return .{ .relational_query_json = query, .include_documents = true, .limit = 4096, .execution_deadline_ns = scan_control.deadline_ns, .cancellation = scan_control.cancellation };
}

fn boundedControl(request: RequestContext) !RequestContext {
    var result = request;
    const now_ns = if (request.deadline_io) |borrow| blk: {
        var receiver = try borrow.receive();
        break :blk @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, receiver.io()).nanoseconds)));
    } else @import("antfly_platform").time.monotonicNs();
    result.deadline_ns = @min(request.deadline_ns orelse std.math.maxInt(u64), now_ns +| 5 * std.time.ns_per_s);
    try result.ensureActive();
    return result;
}

pub fn requiresCoordination(alloc: Allocator, schema_json: []const u8) !bool {
    if (schema_json.len == 0) return false;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return (if (parsed.unique_constraints) |values| values.value.len != 0 else false) or
        (if (parsed.foreign_keys) |values| values.value.len != 0 else false);
}

/// Row-local CHECK enforcement does not add fanout to ordinary mutations.
/// Historical coverage nevertheless shares distributed activation machinery.
pub fn requiresActivation(alloc: Allocator, schema_json: []const u8) !bool {
    if (schema_json.len == 0) return false;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return (if (parsed.unique_constraints) |values| values.value.len != 0 else false) or
        (if (parsed.foreign_keys) |values| values.value.len != 0 else false) or
        (if (parsed.checks) |values| values.value.len != 0 else false);
}

/// Public schema epochs are fences, not evidence that dependency expansion
/// happened. Never forward a client epoch through an unavailable/stale catalog
/// and let the storage receiver mistake it for internal coordination proof.
pub fn metadataRequiresCoordination(alloc: Allocator, metadata: ?[]const TableRecord, requests: []const contract.TableCommitRequest) !bool {
    const tables = metadata orelse {
        for (requests) |request| if (request.relational_schema_version != null) return error.IntegrityCatalogUnavailable;
        return false;
    };
    var coordinated = false;
    for (requests) |request| {
        const record = for (tables) |table| {
            if (std.mem.eql(u8, table.name, request.table_name)) break table;
        } else return error.TableNotFound;
        if (request.relational_schema_version) |version| {
            if (record.schema_json.len == 0) return error.PreparedGenerationChanged;
            var parsed = try schema_api.parseValidatedTableSchema(alloc, record.schema_json);
            defer parsed.deinit(alloc);
            if (parsed.version != version or parsed.storage_mode != .relational) return error.PreparedGenerationChanged;
        }
        coordinated = coordinated or try requiresCoordination(alloc, record.schema_json);
    }
    return coordinated;
}

pub fn authorizePrimaryMutations(request: RequestContext, authentication_required: bool, tables: []const contract.TableCommitRequest) !void {
    for (tables) |table| {
        if (table.writes.len == 0 and table.deletes.len == 0 and table.transforms.len == 0) continue;
        if (request.table_write_authorization) |authorization| {
            if (!authorization.allows(authorization.ptr, table.table_name)) return error.Forbidden;
        } else if (authentication_required or request.principal != null) return error.Forbidden;
    }
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    /// Original primary request bytes remain borrowed through commit. Added
    /// predicates and commands are owned by this preparation arena.
    tables: []const contract.TableCommitRequest,
    /// CHECK activation failures retain the prepared physical source guards;
    /// the worker commits them with an invalid checkpoint, never coverage.
    validation_failure: ?[]const u8 = null,
    /// MATCH PARTIAL can switch witnesses after preparation. One missing
    /// attachment is not proof that the logical dependency has no witness.
    backfill_partial: bool = false,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const Loaded = struct {
    name: []const u8,
    view: registry.SchemaView,
    catalog: catalog.Catalog,
    uniques: []const native.UniqueConstraint,
    foreign: []const native.ForeignKey,
    plan: ?planner.Plan = null,
};

const Work = struct {
    table: *Loaded,
    key: []const u8,
    before: ?reads.LookupResponse,
    after: ?[]const u8,
    observed_version: u64 = 0,
    observed_digest: ?[32]u8 = null,
    assignments: std.StringHashMapUnmanaged(std.json.Value) = .empty,
    queued: bool = false,
    dirty: bool = false,
    explicit: bool = false,
    // Immutable statement input, before any cascading edge changes `after`.
    explicit_after: ?[]const u8 = null,
    expansion: ?planner.Expansion = null,
};

// Use the same scalar coercions as row preparation. JSON spelling is not
// equality (notably timestamps and exact integer number strings).
fn assignmentEqual(table: *const Loaded, name: []const u8, a: std.json.Value, b: std.json.Value) !bool {
    const columns = table.view.tableSchema().relational_columns;
    const ordinal = table.view.physicalLayout().ordinalForName(columns, name) orelse return error.InvalidIntegrityRecord;
    return scalarAssignmentEqual(columns[ordinal].column_type, a, b);
}

fn scalarAssignmentEqual(kind: schema.RelationalColumnType, a: std.json.Value, b: std.json.Value) !bool {
    if (a == .null or b == .null) return a == .null and b == .null;
    return switch (kind) {
        .datetime => (schema_api.documentDateTimeToNs(a) orelse return error.InvalidIntegrityRecord) ==
            (schema_api.documentDateTimeToNs(b) orelse return error.InvalidIntegrityRecord),
        .integer => (schema_api.documentIntegerToI64(a) orelse return error.InvalidIntegrityRecord) ==
            (schema_api.documentIntegerToI64(b) orelse return error.InvalidIntegrityRecord),
        .number => (schema_api.documentNumberToF64(a) orelse return error.InvalidIntegrityRecord) ==
            (schema_api.documentNumberToF64(b) orelse return error.InvalidIntegrityRecord),
        .boolean => if (a == .bool and b == .bool) a.bool == b.bool else error.InvalidIntegrityRecord,
        .string, .blob => if (a == .string and b == .string) std.mem.eql(u8, a.string, b.string) else error.InvalidIntegrityRecord,
        else => error.InvalidIntegrityRecord,
    };
}

test "distributed txn cascade scalar equality uses row coercions without losing integer precision" {
    try std.testing.expect(try scalarAssignmentEqual(.datetime, .{ .string = "2026-01-02T00:00:00Z" }, .{ .string = "2026-01-02T00:00:00.000Z" }));
    try std.testing.expect(try scalarAssignmentEqual(.integer, .{ .number_string = "9007199254740993.0" }, .{ .integer = 9007199254740993 }));
    try std.testing.expect(!try scalarAssignmentEqual(.integer, .{ .number_string = "9007199254740993.0" }, .{ .integer = 9007199254740992 }));
    try std.testing.expect(try scalarAssignmentEqual(.number, .{ .integer = 1 }, .{ .float = 1.0 }));
    try std.testing.expect(!try scalarAssignmentEqual(.number, .{ .integer = 1 }, .{ .integer = 2 }));
    try std.testing.expect(try scalarAssignmentEqual(.integer, .null, .null));
    try std.testing.expect(!try scalarAssignmentEqual(.integer, .null, .{ .integer = 0 }));
    try std.testing.expectError(error.InvalidIntegrityRecord, scalarAssignmentEqual(.datetime, .{ .string = "invalid" }, .{ .integer = 0 }));
}

const Builder = struct {
    const FinalReference = struct { child: *Work, definition: native.ForeignKey, address: planner.storage.Address, reference: planner.storage.Reference };
    const FinalPartial = struct { child: *Work, dependency: planner.PartialDependency };
    const Witness = struct { address: planner.storage.Address, key: []const u8 };
    const Witnesses = struct { required: bool, values: []const Witness };
    alloc: Allocator,
    source: reads.TableReadSource,
    read_view: ?*reads.JoinReadView = null,
    read_view_attempted: bool = false,
    metadata: []const TableRecord,
    loaded: std.ArrayList(*Loaded) = .empty,
    output: std.ArrayList(contract.TableCommitRequest) = .empty,
    command_lists: std.ArrayList(std.ArrayList(planner.storage.Command)) = .empty,
    predicate_lists: std.ArrayList(std.ArrayList(types.TransactionVersionPredicate)) = .empty,
    work: std.ArrayList(*Work) = .empty,
    by_row: std.StringHashMapUnmanaged(*Work) = .empty,
    queue: std.ArrayList(*Work) = .empty,
    final_references: std.ArrayList(FinalReference) = .empty,
    partial_rechecks: std.ArrayList(FinalPartial) = .empty,
    prepared_bytes: usize = 0,
    control: RequestContext = .{},
    repair_table: ?[]const u8 = null,
    statement: bool = false,
    previous: []const contract.TableCommitRequest = &.{},

    fn previousRow(self: *Builder, table: []const u8, key: []const u8) ?struct { value: ?[]const u8 } {
        for (self.previous) |request| if (std.mem.eql(u8, request.table_name, table)) {
            for (request.deletes) |deleted| if (std.mem.eql(u8, deleted, key)) return .{ .value = null };
            var i = request.writes.len;
            while (i != 0) {
                i -= 1;
                if (std.mem.eql(u8, request.writes[i].key, key)) return .{ .value = request.writes[i].value };
            }
        };
        return null;
    }

    fn repairing(self: *const Builder, name: []const u8) bool {
        return if (self.repair_table) |target| std.mem.eql(u8, target, name) else false;
    }

    fn lookup(self: *Builder, table: []const u8, key: []const u8, options: types.LookupOptions) !?reads.LookupResponse {
        try self.control.ensureActive();
        // Preparation observes many primary rows and claims. Pin one routing
        // generation for the request instead of fetching the catalog for each
        // point read. Owner read-index and commit predicates still establish
        // data consistency; the view only amortizes immutable route planning.
        if (!self.read_view_attempted) {
            self.read_view = try self.source.acquireJoinView(self.alloc, .{ .clock = .{ .deadline_ns = self.control.deadline_ns, .io = self.control.deadline_io }, .cancellation = self.control.cancellation });
            if (self.read_view) |view| self.source = view.source;
            self.read_view_attempted = true;
        }
        var opts = options;
        opts.include_primary_digest = !options.relational_integrity_catalog and !options.relational_integrity_action and
            options.relational_integrity_jobs_json.len == 0 and options.relational_index_status_json.len == 0 and options.relational_activation_json.len == 0 and options.relational_topology_json.len == 0;
        opts.execution_deadline_ns = self.control.deadline_ns;
        opts.execution_io = self.control.deadline_io;
        opts.cancellation = self.control.cancellation;
        return self.source.lookup(self.alloc, table, key, opts, .read_index) catch |err| {
            if (preparation_diagnostic_gate.admit(@import("antfly_platform").time.monotonicNs())) std.log.warn("preparation lookup failed catalog={} jobs={} work={d} class={s}", .{ opts.relational_integrity_catalog, opts.relational_integrity_jobs_json.len != 0, self.work.items.len, @errorName(err) });
            return err;
        };
    }

    fn charge(self: *Builder, bytes: usize) !void {
        self.prepared_bytes = std.math.add(usize, self.prepared_bytes, bytes) catch return error.TransactionTooLarge;
        if (self.prepared_bytes > 16 * 1024 * 1024) return error.TransactionTooLarge;
    }

    fn partialMatches(self: *Builder, child: *Loaded, child_json: []const u8, parent: *Loaded, parent_json: []const u8, definition: native.ForeignKey) !bool {
        try self.control.ensureActive();
        const input_bytes = std.math.add(usize, child_json.len, parent_json.len) catch return error.TransactionTooLarge;
        const retained = std.math.mul(usize, input_bytes, 16) catch return error.TransactionTooLarge;
        const columns = std.math.add(usize, child.view.tableSchema().relational_columns.len, parent.view.tableSchema().relational_columns.len) catch return error.TransactionTooLarge;
        const column_bytes = std.math.mul(usize, columns, 256) catch return error.TransactionTooLarge;
        const overhead = std.math.add(usize, column_bytes, 4096) catch return error.TransactionTooLarge;
        try self.charge(std.math.add(usize, retained, overhead) catch return error.TransactionTooLarge);
        // Activation/retirement supply only the declared FK columns. Normal
        // mutation preparation validates the complete row before this matcher.
        var child_row = try mapper.PreparedRelationalProjection.init(self.alloc, child_json, child.view.tableSchema().*, child.view.physicalLayout(), definition.child_columns);
        defer child_row.deinit();
        var parent_row = try mapper.PreparedRelationalWrite.init(self.alloc, "", parent_json, null, parent.view.tableSchema().*, parent.view.physicalLayout());
        defer parent_row.deinit(self.alloc);
        const child_view = child_row.view;
        const parent_view = try parent_row.typedView(parent.view.tableSchema().*, parent.view.physicalLayout());
        var child_keys = std.ArrayList(native.RelationalIndexKey).empty;
        var parent_keys = std.ArrayList(native.RelationalIndexKey).empty;
        for (definition.child_columns, definition.parent_columns) |left, right| {
            const ordinal = child_view.ordinalForName(left) orelse return error.InvalidIntegrityDefinition;
            const cell = (try child_view.findCell(ordinal)) orelse continue;
            if (cell.is_null) continue;
            try child_keys.append(self.alloc, .{ .column = left });
            try parent_keys.append(self.alloc, .{ .column = right });
        }
        if (child_keys.items.len == 0) return false;
        const tuple_mod = @import("../storage/db/relational_index_keys.zig");
        var left = try tuple_mod.TuplePlan.init(self.alloc, child.view.tableSchema().*, child.view.physicalLayout(), child_keys.items);
        defer left.deinit();
        var right = try tuple_mod.TuplePlan.init(self.alloc, parent.view.tableSchema().*, parent.view.physicalLayout(), parent_keys.items);
        defer right.deinit();
        var left_tuple = try left.encodeAlloc(self.alloc, child_view);
        defer left_tuple.deinit(self.alloc);
        var right_tuple = try right.encodeAlloc(self.alloc, parent_view);
        defer right_tuple.deinit(self.alloc);
        return std.mem.eql(u8, left_tuple.bytes, right_tuple.bytes);
    }

    fn partialWitness(self: *Builder, parent: *Loaded, dependency: planner.PartialDependency, key: []const u8, json: []const u8) !Witness {
        const definition = for (parent.uniques) |candidate| {
            const binding = parent.catalog.find(.unique, candidate.name) orelse return error.PreparedGenerationChanged;
            if (std.mem.eql(u8, &binding.generation, &dependency.parent_generation)) break candidate;
        } else return error.ForeignKeyTargetNotUnique;
        var row = try mapper.PreparedRelationalWrite.init(self.alloc, key, json, null, parent.view.tableSchema().*, parent.view.physicalLayout());
        defer row.deinit(self.alloc);
        const keys = try self.alloc.alloc(native.RelationalIndexKey, definition.columns.len);
        for (keys, definition.columns) |*target, column| target.* = .{ .column = column };
        var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(self.alloc, parent.view.tableSchema().*, parent.view.physicalLayout(), keys);
        defer tuple.deinit();
        const encoded = (try planner.encodeUnique(self.alloc, tuple, try row.typedView(parent.view.tableSchema().*, parent.view.physicalLayout()), definition.nulls_not_distinct, key)) orelse return error.InvalidIntegrityRecord;
        return .{ .address = try planner.storage.Address.init(dependency.parent_generation, encoded), .key = try self.alloc.dupe(u8, key) };
    }

    fn partialWitnesses(self: *Builder, dependency: planner.PartialDependency, optional_json: ?[]const u8, final_rows: bool) !Witnesses {
        const json = optional_json orelse return .{ .required = false, .values = &.{} };
        const child = try self.load(dependency.reference.child_table);
        const parent = try self.load(dependency.definition.parent_table);
        const parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, json, .{ .parse_numbers = false });
        defer parsed.deinit();
        const Condition = struct { column: []const u8, op: []const u8 = "eq", value: std.json.Value };
        var conditions = std.ArrayList(Condition).empty;
        for (dependency.definition.child_columns, dependency.definition.parent_columns) |left, right| {
            const value = parsed.value.object.get(left) orelse continue;
            if (value == .null) continue;
            try conditions.append(self.alloc, .{ .column = right, .value = value });
        }
        if (conditions.items.len == 0) return .{ .required = false, .values = &.{} };
        // Never fall back to a full parent scan. A schema-owned equality index
        // makes every supported non-null mask selectively seekable.
        const support = try @import("../schema/relational_witness_indexes.zig").select(self.alloc, parent.view.validator().?.schema, conditions.items);
        const fields = try self.alloc.alloc([]const u8, parent.view.tableSchema().relational_columns.len);
        for (fields, parent.view.tableSchema().relational_columns) |*field, column| field.* = column.name;
        const query = try std.json.Stringify.valueAlloc(self.alloc, .{ .schema_version = parent.view.version(), .fields = fields, .index = support.name, .lower = .{ .values = support.values }, .upper = .{ .values = support.values }, .conditions = conditions.items }, .{});
        try self.control.ensureActive();
        var response = (try self.source.scan(self.alloc, parent.name, "", "", try partialWitnessScanOptions(self.control, query), .read_index)) orelse return error.TableNotFound;
        defer response.deinit(self.alloc);
        try self.charge(response.ndjson.len);
        var witnesses = std.ArrayList(Witness).empty;
        var seen = std.StringHashMapUnmanaged(void).empty;
        var lines = std.mem.splitScalar(u8, response.ndjson, '\n');
        var scanned: usize = 0;
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            try self.control.ensureActive();
            scanned += 1;
            if (scanned >= 4096) return error.TransactionTooLarge;
            const entry = try std.json.parseFromSlice(struct { _id: []const u8, row: std.json.Value }, self.alloc, line, .{ .ignore_unknown_fields = true, .parse_numbers = false });
            defer entry.deinit();
            const identity = try self.rowIdentity(parent.name, entry.value._id);
            if (self.previousRow(parent.name, entry.value._id) != null) continue;
            if (final_rows) if (self.by_row.get(identity)) |work| if (work.dirty) continue;
            const value = try std.json.Stringify.valueAlloc(self.alloc, entry.value.row, .{});
            // The typed matcher independently verifies provider filtering and
            // keeps comparison/null semantics identical to ordinary claims.
            if (!try self.partialMatches(child, json, parent, value, dependency.definition)) return error.InvalidIntegrityRecord;
            const witness = try self.partialWitness(parent, dependency, entry.value._id, value);
            try seen.put(self.alloc, witness.key, {});
            try witnesses.append(self.alloc, witness);
        }
        for (self.previous) |request| if (std.mem.eql(u8, request.table_name, parent.name)) {
            for (request.writes) |write| {
                const overlay = self.previousRow(parent.name, write.key) orelse continue;
                const value = overlay.value orelse continue;
                const identity = try self.rowIdentity(parent.name, write.key);
                if (final_rows) if (self.by_row.get(identity)) |work| if (work.dirty) continue;
                if (seen.contains(write.key) or !try self.partialMatches(child, json, parent, value, dependency.definition)) continue;
                const witness = try self.partialWitness(parent, dependency, write.key, value);
                try seen.put(self.alloc, witness.key, {});
                try witnesses.append(self.alloc, witness);
            }
        };
        if (final_rows) for (self.work.items) |work| {
            if (!work.dirty or work.table != parent or seen.contains(work.key)) continue;
            const value = work.after orelse continue;
            if (!try self.partialMatches(child, json, parent, value, dependency.definition)) continue;
            try witnesses.append(self.alloc, try self.partialWitness(parent, dependency, work.key, value));
        };
        return .{ .required = true, .values = try witnesses.toOwnedSlice(self.alloc) };
    }

    fn appendPartial(self: *Builder, dependency: planner.PartialDependency) !void {
        const before = try self.partialWitnesses(dependency, dependency.before, false);
        const after = try self.partialWitnesses(dependency, dependency.after, true);
        if (after.required and after.values.len == 0 and !(self.statement and dependency.definition.timing == .deferred)) return error.ForeignKeyParentMissing;
        for (before.values) |witness| try self.appendCommand(dependency.definition.parent_table, .{ .address = witness.address, .operation = if (dependency.repair) .{ .repair_detach = dependency.reference } else .{ .detach = dependency.reference } });
        for (after.values) |witness| try self.appendCommand(dependency.definition.parent_table, .{ .address = witness.address, .operation = .{ .attach = dependency.reference } });
    }

    fn rowIdentity(self: *Builder, table: []const u8, key: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.alloc, "{d}:{s}{s}", .{ table.len, table, key });
    }

    fn getWork(self: *Builder, table: *Loaded, key: []const u8) !*Work {
        const identity = try self.rowIdentity(table.name, key);
        if (self.by_row.get(identity)) |existing| return existing;
        if (self.work.items.len >= 4096) return error.TransactionTooLarge;
        return self.recordWork(table, key, try self.lookup(table.name, key, .{}));
    }

    fn recordWork(self: *Builder, table: *Loaded, key: []const u8, observation: ?reads.LookupResponse) !*Work {
        const identity = try self.rowIdentity(table.name, key);
        if (self.by_row.get(identity)) |existing| return existing;
        if (self.work.items.len >= 4096) return error.TransactionTooLarge;
        var old = observation;
        if (old) |row| if (row.expected_content_digest == null)
            return error.MissingPrimaryObservation;
        const observed_version = if (old) |row| row.version else 0;
        const observed_digest = if (old) |row| row.expected_content_digest else null;
        if (self.previousRow(table.name, key)) |overlay| {
            const version = if (old) |row| row.version else 0;
            const digest = if (old) |row| row.expected_content_digest else null;
            old = if (overlay.value) |json| .{ .json = try self.alloc.dupe(u8, json), .version = version, .expected_content_digest = digest } else null;
        }
        try self.charge(key.len + if (old) |row| row.json.len else @as(usize, 0));
        const item = try self.alloc.create(Work);
        item.* = .{ .table = table, .key = try self.alloc.dupe(u8, key), .before = old, .after = if (old) |row| row.json else null, .observed_version = observed_version, .observed_digest = observed_digest };
        try self.work.append(self.alloc, item);
        try self.by_row.put(self.alloc, identity, item);
        return item;
    }

    /// Independent primary observations may share a routing view, but never
    /// the builder's mutable arena. Drain every bounded Io task before copying
    /// results and publishing predicates on the caller, including on failure.
    fn preloadWork(self: *Builder, table: *Loaded, keys: []const []const u8) !void {
        const borrow = self.control.fanout_io orelse self.control.deadline_io orelse return;
        if (keys.len < 2) return;
        const Slot = struct {
            arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
            row: ?reads.LookupResponse = null,
            failure: ?anyerror = null,
            fn run(slot: *@This(), source: reads.TableReadSource, name: []const u8, key: []const u8, control: RequestContext) void {
                slot.row = source.lookup(slot.arena.allocator(), name, key, .{ .include_primary_digest = true, .execution_deadline_ns = control.deadline_ns, .execution_io = control.deadline_io, .cancellation = control.cancellation }, .read_index) catch |err| {
                    slot.failure = err;
                    return;
                };
            }
        };
        var receiver = try borrow.receive();
        const io = receiver.io();
        const width = 8;
        var start: usize = 0;
        while (start < keys.len) : (start += width) {
            try self.control.ensureActive();
            var slots: [width]Slot = @splat(.{});
            defer for (&slots) |*slot| slot.arena.deinit();
            const batch = keys[start..@min(start + width, keys.len)];
            var tasks: std.Io.Group = .init;
            for (batch, 0..) |key, index| tasks.async(io, Slot.run, .{ &slots[index], self.source, table.name, key, self.control });
            tasks.await(io) catch return error.Cancelled;
            try self.control.ensureActive();
            for (slots[0..batch.len]) |slot| if (slot.failure) |err| return err;
            for (batch, slots[0..batch.len]) |key, slot| {
                var owned = slot.row;
                if (owned) |*row| {
                    if (row.json.len > 16 * 1024 * 1024 -| self.prepared_bytes) return error.TransactionTooLarge;
                    row.json = try self.alloc.dupe(u8, row.json);
                }
                _ = try self.recordWork(table, key, owned);
            }
        }
    }

    fn enqueue(self: *Builder, item: *Work) !void {
        // Prior expansions may still be traversed by a cyclic cascade. Their
        // storage lives in the request arena; only invalidate the cache here.
        item.expansion = null;
        item.dirty = true;
        if (item.queued) return;
        try self.queue.append(self.alloc, item);
        item.queued = true;
        if (self.queue.items.len > 16_384) return error.TransactionTooLarge;
    }

    fn expandWork(self: *Builder, item: *Work) !planner.Expansion {
        if (item.expansion) |expansion| return expansion;
        const plan = try self.bindingPlan(item.table);
        const view = item.table.view;
        var before: ?mapper.PreparedRelationalWrite = if (item.before) |row| try mapper.PreparedRelationalWrite.init(self.alloc, item.key, row.json, null, view.tableSchema().*, view.physicalLayout()) else null;
        defer if (before) |*row| row.deinit(self.alloc);
        var after: ?mapper.PreparedRelationalWrite = if (item.after) |json| try mapper.PreparedRelationalWrite.init(self.alloc, item.key, json, view.validator(), view.tableSchema().*, view.physicalLayout()) else null;
        defer if (after) |*row| row.deinit(self.alloc);
        if (after) |*row| if (view.validator()) |validator| if (validator.execution.expressions != null) {
            // Cascading parent assignments and final participant writes must
            // use the same normalized row used to derive integrity claims.
            const normalized = try std.json.Stringify.valueAlloc(self.alloc, row.parsedValue(), .{});
            try self.charge(normalized.len);
            item.after = normalized;
        };
        item.expansion = try plan.expand(self.alloc, &.{.{ .key = item.key, .before = if (before) |*row| try row.typedView(view.tableSchema().*, view.physicalLayout()) else null, .after = if (after) |*row| try row.typedView(view.tableSchema().*, view.physicalLayout()) else null, .repair = self.repairing(item.table.name) }});
        try self.charge(item.expansion.?.arena.queryCapacity());
        return item.expansion.?;
    }

    fn childStillReferences(self: *Builder, item: *Work, definition: native.ForeignKey, address: planner.storage.Address, final_row: bool) !bool {
        const after = item.after orelse return false;
        // Explicit moves sever their old relationship. A row already moved by
        // another cascading edge still participates in its original edges, so
        // contradictory cascades are detected rather than silently skipped.
        const json = if (final_row) after else if (item.explicit) item.explicit_after orelse return false else if (item.before) |before| before.json else after;
        const view = item.table.view;
        var row = try mapper.PreparedRelationalWrite.init(self.alloc, item.key, json, if (item.explicit) view.validator() else null, view.tableSchema().*, view.physicalLayout());
        defer row.deinit(self.alloc);
        const keys = try self.alloc.alloc(native.RelationalIndexKey, definition.child_columns.len);
        for (keys, definition.child_columns) |*key, column| key.* = .{ .column = column };
        var tuple_plan = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(self.alloc, view.tableSchema().*, view.physicalLayout(), keys);
        defer tuple_plan.deinit();
        var tuple = try tuple_plan.encodeAlloc(self.alloc, try row.typedView(view.tableSchema().*, view.physicalLayout()));
        defer tuple.deinit(self.alloc);
        if (tuple.has_null) return false;
        return std.meta.eql(address, try planner.storage.Address.init(address.generation, tuple.bytes));
    }

    fn applyReference(self: *Builder, parent: *Work, transition: planner.ParentTransition, reference: planner.storage.Reference) !void {
        const child_table = try self.load(reference.child_table);
        const binding = child_table.catalog.findGeneration(reference.constraint_generation) orelse return error.PreparedGenerationChanged;
        if (binding.definition.kind != .foreign_key or !std.mem.eql(u8, binding.definition.name, reference.constraint_name)) return error.InvalidIntegrityRecord;
        var parsed = try std.json.parseFromSlice(native.ForeignKey, self.alloc, binding.definition.payload, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const definition = parsed.value;
        if (!std.mem.eql(u8, definition.parent_table, parent.table.name)) return error.InvalidIntegrityRecord;
        const action = if (parent.after == null) definition.on_delete else definition.on_update;
        if (action == .restrict and definition.match != .partial) return;
        const child = try self.getWork(child_table, reference.child_key);
        if (definition.match == .partial) {
            const child_value = child.after orelse return;
            const parent_value = if (parent.before) |before| before.json else return error.InvalidIntegrityRecord;
            if (!try self.partialMatches(child_table, child_value, parent.table, parent_value, definition)) return;
            if (self.partial_rechecks.items.len >= 4096) return error.TransactionTooLarge;
            const copied = try std.json.Stringify.valueAlloc(self.alloc, definition, .{});
            const owned_definition = try std.json.parseFromSliceLeaky(native.ForeignKey, self.alloc, copied, .{ .allocate = .alloc_always });
            const dependency: planner.PartialDependency = .{
                .definition = owned_definition,
                .parent_generation = transition.address.generation,
                .reference = .{ .child_table = child.table.name, .child_key = child.key, .constraint_name = owned_definition.name, .constraint_generation = reference.constraint_generation },
                .before = if (child.before) |before| before.json else null,
                .after = child.after,
                .repair = self.repairing(child.table.name),
            };
            try self.partial_rechecks.append(self.alloc, .{ .child = child, .dependency = dependency });
            const witnesses = try self.partialWitnesses(dependency, child.after, true);
            if (!witnesses.required or witnesses.values.len != 0) return;
            // Referential actions apply only when the final compatible
            // witness disappears, not whenever one of several parents moves.
            if (action == .restrict) return error.ForeignKeyReferenced;
            if (action == .no_action) return;
        }
        if (action == .no_action) {
            // A public mutation request is one statement/transaction boundary.
            // NO ACTION observes its complete final row set (also for an
            // initially deferred declaration), whereas RESTRICT never detaches
            // an unchanged relationship to make a parent replacement legal.
            // Keep only owned generation-bound data after the bounded ref page
            // is released; the final commands are retained by participant 2PC.
            if (self.final_references.items.len >= 4096) return error.TransactionTooLarge;
            const copied = try std.json.Stringify.valueAlloc(self.alloc, definition, .{});
            const owned_definition = try std.json.parseFromSliceLeaky(native.ForeignKey, self.alloc, copied, .{ .allocate = .alloc_always });
            try self.final_references.append(self.alloc, .{ .child = child, .definition = owned_definition, .address = transition.address, .reference = .{
                .child_table = child.table.name,
                .child_key = child.key,
                .constraint_name = owned_definition.name,
                .constraint_generation = reference.constraint_generation,
            } });
            return;
        }
        // An explicit mutation may already move/delete this child. A stale
        // reverse reference may not force a mutation of its new relationship.
        if (definition.match != .partial and !try self.childStillReferences(child, definition, transition.address, false)) return;
        if (action == .cascade and parent.after == null) {
            child.after = null;
            try self.enqueue(child);
            return;
        }
        var child_json = try std.json.parseFromSliceLeaky(std.json.Value, self.alloc, child.after.?, .{ .allocate = .alloc_always, .parse_numbers = false });
        // Parsed values are retained in the shared request arena by assignments.
        const object = &child_json.object;
        const parent_json: ?std.json.Value = if (parent.after) |json| try std.json.parseFromSliceLeaky(std.json.Value, self.alloc, json, .{ .allocate = .alloc_always, .parse_numbers = false }) else null;
        var changed = false;
        for (definition.child_columns, definition.parent_columns) |child_column, parent_column| {
            // MATCH PARTIAL's NULL components are wildcards, not assignments.
            // CASCADE only transports components the child actually supplies.
            if (definition.match == .partial and action == .cascade and (object.get(child_column) orelse .null) == .null) continue;
            const next: std.json.Value = if (action == .set_null) .null else parent_json.?.object.get(parent_column) orelse return error.InvalidIntegrityRecord;
            if (child.assignments.get(child_column)) |prior| {
                if (!try assignmentEqual(child.table, child_column, prior, next)) return error.ForeignKeyActionConflict;
            }
            const existing = object.get(child_column) orelse .null;
            if (try assignmentEqual(child.table, child_column, existing, next)) continue;
            try child.assignments.put(self.alloc, try self.alloc.dupe(u8, child_column), next);
            try object.put(self.alloc, child_column, next);
            changed = true;
        }
        if (changed) {
            child.after = try std.json.Stringify.valueAlloc(self.alloc, child_json, .{});
            try self.charge(child.after.?.len);
            try self.enqueue(child);
        }
    }

    const StatementState = struct {
        claim: ?planner.storage.Claim = null,
        references: std.ArrayList(planner.storage.Reference) = .empty,
    };

    fn sameReference(a: planner.storage.Reference, b: planner.storage.Reference) bool {
        return std.mem.eql(u8, a.child_table, b.child_table) and std.mem.eql(u8, a.child_key, b.child_key) and
            std.mem.eql(u8, a.constraint_name, b.constraint_name) and std.mem.eql(u8, &a.constraint_generation, &b.constraint_generation);
    }

    fn deferredReference(self: *Builder, reference: planner.storage.Reference) !bool {
        const child = try self.load(reference.child_table);
        const binding = child.catalog.findGeneration(reference.constraint_generation) orelse return error.PreparedGenerationChanged;
        if (binding.definition.kind != .foreign_key) return error.InvalidIntegrityRecord;
        const definition = try std.json.parseFromSliceLeaky(native.ForeignKey, self.alloc, binding.definition.payload, .{ .ignore_unknown_fields = true });
        return definition.deferrable and definition.timing == .deferred;
    }

    fn applyStatementCommands(self: *Builder, state: *StatementState, address: planner.storage.Address, commands: []const planner.storage.Command, validate: bool) !void {
        for (0..5) |phase| for (commands) |command| {
            if (!std.meta.eql(address, command.address)) continue;
            const command_phase: usize = switch (command.operation) {
                .detach, .repair_detach => 0,
                .check_owner => 1,
                .release, .repair_release => 2,
                .establish => 3,
                .attach => 4,
                else => return error.InvalidIntegrityCommand,
            };
            if (phase != command_phase) continue;
            switch (command.operation) {
                .detach, .repair_detach => |reference| {
                    var i: usize = 0;
                    while (i < state.references.items.len) {
                        if (sameReference(reference, state.references.items[i])) _ = state.references.swapRemove(i) else i += 1;
                    }
                },
                .check_owner => |owner| if (validate) {
                    const claim = state.claim orelse return error.ForeignKeyParentMissing;
                    if (!std.mem.eql(u8, claim.parent_table, owner.parent_table) or !std.mem.eql(u8, claim.parent_key, owner.parent_key)) return error.UniqueConstraintViolation;
                },
                .release, .repair_release => {
                    if (validate and state.references.items.len != 0) return error.ForeignKeyReferenced;
                    state.claim = null;
                },
                .establish => |claim| {
                    if (validate) if (state.claim) |old| {
                        if (!std.mem.eql(u8, old.parent_table, claim.parent_table) or !std.mem.eql(u8, old.parent_key, claim.parent_key)) return error.UniqueConstraintViolation;
                    };
                    state.claim = claim;
                },
                .attach => |reference| {
                    if (validate and !try self.deferredReference(reference)) {
                        const claim = state.claim orelse return error.ForeignKeyParentMissing;
                        if (claim.state != .live) return error.ForeignKeyActionInProgress;
                    }
                    const found = for (state.references.items) |old| {
                        if (sameReference(reference, old)) break true;
                    } else false;
                    if (!found) try state.references.append(self.alloc, reference);
                },
                else => unreachable,
            }
        };
    }

    fn statementState(self: *Builder, table: []const u8, address: planner.storage.Address) !StatementState {
        var state: StatementState = .{};
        var continuation: ?[]const u8 = null;
        var count: usize = 0;
        while (true) {
            const query = try std.json.Stringify.valueAlloc(self.alloc, .{ .kind = "references", .address = address, .after = continuation, .limit = @as(u32, 128) }, .{});
            var response = (try self.lookup(table, &address.routing, .{ .relational_integrity_jobs_json = query })) orelse break;
            defer response.deinit(self.alloc);
            try self.charge(response.json.len);
            const page = try std.json.parseFromSliceLeaky(struct { address: planner.storage.Address, claim: planner.storage.Claim, references: []const planner.storage.Reference, next: ?[]const u8 = null }, self.alloc, response.json, .{ .allocate = .alloc_always });
            if (!std.meta.eql(page.address, address)) return error.InvalidIntegrityRecord;
            state.claim = page.claim;
            count += page.references.len + 1;
            if (count > 4096) return error.TransactionTooLarge;
            try state.references.appendSlice(self.alloc, page.references);
            const next = page.next orelse break;
            if (continuation) |old| if (std.mem.eql(u8, old, next)) return error.InvalidIntegrityContinuation;
            continuation = next;
        }
        for (self.previous) |request| if (std.mem.eql(u8, request.table_name, table)) try self.applyStatementCommands(&state, address, request.integrity_commands, false);
        return state;
    }

    fn validateStatement(self: *Builder) !void {
        var seen: std.AutoHashMapUnmanaged(planner.storage.Address, void) = .empty;
        for (self.output.items, self.command_lists.items) |table, commands| for (commands.items) |command| {
            if ((try seen.getOrPut(self.alloc, command.address)).found_existing) continue;
            var state = try self.statementState(table.table_name, command.address);
            try self.applyStatementCommands(&state, command.address, commands.items, true);
        };
    }

    fn expandParent(self: *Builder, item: *Work, transition: planner.ParentTransition) !void {
        if (self.statement) {
            const state = try self.statementState(item.table.name, transition.address);
            for (state.references.items) |reference| try self.applyReference(item, transition, reference);
            return;
        }
        var continuation: ?[]const u8 = null;
        var pages: usize = 0;
        while (true) {
            pages += 1;
            if (pages > 4096) return error.TransactionTooLarge;
            const request = try std.json.Stringify.valueAlloc(self.alloc, .{ .kind = "references", .address = transition.address, .after = continuation, .limit = @as(u32, 128) }, .{});
            var response = (try self.lookup(item.table.name, &transition.address.routing, .{ .relational_integrity_jobs_json = request })) orelse {
                if (self.repairing(item.table.name)) return;
                return error.ForeignKeyParentMissing;
            };
            defer response.deinit(self.alloc);
            try self.charge(response.json.len);
            var page = try std.json.parseFromSlice(struct { address: planner.storage.Address, claim: planner.storage.Claim, references: []const planner.storage.Reference, next: ?[]const u8 = null }, self.alloc, response.json, .{ .allocate = .alloc_always });
            // References copied into work own their selected bytes; continuation
            // is copied below before releasing this independently bounded page.
            defer page.deinit();
            if (!std.meta.eql(page.value.address, transition.address)) return error.InvalidIntegrityRecord;
            if (!std.mem.eql(u8, page.value.claim.parent_table, item.table.name) or !std.mem.eql(u8, page.value.claim.parent_key, item.key)) {
                if (self.repairing(item.table.name)) return;
                return error.InvalidIntegrityRecord;
            }
            for (page.value.references) |reference| try self.applyReference(item, transition, reference);
            const next = page.value.next orelse break;
            if (continuation) |previous| if (std.mem.eql(u8, previous, next)) return error.InvalidIntegrityContinuation;
            continuation = try self.alloc.dupe(u8, next);
        }
    }

    fn deinit(self: *Builder) void {
        if (self.read_view) |view| view.deinit();
        for (self.loaded.items) |table| {
            if (table.plan) |*plan| plan.deinit();
            table.view.release();
            table.catalog.deinit();
        }
    }

    fn metadataTable(self: *Builder, name: []const u8) !TableRecord {
        for (self.metadata) |table| if (std.mem.eql(u8, name, table.name)) return table;
        return error.TableNotFound;
    }

    fn load(self: *Builder, name: []const u8) !*Loaded {
        for (self.loaded.items) |table| if (std.mem.eql(u8, name, table.name)) return table;
        const record = try self.metadataTable(name);
        if (record.schema_json.len == 0) return error.ForeignKeyTargetNotUnique;
        var validator = try schema_api.CompiledTableValidator.init(self.alloc, record.schema_json);
        var validator_owned = true;
        defer if (validator_owned) validator.deinit(self.alloc);
        const runtime = try schema_api.deriveRuntimeTableSchema(self.alloc, validator.schema);
        var runtime_owned = true;
        defer if (runtime_owned) schema.freeSchema(self.alloc, runtime);
        const epoch = try registry.Epoch.createOwnedValidated(self.alloc, runtime, validator);
        runtime_owned = false;
        validator_owned = false;
        var view: registry.SchemaView = .{ .epoch = epoch };
        errdefer view.release();
        // Route by the exact first-range boundary, like integrityCatalog().
        // Private reads carry their operation in options, never a synthetic
        // document key that can select a different owner or transport path.
        var response = (try self.lookup(name, "", .{ .relational_integrity_catalog = true })) orelse return error.IntegrityCatalogUnavailable;
        defer response.deinit(self.alloc);
        var envelope = try std.json.parseFromSlice(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, self.alloc, response.json, .{});
        defer envelope.deinit();
        if (envelope.value.schema_version != view.version() or (std.fmt.parseInt(u64, envelope.value.table_id, 10) catch return error.InvalidIntegrityCatalog) != record.table_id) return error.PreparedGenerationChanged;
        const size = std.base64.standard.Decoder.calcSizeForSlice(envelope.value.catalog) catch return error.InvalidIntegrityCatalog;
        if (size > catalog.max_catalog_bytes) return error.InvalidIntegrityCatalog;
        const bytes = try self.alloc.alloc(u8, size);
        defer self.alloc.free(bytes);
        std.base64.standard.Decoder.decode(bytes, envelope.value.catalog) catch return error.InvalidIntegrityCatalog;
        var bindings = try catalog.decode(self.alloc, bytes);
        errdefer bindings.deinit();
        if (bindings.schema_version != view.version() or !std.mem.eql(u8, &bindings.incarnation, &(try catalog.incarnationFromTableId(record.table_id)))) return error.PreparedGenerationChanged;
        const native_schema = try schema.serializeSchema(self.alloc, view.tableSchema().*);
        defer self.alloc.free(native_schema);
        var schema_digest: planner.storage.Digest = undefined;
        std.crypto.hash.Blake3.hash(native_schema, &schema_digest, .{});
        if (!std.mem.eql(u8, &schema_digest, &bindings.schema_digest)) return error.PreparedGenerationChanged;
        const checks_digest: [32]u8 = if (view.validator().?.execution.checks) |checks| checks.fingerprint() else @splat(0);
        if (!std.mem.eql(u8, &checks_digest, &bindings.checks_digest)) return error.PreparedGenerationChanged;
        const expected = try declarations.definitionFingerprints(self.alloc, view.validator().?.schema, view.tableSchema().*);
        defer declarations.freeDefinitions(self.alloc, expected);
        for (expected) |definition| {
            const binding = bindings.find(definition.kind, definition.name) orelse return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &binding.definition.fingerprint, &definition.fingerprint)) return error.PreparedGenerationChanged;
        }
        const table = try self.alloc.create(Loaded);
        table.* = .{
            .name = try self.alloc.dupe(u8, record.name),
            .view = view,
            .catalog = bindings,
            .uniques = try view.validator().?.schema.relationalUniqueDefinitions(self.alloc),
            .foreign = try view.validator().?.schema.relationalForeignKeyDefinitions(self.alloc),
        };
        try self.loaded.append(self.alloc, table);
        return table;
    }

    fn bindingPlan(self: *Builder, table: *Loaded) !*planner.Plan {
        if (table.plan == null) table.plan = try self.bindingPlanSelected(table, true, true);
        return &table.plan.?;
    }

    fn bindingPlanSelected(self: *Builder, table: *Loaded, with_unique: bool, with_foreign: bool) !planner.Plan {
        return self.bindingPlanFiltered(table, with_unique, with_foreign, null);
    }

    fn bindingPlanFiltered(self: *Builder, table: *Loaded, with_unique: bool, with_foreign: bool, retirement: ?@import("../storage/db/relational_integrity_retirement_contract.zig").Progress) !planner.Plan {
        const unique_defs = if (with_unique) table.uniques else &.{};
        const foreign_defs = if (with_foreign) table.foreign else &.{};
        var uniques = std.ArrayList(planner.UniqueBinding).empty;
        for (unique_defs) |definition| {
            const generation = (table.catalog.find(.unique, definition.name) orelse return error.PreparedGenerationChanged).generation;
            if (retirement) |selection| if (!selection.includes(generation)) continue;
            try uniques.append(self.alloc, .{ .generation = generation, .definition = definition });
        }
        var foreign = std.ArrayList(planner.ForeignBinding).empty;
        for (foreign_defs) |definition| {
            const generation = (table.catalog.find(.foreign_key, definition.name) orelse return error.PreparedGenerationChanged).generation;
            if (retirement) |selection| if (!selection.includes(generation)) continue;
            const parent = try self.load(definition.parent_table);
            const target = target: {
                for (parent.uniques) |unique| {
                    if (unique.columns.len != definition.parent_columns.len) continue;
                    var same = true;
                    for (unique.columns, definition.parent_columns) |left, right| if (!std.mem.eql(u8, left, right)) {
                        same = false;
                        break;
                    };
                    if (same) break :target unique;
                }
                return error.ForeignKeyTargetNotUnique;
            };
            var statement_definition = definition;
            // A deferred MATCH FULL mixed-null value is an outstanding
            // commit obligation, not a statement-time rejection. Its exact
            // declaration remains pinned in the catalog and final planner.
            if (self.statement and definition.timing == .deferred and definition.match == .full) statement_definition.match = .simple;
            try foreign.append(self.alloc, .{
                .generation = generation,
                .parent_generation = (parent.catalog.find(.unique, target.name) orelse return error.PreparedGenerationChanged).generation,
                .definition = statement_definition,
                .parent = parent.view,
                .parent_unique = target,
            });
        }
        return planner.Plan.init(self.alloc, table.name, table.view, uniques.items, foreign.items);
    }

    fn outputIndex(self: *Builder, table_name: []const u8, version: u32) !usize {
        const loaded = try self.load(table_name);
        const generation_set = @import("../storage/db/relational_integrity_activation_contract.zig").generationSet(loaded.catalog);
        for (self.output.items, 0..) |*table, i| if (std.mem.eql(u8, table.table_name, table_name)) {
            if (table.relational_schema_version) |old| if (old != version) return error.PreparedGenerationChanged;
            table.relational_schema_version = version;
            table.relational_integrity_generation_set = generation_set;
            return i;
        };
        try self.output.append(self.alloc, .{ .table_name = table_name, .relational_schema_version = version, .relational_integrity_generation_set = generation_set });
        try self.command_lists.append(self.alloc, .empty);
        try self.predicate_lists.append(self.alloc, .empty);
        return self.output.items.len - 1;
    }

    fn appendCommand(self: *Builder, table_name: []const u8, command: planner.storage.Command) !void {
        const target = try self.load(table_name);
        const i = try self.outputIndex(table_name, target.view.version());
        try self.command_lists.items[i].append(self.alloc, command);
    }

    fn appendPredicate(self: *Builder, table_index: usize, key: []const u8, version: u64, digest: ?[32]u8) !void {
        const predicates = &self.predicate_lists.items[table_index];
        for (predicates.items) |*old| if (std.mem.eql(u8, old.key, key)) {
            if (old.expected_version != version) return error.VersionConflict;
            if (old.expected_content_digest) |expected| {
                if (digest) |observed| if (!std.mem.eql(u8, &expected, &observed)) return error.VersionConflict;
            } else old.expected_content_digest = digest;
            return;
        };
        try predicates.append(self.alloc, .{ .key = key, .expected_version = version, .expected_content_digest = digest });
    }
};

/// Metadata comes from one request catalog snapshot. The independently fetched
/// durable generation catalog is checked against its table incarnation, schema
/// version and definition fingerprints; a concurrent publication fails closed.
pub fn prepare(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, requests: []const contract.TableCommitRequest) !Prepared {
    return prepareControlled(alloc, source, metadata, requests, .{});
}

pub fn prepareControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, requests: []const contract.TableCommitRequest, request_control: RequestContext) !Prepared {
    return prepareMode(alloc, source, metadata, requests, request_control, null);
}

fn prepareMode(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, requests: []const contract.TableCommitRequest, request_control: RequestContext, repair_table: ?[]const u8) !Prepared {
    return prepareModeInternal(alloc, source, metadata, requests, request_control, repair_table, false, &.{}, false);
}

fn prepareModeInternal(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, requests: []const contract.TableCommitRequest, request_control: RequestContext, repair_table: ?[]const u8, statement: bool, previous: []const contract.TableCommitRequest, validate_statement: bool) !Prepared {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request_control), .repair_table = repair_table, .statement = statement, .previous = previous };
    defer builder.deinit();
    try builder.output.appendSlice(builder.alloc, requests);
    for (requests) |request| {
        try builder.command_lists.append(builder.alloc, .empty);
        var predicates = std.ArrayList(types.TransactionVersionPredicate).empty;
        try predicates.appendSlice(builder.alloc, request.predicates);
        try builder.predicate_lists.append(builder.alloc, predicates);
    }
    var requested_tables = std.StringHashMapUnmanaged(void).empty;
    for (requests) |request| {
        if ((try requested_tables.getOrPut(builder.alloc, request.table_name)).found_existing) return error.InvalidBatchRequest;
        if (request.integrity.len != 0 or request.integrity_commands.len != 0 or request.relational_activation != null or request.relational_integrity_generation_set != null or request.relational_repair) return error.InvalidBatchRequest;
        const record = try builder.metadataTable(request.table_name);
        if (record.schema_json.len == 0) continue;
        var declaration = try schema_api.parseValidatedTableSchema(builder.alloc, record.schema_json);
        defer declaration.deinit(builder.alloc);
        if (declaration.unique_constraints == null and declaration.foreign_keys == null and (repair_table == null or declaration.checks == null)) continue;
        if (request.transforms.len != 0) return error.UnsupportedOperation;
        const table = try builder.load(request.table_name);
        _ = try builder.outputIndex(table.name, table.view.version());
        var writes = std.StringHashMapUnmanaged(?[]const u8).empty;
        for (request.writes) |write| try writes.put(builder.alloc, write.key, write.value);
        for (request.deletes) |key| try writes.put(builder.alloc, key, null);
        if (writes.count() > 4096) return error.TransactionTooLarge;
        const keys = try builder.alloc.alloc([]const u8, writes.count());
        var key_it = writes.keyIterator();
        for (keys) |*key| key.* = key_it.next().?.*;
        try builder.preloadWork(table, keys);
        var it = writes.iterator();
        while (it.next()) |entry| {
            const item = try builder.getWork(table, entry.key_ptr.*);
            item.explicit = true;
            item.after = entry.value_ptr.*;
            item.explicit_after = item.after;
            if (item.after) |json| {
                try builder.charge(json.len);
                const parsed = try std.json.parseFromSlice(std.json.Value, builder.alloc, json, .{ .allocate = .alloc_always, .parse_numbers = false });
                if (parsed.value != .object) return error.InvalidBatchRequest;
                const before = if (item.before) |row| (try std.json.parseFromSliceLeaky(std.json.Value, builder.alloc, row.json, .{ .parse_numbers = false })).object else null;
                var fields = parsed.value.object.iterator();
                while (fields.next()) |field| {
                    // Replacements carry unchanged fields too. Only a logical
                    // change constrains cascade assignment. Compare scalar FK
                    // columns with row coercions, not JSON spelling; complex
                    // fields cannot participate in an FK and need no comparison.
                    if (before) |old| {
                        const columns = table.view.tableSchema().relational_columns;
                        if (table.view.physicalLayout().ordinalForName(columns, field.key_ptr.*)) |ordinal| switch (columns[ordinal].column_type) {
                            .integer, .number, .datetime, .boolean, .string, .blob => {
                                if (try assignmentEqual(table, field.key_ptr.*, old.get(field.key_ptr.*) orelse .null, field.value_ptr.*)) continue;
                            },
                            else => {},
                        };
                    }
                    try item.assignments.put(builder.alloc, field.key_ptr.*, field.value_ptr.*);
                }
            }
            try builder.enqueue(item);
        }
    }
    // Discover the complete bounded cascade closure before beginning 2PC.
    // Cycles converge on the same row work item; contradictory assignments fail
    // before any participant is contacted. Every later source change is caught
    // by retained row predicates and the receiver's current reference probe.
    var queue_index: usize = 0;
    while (queue_index < builder.queue.items.len) : (queue_index += 1) {
        try builder.control.ensureActive();
        const item = builder.queue.items[queue_index];
        item.queued = false;
        const expansion = try builder.expandWork(item);
        for (expansion.parents) |transition| try builder.expandParent(item, transition);
    }
    // Replace only coordinated primary tables with their final row work set.
    // Unrelated document tables keep their existing primary request envelopes.
    for (builder.work.items) |item| {
        if (!item.dirty) continue;
        const i = try builder.outputIndex(item.table.name, item.table.view.version());
        builder.output.items[i].writes = &.{};
        builder.output.items[i].deletes = &.{};
    }
    const final_writes = try builder.alloc.alloc(std.ArrayList(types.TransactionWrite), builder.output.items.len);
    const final_deletes = try builder.alloc.alloc(std.ArrayList([]const u8), builder.output.items.len);
    @memset(final_writes, .empty);
    @memset(final_deletes, .empty);
    for (builder.work.items) |item| {
        if (!item.dirty) continue;
        const i = try builder.outputIndex(item.table.name, item.table.view.version());
        try builder.appendPredicate(i, item.key, item.observed_version, item.observed_digest);
        if (item.after) |json| try final_writes[i].append(builder.alloc, .{ .key = item.key, .value = json }) else try final_deletes[i].append(builder.alloc, item.key);
        // This arena is owned transitively by the returned request arena.
        const expansion = try builder.expandWork(item);
        for (expansion.commands) |command| try builder.appendCommand(command.table_name, command.command);
        for (expansion.partials) |dependency| try builder.appendPartial(dependency);
        for (expansion.parents) |transition| {
            const owner: planner.storage.ClaimOwner = .{ .parent_table = transition.table_name, .parent_key = transition.parent_key };
            try builder.appendCommand(transition.table_name, .{ .address = transition.address, .operation = if (builder.repairing(transition.table_name)) .{ .repair_release = owner } else .{ .release = owner } });
        }
    }
    for (final_writes, final_deletes, 0..) |writes, deletes, i| {
        if (writes.items.len != 0 or deletes.items.len != 0) {
            builder.output.items[i].writes = writes.items;
            builder.output.items[i].deletes = deletes.items;
        }
    }
    for (builder.final_references.items) |pending| {
        if (!try builder.childStillReferences(pending.child, pending.definition, pending.address, true)) continue;
        const child_index = try builder.outputIndex(pending.child.table.name, pending.child.table.view.version());
        try builder.appendPredicate(child_index, pending.child.key, pending.child.observed_version, pending.child.observed_digest);
        try builder.appendCommand(pending.definition.parent_table, .{ .address = pending.address, .operation = .{ .detach = pending.reference } });
        try builder.appendCommand(pending.definition.parent_table, .{ .address = pending.address, .operation = .{ .attach = pending.reference } });
    }
    for (builder.partial_rechecks.items) |pending| {
        var dependency = pending.dependency;
        dependency.after = pending.child.after;
        const child_index = try builder.outputIndex(pending.child.table.name, pending.child.table.view.version());
        try builder.appendPredicate(child_index, pending.child.key, pending.child.observed_version, pending.child.observed_digest);
        try builder.appendPartial(dependency);
    }
    if (validate_statement) try builder.validateStatement();
    for (builder.output.items, builder.command_lists.items, builder.predicate_lists.items) |*table, commands, predicates| {
        _ = try planner.storage.validateCommandAdmission(commands.items);
        table.integrity_commands = commands.items;
        table.predicates = predicates.items;
        table.relational_repair = builder.repairing(table.table_name);
    }
    return .{ .arena = arena, .tables = try builder.output.toOwnedSlice(builder.alloc) };
}

/// A session stage is one statement. Its read-your-writes view includes all
/// previously staged primary rows and reference effects, but no live mutation
/// is made here. Final commit still prepares guarded native 2PC operations.
pub fn prepareSessionStatement(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, previous: []const contract.TableCommitRequest, statement: []const contract.TableCommitRequest, control: RequestContext) !Prepared {
    var before = try prepareModeInternal(alloc, source, metadata, previous, control, null, true, &.{}, false);
    defer before.deinit();
    var result = try prepareModeInternal(alloc, source, metadata, statement, control, null, true, before.tables, true);
    errdefer result.deinit();
    const names = try alloc.alloc([]const u8, result.tables.len);
    defer alloc.free(names);
    for (result.tables, names) |table, *name| name.* = table.table_name;
    try ensureUniqueCoverageControlled(alloc, source, metadata, ranges, names, control);
    return result;
}

pub const BackfillRow = struct { key: []const u8, json: []const u8, version: u64, expected_content_digest: ?[32]u8 = null };
pub const BackfillPhase = enum { unique, foreign_key, check };

/// A positive local proof does not imply global uniqueness: another owner's
/// historical rows may still be unclaimed. Check every current owner once per
/// request. Proofs deliberately are not cached across restore incarnations.
pub fn ensureUniqueCoverage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_names: []const []const u8) !void {
    return ensureUniqueCoverageControlled(alloc, source, metadata, ranges, table_names, .{});
}

fn ensureUniqueCoverageControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_names: []const []const u8, request_control: RequestContext) !void {
    const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request_control) };
    defer builder.deinit();
    var checked = std.StringHashMapUnmanaged(void).empty;
    for (table_names) |name| {
        if ((try checked.getOrPut(builder.alloc, name)).found_existing) continue;
        const record = try builder.metadataTable(name);
        if (!try requiresCoordination(builder.alloc, record.schema_json)) continue;
        const table = try builder.load(name);
        var owners = std.ArrayList(*const RangeRecord).empty;
        for (ranges) |*range| if (range.table_id == record.table_id) try owners.append(builder.alloc, range);
        if (owners.items.len == 0 or owners.items.len > 4096) return error.ConstraintActivationPending;
        std.mem.sort(*const RangeRecord, owners.items, {}, struct {
            fn less(_: void, a: *const RangeRecord, b: *const RangeRecord) bool {
                return std.mem.order(u8, a.start_key, b.start_key) == .lt;
            }
        }.less);
        var expected_start: []const u8 = "";
        for (owners.items, 0..) |owner, i| {
            if (owner.restore_backup_id.len != 0) return error.ConstraintActivationPending;
            if (!std.mem.eql(u8, owner.start_key, expected_start) or ((i + 1 == owners.items.len) != (owner.end_key == null))) return error.TopologyChanged;
            if (owner.end_key) |end| if (std.mem.order(u8, owner.start_key, end) != .lt) return error.TopologyChanged;
            var response = (try builder.lookup(name, owner.start_key, .{ .relational_activation_json = "{\"mode\":\"status\"}" })) orelse return error.ConstraintActivationPending;
            defer response.deinit(builder.alloc);
            const Status = struct {
                schema_version: u32,
                schema_digest: planner.storage.Digest,
                generation_set: planner.storage.Digest,
                owner: planner.storage.Digest,
                range_start: []const u8,
                range_end: []const u8,
                unique_covered: bool,
                state: activation.State,
                phase: activation.Phase,
                rows_scanned: u64,
                failure: []const u8,
            };
            var status = try std.json.parseFromSlice(Status, builder.alloc, response.json, .{});
            defer status.deinit();
            if (status.value.schema_version != table.view.version() or !std.mem.eql(u8, &status.value.schema_digest, &table.catalog.schema_digest) or
                !std.mem.eql(u8, &status.value.generation_set, &activation.generationSet(table.catalog)) or
                !std.mem.eql(u8, status.value.range_start, owner.start_key) or !std.mem.eql(u8, status.value.range_end, owner.end_key orelse "")) return error.PreparedGenerationChanged;
            if (!status.value.unique_covered or status.value.state == .invalid) return error.ConstraintActivationPending;
            expected_start = owner.end_key orelse "";
        }
        if (expected_start.len != 0) return error.TopologyChanged;
    }
}

pub fn prepareWithCoverage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, requests: []const contract.TableCommitRequest) !Prepared {
    return prepareWithCoverageControlled(alloc, source, metadata, ranges, requests, .{});
}

pub fn prepareWithCoverageControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, requests: []const contract.TableCommitRequest, request: RequestContext) !Prepared {
    return prepareWithCoverageOnce(alloc, source, metadata, ranges, requests, request) catch |err| return preparationError(err);
}

/// This phase only reads and prepares owned commands. A leader read barrier
/// timing out here proves no commit has been attempted, unlike a timeout from
/// the distributed commit call itself. Preserve that distinction at the API.
fn preparationError(err: anyerror) anyerror {
    return switch (err) {
        error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.Timeout, error.NotLeader, error.GroupLeaderUnavailable, error.LeaderUnavailable, error.DistributedQueryUnavailable => blk: {
            if (preparation_diagnostic_gate.admit(@import("antfly_platform").time.monotonicNs()))
                std.log.warn("relational preparation read deferred class={s}", .{@errorName(err)});
            break :blk error.IntegrityCatalogUnavailable;
        },
        error.DeadlineExceeded => error.PreDecisionDeadlineExceeded,
        else => err,
    };
}

fn prepareWithCoverageOnce(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, requests: []const contract.TableCommitRequest, request: RequestContext) !Prepared {
    const control = try boundedControl(request);
    var prepared = try prepareControlled(alloc, source, metadata, requests, control);
    errdefer prepared.deinit();
    const names = try alloc.alloc([]const u8, prepared.tables.len);
    defer alloc.free(names);
    for (prepared.tables, names) |table, *name| name.* = table.table_name;
    try ensureUniqueCoverageControlled(alloc, source, metadata, ranges, names, control);
    return prepared;
}

test "distributed txn integrity preparation read barrier failure is precommit unavailability" {
    inline for (.{ error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.Timeout, error.NotLeader, error.GroupLeaderUnavailable, error.LeaderUnavailable, error.DistributedQueryUnavailable }) |err| {
        try std.testing.expectEqual(error.IntegrityCatalogUnavailable, preparationError(err));
    }
    try std.testing.expectEqual(error.PreDecisionDeadlineExceeded, preparationError(error.DeadlineExceeded));
    inline for (.{ error.OutOfMemory, error.ForeignKeyParentMissing, error.UniqueConstraintViolation, error.Canceled, error.CommitDecisionUnknown }) |err| {
        try std.testing.expectEqual(err, preparationError(err));
    }
}

test "distributed txn partial witness scan translates distinct clock epochs without extending budget" {
    const FakeClock = struct {
        fn now(ptr: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const value: *const u64 = @ptrCast(@alignCast(ptr.?));
            return .{ .nanoseconds = value.* };
        }
    };
    var now: u64 = 17;
    var vtable = std.testing.io.vtable.*;
    vtable.now = FakeClock.now;
    const io: std.Io = .{ .userdata = &now, .vtable = &vtable };
    const control: RequestContext = .{ .deadline_ns = now + std.time.ns_per_s, .deadline_io = @import("../runtime_io_abi.zig").Borrow.init(&io) };
    const before = @import("antfly_platform").time.monotonicNs();
    const options = try partialWitnessScanOptions(control, "query");
    const after = @import("antfly_platform").time.monotonicNs();
    try std.testing.expect(options.execution_deadline_ns.? >= before + std.time.ns_per_s);
    try std.testing.expect(options.execution_deadline_ns.? <= after + std.time.ns_per_s);
    try std.testing.expectEqualStrings("query", options.relational_query_json);
    try std.testing.expectEqual(@as(usize, 4096), options.limit);
    now += std.time.ns_per_s;
    try std.testing.expectError(error.DeadlineExceeded, partialWitnessScanOptions(control, "query"));
    try std.testing.expect((try partialWitnessScanOptions(.{}, "query")).execution_deadline_ns == null);
    const platform: RequestContext = .{ .deadline_ns = after + std.time.ns_per_s };
    try std.testing.expectEqual(platform.deadline_ns, (try partialWitnessScanOptions(platform, "query")).execution_deadline_ns);
}

/// Administrative recovery may edit invalid rows, not bypass new-value checks.
/// Only the target table's incomplete historical coverage is exempted; every
/// external parent still needs global UNIQUE coverage. Native prepares require
/// failed/diagnosed activation and lock its exact checkpoint through the repair decision.
pub fn prepareRepair(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, request: contract.TableCommitRequest, control: RequestContext) !Prepared {
    _ = try metadataRequiresCoordination(alloc, metadata, &.{request});
    if (!try requiresActivation(alloc, (for (metadata) |table| {
        if (std.mem.eql(u8, table.name, request.table_name)) break table.schema_json;
    } else return error.TableNotFound))) return error.InvalidConstraintActivation;
    var prepared = try prepareMode(alloc, source, metadata, &.{request}, control, request.table_name);
    errdefer prepared.deinit();
    var parents: std.ArrayList([]const u8) = .empty;
    defer parents.deinit(alloc);
    for (prepared.tables) |table| if (!std.mem.eql(u8, table.table_name, request.table_name)) try parents.append(alloc, table.table_name);
    try ensureUniqueCoverageControlled(alloc, source, metadata, ranges, parents.items, control);
    return prepared;
}

/// Backfill writes only globally routed derived claims/references, never the
/// primary rows. Source versions remain real read-only 2PC participants. The
/// caller enlists its owner-bound activation checkpoint in this SAME decision.
pub fn prepareRetirementPage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, table_name: []const u8, rows: []const BackfillRow, progress: @import("../storage/db/relational_integrity_retirement_contract.zig").Progress, request: RequestContext) !Prepared {
    if (rows.len > 128 or (progress.phase != .foreign_keys and progress.phase != .unique)) return error.InvalidConstraintRetirement;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request) };
    defer builder.deinit();
    const table = try builder.load(table_name);
    if (table.view.version() != progress.schema_version or !std.mem.eql(u8, &@import("../storage/db/relational_integrity_activation_contract.zig").generationSet(table.catalog), &progress.generation_set)) return error.ConstraintRetirementChanged;
    const row_table_index = try builder.outputIndex(table_name, table.view.version());
    var plan = try builder.bindingPlanFiltered(table, progress.phase == .unique, progress.phase == .foreign_keys, progress);
    defer plan.deinit();
    var selected_fields: std.ArrayList([]const u8) = .empty;
    for (plan.uniques) |binding| try selected_fields.appendSlice(builder.alloc, binding.definition.columns);
    for (plan.foreign) |binding| try selected_fields.appendSlice(builder.alloc, binding.definition.child_columns);
    for (rows) |row| {
        try builder.charge(row.key.len + row.json.len);
        if (row.expected_content_digest == null) return error.MissingPrimaryObservation;
        try builder.appendPredicate(row_table_index, row.key, row.version, row.expected_content_digest);
        var prepared = try mapper.PreparedRelationalProjection.init(builder.alloc, row.json, table.view.tableSchema().*, table.view.physicalLayout(), selected_fields.items);
        defer prepared.deinit();
        const expansion = try plan.expand(builder.alloc, &.{.{ .key = row.key, .before = prepared.view, .repair = true }});
        for (expansion.partials) |dependency| if (progress.includes(dependency.reference.constraint_generation)) try builder.appendPartial(dependency);
        for (expansion.commands) |command| switch (command.command.operation) {
            .repair_detach => |reference| if (progress.includes(reference.constraint_generation)) try builder.appendCommand(command.table_name, command.command),
            else => {},
        };
        for (expansion.parents) |parent| if (progress.includes(parent.address.generation)) try builder.appendCommand(parent.table_name, .{
            .address = parent.address,
            .operation = .{ .repair_release = .{ .parent_table = parent.table_name, .parent_key = parent.parent_key } },
        });
    }
    for (builder.output.items, builder.command_lists.items, builder.predicate_lists.items) |*table_request, commands, predicates| {
        _ = try planner.storage.validateCommandAdmission(commands.items);
        table_request.integrity_commands = commands.items;
        table_request.predicates = predicates.items;
    }
    return .{ .arena = arena, .tables = try builder.output.toOwnedSlice(builder.alloc) };
}

pub fn prepareBackfill(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, table_name: []const u8, rows: []const BackfillRow, phase: BackfillPhase) !Prepared {
    return prepareBackfillControlled(alloc, source, metadata, table_name, rows, phase, .{});
}

/// Revalidate a rejected page without replaying its mutations. One witnessed
/// violation is sufficient, but its exact claim bytes (including absence) and
/// every original source observation must survive through the 2PC decision.
/// Returns false when the rejected dependency is no longer invalid.
pub fn guardBackfillFailure(prepared: *Prepared, source: reads.TableReadSource, failure: []const u8, control: RequestContext) !bool {
    const unique = std.mem.eql(u8, failure, "UniqueConstraintViolation") or std.mem.eql(u8, failure, "unique_constraint_violation");
    const missing = std.mem.eql(u8, failure, "ForeignKeyParentMissing") or std.mem.eql(u8, failure, "foreign_key_parent_missing");
    if (!unique and !missing) return true;
    const owned = prepared.arena.allocator();
    const requests = try owned.dupe(contract.TableCommitRequest, prepared.tables);
    prepared.tables = requests;
    var observed_bytes: usize = 0;
    for (requests) |*request| {
        var proposed = std.AutoHashMap(planner.storage.Address, planner.storage.Claim).init(owned);
        defer proposed.deinit();
        for (request.integrity_commands) |command| {
            try control.ensureActive();
            if (unique) {
                if (command.operation != .establish) continue;
                const claim = command.operation.establish;
                if (proposed.get(command.address)) |old| {
                    if (!std.mem.eql(u8, old.parent_table, claim.parent_table) or !std.mem.eql(u8, old.parent_key, claim.parent_key)) return true;
                    continue;
                }
                try proposed.put(command.address, claim);
            } else if (command.operation != .attach) continue;
            const query = try std.json.Stringify.valueAlloc(owned, .{ .kind = "references", .address = command.address, .limit = @as(u32, 1) }, .{});
            var response = try source.lookup(owned, request.table_name, &command.address.routing, .{
                .relational_integrity_jobs_json = query,
                .execution_deadline_ns = control.deadline_ns,
                .execution_io = control.deadline_io,
                .cancellation = control.cancellation,
            }, .read_index);
            defer if (response) |*value| value.deinit(owned);
            var expected: ?[]const u8 = null;
            if (response) |value| {
                if (value.json.len > 4 * 1024 * 1024 - observed_bytes) return error.TransactionTooLarge;
                observed_bytes += value.json.len;
                const observed = try std.json.parseFromSliceLeaky(struct { address: planner.storage.Address, claim: planner.storage.Claim }, owned, value.json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
                if (!std.meta.eql(observed.address, command.address)) return error.InvalidIntegrityAddress;
                if (missing) continue;
                const wanted = command.operation.establish;
                if (std.mem.eql(u8, observed.claim.parent_table, wanted.parent_table) and std.mem.eql(u8, observed.claim.parent_key, wanted.parent_key)) continue;
                expected = try observed.claim.encode(owned, command.address);
            } else if (unique) continue;
            const guards = try owned.alloc(types.TransactionIntegrityOperation, 1);
            guards[0] = .{
                .routing_key = try owned.dupe(u8, &command.address.routing),
                .key = try owned.dupe(u8, &command.address.claimKey()),
                .kind = .guard,
                .expected_value = expected,
            };
            request.integrity = guards;
            return true;
        }
    }
    return false;
}

test "distributed txn activation failure guards absence and abandons repaired parent observations" {
    const a = std.testing.allocator;
    const address = try planner.storage.Address.init(@splat(1), "tuple");
    const reference: planner.storage.Reference = .{ .child_table = "children", .child_key = "c", .constraint_name = "fk", .constraint_generation = @splat(2) };
    const Fixture = struct {
        present: bool,
        address: planner.storage.Address,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, _: []const u8, _: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.present) return null;
            return .{ .json = try std.json.Stringify.valueAlloc(allocator, .{ .address = self.address, .claim = planner.storage.Claim{ .tuple = "tuple", .parent_table = "parents", .parent_key = "p", .schema_version = 1 } }, .{}), .version = 0 };
        }
    };
    for ([_]bool{ false, true }) |present| {
        var fixture: Fixture = .{ .present = present, .address = address };
        const source: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
        var prepared: Prepared = .{ .arena = std.heap.ArenaAllocator.init(a), .tables = &.{
            .{ .table_name = "children", .predicates = &.{.{ .key = "c", .expected_version = 7, .expected_content_digest = @splat(3) }} },
            .{ .table_name = "parents", .integrity_commands = &.{.{ .address = address, .operation = .{ .attach = reference } }} },
        } };
        defer prepared.deinit();
        try std.testing.expectEqual(!present, try guardBackfillFailure(&prepared, source, "ForeignKeyParentMissing", .{}));
        if (!present) {
            const guard = prepared.tables[1].integrity[0];
            try std.testing.expectEqual(.guard, guard.kind);
            try std.testing.expectEqual(null, guard.expected_value);
            try std.testing.expectEqualSlices(u8, &address.claimKey(), guard.key);
            try std.testing.expectEqualSlices(u8, &address.routing, guard.routing_key);
            try std.testing.expectEqual(@as(usize, 1), prepared.tables[0].predicates.len);
        }
    }
}

fn prepareBackfillControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, table_name: []const u8, rows: []const BackfillRow, phase: BackfillPhase, request: RequestContext) !Prepared {
    if (rows.len > 4096) return error.TransactionTooLarge;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request) };
    defer builder.deinit();
    const table = try builder.load(table_name);
    const row_table_index = try builder.outputIndex(table_name, table.view.version());
    var plan = try builder.bindingPlanSelected(table, phase == .unique, phase == .foreign_key);
    defer plan.deinit();
    var selected_fields: std.ArrayList([]const u8) = .empty;
    for (plan.uniques) |binding| try selected_fields.appendSlice(builder.alloc, binding.definition.columns);
    for (plan.foreign) |binding| try selected_fields.appendSlice(builder.alloc, binding.definition.child_columns);
    var validation_failure: ?[]const u8 = null;
    var backfill_partial = false;
    for (rows) |row| {
        try builder.charge(row.key.len + row.json.len);
        if (row.expected_content_digest == null) return error.MissingPrimaryObservation;
        try builder.appendPredicate(row_table_index, row.key, row.version, row.expected_content_digest);
        if (phase == .check) {
            const checks = table.view.validator().?.execution.checks orelse return error.ConstraintNotFound;
            var parsed = try std.json.parseFromSlice(std.json.Value, builder.alloc, row.json, .{ .allocate = .alloc_always, .parse_numbers = false });
            defer parsed.deinit();
            if (try checks.firstFailureJson(builder.alloc, parsed.value)) |failure| {
                validation_failure = try std.fmt.allocPrint(builder.alloc, "CHECK {s}: {s}", .{ checks.definitions[failure.index].name, @errorName(failure.reason) });
                break;
            }
            continue;
        }
        var prepared = try mapper.PreparedRelationalProjection.init(builder.alloc, row.json, table.view.tableSchema().*, table.view.physicalLayout(), selected_fields.items);
        defer prepared.deinit();
        const expansion = plan.expand(builder.alloc, &.{.{ .key = row.key, .after = prepared.view }}) catch |err| switch (err) {
            error.ForeignKeyMatchFullViolation => {
                validation_failure = @errorName(err);
                break;
            },
            else => return err,
        };
        if (phase == .foreign_key) for (expansion.partials) |dependency| {
            backfill_partial = true;
            try builder.appendPartial(dependency);
        };
        for (expansion.commands) |command| {
            const selected = switch (command.command.operation) {
                .establish => phase == .unique,
                .attach => phase == .foreign_key,
                else => false,
            };
            if (selected) try builder.appendCommand(command.table_name, command.command);
        }
    }
    for (builder.output.items, builder.command_lists.items, builder.predicate_lists.items) |*table_request, commands, predicates| {
        _ = try planner.storage.validateCommandAdmission(commands.items);
        table_request.integrity_commands = if (validation_failure == null) commands.items else &.{};
        table_request.predicates = predicates.items;
    }
    return .{ .arena = arena, .tables = try builder.output.toOwnedSlice(builder.alloc), .validation_failure = validation_failure, .backfill_partial = backfill_partial };
}

pub fn prepareBackfillWithCoverage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_name: []const u8, rows: []const BackfillRow, phase: BackfillPhase) !Prepared {
    return prepareBackfillWithCoverageControlled(alloc, source, metadata, ranges, table_name, rows, phase, .{});
}

pub fn prepareBackfillWithCoverageControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_name: []const u8, rows: []const BackfillRow, phase: BackfillPhase, request: RequestContext) !Prepared {
    const control = try boundedControl(request);
    var prepared = try prepareBackfillControlled(alloc, source, metadata, table_name, rows, phase, control);
    errdefer prepared.deinit();
    if (phase == .foreign_key) {
        var parent_names = std.ArrayList([]const u8).empty;
        defer parent_names.deinit(alloc);
        for (prepared.tables) |table| {
            if (table.integrity_commands.len != 0) try parent_names.append(alloc, table.table_name);
        }
        try ensureUniqueCoverageControlled(alloc, source, metadata, ranges, parent_names.items, control);
    }
    return prepared;
}

test "distributed txn preparation pins one routing view and releases it" {
    const Fixture = struct {
        view: reads.JoinReadView = undefined,
        acquisitions: usize = 0,
        lookups: usize = 0,
        releases: usize = 0,
        fn acquire(ptr: *anyopaque, _: Allocator, budget: @import("table_router.zig").RouteBudget) !*reads.JoinReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.check();
            self.acquisitions += 1;
            return &self.view;
        }
        fn destroy(view: *reads.JoinReadView) void {
            const self: *@This() = @fieldParentPtr("view", view);
            self.releases += 1;
        }
        fn original(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            return error.TestUnexpectedResult;
        }
        fn lookup(ptr: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(.read_index, consistency);
            self.lookups += 1;
            return null;
        }
    };
    var fixture: Fixture = .{};
    fixture.view = .{ .session = undefined, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } }, .destroy = Fixture.destroy };
    var builder: Builder = .{ .alloc = std.testing.allocator, .metadata = &.{}, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.original, .scan = undefined, .query = undefined, .acquire_join_view = Fixture.acquire } } };
    {
        defer builder.deinit();
        try std.testing.expect(try builder.lookup("rows", "a", .{}) == null);
        try std.testing.expect(try builder.lookup("rows", "b", .{}) == null);
        try std.testing.expectEqual(@as(usize, 1), fixture.acquisitions);
        try std.testing.expectEqual(@as(usize, 2), fixture.lookups);
    }
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
}

test "distributed txn primary prefetch owns observations and drains failed batches" {
    const Fixture = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fail: bool,
        fn lookup(ptr: *anyopaque, alloc: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .monotonic);
            try std.testing.expectEqual(.read_index, consistency);
            try std.testing.expect(opts.include_primary_digest);
            try std.testing.expect(opts.execution_io == null);
            if (self.fail and std.mem.eql(u8, key, "b")) return error.InjectedReadFailure;
            return .{ .json = try alloc.dupe(u8, "{}"), .version = 7, .expected_content_digest = @splat(3) };
        }
    };
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    for ([_]bool{ false, true }) |fail| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var fixture: Fixture = .{ .fail = fail };
        var table: Loaded = undefined;
        table.name = "rows";
        var builder: Builder = .{ .alloc = arena.allocator(), .metadata = &.{}, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } }, .control = .{ .fanout_io = @import("../runtime_io_abi.zig").Borrow.init(&io) } };
        defer builder.deinit();
        const keys = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        if (fail) {
            try std.testing.expectError(error.InjectedReadFailure, builder.preloadWork(&table, &keys));
            try std.testing.expectEqual(@as(usize, 8), fixture.calls.load(.monotonic));
            try std.testing.expectEqual(@as(usize, 0), builder.work.items.len);
        } else {
            try builder.preloadWork(&table, &keys);
            try std.testing.expectEqual(keys.len, fixture.calls.load(.monotonic));
            for (keys, builder.work.items) |key, work| {
                try std.testing.expectEqualStrings(key, work.key);
                try std.testing.expectEqualStrings("{}", work.before.?.json);
                try std.testing.expectEqual(@as(u64, 7), work.observed_version);
                try std.testing.expectEqual(@as([32]u8, @splat(3)), work.observed_digest.?);
                try std.testing.expect(work == try builder.getWork(&table, key));
            }
            try std.testing.expectEqual(keys.len, fixture.calls.load(.monotonic));
        }
    }
}

fn testCatalogEnvelope(alloc: Allocator, table_id: u64, json: []const u8) ![]u8 {
    var parsed = try schema_api.parseValidatedTableSchema(alloc, json);
    defer parsed.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer schema.freeSchema(alloc, runtime);
    const bytes = try schema.serializeSchema(alloc, runtime);
    defer alloc.free(bytes);
    var digest: planner.storage.Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    const definitions = try declarations.definitionFingerprints(alloc, parsed, runtime);
    defer declarations.freeDefinitions(alloc, definitions);
    var update = try catalog.prepare(alloc, null, try catalog.incarnationFromTableId(table_id), runtime.version, digest, definitions);
    defer update.deinit();
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(update.value.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, update.value);
    const id = try std.fmt.allocPrint(alloc, "{d}", .{table_id});
    defer alloc.free(id);
    return std.json.Stringify.valueAlloc(alloc, .{ .catalog = encoded, .schema_version = runtime.version, .table_id = id }, .{});
}

test "distributed txn global unique coverage checks every owner and rejects stale incomplete proofs" {
    const alloc = std.testing.allocator;
    const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const envelope = try testCatalogEnvelope(alloc, 1, schema_json);
    defer alloc.free(envelope);
    var parsed = try std.json.parseFromSlice(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, alloc, envelope, .{});
    defer parsed.deinit();
    const bytes = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(parsed.value.catalog));
    defer alloc.free(bytes);
    try std.base64.standard.Decoder.decode(bytes, parsed.value.catalog);
    var native_catalog = try catalog.decode(alloc, bytes);
    defer native_catalog.deinit();
    const Fake = struct {
        envelope: []const u8,
        digest: planner.storage.Digest,
        generation_set: planner.storage.Digest,
        calls: usize = 0,
        ready: bool = false,
        stale: bool = false,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@import("../raft/read_gate.zig").ReadConsistency.read_index, consistency);
            try std.testing.expect(opts.execution_deadline_ns != null);
            if (opts.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, self.envelope), .version = 0 };
            try std.testing.expectEqualStrings("{\"mode\":\"status\"}", opts.relational_activation_json);
            const first = key.len == 0;
            self.calls += 1;
            const status = .{
                .schema_version = @as(u32, if (self.stale) 2 else 1),
                .schema_digest = self.digest,
                .generation_set = self.generation_set,
                .owner = [_]u8{1} ** 32,
                .range_start = @as([]const u8, if (first) "" else "m"),
                .range_end = @as([]const u8, if (first) "m" else ""),
                .unique_covered = first or self.ready,
                .state = activation.State.validating,
                .phase = activation.Phase.foreign_key,
                .rows_scanned = @as(u64, 5),
                .failure = @as([]const u8, ""),
            };
            return .{ .json = try std.json.Stringify.valueAlloc(allocator, status, .{}), .version = 0 };
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
    };
    var fake: Fake = .{ .envelope = envelope, .digest = native_catalog.schema_digest, .generation_set = activation.generationSet(native_catalog) };
    const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
    const tables = [_]TableRecord{.{ .table_id = 1, .name = "rows", .placement_role = "data", .schema_json = schema_json }};
    const ranges = [_]RangeRecord{ .{ .group_id = 11, .table_id = 1, .start_key = "m" }, .{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = "m" } };
    try std.testing.expectError(error.ConstraintActivationPending, ensureUniqueCoverage(alloc, source, &tables, &ranges, &.{"rows"}));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    fake.calls = 0;
    fake.ready = true;
    try ensureUniqueCoverage(alloc, source, &tables, &ranges, &.{ "rows", "rows" });
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    fake.stale = true;
    try std.testing.expectError(error.PreparedGenerationChanged, ensureUniqueCoverage(alloc, source, &tables, &ranges, &.{"rows"}));
    var gap = ranges;
    gap[0].start_key = "n";
    fake.stale = false;
    try std.testing.expectError(error.TopologyChanged, ensureUniqueCoverage(alloc, source, &tables, &gap, &.{"rows"}));
}

test "distributed txn typed mutation cannot bypass coordination through stale or absent metadata" {
    const request = [_]contract.TableCommitRequest{.{ .table_name = "rows", .relational_schema_version = 2, .writes = &.{.{ .key = "k", .value = "{\"id\":1}" }} }};
    try std.testing.expectError(error.IntegrityCatalogUnavailable, metadataRequiresCoordination(std.testing.allocator, null, &request));
    const tables = [_]TableRecord{.{ .table_id = 1, .name = "rows", .placement_role = "data", .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    }};
    try std.testing.expectError(error.PreparedGenerationChanged, metadataRequiresCoordination(std.testing.allocator, &tables, &request));
}

test "distributed txn cascade authorization covers generated primary writes without requiring claim-owner write permission" {
    const Policy = struct {
        fn allows(_: *const anyopaque, table: []const u8) bool {
            return std.mem.eql(u8, table, "parents");
        }
    };
    const context: RequestContext = .{ .principal = .{ .kind = .user, .subject = "parent-writer" }, .table_write_authorization = .{ .ptr = "scope", .allows = Policy.allows } };
    const original = contract.TableCommitRequest{ .table_name = "parents", .deletes = &.{"p"} };
    const generated = contract.TableCommitRequest{ .table_name = "children", .deletes = &.{"c"} };
    try std.testing.expectError(error.Forbidden, authorizePrimaryMutations(context, true, &.{ original, generated }));
    try authorizePrimaryMutations(context, true, &.{ original, .{ .table_name = "claim-owner", .predicates = &.{.{ .key = "proof", .expected_version = 1 }} } });
    try std.testing.expectError(error.Forbidden, authorizePrimaryMutations(.{ .principal = .{ .kind = .user, .subject = "missing-scopes" } }, true, &.{original}));
    try authorizePrimaryMutations(.{}, false, &.{ original, generated });
}

test "distributed txn session statement checks immediate references and overlays prior cascades" {
    const alloc = std.testing.allocator;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    inline for (.{ false, true }) |deferred| inline for (.{ "cascade", "restrict", "no_action" }) |action| {
        const child_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"foreign_keys\":[{\"name\":\"fk\",\"child_columns\":[\"id\"],\"parent_table\":\"p\",\"parent_columns\":[\"id\"],\"on_update\":\"" ++ action ++ "\",\"on_delete\":\"" ++ action ++ "\",\"deferrable\":true,\"timing\":\"" ++ (if (deferred) "deferred" else "immediate") ++ "\"}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
        const p = try testCatalogEnvelope(alloc, 1, parent_schema);
        defer alloc.free(p);
        const c = try testCatalogEnvelope(alloc, 2, child_schema);
        defer alloc.free(c);
        const Fake = struct {
            p: []const u8,
            c: []const u8,
            fn lookup(ptr: *anyopaque, allocator: Allocator, table: []const u8, _: []const u8, options: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (options.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, if (std.mem.eql(u8, table, "p")) self.p else self.c), .version = 0 };
                return null;
            }
            fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
                return error.UnexpectedCall;
            }
            fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
                return error.UnexpectedCall;
            }
        };
        var fake: Fake = .{ .p = p, .c = c };
        const reader: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
        const metadata = [_]TableRecord{ .{ .table_id = 1, .name = "p", .schema_json = parent_schema }, .{ .table_id = 2, .name = "c", .schema_json = child_schema } };
        const child = [_]contract.TableCommitRequest{.{ .table_name = "c", .writes = &.{.{ .key = "child", .value = "{\"id\":1}" }} }};
        if (deferred) {
            var accepted = try prepareModeInternal(alloc, reader, &metadata, &child, .{}, null, true, &.{}, true);
            accepted.deinit();
        } else try std.testing.expectError(error.ForeignKeyParentMissing, prepareModeInternal(alloc, reader, &metadata, &child, .{}, null, true, &.{}, true));
        const initial = [_]contract.TableCommitRequest{ .{ .table_name = "p", .writes = &.{.{ .key = "parent", .value = "{\"id\":1}" }} }, child[0] };
        var previous = try prepareModeInternal(alloc, reader, &metadata, &initial, .{}, null, true, &.{}, true);
        defer previous.deinit();
        const change = [_]contract.TableCommitRequest{.{ .table_name = "p", .writes = &.{.{ .key = "parent", .value = "{\"id\":2}" }} }};
        if (comptime std.mem.eql(u8, action, "restrict")) {
            try std.testing.expectError(error.ForeignKeyReferenced, prepareModeInternal(alloc, reader, &metadata, &change, .{}, null, true, previous.tables, true));
        } else if (comptime std.mem.eql(u8, action, "no_action") and !deferred) {
            try std.testing.expectError(error.ForeignKeyParentMissing, prepareModeInternal(alloc, reader, &metadata, &change, .{}, null, true, previous.tables, true));
        } else {
            var next = try prepareModeInternal(alloc, reader, &metadata, &change, .{}, null, true, previous.tables, true);
            defer next.deinit();
            if (comptime std.mem.eql(u8, action, "cascade")) {
                const generated = for (next.tables) |table| {
                    if (std.mem.eql(u8, table.table_name, "c")) break table;
                } else return error.TestExpectedEqual;
                try std.testing.expectEqualStrings("{\"id\":2}", generated.writes[0].value);
                const normalized = [_]contract.TableCommitRequest{ .{ .table_name = "p", .writes = change[0].writes }, .{ .table_name = "c", .writes = generated.writes } };
                var staged_again = try prepareModeInternal(alloc, reader, &metadata, &normalized, .{}, null, true, &.{}, false);
                defer staged_again.deinit();
                var third = try prepareModeInternal(alloc, reader, &metadata, &.{.{ .table_name = "p", .writes = &.{.{ .key = "parent", .value = "{\"id\":3}" }} }}, .{}, null, true, staged_again.tables, true);
                defer third.deinit();
                for (third.tables) |table| if (std.mem.eql(u8, table.table_name, "c")) try std.testing.expectEqualStrings("{\"id\":3}", table.writes[0].value);
            }
        }
    };
}

test "distributed txn activation projection validates selected fields without full row required fields" {
    const alloc = std.testing.allocator;
    const public_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"foreign_keys":[{"name":"parent","child_columns":["parent_id"],"parent_table":"parents","parent_columns":["id"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent_id":{"type":"integer"},"x":{"type":"integer"},"g":{"type":"integer"}},"required":["id","parent_id","x","g"],"additionalProperties":false}}}}
    ;
    const catalog_payload = try testCatalogEnvelope(alloc, 1, public_schema);
    defer alloc.free(catalog_payload);
    const Fake = struct {
        catalog: []const u8,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (opts.relational_integrity_catalog) {
                try std.testing.expectEqualStrings("", key);
                return .{ .json = try allocator.dupe(u8, self.catalog), .version = 0 };
            }
            return error.UnexpectedCall;
        }
        fn scan(_: *anyopaque, allocator: Allocator, _: []const u8, _: []const u8, _: []const u8, opts: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            var query_input = try std.json.parseFromSlice(struct { index: []const u8, fields: []const []const u8 }, allocator, opts.relational_query_json, .{ .ignore_unknown_fields = true });
            defer query_input.deinit();
            try std.testing.expectEqualStrings("by_id", query_input.value.index);
            try std.testing.expectEqual(@as(usize, 4), query_input.value.fields.len);
            return .{ .ndjson = try allocator.dupe(u8, "{\"_id\":\"parent-row\",\"row\":{\"id\":9007199254740993,\"parent_id\":9007199254740993,\"x\":1,\"g\":1}}\n") };
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
    };
    var fake: Fake = .{ .catalog = catalog_payload };
    const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
    const metadata = [_]TableRecord{.{ .table_id = 1, .name = "parents", .placement_role = "data", .schema_json = public_schema }};
    var result = try prepareBackfill(alloc, source, &metadata, "parents", &.{.{ .key = "row", .json = "{\"id\":9007199254740993}", .version = 7, .expected_content_digest = @splat(7) }}, .unique);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.tables[0].integrity_commands.len);
    try std.testing.expect(result.tables[0].integrity_commands[0].operation == .establish);
    try std.testing.expectEqual(@as(usize, 0), result.tables[0].writes.len);
    try std.testing.expectEqual(@as(u64, 7), result.tables[0].predicates[0].expected_version);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &result.tables[0].predicates[0].expected_content_digest.?);
    var partial_activation = try prepareBackfill(alloc, source, &metadata, "parents", &.{.{ .key = "child-row", .json = "{\"parent_id\":9007199254740993}", .version = 7, .expected_content_digest = @splat(7) }}, .foreign_key);
    defer partial_activation.deinit();
    try std.testing.expectEqual(@as(usize, 1), partial_activation.tables[0].integrity_commands.len);
    try std.testing.expect(partial_activation.tables[0].integrity_commands[0].operation == .attach);
    try std.testing.expectEqual(@as(usize, 0), partial_activation.tables[0].writes.len);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &partial_activation.tables[0].predicates[0].expected_content_digest.?);
    // Retirement scans are projections too. Removing a UNIQUE claim or FK
    // reference must not revalidate unrelated required primary columns, and
    // must keep the observed full-primary proof rather than hashing this JSON.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = &metadata };
    defer builder.deinit();
    const loaded = try builder.load("parents");
    inline for (.{ .{ .unique, "pk", "id" }, .{ .foreign_key, "parent", "parent_id" } }) |selection| {
        const generation = loaded.catalog.find(selection[0], selection[1]).?.generation;
        const progress: @import("../storage/db/relational_integrity_retirement_contract.zig").Progress = .{
            .job_id = @splat(1),
            .owner = @splat(2),
            .target_schema_digest = @splat(3),
            .generation_set = @import("../storage/db/relational_integrity_activation_contract.zig").generationSet(loaded.catalog),
            .schema_version = 1,
            .phase = if (selection[0] == .unique) .unique else .foreign_keys,
            .generations = &.{generation},
        };
        const projected = "{\"" ++ selection[2] ++ "\":9007199254740993}";
        var retired = try prepareRetirementPage(alloc, source, &metadata, "parents", &.{.{ .key = "row", .json = projected, .version = 7, .expected_content_digest = @splat(7) }}, progress, .{});
        defer retired.deinit();
        try std.testing.expectEqual(@as(usize, 1), retired.tables[0].integrity_commands.len);
        if (selection[0] == .unique)
            try std.testing.expect(retired.tables[0].integrity_commands[0].operation == .repair_release)
        else
            try std.testing.expect(retired.tables[0].integrity_commands[0].operation == .repair_detach);
        try std.testing.expectEqual(@as(usize, 0), retired.tables[0].writes.len);
        try std.testing.expectEqual(@as(u64, 7), retired.tables[0].predicates[0].expected_version);
        try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(7)), &retired.tables[0].predicates[0].expected_content_digest.?);
        try std.testing.expectError(error.MissingPrimaryObservation, prepareRetirementPage(alloc, source, &metadata, "parents", &.{.{ .key = "row", .json = projected, .version = 7 }}, progress, .{}));
        for ([_][]const u8{ "{}", "{\"" ++ selection[2] ++ "\":null}", "{\"" ++ selection[2] ++ "\":\"invalid\"}" }) |invalid|
            try std.testing.expectError(error.InvalidBatchRequest, prepareRetirementPage(alloc, source, &metadata, "parents", &.{.{ .key = "row", .json = invalid, .version = 7, .expected_content_digest = @splat(7) }}, progress, .{}));
    }
    for ([_][]const u8{ "{}", "{\"id\":null}", "{\"id\":\"invalid\"}", "{\"id\":1,\"unknown\":2}" }) |invalid| {
        try std.testing.expectError(error.InvalidBatchRequest, prepareBackfill(alloc, source, &metadata, "parents", &.{.{ .key = "row", .json = invalid, .version = 7, .expected_content_digest = @splat(7) }}, .unique));
    }
    var validator = try schema_api.CompiledTableValidator.init(alloc, public_schema);
    defer validator.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, validator.schema);
    defer @import("../storage/schema.zig").freeSchema(alloc, runtime);
    var layout = try @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout.init(alloc, runtime);
    defer layout.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, mapper.PreparedRelationalWrite.init(alloc, "row", "{\"id\":1}", null, runtime, &layout));
}

test "distributed txn public integrity adapter enlists generated parent commands and guarded child writes" {
    const alloc = std.testing.allocator;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent","child_columns":["tenant","id"],"parent_table":"parents","parent_columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"},"payload":{"type":"string"}},"required":["tenant","id","payload"],"additionalProperties":false}}}}
    ;
    const parent_catalog = try testCatalogEnvelope(alloc, 1, parent_schema);
    defer alloc.free(parent_catalog);
    const child_catalog = try testCatalogEnvelope(alloc, 2, child_schema);
    defer alloc.free(child_catalog);
    const Fake = struct {
        parent_catalog: []const u8,
        child_catalog: []const u8,
        lookups: usize = 0,
        fn lookup(ptr: *anyopaque, allocator: Allocator, table: []const u8, key: []const u8, opts: types.LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@import("../raft/read_gate.zig").ReadConsistency.read_index, consistency);
            if (opts.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, if (std.mem.eql(u8, table, "parents")) self.parent_catalog else self.child_catalog), .version = 0 };
            self.lookups += 1;
            try std.testing.expectEqualStrings("child-1", key);
            return null;
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
    };
    var fake: Fake = .{ .parent_catalog = parent_catalog, .child_catalog = child_catalog };
    const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
    const metadata = [_]TableRecord{ .{ .table_id = 1, .name = "parents", .placement_role = "data", .schema_json = parent_schema }, .{ .table_id = 2, .name = "children", .placement_role = "data", .schema_json = child_schema } };
    const request = [_]contract.TableCommitRequest{.{ .table_name = "children", .writes = &.{.{ .key = "child-1", .value = "{\"tenant\":\"Acme\",\"id\":9007199254740993,\"payload\":\"not projected for FK validation\"}" }} }};
    var prepared = try prepare(alloc, source, &metadata, &request);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.tables.len);
    try std.testing.expectEqual(@as(usize, 1), fake.lookups);
    try std.testing.expectEqual(@as(?u32, 1), prepared.tables[0].relational_schema_version);
    try std.testing.expectEqual(@as(u64, 0), prepared.tables[0].predicates[0].expected_version);
    try std.testing.expectEqualStrings("parents", prepared.tables[1].table_name);
    try std.testing.expectEqual(@as(usize, 1), prepared.tables[1].integrity_commands.len);
    try std.testing.expect(prepared.tables[1].integrity_commands[0].operation == .attach);
    try std.testing.expectEqualStrings("child-1", prepared.tables[1].integrity_commands[0].operation.attach.child_key);
    // Unique coverage is independent of target FK availability. This is also
    // required for cyclic table activation to make progress phase by phase.
    var unique_backfill = try prepareBackfill(alloc, source, metadata[1..], "children", &.{.{ .key = "child-1", .json = request[0].writes[0].value, .version = 7, .expected_content_digest = @splat(7) }}, .unique);
    defer unique_backfill.deinit();
    try std.testing.expectEqual(@as(usize, 1), unique_backfill.tables.len);
    try std.testing.expectEqual(@as(usize, 0), unique_backfill.tables[0].integrity_commands.len);
    try std.testing.expectEqual(@as(u64, 7), unique_backfill.tables[0].predicates[0].expected_version);
    var fk_backfill = try prepareBackfill(alloc, source, &metadata, "children", &.{.{ .key = "child-1", .json = "{\"tenant\":\"Acme\",\"id\":9007199254740993}", .version = 7, .expected_content_digest = @splat(7) }}, .foreign_key);
    defer fk_backfill.deinit();
    try std.testing.expectEqual(@as(usize, 2), fk_backfill.tables.len);
    try std.testing.expect(fk_backfill.tables[1].integrity_commands[0].operation == .attach);
    try std.testing.expectEqual(@as(usize, 0), fk_backfill.tables[0].writes.len);
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, source, &metadata, &.{.{ .table_name = "children", .writes = &.{.{ .key = "child-1", .value = "[]" }} }}));
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, source, &metadata, &.{ request[0], request[0] }));
    var stale = request[0];
    stale.relational_schema_version = 2;
    try std.testing.expectError(error.PreparedGenerationChanged, prepare(alloc, source, &metadata, &.{stale}));

    // Closure traversal and command emission share the exact expansion. A
    // changed row invalidates only that expansion, not immutable table plans.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = &metadata };
    defer builder.deinit();
    const loaded = try builder.load("children");
    var work: Work = .{ .table = loaded, .key = "child-1", .before = null, .after = request[0].writes[0].value };
    const first = try builder.expandWork(&work);
    const bytes = builder.prepared_bytes;
    for (0..1000) |_| {
        const cached = try builder.expandWork(&work);
        try std.testing.expectEqual(first.commands.ptr, cached.commands.ptr);
    }
    try std.testing.expectEqual(bytes, builder.prepared_bytes);
    const plan = try builder.bindingPlan(loaded);
    work.after = "{\"tenant\":\"Acme\",\"id\":2,\"payload\":\"changed\"}";
    try builder.enqueue(&work);
    const changed = try builder.expandWork(&work);
    try std.testing.expect(first.commands.ptr != changed.commands.ptr);
    try std.testing.expectEqual(plan, try builder.bindingPlan(loaded));
}

test "distributed txn atomic cascade closure handles delete cycles update cycles and set null" {
    const alloc = std.testing.allocator;
    inline for (.{ "cascade", "set_null" }) |delete_action| {
        const schema_a =
            "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"id\"]}],\"foreign_keys\":[{\"name\":\"fk\",\"child_columns\":[\"id\"],\"parent_table\":\"b\",\"parent_columns\":[\"id\"],\"on_delete\":\"cascade\",\"on_update\":\"cascade\"}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\",\"nullable\":true}},\"additionalProperties\":false}}}}";
        const schema_b =
            "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"id\"]}],\"foreign_keys\":[{\"name\":\"fk\",\"child_columns\":[\"id\"],\"parent_table\":\"a\",\"parent_columns\":[\"id\"],\"on_delete\":\"" ++ delete_action ++ "\",\"on_update\":\"cascade\"}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\",\"nullable\":true}},\"additionalProperties\":false}}}}";
        const envelope_a = try testCatalogEnvelope(alloc, 10, schema_a);
        defer alloc.free(envelope_a);
        const envelope_b = try testCatalogEnvelope(alloc, 20, schema_b);
        defer alloc.free(envelope_b);
        const Fake = struct {
            a: []const u8,
            b: []const u8,
            claims: [2]planner.storage.Claim = undefined,
            references: [2]planner.storage.Reference = undefined,
            addresses: [2]planner.storage.Address = undefined,
            reference_reads: usize = 0,
            omit_primary_proof: bool = false,
            fn lookup(ptr: *anyopaque, allocator: Allocator, table: []const u8, _: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const i: usize = if (std.mem.eql(u8, table, "a")) 0 else 1;
                if (opts.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, if (i == 0) self.a else self.b), .version = 0 };
                if (opts.relational_integrity_jobs_json.len != 0) {
                    self.reference_reads += 1;
                    const Response = struct {
                        address: planner.storage.Address,
                        claim: planner.storage.Claim,
                        references: []const planner.storage.Reference,
                        next: ?[]const u8 = null,
                        pub fn jsonStringify(self_response: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
                            return @import("../storage/db/relational_integrity_json.zig").write(self_response, stream);
                        }
                    };
                    return .{ .json = try std.json.Stringify.valueAlloc(allocator, Response{ .address = self.addresses[i], .claim = self.claims[i], .references = self.references[i..][0..1] }, .{}), .version = 0 };
                }
                return .{ .json = try allocator.dupe(u8, "{\"id\":1}"), .version = 7, .expected_content_digest = if (self.omit_primary_proof) null else @as([32]u8, @splat(7)) };
            }
            fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
                return error.UnexpectedCall;
            }
            fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
                return error.UnexpectedCall;
            }
        };
        var fake: Fake = .{ .a = envelope_a, .b = envelope_b };
        const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
        const metadata = [_]TableRecord{ .{ .table_id = 10, .name = "a", .placement_role = "data", .schema_json = schema_a }, .{ .table_id = 20, .name = "b", .placement_role = "data", .schema_json = schema_b } };
        // No pre-release peer compatibility fallback: even non-TTL rows must
        // supply an exact physical observation, not merely a timestamp.
        fake.omit_primary_proof = true;
        try std.testing.expectError(error.MissingPrimaryObservation, prepare(alloc, source, &metadata, &.{.{ .table_name = "a", .deletes = &.{"a"} }}));
        fake.omit_primary_proof = false;
        try std.testing.expectError(error.MissingPrimaryObservation, prepareBackfill(alloc, source, &metadata, "a", &.{.{ .key = "a", .json = "{\"id\":1}", .version = 7 }}, .unique));
        var a_unique = try prepareBackfill(alloc, source, &metadata, "a", &.{.{ .key = "a", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .unique);
        defer a_unique.deinit();
        var b_unique = try prepareBackfill(alloc, source, &metadata, "b", &.{.{ .key = "b", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .unique);
        defer b_unique.deinit();
        var a_foreign = try prepareBackfill(alloc, source, &metadata, "a", &.{.{ .key = "a", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .foreign_key);
        defer a_foreign.deinit();
        var b_foreign = try prepareBackfill(alloc, source, &metadata, "b", &.{.{ .key = "b", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .foreign_key);
        defer b_foreign.deinit();
        fake.claims = .{ a_unique.tables[0].integrity_commands[0].operation.establish, b_unique.tables[0].integrity_commands[0].operation.establish };
        fake.addresses = .{ a_unique.tables[0].integrity_commands[0].address, b_unique.tables[0].integrity_commands[0].address };
        fake.references = .{ b_foreign.tables[1].integrity_commands[0].operation.attach, a_foreign.tables[1].integrity_commands[0].operation.attach };
        var deletion = try prepare(alloc, source, &metadata, &.{.{ .table_name = "a", .deletes = &.{"a"} }});
        defer deletion.deinit();
        try std.testing.expectEqual(@as(usize, 2), deletion.tables.len);
        try std.testing.expectEqualStrings("a", deletion.tables[0].deletes[0]);
        if (comptime std.mem.eql(u8, delete_action, "cascade")) {
            try std.testing.expectEqualStrings("b", deletion.tables[1].deletes[0]);
        } else {
            try std.testing.expectEqualStrings("{\"id\":null}", deletion.tables[1].writes[0].value);
        }
        try std.testing.expectEqual(@as(usize, 2), fake.reference_reads);
        var updated = try prepare(alloc, source, &metadata, &.{.{ .table_name = "a", .writes = &.{.{ .key = "a", .value = "{\"id\":2}" }} }});
        defer updated.deinit();
        try std.testing.expectEqual(@as(usize, 2), updated.tables.len);
        try std.testing.expectEqualStrings("{\"id\":2}", updated.tables[0].writes[0].value);
        try std.testing.expectEqualStrings("{\"id\":2}", updated.tables[1].writes[0].value);
    }
}

test "distributed txn deferred NO ACTION retains unchanged child proof through final parent replacement" {
    const alloc = std.testing.allocator;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"p","parent_columns":["id"],"on_delete":"no_action","on_update":"no_action","deferrable":true,"timing":"deferred"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const parent_catalog = try testCatalogEnvelope(alloc, 1, parent_schema);
    defer alloc.free(parent_catalog);
    const child_catalog = try testCatalogEnvelope(alloc, 2, child_schema);
    defer alloc.free(child_catalog);
    const Fake = struct {
        parent_catalog: []const u8,
        child_catalog: []const u8,
        claim: planner.storage.Claim = undefined,
        address: planner.storage.Address = undefined,
        reference: planner.storage.Reference = undefined,
        fn lookup(ptr: *anyopaque, allocator: Allocator, table: []const u8, key: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (opts.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, if (std.mem.eql(u8, table, "p")) self.parent_catalog else self.child_catalog), .version = 0 };
            if (opts.relational_integrity_jobs_json.len != 0) {
                var output: std.Io.Writer.Allocating = .init(allocator);
                defer output.deinit();
                var stream: std.json.Stringify = .{ .writer = &output.writer };
                try @import("../storage/db/relational_integrity_json.zig").write(.{ .address = self.address, .claim = self.claim, .references = @as([]const planner.storage.Reference, &.{self.reference}), .next = @as(?[]const u8, null) }, &stream);
                return .{ .json = try output.toOwnedSlice(), .version = 0 };
            }
            if (std.mem.eql(u8, key, "new")) return null;
            return .{ .json = try allocator.dupe(u8, "{\"id\":1}"), .version = 7, .expected_content_digest = @splat(7) };
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
    };
    var fake: Fake = .{ .parent_catalog = parent_catalog, .child_catalog = child_catalog };
    const source: reads.TableReadSource = .{ .ptr = &fake, .vtable = &.{ .lookup = Fake.lookup, .scan = Fake.scan, .query = Fake.query } };
    const metadata = [_]TableRecord{ .{ .table_id = 1, .name = "p", .placement_role = "data", .schema_json = parent_schema }, .{ .table_id = 2, .name = "c", .placement_role = "data", .schema_json = child_schema } };
    var parents = try prepareBackfill(alloc, source, &metadata, "p", &.{.{ .key = "old", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .unique);
    defer parents.deinit();
    fake.address = parents.tables[0].integrity_commands[0].address;
    fake.claim = parents.tables[0].integrity_commands[0].operation.establish;
    var children = try prepareBackfill(alloc, source, &metadata, "c", &.{.{ .key = "child", .json = "{\"id\":1}", .version = 7, .expected_content_digest = @splat(7) }}, .foreign_key);
    defer children.deinit();
    fake.reference = children.tables[1].integrity_commands[0].operation.attach;
    var result = try prepare(alloc, source, &metadata, &.{.{ .table_name = "p", .deletes = &.{"old"}, .writes = &.{.{ .key = "new", .value = "{\"id\":1}" }} }});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.tables.len);
    const child = result.tables[1];
    try std.testing.expectEqualStrings("c", child.table_name);
    try std.testing.expectEqual(@as(usize, 0), child.writes.len);
    try std.testing.expectEqual(@as(usize, 0), child.deletes.len);
    try std.testing.expectEqualStrings("child", child.predicates[0].key);
    try std.testing.expectEqual(@as(u64, 7), child.predicates[0].expected_version);
    var detaches: usize = 0;
    var attaches: usize = 0;
    for (result.tables[0].integrity_commands) |command| switch (command.operation) {
        .detach => detaches += 1,
        .attach => attaches += 1,
        else => {},
    };
    try std.testing.expectEqual(@as(usize, 1), detaches);
    try std.testing.expectEqual(@as(usize, 1), attaches);
    // Deleting the child in the same request must not restore its old FK.
    var both = try prepare(alloc, source, &metadata, &.{ .{ .table_name = "p", .deletes = &.{"old"} }, .{ .table_name = "c", .deletes = &.{"child"} } });
    defer both.deinit();
    for (both.tables[0].integrity_commands) |command| try std.testing.expect(command.operation != .attach);
}

test "distributed txn atomic cascade adapter composes real native reference pages and commit guards" {
    try testNativeCascade(false);
}

test "distributed txn generated parent values drive canonical cascade assignments" {
    try testNativeCascade(true);
}

fn testNativeCascade(generated: bool) !void {
    const db_mod = @import("../storage/db/db.zig");
    const gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/cascade", .{tmp.sub_path});
    var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 500, .shard_id = 501 }, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    const declaration = if (generated)
        \\{"version":1,"storage_mode":"relational","default_type":"row","generated_columns":[{"column":"id","expression":{"op":"add","args":[{"op":"column","column":"seed"},{"op":"literal","type":"integer","value":"1"}]}}],"unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"],"on_delete":"cascade","on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"seed":{"type":"integer"},"id":{"type":"integer"},"parent":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    else
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"],"on_delete":"cascade","on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    try db.setSchemaJson(alloc, declaration);
    const Fixture = struct {
        db: *db_mod.DB,
        reference_pages: usize = 0,
        fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, options: types.LookupOptions, consistency: gate.ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(gate.ReadConsistency.read_index, consistency);
            if (options.relational_integrity_jobs_json.len != 0) self.reference_pages += 1;
            const result = (try self.db.lookup(allocator, key, options)) orelse return null;
            const internal = options.relational_integrity_catalog or options.relational_integrity_jobs_json.len != 0 or options.relational_index_status_json.len != 0 or options.relational_activation_json.len != 0;
            return .{ .json = result.json, .version = if (internal) 0 else result.version orelse try self.db.getTimestamp(allocator, key), .expected_content_digest = result.expected_content_digest };
        }
        fn scan(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: gate.ReadConsistency) !?reads.ScanResponse {
            return error.UnexpectedCall;
        }
        fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
            return error.UnexpectedCall;
        }
        fn commit(database: *db_mod.DB, prepared: Prepared, id: u8) !void {
            try std.testing.expectEqual(@as(usize, 1), prepared.tables.len);
            const request = prepared.tables[0];
            const txn = try database.beginTransactionWithId(@splat(id), @as(u64, id) * 100);
            try database.writeTransaction(txn, .{ .relational_schema_version = request.relational_schema_version, .relational_integrity_generation_set = request.relational_integrity_generation_set, .writes = request.writes, .deletes = request.deletes, .predicates = request.predicates, .integrity_commands = request.integrity_commands });
            try database.commitTransaction(txn, @as(u64, id) * 100 + 1);
        }
    };
    var fixture: Fixture = .{ .db = &db };
    const source: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
    const tables = [_]TableRecord{.{ .table_id = 500, .name = "rows", .placement_role = "data", .schema_json = declaration }};
    const ranges = [_]RangeRecord{.{ .group_id = 501, .table_id = 500, .start_key = "" }};
    var inserted = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{
        .{ .key = "p", .value = if (generated) "{\"seed\":0,\"parent\":null}" else "{\"id\":1,\"parent\":null}" },
        .{ .key = "c", .value = if (generated) "{\"seed\":1,\"parent\":1}" else "{\"id\":2,\"parent\":1}" },
    } }});
    defer inserted.deinit();
    try Fixture.commit(&db, inserted, 1);
    if (generated) {
        var updated = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "p", .value = "{\"seed\":9007199254740993,\"parent\":null}" }} }});
        defer updated.deinit();
        try Fixture.commit(&db, updated, 2);
        const child = (try db.lookup(alloc, "c", .{})).?;
        defer alloc.free(child.json);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, child.json, .{ .parse_numbers = false });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("9007199254740994", parsed.value.object.get("parent").?.number_string);
        fixture.reference_pages = 0;
    }
    var removed = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p"} }});
    defer removed.deinit();
    try std.testing.expectEqual(@as(usize, 2), removed.tables[0].deletes.len);
    try std.testing.expectEqual(@as(usize, 2), removed.tables[0].predicates.len);
    try std.testing.expectEqual(@as(usize, 2), fixture.reference_pages);
    try Fixture.commit(&db, removed, if (generated) 3 else 2);
    try std.testing.expect((try db.lookup(alloc, "p", .{})) == null);
    try std.testing.expect((try db.lookup(alloc, "c", .{})) == null);
}

test "distributed txn MATCH PARTIAL nullable witnesses survive alternate deletion and apply last-witness actions" {
    const db_mod = @import("../storage/db/db.zig");
    const gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    inline for (.{ "restrict", "cascade", "set_null" }) |action| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/partial", .{tmp.sub_path});
        defer alloc.free(path);
        const open_options = db_mod.OpenOptions{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 500, .shard_id = 501 }, .primary_backend = .{ .lsm = .{} } };
        var db = try db_mod.DB.open(alloc, path, open_options);
        defer db.close();
        const declaration =
            "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"unique_constraints\":[{\"name\":\"pk\",\"columns\":[\"a\",\"b\"]}],\"relational_indexes\":[{\"name\":\"by_a\",\"keys\":[{\"column\":\"a\"}]},{\"name\":\"by_b\",\"keys\":[{\"column\":\"b\"}]}],\"foreign_keys\":[{\"name\":\"fk\",\"child_columns\":[\"x\",\"y\"],\"parent_table\":\"rows\",\"parent_columns\":[\"a\",\"b\"],\"match\":\"partial\",\"on_delete\":\"" ++ action ++ "\",\"on_update\":\"cascade\"}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\",\"nullable\":true},\"b\":{\"type\":\"integer\",\"nullable\":true},\"x\":{\"type\":\"integer\",\"nullable\":true},\"y\":{\"type\":\"integer\",\"nullable\":true}},\"additionalProperties\":false}}}}";
        try db.setSchemaJson(alloc, declaration);
        const Fixture = struct {
            db: *db_mod.DB,
            scans: usize = 0,
            fn lookup(ptr: *anyopaque, allocator: Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, _: gate.ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const result = (try self.db.lookup(allocator, key, opts)) orelse return null;
                return .{ .json = result.json, .version = result.version orelse 0, .expected_content_digest = result.expected_content_digest };
            }
            fn scan(ptr: *anyopaque, allocator: Allocator, _: []const u8, from: []const u8, to: []const u8, opts: types.ScanOptions, consistency: gate.ReadConsistency) !?reads.ScanResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(gate.ReadConsistency.read_index, consistency);
                var query_input = try std.json.parseFromSlice(struct { index: []const u8 }, allocator, opts.relational_query_json, .{ .ignore_unknown_fields = true });
                defer query_input.deinit();
                try std.testing.expect(query_input.value.index.len != 0);
                self.scans += 1;
                var result = try self.db.scan(allocator, from, to, opts);
                defer result.deinit(allocator);
                return .{ .ndjson = try @import("local_query_contract.zig").encodeStorageKernelScanNdjson(allocator, result, opts.include_documents) };
            }
            fn query(_: *anyopaque, _: Allocator, _: []const u8, _: types.SearchRequest, _: gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
                return error.UnexpectedCall;
            }
            fn commit(database: *db_mod.DB, prepared: Prepared, id: u8) !void {
                try std.testing.expectEqual(@as(usize, 1), prepared.tables.len);
                const request = prepared.tables[0];
                const txn = try database.beginTransactionWithId(@splat(id), @as(u64, id) * 100);
                errdefer database.abortTransaction(txn, @as(u64, id) * 100 + 1) catch {};
                try database.writeTransaction(txn, .{ .relational_schema_version = request.relational_schema_version, .relational_integrity_generation_set = request.relational_integrity_generation_set, .writes = request.writes, .deletes = request.deletes, .predicates = request.predicates, .integrity_commands = request.integrity_commands });
                try database.commitTransaction(txn, @as(u64, id) * 100 + 1);
            }
        };
        var fixture: Fixture = .{ .db = &db };
        const source: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
        const tables = [_]TableRecord{.{ .table_id = 500, .name = "rows", .placement_role = "data", .schema_json = declaration }};
        const ranges = [_]RangeRecord{.{ .group_id = 501, .table_id = 500, .start_key = "" }};
        inline for (.{ "by_a", "by_b" }) |index_name| {
            for (0..16) |_| {
                if ((try db.relationalIndexBuildStatus(index_name)).state == .ready) break;
                try db.buildRelationalIndexStep(index_name, .{});
            } else return error.IndexBuildDidNotConverge;
        }
        // A new session child can use a nullable witness inserted by a prior
        // statement, even though the physical support index remains empty.
        var staged_partial = try prepareSessionStatement(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "staged-parent", .value = "{\"a\":44,\"b\":null}" }} }}, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "staged-child", .value = "{\"a\":55,\"b\":55,\"x\":44,\"y\":null}" }} }}, .{});
        staged_partial.deinit();
        var inserted = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{
            .{ .key = "p1", .value = "{\"a\":1,\"b\":null}" },
            .{ .key = "p2", .value = "{\"a\":1,\"b\":2}" },
            .{ .key = "child", .value = "{\"a\":9,\"b\":9,\"x\":1,\"y\":null}" },
            .{ .key = "exempt", .value = "{\"x\":null,\"y\":null}" },
        } }});
        defer inserted.deinit();
        try Fixture.commit(&db, inserted, 1);
        var removed = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p2"} }});
        defer removed.deinit();
        var racing = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p1"} }});
        defer racing.deinit();
        try std.testing.expectEqual(@as(usize, 1), removed.tables[0].deletes.len);
        try Fixture.commit(&db, removed, 2);
        // Both planners saw another compatible parent. The exact witness
        // guards must prevent the second removal from committing an orphan.
        try std.testing.expectError(error.ForeignKeyParentMissing, Fixture.commit(&db, racing, 3));
        const retained = (try db.lookup(alloc, "child", .{})).?;
        alloc.free(retained.json);
        var changed = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "p1", .value = "{\"a\":2,\"b\":null}" }} }});
        defer changed.deinit();
        try Fixture.commit(&db, changed, 4);
        const cascaded = (try db.lookup(alloc, "child", .{})).?;
        defer alloc.free(cascaded.json);
        var cascaded_value = try std.json.parseFromSlice(std.json.Value, alloc, cascaded.json, .{});
        defer cascaded_value.deinit();
        try std.testing.expectEqual(@as(i64, 2), cascaded_value.value.object.get("x").?.integer);
        try std.testing.expect(cascaded_value.value.object.get("y").? == .null);
        db.close();
        db = try db_mod.DB.open(alloc, path, open_options);
        if (comptime std.mem.eql(u8, action, "restrict")) {
            try std.testing.expectError(error.ForeignKeyReferenced, prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p1"} }}));
        } else {
            var last = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p1"} }});
            defer last.deinit();
            try Fixture.commit(&db, last, 5);
            const result = try db.lookup(alloc, "child", .{});
            if (comptime std.mem.eql(u8, action, "cascade")) {
                try std.testing.expect(result == null);
            } else {
                const child = result orelse return error.TestExpectedEqual;
                defer alloc.free(child.json);
                var value = try std.json.parseFromSlice(std.json.Value, alloc, child.json, .{});
                defer value.deinit();
                try std.testing.expect(value.value.object.get("x").? == .null);
                try std.testing.expect(value.value.object.get("y").? == .null);
            }
        }
        try std.testing.expect(fixture.scans > 0);
        missing: {
            var orphan = prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{.{ .key = "orphan", .value = "{\"x\":99,\"y\":null}" }} }}) catch |err| {
                try std.testing.expectEqual(error.ForeignKeyParentMissing, err);
                break :missing;
            };
            defer orphan.deinit();
            return error.TestExpectedError;
        }
    }
}
