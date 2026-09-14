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
};

const Work = struct {
    table: *Loaded,
    key: []const u8,
    before: ?reads.LookupResponse,
    after: ?[]const u8,
    assignments: std.StringHashMapUnmanaged(std.json.Value) = .empty,
    queued: bool = false,
    dirty: bool = false,
    explicit: bool = false,
};

const Builder = struct {
    alloc: Allocator,
    source: reads.TableReadSource,
    metadata: []const TableRecord,
    loaded: std.ArrayList(*Loaded) = .empty,
    output: std.ArrayList(contract.TableCommitRequest) = .empty,
    command_lists: std.ArrayList(std.ArrayList(planner.storage.Command)) = .empty,
    predicate_lists: std.ArrayList(std.ArrayList(types.TransactionVersionPredicate)) = .empty,
    work: std.ArrayList(*Work) = .empty,
    by_row: std.StringHashMapUnmanaged(*Work) = .empty,
    queue: std.ArrayList(*Work) = .empty,
    prepared_bytes: usize = 0,
    control: RequestContext = .{},
    repair_table: ?[]const u8 = null,

    fn repairing(self: *const Builder, name: []const u8) bool {
        return if (self.repair_table) |target| std.mem.eql(u8, target, name) else false;
    }

    fn lookup(self: *Builder, table: []const u8, key: []const u8, options: types.LookupOptions) !?reads.LookupResponse {
        try self.control.ensureActive();
        var opts = options;
        opts.include_primary_digest = !options.relational_integrity_catalog and !options.relational_integrity_action and
            options.relational_integrity_jobs_json.len == 0 and options.relational_activation_json.len == 0 and options.relational_topology_json.len == 0;
        opts.execution_deadline_ns = self.control.deadline_ns;
        opts.execution_io = self.control.deadline_io;
        opts.cancellation = self.control.cancellation;
        return self.source.lookup(self.alloc, table, key, opts, .read_index);
    }

    fn charge(self: *Builder, bytes: usize) !void {
        self.prepared_bytes = std.math.add(usize, self.prepared_bytes, bytes) catch return error.TransactionTooLarge;
        if (self.prepared_bytes > 16 * 1024 * 1024) return error.TransactionTooLarge;
    }

    fn rowIdentity(self: *Builder, table: []const u8, key: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.alloc, "{d}:{s}{s}", .{ table.len, table, key });
    }

    fn getWork(self: *Builder, table: *Loaded, key: []const u8) !*Work {
        const identity = try self.rowIdentity(table.name, key);
        if (self.by_row.get(identity)) |existing| return existing;
        if (self.work.items.len >= 4096) return error.TransactionTooLarge;
        const old = try self.lookup(table.name, key, .{});
        if (old) |row| if (row.expected_content_digest == null)
            return error.MissingPrimaryObservation;
        try self.charge(key.len + if (old) |row| row.json.len else @as(usize, 0));
        const item = try self.alloc.create(Work);
        item.* = .{ .table = table, .key = try self.alloc.dupe(u8, key), .before = old, .after = if (old) |row| row.json else null };
        try self.work.append(self.alloc, item);
        try self.by_row.put(self.alloc, identity, item);
        return item;
    }

    fn enqueue(self: *Builder, item: *Work) !void {
        item.dirty = true;
        if (item.queued) return;
        try self.queue.append(self.alloc, item);
        item.queued = true;
        if (self.queue.items.len > 16_384) return error.TransactionTooLarge;
    }

    fn expandWork(self: *Builder, item: *Work) !planner.Expansion {
        var plan = try self.bindingPlan(item.table);
        defer plan.deinit();
        const view = item.table.view;
        var before: ?mapper.PreparedRelationalWrite = if (item.before) |row| try mapper.PreparedRelationalWrite.init(self.alloc, item.key, row.json, null, view.tableSchema().*, view.physicalLayout()) else null;
        defer if (before) |*row| row.deinit(self.alloc);
        var after: ?mapper.PreparedRelationalWrite = if (item.after) |json| try mapper.PreparedRelationalWrite.init(self.alloc, item.key, json, view.validator(), view.tableSchema().*, view.physicalLayout()) else null;
        defer if (after) |*row| row.deinit(self.alloc);
        return plan.expand(self.alloc, &.{.{ .key = item.key, .before = if (before) |*row| try row.typedView(view.tableSchema().*, view.physicalLayout()) else null, .after = if (after) |*row| try row.typedView(view.tableSchema().*, view.physicalLayout()) else null, .repair = self.repairing(item.table.name) }});
    }

    fn childStillReferences(self: *Builder, item: *Work, definition: native.ForeignKey, address: planner.storage.Address) !bool {
        const after = item.after orelse return false;
        // Explicit moves sever their old relationship. A row already moved by
        // another cascading edge still participates in its original edges, so
        // contradictory cascades are detected rather than silently skipped.
        const json = if (!item.explicit and item.before != null) item.before.?.json else after;
        const view = item.table.view;
        var row = try mapper.PreparedRelationalWrite.init(self.alloc, item.key, json, null, view.tableSchema().*, view.physicalLayout());
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
        if (action == .restrict or action == .no_action) return;
        const child = try self.getWork(child_table, reference.child_key);
        // An explicit mutation may already move/delete this child. A stale
        // reverse reference may not force a mutation of its new relationship.
        if (!try self.childStillReferences(child, definition, transition.address)) return;
        if (action == .cascade and parent.after == null) {
            child.after = null;
            try self.enqueue(child);
            return;
        }
        var child_json = try std.json.parseFromSlice(std.json.Value, self.alloc, child.after.?, .{ .allocate = .alloc_always });
        // Parsed values are retained in the shared request arena by assignments.
        const object = &child_json.value.object;
        var parent_json: ?std.json.Parsed(std.json.Value) = if (parent.after) |json| try std.json.parseFromSlice(std.json.Value, self.alloc, json, .{}) else null;
        defer if (parent_json) |*value| value.deinit();
        var changed = false;
        for (definition.child_columns, definition.parent_columns) |child_column, parent_column| {
            const next: std.json.Value = if (action == .set_null) .null else parent_json.?.value.object.get(parent_column) orelse return error.InvalidIntegrityRecord;
            const encoded_next = try std.json.Stringify.valueAlloc(self.alloc, next, .{});
            const copied = try std.json.parseFromSlice(std.json.Value, self.alloc, encoded_next, .{ .allocate = .alloc_always });
            if (child.assignments.get(child_column)) |prior| {
                const encoded_prior = try std.json.Stringify.valueAlloc(self.alloc, prior, .{});
                if (!std.mem.eql(u8, encoded_prior, encoded_next)) return error.ForeignKeyActionConflict;
            }
            const existing = object.get(child_column) orelse .null;
            const encoded_existing = try std.json.Stringify.valueAlloc(self.alloc, existing, .{});
            if (std.mem.eql(u8, encoded_existing, encoded_next)) continue;
            try child.assignments.put(self.alloc, try self.alloc.dupe(u8, child_column), copied.value);
            try object.put(self.alloc, child_column, copied.value);
            changed = true;
        }
        if (changed) {
            child.after = try std.json.Stringify.valueAlloc(self.alloc, child_json.value, .{});
            try self.charge(child.after.?.len);
            try self.enqueue(child);
        }
    }

    fn expandParent(self: *Builder, item: *Work, transition: planner.ParentTransition) !void {
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
        for (self.loaded.items) |table| {
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
        var response = (try self.lookup(name, reads.TableReadSource.integrity_catalog_lookup_key, .{ .relational_integrity_catalog = true })) orelse return error.IntegrityCatalogUnavailable;
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
        const expected = try declarations.definitionFingerprints(self.alloc, view.validator().?.schema, view.tableSchema().*);
        defer declarations.freeDefinitions(self.alloc, expected);
        for (expected) |definition| {
            const binding = bindings.find(definition.kind, definition.name) orelse return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &binding.definition.fingerprint, &definition.fingerprint)) return error.PreparedGenerationChanged;
        }
        const table = try self.alloc.create(Loaded);
        table.* = .{
            .name = record.name,
            .view = view,
            .catalog = bindings,
            .uniques = try view.validator().?.schema.relationalUniqueDefinitions(self.alloc),
            .foreign = try view.validator().?.schema.relationalForeignKeyDefinitions(self.alloc),
        };
        try self.loaded.append(self.alloc, table);
        return table;
    }

    fn bindingPlan(self: *Builder, table: *Loaded) !planner.Plan {
        return self.bindingPlanSelected(table, true, true);
    }

    fn bindingPlanSelected(self: *Builder, table: *Loaded, with_unique: bool, with_foreign: bool) !planner.Plan {
        return self.bindingPlanFiltered(table, with_unique, with_foreign, null);
    }

    fn bindingPlanFiltered(self: *Builder, table: *Loaded, with_unique: bool, with_foreign: bool, retirement: ?@import("../storage/db/relational_integrity_retirement.zig").Progress) !planner.Plan {
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
            try foreign.append(self.alloc, .{
                .generation = generation,
                .parent_generation = (parent.catalog.find(.unique, target.name) orelse return error.PreparedGenerationChanged).generation,
                .definition = definition,
                .parent = parent.view,
                .parent_unique = target,
            });
        }
        return planner.Plan.init(self.alloc, table.name, table.view, uniques.items, foreign.items);
    }

    fn outputIndex(self: *Builder, table_name: []const u8, version: u32) !usize {
        const loaded = try self.load(table_name);
        const generation_set = @import("../storage/db/relational_integrity_activation.zig").generationSet(loaded.catalog);
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
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request_control), .repair_table = repair_table };
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
        if (declaration.unique_constraints == null and declaration.foreign_keys == null) continue;
        if (request.transforms.len != 0) return error.UnsupportedOperation;
        const table = try builder.load(request.table_name);
        _ = try builder.outputIndex(table.name, table.view.version());
        var writes = std.StringHashMapUnmanaged(?[]const u8).empty;
        for (request.writes) |write| try writes.put(builder.alloc, write.key, write.value);
        for (request.deletes) |key| try writes.put(builder.alloc, key, null);
        if (writes.count() > 4096) return error.TransactionTooLarge;
        var it = writes.iterator();
        while (it.next()) |entry| {
            const item = try builder.getWork(table, entry.key_ptr.*);
            item.explicit = true;
            item.after = entry.value_ptr.*;
            if (item.after) |json| {
                try builder.charge(json.len);
                const parsed = try std.json.parseFromSlice(std.json.Value, builder.alloc, json, .{ .allocate = .alloc_always });
                if (parsed.value != .object) return error.InvalidBatchRequest;
                var fields = parsed.value.object.iterator();
                while (fields.next()) |field| try item.assignments.put(builder.alloc, field.key_ptr.*, field.value_ptr.*);
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
        var expansion = try builder.expandWork(item);
        defer expansion.deinit();
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
        try builder.appendPredicate(i, item.key, if (item.before) |row| row.version else 0, if (item.before) |row| row.expected_content_digest else null);
        if (item.after) |json| try final_writes[i].append(builder.alloc, .{ .key = item.key, .value = json }) else try final_deletes[i].append(builder.alloc, item.key);
        // This arena is owned transitively by the returned request arena.
        const expansion = try builder.expandWork(item);
        for (expansion.commands) |command| try builder.appendCommand(command.table_name, command.command);
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
    for (builder.output.items, builder.command_lists.items, builder.predicate_lists.items) |*table, commands, predicates| {
        _ = try planner.storage.validateCommandAdmission(commands.items);
        table.integrity_commands = commands.items;
        table.predicates = predicates.items;
        table.relational_repair = builder.repairing(table.table_name);
    }
    return .{ .arena = arena, .tables = try builder.output.toOwnedSlice(builder.alloc) };
}

pub const BackfillRow = struct { key: []const u8, json: []const u8, version: u64, expected_content_digest: ?[32]u8 = null };
pub const BackfillPhase = enum { unique, foreign_key };

/// A positive local proof does not imply global uniqueness: another owner's
/// historical rows may still be unclaimed. Check every current owner once per
/// request. Proofs deliberately are not cached across restore incarnations.
pub fn ensureUniqueCoverage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_names: []const []const u8) !void {
    return ensureUniqueCoverageControlled(alloc, source, metadata, ranges, table_names, .{});
}

fn ensureUniqueCoverageControlled(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, table_names: []const []const u8, request_control: RequestContext) !void {
    const activation = @import("../storage/db/relational_integrity_activation.zig");
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
    const control = try boundedControl(request);
    var prepared = try prepareControlled(alloc, source, metadata, requests, control);
    errdefer prepared.deinit();
    const names = try alloc.alloc([]const u8, prepared.tables.len);
    defer alloc.free(names);
    for (prepared.tables, names) |table, *name| name.* = table.table_name;
    try ensureUniqueCoverageControlled(alloc, source, metadata, ranges, names, control);
    return prepared;
}

/// Administrative recovery may edit invalid rows, not bypass new-value checks.
/// Only the target table's incomplete historical coverage is exempted; every
/// external parent still needs global UNIQUE coverage. Native prepares require
/// failed activation and lock its exact checkpoint through the repair decision.
pub fn prepareRepair(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, ranges: []const RangeRecord, request: contract.TableCommitRequest, control: RequestContext) !Prepared {
    _ = try metadataRequiresCoordination(alloc, metadata, &.{request});
    if (!try requiresCoordination(alloc, (for (metadata) |table| {
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
pub fn prepareRetirementPage(alloc: Allocator, source: reads.TableReadSource, metadata: []const TableRecord, table_name: []const u8, rows: []const BackfillRow, progress: @import("../storage/db/relational_integrity_retirement.zig").Progress, request: RequestContext) !Prepared {
    if (rows.len > 128 or (progress.phase != .foreign_keys and progress.phase != .unique)) return error.InvalidConstraintRetirement;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    var builder: Builder = .{ .alloc = arena.allocator(), .source = source, .metadata = metadata, .control = try boundedControl(request) };
    defer builder.deinit();
    const table = try builder.load(table_name);
    if (table.view.version() != progress.schema_version or !std.mem.eql(u8, &@import("../storage/db/relational_integrity_activation.zig").generationSet(table.catalog), &progress.generation_set)) return error.ConstraintRetirementChanged;
    const row_table_index = try builder.outputIndex(table_name, table.view.version());
    var plan = try builder.bindingPlanFiltered(table, progress.phase == .unique, progress.phase == .foreign_keys, progress);
    defer plan.deinit();
    for (rows) |row| {
        try builder.charge(row.key.len + row.json.len);
        if (row.expected_content_digest == null) return error.MissingPrimaryObservation;
        try builder.appendPredicate(row_table_index, row.key, row.version, row.expected_content_digest);
        var prepared = try mapper.PreparedRelationalWrite.init(builder.alloc, row.key, row.json, null, table.view.tableSchema().*, table.view.physicalLayout());
        defer prepared.deinit(builder.alloc);
        const expansion = try plan.expand(builder.alloc, &.{.{ .key = row.key, .before = try prepared.typedView(table.view.tableSchema().*, table.view.physicalLayout()), .repair = true }});
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
    for (rows) |row| {
        try builder.charge(row.key.len + row.json.len);
        if (row.expected_content_digest == null) return error.MissingPrimaryObservation;
        try builder.appendPredicate(row_table_index, row.key, row.version, row.expected_content_digest);
        var prepared = try mapper.PreparedRelationalWrite.init(builder.alloc, row.key, row.json, null, table.view.tableSchema().*, table.view.physicalLayout());
        defer prepared.deinit(builder.alloc);
        const expansion = try plan.expand(builder.alloc, &.{.{ .key = row.key, .after = try prepared.typedView(table.view.tableSchema().*, table.view.physicalLayout()) }});
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
        table_request.integrity_commands = commands.items;
        table_request.predicates = predicates.items;
    }
    return .{ .arena = arena, .tables = try builder.output.toOwnedSlice(builder.alloc) };
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
    const activation = @import("../storage/db/relational_integrity_activation.zig");
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

test "distributed txn public integrity adapter enlists generated parent commands and guarded child writes" {
    const alloc = std.testing.allocator;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent","child_columns":["tenant","id"],"parent_table":"parents","parent_columns":["tenant","id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"integer"}},"additionalProperties":false}}}}
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
    const request = [_]contract.TableCommitRequest{.{ .table_name = "children", .writes = &.{.{ .key = "child-1", .value = "{\"tenant\":\"Acme\",\"id\":9007199254740993}" }} }};
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
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, source, &metadata, &.{.{ .table_name = "children", .writes = &.{.{ .key = "child-1", .value = "[]" }} }}));
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, source, &metadata, &.{ request[0], request[0] }));
    var stale = request[0];
    stale.relational_schema_version = 2;
    try std.testing.expectError(error.PreparedGenerationChanged, prepare(alloc, source, &metadata, &.{stale}));
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

test "distributed txn atomic cascade adapter composes real native reference pages and commit guards" {
    const db_mod = @import("../storage/db/db.zig");
    const gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}/cascade", .{tmp.sub_path});
    var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 500, .shard_id = 501 }, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    const declaration =
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
            const internal = options.relational_integrity_catalog or options.relational_integrity_jobs_json.len != 0 or options.relational_activation_json.len != 0;
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
    var inserted = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .writes = &.{ .{ .key = "p", .value = "{\"id\":1,\"parent\":null}" }, .{ .key = "c", .value = "{\"id\":2,\"parent\":1}" } } }});
    defer inserted.deinit();
    try Fixture.commit(&db, inserted, 1);
    var removed = try prepareWithCoverage(alloc, source, &tables, &ranges, &.{.{ .table_name = "rows", .deletes = &.{"p"} }});
    defer removed.deinit();
    try std.testing.expectEqual(@as(usize, 2), removed.tables[0].deletes.len);
    try std.testing.expectEqual(@as(usize, 2), removed.tables[0].predicates.len);
    try std.testing.expectEqual(@as(usize, 2), fixture.reference_pages);
    try Fixture.commit(&db, removed, 2);
    try std.testing.expect((try db.lookup(alloc, "p", .{})) == null);
    try std.testing.expect((try db.lookup(alloc, "c", .{})) == null);
}
