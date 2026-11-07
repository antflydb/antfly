// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Statement-owned trigger preparation shared by SQL, document mutations and
//! FK action expansion. This owner performs metadata reads before native apply.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const preparation = catalog.trigger_preparation;
const provider = @import("antfly_local_sources").api_operation;
const Capture = @import("sql_trigger_capture.zig");
const execution = @import("sql_execution.zig");
const domain = local.system_catalog_domain;
const Table = local.common_topology_records.TableRecord;
const A = std.mem.Allocator;
fn invalidSnapshot(comptime phase: []const u8) error{InvalidSqlBackendResponse} {
    if (comptime @import("builtin").mode == .debug) {
        const Once = struct {
            var seen: std.atomic.Value(bool) = .init(false);
        };
        if (!Once.seen.swap(true, .monotonic)) std.debug.print("raw broker invalid response phase={s}\n", .{phase});
    }
    return error.InvalidSqlBackendResponse;
}
const Entry = struct {
    binding: catalog.Table,
    record: Table,
    snapshot: @import("../system_catalog/trigger_snapshot.zig").Snapshot,
    incoming_records: []const Table,
    owner: *preparation.Owner,
    settings: ?*local.sql_setting_catalog.View = null,
    columns: local.system_catalog_column_identities.State,
    incoming_tables: []const u64,
    incoming_stamp: @FieldType(domain.QualifiedIncomingForeignKeys, "stamp"),
    coverage_revision: u64,
    begun: @FieldType(catalog.triggers.Entry, "events") = .{},
    update_names: []const []const u8 = &.{},
    update_ids: []const u64 = &.{},
    check: ?*@import("sql_policy_evaluation.zig").Evaluation = null,
    no_policy: bool = false,
    after_generations: [4]u32 = @splat(0),
    closure_captured: bool = false,
};
const Stage = struct { entry: *Entry, row: preparation.Row, output_digest: ?catalog.trigger_handoff.Digest = null, preimage_version: u64 = 0, preimage_digest: ?[32]u8 = null, physical_bound: bool = false, selected: bool = false, queued: bool = false, after_started: bool = false, actions: std.ArrayList(NamedReferentialAction) = .empty };
const AfterPhase = struct { entry: *Entry, event: @FieldType(preparation.Input, "event"), update_ids: []const u64, generation: u32 };
/// Trusted request-owned planner continuation, never part of a VM or manifest.
/// Its name is derived from the existing non-reused FK catalog allocation.
pub const ReferentialAction = catalog.trigger_provider.ReferentialAction;
const NamedReferentialAction = struct {
    name: []const u8,
    ptr: *anyopaque,
    run: *const fn (*anyopaque) anyerror!void,
};
const AfterEvent = union(enum) { row: *Stage, statement: AfterPhase, action: NamedReferentialAction };

pub const Broker = struct {
    root: *execution.Adapter,
    a: A,
    entries: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    records_by_name: std.StringHashMapUnmanaged(Table) = .empty,
    schema_absent_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,
    snapshot_bytes: usize = 0,
    stages: std.ArrayList(*Stage) = .empty,
    stage_tokens: std.AutoHashMapUnmanaged(*const anyopaque, *Stage) = .empty,
    after_phases: std.ArrayList(AfterPhase) = .empty,
    after_events: std.ArrayList(AfterEvent) = .empty,
    after_cursor: usize = 0,
    draining: bool = false,
    clock: catalog.trigger_manifest.Clock,
    statement_id: [16]u8,
    finished: bool = false,

    pub fn create(a: A, root: *execution.Adapter) !*Broker {
        const io = root.server.sharedApiIo() orelse return error.SqlClockUnavailable;
        const now = root.statement_instant orelse try (catalog.Clock.Source{ .io = io }).sample();
        const self = try a.create(Broker);
        var statement_id: [16]u8 = undefined;
        io.random(&statement_id);
        self.* = .{ .root = root, .a = a, .statement_id = statement_id, .clock = .{ .statement_micros = now.micros, .transaction_micros = root.context.sql_transaction_micros orelse now.micros } };
        return self;
    }
    pub fn deinit(self: *Broker) void {
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| {
            if (entry.*.check) |check| check.deinit();
            entry.*.owner.deinit();
            if (entry.*.settings) |settings| settings.deinit();
        }
        self.entries.deinit(self.a);
        self.records_by_name.deinit(self.a);
        self.schema_absent_ids.deinit(self.a);
        self.stages.deinit(self.a);
        self.stage_tokens.deinit(self.a);
        self.after_phases.deinit(self.a);
        self.after_events.deinit(self.a);
    }
    pub fn attach(self: *Broker, context: provider.RequestContext) provider.RequestContext {
        var result = context;
        result.table_triggers = .{ .ptr = self, .before = before, .after = after, .begin_statement = beginProviderStatement, .end_statement = endProviderStatement, .queue_action = queueProviderAction, .drain = drainProvider, .incoming = incomingProvider, .table_record = tableRecordProvider };
        return result;
    }
    pub fn attachSql(self: *Broker, context: provider.RequestContext) provider.RequestContext {
        var result = self.attach(context);
        result.table_triggers.?.reuse_explicit = true;
        return result;
    }
    fn incomingProvider(raw: *anyopaque, table: Table) ![]const Table {
        const self: *Broker = @ptrCast(@alignCast(raw));
        if (table.schema_json.len == 0) {
            const record = self.records_by_name.get(table.name) orelse return error.CatalogGenerationChanged;
            if (record.table_id != table.table_id or !self.schema_absent_ids.contains(table.table_id)) return error.CatalogGenerationChanged;
            return &.{};
        }
        const entry = try self.load(table);
        if (entry.incoming_records.len != entry.incoming_tables.len) return error.InvalidSqlBackendResponse;
        for (entry.incoming_records, entry.incoming_tables) |record, id| if (record.table_id != id) return error.InvalidSqlBackendResponse;
        return entry.incoming_records;
    }
    fn tableRecordProvider(raw: *anyopaque, name: []const u8) !?Table {
        const self: *Broker = @ptrCast(@alignCast(raw));
        return self.records_by_name.get(name);
    }
    fn rememberRecord(self: *Broker, record: Table) !void {
        if (self.records_by_name.get(record.name)) |known| {
            if (known.table_id != record.table_id or !std.mem.eql(u8, known.schema_json, record.schema_json)) return error.CatalogGenerationChanged;
            return;
        }
        if (self.records_by_name.count() >= 4096) return error.SqlProgramLimitExceeded;
        try self.records_by_name.put(self.a, record.name, record);
    }
    /// Called only with the actual native metadata preparation cut. A
    /// schema-free row cannot declare a VM or an incoming FK definition.
    pub fn rememberSchemaAbsent(self: *Broker, cut: @import("../system_catalog/trigger_snapshot.zig").Snapshot) !void {
        if (cut.record.schema_json.len != 0 or cut.incoming_records.len != 0 or cut.incoming_tables.len != 0 or cut.incoming_stamp.active_count != 0 or cut.source.value != .table or cut.source.value.table.binding.table_id != cut.record.table_id) return error.CatalogGenerationChanged;
        try self.rememberRecord(cut.record);
        try self.schema_absent_ids.put(self.a, cut.record.table_id, {});
    }
    fn beginProviderStatement(raw: *anyopaque, table: Table, event: @FieldType(preparation.Input, "event"), updated: []const []const u8) !void {
        const self: *Broker = @ptrCast(@alignCast(raw));
        try self.begin(table, event, updated);
    }
    fn endProviderStatement(raw: *anyopaque, table: Table, event: @FieldType(preparation.Input, "event"), updated: []const []const u8) !void {
        const self: *Broker = @ptrCast(@alignCast(raw));
        try self.queueAfter(table, event, updated);
    }
    fn queueProviderAction(raw: *anyopaque, token: ?*anyopaque, action: ReferentialAction) !void {
        const self: *Broker = @ptrCast(@alignCast(raw));
        try self.queueReferentialAction(token, action);
    }
    fn drainProvider(raw: *anyopaque) !void {
        const self: *Broker = @ptrCast(@alignCast(raw));
        // Check the complete initial candidate set before releasing its first
        // AFTER event. FK-produced candidates are checked at their queue turn.
        for (self.stages.items) |stage| {
            if (!stage.selected or stage.row.skipped) continue;
            try self.validateAfterRow(stage);
        }
        try self.drainAfter();
    }
    pub fn begin(self: *Broker, record: Table, event: @FieldType(preparation.Input, "event"), updated: []const []const u8) !void {
        const entry = try self.load(record);
        try self.captureClosure(entry);
        return self.beginCaptured(entry, event, updated);
    }
    fn beginCaptured(self: *Broker, entry: *Entry, event: @FieldType(preparation.Input, "event"), updated: []const []const u8) !void {
        const already = switch (event) {
            .insert => entry.begun.insert,
            .update => entry.begun.update,
            .delete => entry.begun.delete,
            .truncate => entry.begun.truncate,
        };
        const ids = try self.updateIds(entry, updated);
        if (event == .update) {
            entry.update_names = try self.a.dupe([]const u8, updated);
            entry.update_ids = ids;
        }
        if (already) return;
        _ = entry.owner.statement(.before, event, ids) catch |err| return normalizeNativeError(err);
        switch (event) {
            .insert => entry.begun.insert = true,
            .update => entry.begun.update = true,
            .delete => entry.begun.delete = true,
            .truncate => entry.begun.truncate = true,
        }
    }
    /// Called before either phase of predecision TRUNCATE evaluation.
    pub fn beginTruncate(self: *Broker, record: Table) !void {
        const entry = try self.load(record);
        // TRUNCATE has no row mutations or referential-action VM calls. Its
        // authoritative CASCADE set is already selected by the DDL owner;
        // never capture unrelated incoming FK executable closures here.
        const vm = entry.owner.execution orelse return error.CatalogGenerationChanged;
        try vm.requirePredecisionContract(catalog.trigger_runtime.predecision_contract_version);
        try self.beginCaptured(entry, .truncate, &.{});
    }
    pub fn beginStatement(self: *Broker, table: catalog.Table, statement: local.sql_ast.Statement) !void {
        return self.statementPhase(table, statement, false);
    }
    pub fn endStatement(self: *Broker, table: catalog.Table, statement: local.sql_ast.Statement) !void {
        return self.statementPhase(table, statement, true);
    }
    fn statementPhase(self: *Broker, table: catalog.Table, statement: local.sql_ast.Statement, end: bool) !void {
        var events: @FieldType(catalog.triggers.Entry, "events") = .{};
        var updated: std.ArrayList([]const u8) = .empty;
        switch (statement) {
            .insert => |insert| {
                events.insert = true;
                if (insert.conflict) |conflict| if (conflict.assignments.len != 0) {
                    events.update = true;
                    for (conflict.assignments) |assignment| try appendUpdated(self.a, &updated, assignment.field);
                };
            },
            .update => |update| {
                events.update = true;
                for (update.assignments) |assignment| try appendUpdated(self.a, &updated, assignment.field);
            },
            .delete => events.delete = true,
            .merge => |merge| for (merge.arms) |arm| switch (arm.action) {
                .insert => events.insert = true,
                .delete => events.delete = true,
                .update => |assignments| {
                    events.update = true;
                    for (assignments) |assignment| try appendUpdated(self.a, &updated, assignment.field);
                },
                .nothing => {},
            },
            else => return error.InvalidSqlBackendResponse,
        }
        const record: Table = .{ .table_id = table.id, .name = table.physical_name, .schema_json = table.schema_json };
        if (events.insert and !end) try self.begin(record, .insert, &.{});
        if (events.update) {
            if (end) try self.queueAfter(record, .update, updated.items) else try self.begin(record, .update, updated.items);
        }
        if (events.delete) {
            if (end) try self.queueAfter(record, .delete, &.{}) else try self.begin(record, .delete, &.{});
        }
        if (events.insert and end) try self.queueAfter(record, .insert, &.{});
    }
    fn queueAfter(self: *Broker, record: Table, event: @FieldType(preparation.Input, "event"), updated: []const []const u8) !void {
        const entry = try self.load(record);
        if (!entry.begun.contains(event)) return error.InvalidSqlTriggerManifest;
        for (self.after_phases.items, 0..) |phase, i| if (phase.entry == entry and phase.event == event) {
            _ = self.after_phases.orderedRemove(i);
            break;
        };
        const slot = &entry.after_generations[@backingInt(event)];
        slot.* = std.math.add(u32, slot.*, 1) catch return error.SqlProgramLimitExceeded;
        const phase: AfterPhase = .{ .entry = entry, .event = event, .update_ids = try self.updateIds(entry, updated), .generation = slot.* };
        try self.after_phases.append(self.a, phase);
        try self.appendAfter(.{ .statement = phase });
    }
    fn appendUpdated(a: A, names: *std.ArrayList([]const u8), name: []const u8) !void {
        for (names.items) |previous| if (std.mem.eql(u8, previous, name)) return;
        try names.append(a, name);
    }
    pub fn updateNames(self: *const Broker, table_id: u64) []const []const u8 {
        return if (self.entries.get(table_id)) |entry| entry.update_names else &.{};
    }
    pub fn finish(self: *Broker) !void {
        if (self.finished) return error.InvalidSqlTriggerManifest;
        // Every row is already admitted by its caller's final RLS and integrity
        // checks. AFTER effects remain inside the unpublished transaction.
        for (self.stages.items) |stage| {
            if (!stage.selected or stage.row.skipped) continue;
            if (stage.row.new) |row| if (try self.policyCheck(stage.entry)) |check| {
                if (!try check.permitsTyped(if (stage.row.event == .insert) .insert else .update_new, row.*)) return error.RowPolicyDenied;
            };
        }
        // PostgreSQL queues AFTER ROW work until every candidate passed its
        // statement admission. A later denied row must not release earlier
        // AFTER notices or effects from this same unpublished command.
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            inline for ([_]@FieldType(preparation.Input, "event"){ .insert, .update, .delete, .truncate }) |event| {
                if (entry.begun.contains(event)) {
                    const queued = for (self.after_phases.items) |phase| {
                        if (phase.entry == entry and phase.event == event) break true;
                    } else false;
                    if (!queued) {
                        const slot = &entry.after_generations[@backingInt(event)];
                        slot.* = std.math.add(u32, slot.*, 1) catch return error.SqlProgramLimitExceeded;
                        const phase: AfterPhase = .{ .entry = entry, .event = event, .update_ids = if (event == .update) entry.update_ids else &.{}, .generation = slot.* };
                        try self.after_phases.append(self.a, phase);
                        try self.appendAfter(.{ .statement = phase });
                    }
                }
            }
        }
        try self.drainAfter();
        self.finished = true;
    }
    fn appendAfter(self: *Broker, event: AfterEvent) !void {
        if (self.after_events.items.len >= catalog.trigger_handoff.max_rows * 4 + catalog.trigger_handoff.max_receipts * 4) return error.SqlProgramLimitExceeded;
        try self.after_events.append(self.a, event);
    }
    fn selectStage(self: *Broker, stage: *Stage) !void {
        stage.selected = true;
        if (stage.row.skipped or stage.queued) return;
        try self.appendAfter(.{ .row = stage });
        stage.queued = true;
    }
    /// Add the existing native FK planner continuation at its parent's logical
    /// row position. A triggerless parent still occupies its ordinary queue
    /// position. Callers retain the continuation owner until drain completes.
    pub fn queueReferentialAction(self: *Broker, token: ?*anyopaque, action: ReferentialAction) !void {
        if (self.finished or action.allocation == 0 or action.child_table_id == 0) return error.InvalidSqlTriggerManifest;
        // Allocations are unique within a child table, not within a database.
        // Both catalog identities participate; names and hashes grant nothing.
        const owned: NamedReferentialAction = .{ .name = try std.fmt.allocPrint(self.a, "RI_ConstraintTrigger_a_{x:0>16}_{x:0>16}", .{ action.child_table_id, action.allocation }), .ptr = action.ptr, .run = action.run };
        const value = token orelse return error.InvalidSqlTriggerManifest;
        {
            const stage = self.stage_tokens.get(value) orelse return error.InvalidSqlTriggerManifest;
            if (!stage.selected or stage.row.skipped or stage.after_started) return error.InvalidSqlTriggerManifest;
            const captured = for (stage.entry.incoming_tables) |id| {
                if (id == action.child_table_id) break true;
            } else false;
            if (!captured) return error.CatalogGenerationChanged;
            if (stage.actions.items.len >= 256) return error.SqlProgramLimitExceeded;
            if (stage.entry.owner.execution) |execution_owner| for (execution_owner.captured.attachmentEntries()) |attachment| {
                if (std.mem.eql(u8, attachment.name, owned.name)) return error.InvalidSqlTriggerManifest;
            };
            for (stage.actions.items) |previous| if (std.mem.eql(u8, previous.name, owned.name)) return error.InvalidSqlTriggerManifest;
            try stage.actions.append(self.a, owned);
        }
    }
    fn validateAfterRow(self: *Broker, stage: *Stage) !void {
        if (stage.row.new) |row| if (try self.policyCheck(stage.entry)) |check| {
            if (!try check.permitsTyped(if (stage.row.event == .insert) .insert else .update_new, row.*)) return error.RowPolicyDenied;
        };
    }
    /// The bounded queue may grow when an FK continuation prepares children.
    /// Their BEFORE phases execute immediately; their AFTER work stays at the
    /// tail of the same statement queue, including empty-child statement work.
    pub fn drainAfter(self: *Broker) !void {
        if (self.finished or self.draining) return error.InvalidSqlTriggerManifest;
        self.draining = true;
        defer self.draining = false;
        while (self.after_cursor < self.after_events.items.len) {
            try self.root.context.ensureActive();
            const event = self.after_events.items[self.after_cursor];
            self.after_cursor += 1;
            switch (event) {
                .statement => |phase| {
                    if (phase.generation != phase.entry.after_generations[@backingInt(phase.event)]) continue;
                    _ = phase.entry.owner.statement(.after, phase.event, phase.update_ids) catch |err| return normalizeNativeError(err);
                },
                .action => |action| try action.run(action.ptr),
                .row => |stage| {
                    if (!stage.selected or stage.row.skipped or stage.row.after_executed) continue;
                    stage.after_started = true;
                    try self.validateAfterRow(stage);
                    std.mem.sort(NamedReferentialAction, stage.actions.items, {}, struct {
                        fn less(_: void, left: NamedReferentialAction, right: NamedReferentialAction) bool {
                            return std.mem.order(u8, left.name, right.name) == .lt;
                        }
                    }.less);
                    var action_index: usize = 0;
                    const attachments: []const catalog.triggers.Entry = if (stage.entry.owner.execution) |execution_owner| execution_owner.captured.attachmentEntries() else &.{};
                    for (attachments) |attachment| {
                        if (!attachment.matches(.after, .row, stage.row.event, stage.row.update_columns)) continue;
                        while (action_index < stage.actions.items.len and std.mem.order(u8, stage.actions.items[action_index].name, attachment.name) == .lt) : (action_index += 1) {
                            const action = stage.actions.items[action_index];
                            try action.run(action.ptr);
                        }
                        _ = stage.entry.owner.afterAttachment(&stage.row, attachment.id) catch |err| return normalizeNativeError(err);
                    }
                    while (action_index < stage.actions.items.len) : (action_index += 1) {
                        const action = stage.actions.items[action_index];
                        try action.run(action.ptr);
                    }
                    stage.row.after_executed = true;
                },
            }
        }
    }
    /// Logical OLD may be a staged row. Bind every selected effect to the
    /// coordinator retained physical predicate before producing any receipt.
    pub fn bindPhysicalPredicates(self: *Broker, tables: []const local.api_distributed_txn_contract.TableCommitRequest) !void {
        for (self.stages.items) |stage| {
            if (!stage.selected or stage.row.skipped) continue;
            const table = for (tables) |candidate| {
                if (std.mem.eql(u8, candidate.table_name, stage.entry.binding.physical_name)) break candidate;
            } else return error.SqlTriggerCoordinationRequired;
            const guard = for (table.predicates) |candidate| {
                if (std.mem.eql(u8, candidate.key, stage.row.key)) break candidate;
            } else return error.SqlTriggerCoordinationRequired;
            if (guard.expected_version != 0 and guard.expected_content_digest == null) return error.MissingPrimaryObservation;
            if (stage.physical_bound and (stage.preimage_version != guard.expected_version or !std.meta.eql(stage.preimage_digest, guard.expected_content_digest))) return error.VersionConflict;
            stage.preimage_version = guard.expected_version;
            stage.preimage_digest = guard.expected_content_digest;
            stage.physical_bound = true;
        }
    }
    pub fn receipts(self: *Broker, a: A) ![]const catalog.trigger_handoff.Receipt {
        if (!self.finished) return error.InvalidSqlTriggerManifest;
        const handoff = catalog.trigger_handoff;
        var result: std.ArrayList(handoff.Receipt) = .empty;
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            const execution_owner = entry.owner.execution orelse continue;
            var occurrences: std.StringHashMapUnmanaged(usize) = .empty;
            defer occurrences.deinit(a);
            var waves: std.ArrayList(std.ArrayList(handoff.Row)) = .empty;
            defer waves.deinit(a);
            for (self.stages.items) |stage| {
                if (stage.entry != entry or !stage.selected or stage.row.skipped) continue;
                if (!stage.row.after_executed or !stage.physical_bound) return error.InvalidSqlTriggerManifest;
                const position = try occurrences.getOrPut(a, stage.row.key);
                if (!position.found_existing) position.value_ptr.* = 0;
                const wave = position.value_ptr.*;
                position.value_ptr.* += 1;
                if (wave >= handoff.max_receipts) return error.SqlProgramLimitExceeded;
                while (waves.items.len <= wave) try waves.append(a, .empty);
                const old: handoff.Preimage = if (stage.preimage_version != 0) .{ .present = .{ .version = stage.preimage_version, .digest = stage.preimage_digest orelse return error.InvalidSqlBackendResponse } } else .absent;
                try waves.items[wave].append(a, .{ .key = stage.row.key, .event = stage.row.event, .preimage = old, .after = stage.output_digest, .update_columns = if (stage.row.event == .update) stage.row.update_columns else &.{} });
            }
            if (waves.items.len == 0) try waves.append(a, .empty);
            const program = execution_owner.captured;
            for (waves.items, 0..) |*rows, wave| {
                std.mem.sort(handoff.Row, rows.items, {}, struct {
                    fn less(_: void, x: handoff.Row, y: handoff.Row) bool {
                        return std.mem.order(u8, x.key, y.key) == .lt;
                    }
                }.less);
                const receipt: handoff.Receipt = .{ .program = program, .processed = .{ .header = .{ .statement_id = self.statement_id, .sequence = @intCast(wave), .metadata_incarnation = program.metadata_incarnation, .table = program.table, .program_digest = try program.digest(), .events = entry.begun }, .rows = try rows.toOwnedSlice(a) } };
                try receipt.validate(a);
                try result.append(a, receipt);
            }
        }
        std.mem.sort(handoff.Receipt, result.items, {}, struct {
            fn less(_: void, x: handoff.Receipt, y: handoff.Receipt) bool {
                return x.program.table.id < y.program.table.id or (x.program.table.id == y.program.table.id and x.processed.header.sequence < y.processed.header.sequence);
            }
        }.less);
        return result.toOwnedSlice(a);
    }
    fn policyCheck(self: *Broker, entry: *Entry) !?*@import("sql_policy_evaluation.zig").Evaluation {
        if (entry.no_policy) return null;
        if (entry.check) |check| return check;
        const scope = entry.binding.scope orelse return invalidSnapshot("policy_scope");
        const token = try self.root.server.rowPolicyWriteProofForPurpose(self.a, self.root.context, entry.binding.id, entry.binding.physical_name, scope.database, entry.binding.schema_version, .sql_write);
        if (token == null) {
            entry.no_policy = true;
            return null;
        }
        const identity = self.root.identity.* orelse return error.RowPolicyAuthenticationRequired;
        var binding = entry.binding;
        binding.row_policy_purpose = .sql_write;
        entry.check = try @import("sql_policy_evaluation.zig").Evaluation.initForWrite(self.a, self.root.server, self.root.context, binding, identity.username, token.?, self.root.setting_overlay);
        return entry.check;
    }
    fn updateIds(self: *Broker, entry: *const Entry, names: []const []const u8) ![]const u64 {
        const ids = try self.a.alloc(u64, names.len);
        for (names, ids) |name, *id| id.* = for (entry.columns.columns) |column| {
            if (std.mem.eql(u8, name, column.name)) break column.id;
        } else return error.UndefinedColumn;
        std.mem.sort(u64, ids, {}, std.sort.asc(u64));
        for (ids, 0..) |id, i| if (i != 0 and ids[i - 1] == id) return error.DuplicateSqlColumn;
        return ids;
    }
    fn load(self: *Broker, record: Table) !*Entry {
        if (self.entries.get(record.table_id)) |entry| {
            if (!std.mem.eql(u8, entry.binding.physical_name, record.name) or !std.mem.eql(u8, entry.binding.schema_json, record.schema_json)) return error.CatalogGenerationChanged;
            return entry;
        }
        var view = try preparation.captureView(self.a, record.schema_json);
        defer view.release();
        const state = view.triggerState();
        if (state) |value| if (value.entries.len != 0) try @import("sql_trigger_authority.zig").requireConfiguration(self.root.server.cfg);
        if (self.entries.count() >= 128) return error.SqlProgramLimitExceeded;
        const contract = @import("../system_catalog/trigger_snapshot.zig");
        if (self.root.server.cfg.auth_enabled and self.root.identity.* == null) return error.Forbidden;
        var context = self.root.context;
        context.setting_admin = true;
        const bytes = try self.root.server.source.systemCatalog(self.root.server.alloc, context, .{ .sql_trigger_snapshot = .{ .table_id = record.table_id, .expected_revision = self.root.revision } });
        defer self.root.server.alloc.free(bytes);
        if (bytes.len > 16 * 1024 * 1024 -| self.snapshot_bytes) return error.SqlWorkingMemoryLimitExceeded;
        self.snapshot_bytes += bytes.len;
        const snapshot = try std.json.parseFromSliceLeaky(contract.Snapshot, self.a, bytes, .{ .allocate = .alloc_always });
        const source = snapshot.source;
        if (source.value != .table) return error.CatalogGenerationChanged;
        const resolved = source.value.table;
        try resolved.binding.requireRequestAccess(self.root.context.native_service, self.root.context.sql_relation_owner);
        if (snapshot.record.table_id != record.table_id or !std.mem.eql(u8, snapshot.record.name, record.name)) return error.CatalogGenerationChanged;
        try self.rememberRecord(snapshot.record);
        for (snapshot.incoming_records) |incoming_record| try self.rememberRecord(incoming_record);
        const epoch = snapshot.epoch;
        if (state) |value| if (value.table_id != record.table_id or !std.mem.eql(u8, &epoch.incarnation, &value.metadata_incarnation)) return error.CatalogGenerationChanged;
        const definition = resolved.binding.query_definition orelse return invalidSnapshot("snapshot_definition");
        if (resolved.binding.table_id != record.table_id or !std.mem.eql(u8, resolved.binding.name, record.name) or !std.mem.eql(u8, definition.schema_json, record.schema_json)) return error.CatalogGenerationChanged;
        self.root.revision = snapshot.revision;
        const raw_columns = resolved.column_identities_json orelse return invalidSnapshot("snapshot_column_identities");
        const columns = (try local.system_catalog_column_identities.State.decode(self.a, raw_columns)).value;
        if (columns.table_id != record.table_id or columns.epoch.version != view.version() or !std.mem.eql(u8, &columns.epoch.digest, &(view.validator().?.source_digest orelse return error.CatalogGenerationChanged))) return error.CatalogGenerationChanged;
        const binding: catalog.Table = .{ .logical_relation_id = resolved.binding.logical_relation_id, .id = record.table_id, .physical_name = record.name, .schema_json = record.schema_json, .schema_version = view.version(), .sql_persistence = resolved.binding.sql_persistence, .has_triggers = if (state) |value| value.entries.len != 0 else false, .trigger_functions_active = resolved.binding.sql_trigger_functions_active, .has_incoming_foreign_keys = resolved.binding.sql_trigger_functions_active and snapshot.incoming_stamp.active_count != 0, .incoming_foreign_keys = if (resolved.binding.sql_trigger_functions_active) .{ .coverage_revision = snapshot.coverage_revision, .stamp = snapshot.incoming_stamp } else null, .storage_mode = if (view.storageMode() == .relational) .relational else .document, .columns = try preparation.deriveColumns(self.a, view), .scope = .{ .database = source.target.database, .namespace = source.target.namespace, .name = source.target.table, .revision = snapshot.revision } };
        for (binding.columns) |column| {
            const stored = for (columns.columns) |candidate| {
                if (std.mem.eql(u8, candidate.name, column.name)) break candidate;
            } else return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &stored.signature, &try column.identitySignature(self.a))) return error.CatalogGenerationChanged;
        }
        var capture_root = self.root.*;
        capture_root.database = source.target.database;
        const captured: ?catalog.trigger_manifest.Manifest = if (binding.has_triggers) try Capture.capture(&capture_root, self.a, binding, view.validator().?, self.clock) else null;
        self.root.revision = capture_root.revision;
        const entry = try self.a.create(Entry);
        const setting_capture = capture_root.settingCapture() orelse return error.SqlTriggerContextRequired;
        const settings = try self.a.create(local.sql_setting_catalog.View);
        settings.* = try local.sql_setting_catalog.View.capture(self.a, setting_capture.owner, setting_capture.scope, setting_capture.overlay);
        errdefer settings.deinit();
        try settings.pinSqlUser(setting_capture.sql_user);
        const owner = try preparation.Owner.create(self.a, view, binding, captured, .{ .settings = settings, .execution = .{ .io = self.root.server.sharedApiIo(), .notice_sink = self.root.notice_sink }, .expressions = .{ .io = self.root.server.sharedApiIo() orelse return error.SqlClockUnavailable, .transaction_start = try catalog.Clock.Instant.init(self.clock.transaction_micros), .sequences = self.root.context.column_sequences, .sequence_scope = .{ .table_id = record.table_id, .schema_version = view.version(), .storage_name = record.name } } });
        errdefer owner.deinit();
        entry.* = .{ .binding = binding, .record = snapshot.record, .snapshot = snapshot, .incoming_records = snapshot.incoming_records, .owner = owner, .settings = settings, .columns = columns, .incoming_tables = snapshot.incoming_tables, .incoming_stamp = snapshot.incoming_stamp, .coverage_revision = snapshot.coverage_revision };
        try self.entries.put(self.a, record.table_id, entry);
        return entry;
    }
    /// Establish the format floor before any demanded VM execution. The
    /// captured table/source/index identities remain the native vote witness.
    pub fn ensureGraphPreparation(self: *Broker, record: Table, names: []const []const u8) !local.storage_db_prepared_graph_effects.Admission {
        const graph = @import("../system_catalog/graph_preparation.zig");
        const entry = try self.load(record);
        const source = try graph.capture(self.a, entry.snapshot, names);
        if (entry.snapshot.metadata_group_id == 0) return invalidSnapshot("snapshot_metadata_group");
        if (entry.snapshot.graph_intent_protocol >= 7) return self.enableGraphProof(entry, try source.admitted(entry.snapshot.metadata_group_id));
        const io = self.root.server.sharedApiIo() orelse return error.SqlClockUnavailable;
        var nonce: [16]u8 = undefined;
        io.random(&nonce);
        const request: graph.Mutation = .{ .source = source, .admission = .{ .nonce = nonce, .head = entry.snapshot.graph_outcome_head } };
        const observation = try graph.observation(self.a, request);
        var context = self.root.context;
        context.setting_admin = true;
        // A capability-only operation must never complete the surrounding
        // SQL command or borrow its effect admission/submission journal.
        context.sql_command_fence = null;
        context.sql_completion = null;
        context.sql_completion_submission = null;
        const raw = self.root.server.source.systemCatalog(self.a, context, .{ .sql_graph_preparation_admit = request }) catch |err| {
            if (err != error.MetadataMutationOutcomeUnknown and !std.mem.eql(u8, execution.diagnostic(err).code, "40003")) return err;
            const known = try self.root.server.source.systemCatalog(self.a, context, .{ .sql_graph_preparation_outcome = observation });
            if (known.len > graph.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
            const result = try std.json.parseFromSliceLeaky(?graph.outcomes.Outcome, self.a, known, .{ .allocate = .alloc_always });
            const outcome = result orelse return error.SqlReadRetryRequired;
            _ = try outcome.result();
            entry.snapshot.graph_intent_protocol = 7;
            return self.enableGraphProof(entry, try source.admitted(entry.snapshot.metadata_group_id));
        };
        if (raw.len > graph.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
        const outcome = try std.json.parseFromSliceLeaky(graph.outcomes.Outcome, self.a, raw, .{ .allocate = .alloc_always });
        _ = try outcome.result();
        entry.snapshot.graph_intent_protocol = 7;
        return self.enableGraphProof(entry, try source.admitted(entry.snapshot.metadata_group_id));
    }

    fn enableGraphProof(self: *Broker, entry: *Entry, admission: local.storage_db_prepared_graph_effects.Admission) !local.storage_db_prepared_graph_effects.Admission {
        try @import("sql_trigger_authority.zig").requireConfiguration(self.root.server.cfg);
        try entry.owner.enableGraphProof(admission, entry.snapshot.revision, self.clock, .{ .settings = entry.settings, .execution = .{ .io = self.root.server.sharedApiIo(), .notice_sink = self.root.notice_sink } });
        return admission;
    }

    pub fn captureTable(self: *Broker, record: Table) !catalog.Table {
        const entry = try self.load(record);
        try self.captureClosure(entry);
        return entry.binding;
    }
    /// Include real statement owners even when scanning selected no rows.
    /// Their rowless receipts retain a native schema fence through commit.
    pub fn statementTables(self: *Broker, a: A) ![]const local.api_distributed_txn_contract.TableCommitRequest {
        var tables: std.ArrayList(local.api_distributed_txn_contract.TableCommitRequest) = .empty;
        var entries = self.entries.valueIterator();
        while (entries.next()) |pointer| {
            const entry = pointer.*;
            if (entry.owner.execution == null) continue;
            const begun = entry.begun;
            if (!begun.insert and !begun.update and !begun.delete and !begun.truncate) continue;
            try tables.append(a, .{ .table_name = entry.binding.physical_name, .schema_version = if (entry.binding.storage_mode == .document) entry.binding.schema_version else null, .relational_schema_version = if (entry.binding.storage_mode == .relational) entry.binding.schema_version else null, .sql_persistence = entry.binding.sql_persistence });
        }
        return tables.toOwnedSlice(a);
    }
    /// The indexed incoming graph bounds which tables an FK action can mutate.
    /// Pin their executable closures before releasing the first BEFORE phase;
    /// cyclic graphs reuse entries instead of recursively reopening snapshots.
    fn captureClosure(self: *Broker, first: *Entry) !void {
        if (first.closure_captured) return;
        var queue: std.ArrayList(*Entry) = .empty;
        defer queue.deinit(self.a);
        try queue.append(self.a, first);
        var position: usize = 0;
        while (position < queue.items.len) : (position += 1) {
            const entry = queue.items[position];
            if (entry.closure_captured) continue;
            entry.closure_captured = true;
            for (entry.incoming_records) |record| {
                const child = try self.load(record);
                if (!child.closure_captured) {
                    if (queue.items.len >= 4096) return error.SqlProgramLimitExceeded;
                    try queue.append(self.a, child);
                }
            }
        }
    }
    pub fn capturedTableId(self: *const Broker, physical: []const u8) ?u64 {
        return if (self.records_by_name.get(physical)) |record| record.table_id else null;
    }
    pub fn preparedDomain(self: *Broker, table_id: u64) !@FieldType(local.storage_db_types.TransactionWrite, "prepared_domain") {
        const entry = self.entries.get(table_id) orelse return error.InvalidSqlTriggerManifest;
        const contract = entry.owner.view.validator().?.preparedDomainContract() orelse return null;
        const owned = try self.a.create(@TypeOf(contract));
        owned.* = contract;
        return owned;
    }
    pub fn prepareSql(self: *Broker, table: catalog.Table, mutation: *catalog.Mutation, updated: []const []const u8) !bool {
        if (mutation.predicate_only) return mutation.trigger_suppressed;
        const event: @FieldType(preparation.Input, "event") = if (mutation.row == null) .delete else if (mutation.previous != null) .update else .insert;
        if (mutation.prepared_trigger) |token| {
            const existing = self.stage_tokens.get(token) orelse return error.InvalidSqlTriggerManifest;
            if (existing.entry.binding.id != table.id or !std.mem.eql(u8, existing.row.key, mutation.key)) {
                // ON CONFLICT may select a different physical owner and changes
                // INSERT into UPDATE; its BEFORE INSERT has already happened.
                if (existing.row.event != .insert or event != .update) return error.InvalidSqlTriggerManifest;
            } else if (existing.row.event == event) {
                const digest = try mutationDigest(self.a, mutation.*);
                if (!std.meta.eql(digest, existing.output_digest)) return error.InvalidSqlTriggerManifest;
                return existing.row.skipped;
            } else if (existing.row.event != .insert or event != .update) return error.InvalidSqlTriggerManifest;
        }
        const prepared = try before(self, self.a, .{ .table = .{ .table_id = table.id, .name = table.physical_name, .schema_json = table.schema_json }, .key = mutation.key, .event = event, .before_typed = mutation.previous, .after = if (mutation.row) |*row| row else null, .json_null_fields = mutation.json_null_fields, .update_columns = updated, .candidate_prepared = true, .domain_admission = mutation.domain_admission });
        mutation.json_null_fields = prepared.json_null_fields;
        mutation.prepared_domain = null;
        mutation.prepared_trigger = prepared.stage;
        mutation.trigger_suppressed = prepared.skipped;
        return prepared.skipped;
    }
    pub fn observeSql(self: *Broker, table: catalog.Table, mutation: catalog.Mutation, updated: []const []const u8) !void {
        if (mutation.predicate_only or mutation.trigger_suppressed) return;
        if (table.has_triggers) return self.selectSql(table, mutation);
        const record: Table = .{ .table_id = table.id, .name = table.physical_name, .schema_json = table.schema_json };
        const entry = try self.load(record);
        if (entry.binding.has_triggers) return error.CatalogGenerationChanged;
        const event: @FieldType(preparation.Input, "event") = if (mutation.row == null) .delete else if (mutation.unique_absence) .insert else .update;
        try self.begin(record, event, updated);
        const stage = try self.a.create(Stage);
        stage.* = .{ .entry = entry, .row = .{ .key = try self.a.dupe(u8, mutation.key), .event = event, .old = if (mutation.previous) |row| try entry.owner.sourceTypedRow(row.*) else null, .new = if (mutation.row) |row| try entry.owner.sourceRow(mutation.key, row, mutation.json_null_fields, 0, null) else null, .value = null, .json_null_fields = mutation.json_null_fields, .update_columns = try self.updateIds(entry, updated) }, .output_digest = try mutationDigest(self.a, mutation), .preimage_version = mutation.expected_version, .preimage_digest = mutation.expected_content_digest };
        try self.stage_tokens.put(self.a, stage, stage);
        try self.stages.append(self.a, stage);
        try self.selectStage(stage);
    }
    pub fn selectSql(self: *Broker, table: catalog.Table, mutation: catalog.Mutation) !void {
        if (mutation.predicate_only or mutation.trigger_suppressed) return;
        const token = mutation.prepared_trigger orelse return error.SqlTriggerCoordinationRequired;
        const stage = self.stage_tokens.get(token) orelse return error.InvalidSqlTriggerManifest;
        if (stage.entry.binding.id != table.id or !std.mem.eql(u8, stage.row.key, mutation.key) or !std.meta.eql(stage.output_digest, try mutationDigest(self.a, mutation))) return error.InvalidSqlTriggerManifest;
        try self.selectStage(stage);
    }
    fn mutationDigest(a: A, mutation: catalog.Mutation) !?catalog.trigger_handoff.Digest {
        const row = mutation.row orelse return null;
        var scratch: std.heap.ArenaAllocator = .init(a);
        defer scratch.deinit();
        const canonical = try std.json.Stringify.valueAlloc(scratch.allocator(), row, .{});
        return try catalog.trigger_handoff.rowDigest(mutation.key, canonical, mutation.json_null_fields);
    }
    fn before(raw: *anyopaque, a: A, input: catalog.trigger_provider.Input) !catalog.trigger_provider.Prepared {
        return beforeImpl(raw, a, input) catch |err| return normalizeNativeError(err);
    }
    fn normalizeNativeError(err: anyerror) anyerror {
        return switch (err) {
            error.UnknownColumn => error.UndefinedColumn,
            error.SqlDivisionByZero => error.RelationalExpressionDivisionByZero,
            else => err,
        };
    }
    fn beforeImpl(raw: *anyopaque, _: A, input: catalog.trigger_provider.Input) !catalog.trigger_provider.Prepared {
        const self: *Broker = @ptrCast(@alignCast(raw));
        try self.root.context.ensureActive();
        if (input.adopt_existing) {
            const entry = try self.load(input.table);
            var digest: ?catalog.trigger_handoff.Digest = null;
            if (input.after) |value| {
                var scratch: std.heap.ArenaAllocator = .init(self.a);
                defer scratch.deinit();
                const canonical = try std.json.Stringify.valueAlloc(scratch.allocator(), value.*, .{});
                digest = try catalog.trigger_handoff.rowDigest(input.key, canonical, input.json_null_fields);
            }
            var index = self.stages.items.len;
            while (index != 0) {
                index -= 1;
                const stage = self.stages.items[index];
                if (stage.entry != entry or !stage.selected or stage.row.skipped or stage.row.event != input.event or !std.mem.eql(u8, stage.row.key, input.key)) continue;
                if (!std.meta.eql(stage.output_digest, digest)) continue;
                if (input.before) |old| {
                    if (stage.physical_bound and (stage.preimage_version != old.version or !std.meta.eql(stage.preimage_digest, old.expected_content_digest))) continue;
                } else if (input.before_parsed) |old| {
                    if (stage.physical_bound and (stage.preimage_version != old.version or !std.meta.eql(stage.preimage_digest, old.digest))) continue;
                } else if (stage.physical_bound and stage.preimage_version != 0) continue;
                return .{ .skipped = false, .json_null_fields = stage.row.json_null_fields, .stage = stage };
            }
            // Re-normalization may adopt only a selected, exact current-command
            // image. An absent match is never permission to fire its VM again.
            return error.InvalidSqlTriggerManifest;
        }
        try self.begin(input.table, input.event, input.update_columns);
        if (self.stages.items.len >= catalog.trigger_handoff.max_rows) return error.SqlProgramLimitExceeded;
        const entry = try self.load(input.table);
        const ids = try self.updateIds(entry, input.update_columns);
        if ((input.before != null and (input.before_typed != null or input.before_parsed != null)) or (input.before_typed != null and input.before_parsed != null)) return error.InvalidSqlTriggerManifest;
        const old = if (input.before_parsed) |source| try entry.owner.sourceRow(input.key, source.value, source.json_null_fields, source.version, source.digest) else if (input.before_typed) |source| try entry.owner.sourceTypedRow(source.*) else if (input.before) |source| old: {
            const value = try std.json.parseFromSliceLeaky(std.json.Value, self.a, source.json, .{ .parse_numbers = false });
            break :old try entry.owner.sourceRow(input.key, value, source.json_null_fields, source.version, source.expected_content_digest);
        } else null;
        if (old) |row| if (try self.policyCheck(entry)) |check| {
            if (!try check.permitsTyped(if (input.event == .delete) .delete else .update_old, row.*)) return error.RowPolicyDenied;
        };
        const stage = try self.a.create(Stage);
        stage.* = .{ .entry = entry, .row = try entry.owner.before(.{ .key = input.key, .event = input.event, .old = old, .after = input.after, .json_null_fields = input.json_null_fields, .update_columns = ids, .candidate_prepared = input.candidate_prepared, .domain_admission = input.domain_admission }) };
        stage.row.key = try self.a.dupe(u8, input.key);
        stage.preimage_version = if (old) |value| value.version else 0;
        stage.preimage_digest = if (old) |value| value.expected_content_digest else null;
        stage.physical_bound = input.before_typed == null;
        if (input.after) |value| {
            var scratch: std.heap.ArenaAllocator = .init(self.a);
            defer scratch.deinit();
            const canonical = try std.json.Stringify.valueAlloc(scratch.allocator(), value.*, .{});
            stage.output_digest = try catalog.trigger_handoff.rowDigest(input.key, canonical, stage.row.json_null_fields);
        }
        // The candidate DOM can belong to a resettable SQL producer page.
        // Typed OLD/NEW and the exact output digest are already owned above.
        stage.row.value = null;
        try self.stage_tokens.put(self.a, stage, stage);
        try self.stages.append(self.a, stage);
        return .{ .skipped = stage.row.skipped, .json_null_fields = stage.row.json_null_fields, .stage = stage };
    }
    fn after(raw: *anyopaque, token: *anyopaque) !void {
        const self: *Broker = @ptrCast(@alignCast(raw));
        const stage: *Stage = @ptrCast(@alignCast(token));
        // The token is a live, same-owner capability, never a wire pointer.
        for (self.stages.items) |owned| if (owned == stage) {
            // Completion is selected here; final WITH CHECK for the whole
            // unpublished row set precedes every AFTER invocation in finish.
            try self.selectStage(stage);
            return;
        };
        return error.InvalidSqlTriggerManifest;
    }
};

test "SQL trigger demanded division retains its native client error and PostgreSQL diagnostic" {
    const native = Broker.normalizeNativeError(error.SqlDivisionByZero);
    try std.testing.expectEqual(@as(anyerror, error.RelationalExpressionDivisionByZero), native);
    try std.testing.expect(local.schema_relational_expression_errors.isInvalidInput(native));
    try std.testing.expectEqualStrings("22012", local.sql_errors.describe(native).code);
    try std.testing.expectEqualStrings(local.sql_errors.describe(error.SqlDivisionByZero).code, local.sql_errors.describe(native).code);
}
