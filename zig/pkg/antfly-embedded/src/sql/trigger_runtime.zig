// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Statement-owned trigger dispatch over captured executable data. This owner
//! cannot read/write tables, allocate sequences or resolve ambient catalogs.
//! Assignment changes only the mutation owner's unpublished candidate frame.
const std = @import("std");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const procedure = @import("procedure.zig");
const native = @import("routine_definition.zig");
const records = native.records;
const bindings = native.bindings;
const manifest = @import("trigger_manifest.zig");
const definition = @import("trigger_definition.zig");
const A = std.mem.Allocator;
const frames = @import("trigger_frame.zig");
pub const FrameLayout = frames.Layout;
pub const OwnedFrame = frames.Owned;
pub const referencesImage = definition.referencesImage;
test {
    _ = frames;
}
pub const Limits = struct { notice_sink: ?catalog.notices.Sink = null, work: usize = 1_000_000, max_depth: usize = 32, io: ?std.Io = null, control: @FieldType(catalog.Backend, "scalar_control") = null, regex_execution: ?*@import("regex_execution.zig") = null };
pub const Dispatch = struct {
    /// The coordinator interleaves one AFTER ROW attachment with internal FK
    /// actions. This selector never comes from the executable manifest/wire.
    attachment_id: ?u64 = null,
    timing: definition.Timing,
    level: definition.Level,
    event: definition.Event,
    /// Explicit UPDATE targets, not modified cells, as immutable attribute IDs.
    updated_columns: []const u64 = &.{},
    frame: *procedure.TriggerFrame,
    owned: ?*frames.Owned = null,
};
pub const Output = struct { row: ?*const catalog.Row, notices: []const procedure.Notice, fired: usize };
const Prepared = struct { body: *procedure.Resolved, predicate: ?scalar.Program = null, variables: ?[]const scalar.Datum = null };

/// Allocate once at its stable statement-owned address. Authentication and
/// fencing against the receiver's real immutable table precede construction.
pub const Statement = struct {
    a: A,
    captured: manifest.Manifest,
    settings: ?*const @import("setting_catalog.zig").View,
    remaining: usize,
    max_depth: usize,
    depth: usize = 0,
    helpers: std.AutoHashMapUnmanaged(u64, *const bindings.Resolved) = .empty,
    bodies: std.AutoHashMapUnmanaged(u64, *procedure.Resolved) = .empty,
    prepared: []?Prepared,
    columns: ?[]const scalar.Column = null,
    io: ?std.Io = null,
    notice_sink: ?catalog.notices.Sink = null,
    control: @FieldType(catalog.Backend, "scalar_control") = null,
    regex_execution: ?*@import("regex_execution.zig") = null,
    clocks: ?catalog.Clock.Frame = null,
    statement_phases: [8]bool = @splat(false),

    pub fn init(a: A, captured: manifest.Manifest, settings: ?*const @import("setting_catalog.zig").View, limits: Limits) !*Statement {
        try captured.validate(a);
        if (limits.work == 0 or limits.max_depth == 0 or limits.max_depth > 32) return error.SqlProgramLimitExceeded;
        const self = try a.create(Statement);
        const prepared = try a.alloc(?Prepared, captured.attachmentEntries().len);
        @memset(prepared, null);
        self.* = .{ .a = a, .captured = captured, .settings = settings, .remaining = limits.work, .max_depth = limits.max_depth, .io = limits.io, .notice_sink = limits.notice_sink, .control = limits.control, .regex_execution = limits.regex_execution, .prepared = prepared };
        if (captured.clock) |clock| self.clocks = try catalog.Clock.Frame.init(.{
            .source = .{ .callback = .{ .ptr = self, .now = now } },
            .transaction = try catalog.Clock.Instant.init(clock.transaction_micros),
            .statement = try catalog.Clock.Instant.init(clock.statement_micros),
        });
        return self;
    }
    fn now(raw: ?*anyopaque) !i128 {
        const self: *Statement = @ptrCast(@alignCast(raw.?));
        return std.Io.Clock.now(.real, self.io orelse return error.SqlClockUnavailable).nanoseconds;
    }
    fn charge(raw: *anyopaque) !void {
        const self: *Statement = @ptrCast(@alignCast(raw));
        if (self.control) |control| try control.checkpoint(control.ptr);
        if (self.remaining == 0) return error.SqlProgramLimitExceeded;
        self.remaining -= 1;
    }
    fn scalarCharge(raw: ?*anyopaque) !void {
        return charge(raw orelse return error.InvalidSqlBackendResponse);
    }
    fn rejectResolve(_: *anyopaque, _: A, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return error.UnsupportedSqlShape;
    }
    fn rejectScan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnsupportedSqlShape;
    }
    fn rejectMutation(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnsupportedSqlShape;
    }
    fn backend(self: *Statement) catalog.Backend {
        return .{
            .ptr = self,
            .vtable = &.{ .resolve = rejectResolve, .scan = rejectScan, .mutate = rejectMutation, .checkpoint = charge },
            .scalar_control = .{ .ptr = self, .checkpoint = scalarCharge },
            .settings_view = self.settings,
            .notice_sink = self.notice_sink,
            .routine_resolver = self.resolver(),
            .regex_execution = self.regex_execution,
            .statement_clock = if (self.clocks) |*clock| clock.view else null,
            .clock_values = if (self.clocks) |*clock| clock else null,
        };
    }
    fn resolver(self: *Statement) bindings.Resolver {
        return .{ .ptr = self, .resolve = resolve, .resolve_reference = resolveReference, .resolve_operator = resolveOperator };
    }
    fn resolve(raw: *anyopaque, _: A, name: []const u8, arguments: []const bindings.Argument) anyerror!*const bindings.Resolved {
        return lookup(@ptrCast(@alignCast(raw)), name, arguments, .function);
    }
    fn resolveOperator(raw: *anyopaque, _: A, name: []const u8, arguments: []const bindings.Argument) anyerror!*const bindings.Resolved {
        return lookup(@ptrCast(@alignCast(raw)), name, arguments, .operator);
    }
    fn resolveReference(raw: *anyopaque, _: A, reference: records.FunctionReference, arguments: []const bindings.Argument) anyerror!*const bindings.Resolved {
        const self: *Statement = @ptrCast(@alignCast(raw));
        const captured = self.captured.findRoutine(reference.id) orelse return error.CatalogGenerationChanged;
        if (captured.record.definition.arguments.len != arguments.len) return error.SqlUndefinedFunction;
        return self.prepareHelper(captured);
    }
    fn lookup(self: *Statement, name: []const u8, arguments: []const bindings.Argument, kind: records.Kind) !*const bindings.Resolved {
        try charge(self);
        var candidates: std.ArrayList(records.Header) = .empty;
        for (self.captured.lookups) |entry| {
            if (!std.mem.eql(u8, name, entry.name)) continue;
            const record = (self.captured.findRoutine(entry.routine_id) orelse return error.InvalidSqlTriggerManifest).record;
            if (record.kind == kind and !record.definition.returns_trigger) try candidates.append(self.a, .{ .id = record.id, .signature = try record.definition.signatureAlloc(self.a) });
        }
        const inputs = try self.a.alloc(records.Input, arguments.len);
        for (arguments, inputs) |argument, *input| input.* = .{
            .unknown_literal = argument.unknown_literal,
            .type = if (argument.type.kind) |type_kind| try native.storageType(.{ .kind = type_kind, .element_type = argument.type.element_type, .temporal_kind = argument.type.temporal_kind }) else null,
        };
        const chosen = try records.choose(self.a, candidates.items, inputs) orelse return error.SqlUndefinedFunction;
        return self.prepareHelper(self.captured.findRoutine(chosen.id) orelse return error.InvalidSqlTriggerManifest);
    }
    fn prepareHelper(self: *Statement, captured: manifest.Routine) anyerror!*const bindings.Resolved {
        if (captured.record.definition.returns_trigger) return error.SqlTriggerContextRequired;
        if (self.helpers.get(captured.record.id)) |ready| return ready;
        if (self.depth >= self.max_depth) return error.SqlProgramLimitExceeded;
        self.depth += 1;
        defer self.depth -= 1;
        const ready = try native.prepare(self.a, captured.record, self.resolver(), self.settings);
        try self.helpers.put(self.a, captured.record.id, ready);
        return ready;
    }
    pub fn fire(self: *Statement, call: Dispatch) !Output {
        try charge(self);
        if (call.attachment_id) |id| {
            if (call.level != .row or call.timing != .after) return error.InvalidSqlTriggerManifest;
            const valid = for (self.captured.attachmentEntries()) |entry| {
                if (entry.id == id and entry.matches(call.timing, call.level, call.event, call.updated_columns)) break true;
            } else false;
            if (!valid) return error.InvalidSqlTriggerManifest;
        }
        const phase = @as(usize, @backingInt(call.event)) * 2 + @intFromBool(call.timing == .after);
        if (call.level == .statement and self.statement_phases[phase]) return error.InvalidSqlBackendResponse;
        if (self.columns) |columns| {
            if (columns.ptr != call.frame.columns.ptr or columns.len != call.frame.columns.len) return error.CatalogGenerationChanged;
        } else self.columns = call.frame.columns;
        if (call.owned) |owned| if (&owned.frame != call.frame) return error.InvalidSqlBackendResponse;
        const original_new = call.frame.new;
        const original_old = call.frame.old;
        var notices: std.ArrayList(procedure.Notice) = .empty;
        var row: ?*const catalog.Row = if (call.event == .delete) call.frame.old else call.frame.new;
        var fired: usize = 0;
        for (self.captured.attachmentEntries(), 0..) |entry, i| {
            if (call.attachment_id) |id| if (entry.id != id) continue;
            if (!entry.matches(call.timing, call.level, call.event, call.updated_columns)) continue;
            try charge(self);
            if (call.owned) |owned| {
                try owned.reset(if (call.level == .statement) null else if (call.timing == .before and call.event != .delete) row else original_new, if (call.level == .statement) null else original_old);
            }
            if (self.prepared[i] == null) {
                const routine = self.captured.findRoutine(entry.function.id) orelse return error.InvalidSqlTriggerManifest;
                const body = self.bodies.get(routine.record.id) orelse body: {
                    const result = try procedure.prepare(self.a, routine.record, self.resolver(), null, self.settings);
                    try self.bodies.put(self.a, routine.record.id, result);
                    break :body result;
                };
                var prepared: Prepared = .{ .body = body };
                if (call.owned) |owned| {
                    try owned.setVariables(self.a, entry, call.event);
                    prepared.variables = try self.a.dupe(scalar.Datum, owned.frame.values[owned.layout.width * 2 ..]);
                }
                if (entry.predicate) |sql| {
                    const expression = try @import("compiler.zig").compileScalar(self.a, sql, .{});
                    prepared.predicate = try scalar.bindTypedExpectedWithSettings(self.a, expression.expression, if (call.level == .statement) &.{} else call.frame.columns[0 .. call.frame.width * 2], &.{}, .{ .kind = .boolean }, .{ .invocation = prepared.body.invocation }, self.settings);
                }
                self.prepared[i] = prepared;
            }
            const prepared = &self.prepared[i].?;
            if (call.owned) |owned| if (prepared.variables) |values| {
                const variables = owned.frame.values[owned.layout.width * 2 ..];
                @memcpy(variables, values);
                variables[3] = .json(.{ .string = switch (call.event) {
                    .insert => "INSERT",
                    .update => "UPDATE",
                    .delete => "DELETE",
                    .truncate => "TRUNCATE",
                } });
            };
            const context: @import("runtime.zig").Context = .{ .alloc = self.a, .arena = self.a, .backend = self.backend(), .binding = undefined, .parameters = &.{}, .limits = .{} };
            if (prepared.predicate) |predicate| {
                const value = try context.evaluate(self.a, predicate, call.frame.values);
                if (value.sql_null) continue;
                if (value.value != .bool) return error.InvalidSqlBackendResponse;
                if (!value.value.bool) continue;
            }
            const result = try procedure.executeTrigger(context, prepared.body, .{
                .event = switch (call.event) {
                    .insert => .insert,
                    .update => .update,
                    .delete => .delete,
                    .truncate => .truncate,
                },
                .level = if (call.level == .row) .row else .statement,
                .new = call.frame.new,
                .old = call.frame.old,
                .frame = call.frame,
            });
            fired += 1;
            try notices.appendSlice(self.a, result.notices);
            if (call.timing == .before and call.level == .row) {
                row = result.row;
                if (row == null) break;
                if (call.event != .delete and row != call.frame.new) {
                    // RETURN OLD on UPDATE replaces the complete candidate,
                    // not merely the cells touched by this function. The same
                    // owner-local assignment boundary preserves typed domains.
                    if (row != call.frame.old) return error.InvalidSqlBackendResponse;
                    const old_values = try self.a.dupe(scalar.Datum, call.frame.values[call.frame.old_offset..][0..call.frame.width]);
                    for (old_values, 0..) |value, ordinal| {
                        call.frame.new = try call.frame.assign(call.frame.ptr, self.a, .new, ordinal, value);
                        call.frame.values[call.frame.new_offset + ordinal] = value;
                    }
                    row = call.frame.new;
                }
            }
        }
        const owned_notices = try notices.toOwnedSlice(self.a);
        if (call.level == .statement) self.statement_phases[phase] = true;
        return .{ .row = row, .notices = owned_notices, .fired = fired };
    }
};

const Test = struct {
    fn captured(a: A) !manifest.Manifest {
        const reference: records.RoutineReference = .{ .database = "antfly", .namespace = "public", .name = "touch_updated_at", .namespace_id = 2, .id = 9 };
        const state: @import("../system_catalog/triggers.zig").State = .{
            .metadata_incarnation = @splat(1),
            .table_id = 7,
            .generation = 1,
            .next_id = 2,
            .entries = try a.dupe(@import("../system_catalog/triggers.zig").Entry, &.{.{
                .id = 1,
                .generation = 1,
                .name = "update_timestamp",
                .timing = .before,
                .events = .{ .update = true },
                .level = .statement,
                .function = reference,
                .arguments = &.{"updated_at_ns"},
            }}),
        };
        return .{
            .metadata_incarnation = @splat(1),
            .catalog_revision = 1,
            .table = .{ .id = 7, .schema_version = 3, .schema_digest = @splat(2), .attachment_generation = 1, .attachment_digest = try manifest.attachmentDigest(state) },
            .attachments = state,
            .routines = try a.dupe(manifest.Routine, &.{.{ .reference = reference, .record = .{
                .id = 9,
                .generation = 1,
                .definition = .{ .returns_trigger = true, .arguments = &.{}, .language = .plpgsql, .body = "BEGIN RAISE NOTICE 'statement fired'; RETURN NEW; END", .owner = .auth_disabled },
            } }}),
        };
    }
    fn assign(_: *anyopaque, _: A, _: procedure.TriggerImage, _: usize, _: scalar.Datum) !*const catalog.Row {
        return error.TestUnexpectedAssignment;
    }
};

test "SQL trigger dispatcher fires default statement triggers once even without row images" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const owner = try Statement.init(a, try Test.captured(a), null, .{});
    var marker: u8 = 0;
    var frame: procedure.TriggerFrame = .{ .columns = &.{}, .values = &.{}, .width = 0, .new_offset = 0, .old_offset = 0, .new = null, .old = null, .ptr = &marker, .assign = Test.assign };
    const call: Dispatch = .{ .timing = .before, .level = .statement, .event = .update, .frame = &frame };
    const result = try owner.fire(call);
    try std.testing.expectEqual(@as(usize, 1), result.fired);
    try std.testing.expectEqual(@as(usize, 1), result.notices.len);
    try std.testing.expectEqualStrings("statement fired", result.notices[0].message);
    try std.testing.expectEqual(@as(?*const catalog.Row, null), result.row);
    try std.testing.expectError(error.InvalidSqlBackendResponse, owner.fire(call));
}

test "SQL trigger manifest is canonical and stable IDs survive routine namespace moves" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var captured = try Test.captured(a);
    // Attachment labels retain creation namespace; lookup proof records the
    // current namespace of the same immutable routine, never a reused name.
    const moved = try a.dupe(manifest.Routine, captured.routines);
    moved[0].reference.namespace_id = 13;
    moved[0].reference.namespace = "renamed";
    captured.routines = moved;
    try captured.validate(a);
    const encoded = try captured.encode(a);
    var decoded = try manifest.Manifest.decode(a, encoded);
    defer decoded.deinit();
    try std.testing.expectEqual(try captured.digest(), try decoded.value.digest());
    var wrong = captured.table;
    wrong.id += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, captured.requireFor(wrong, captured.metadata_incarnation));
    wrong = captured.table;
    wrong.schema_version += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, captured.requireFor(wrong, captured.metadata_incarnation));
    captured.table.attachment_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidSqlTriggerManifest, captured.validate(a));
}

test "SQL trigger dispatcher isolates statement local records and TG_ARGV between attachments" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var captured = try Test.captured(a);
    const routines_copy = try a.dupe(manifest.Routine, captured.routines);
    routines_copy[0].record.definition.body = "BEGIN NEW.updated_at_ns := 42; PERFORM TG_ARGV[0]; RAISE NOTICE 'trigger fired'; RETURN NEW; END";
    captured.routines = routines_copy;
    const entries = try a.alloc(@import("../system_catalog/triggers.zig").Entry, 2);
    entries[0] = captured.attachments.?.entries[0];
    entries[0].name = "a_touch";
    entries[1] = entries[0];
    entries[1].id = 2;
    entries[1].name = "z_touch";
    captured.attachments.?.entries = entries;
    captured.attachments.?.next_id = 3;
    captured.table.attachment_digest = try manifest.attachmentDigest(captured.attachments.?);
    const owner = try Statement.init(a, captured, null, .{});
    const layout = try frames.Layout.init(a, &.{.{ .name = "updated_at_ns", .path = "updated_at_ns", .type = .integer, .element_type = .int64 }}, 7, "usage_records", "public");
    const frame = try layout.frame(a, null, null);
    const result = try owner.fire(.{ .timing = .before, .level = .statement, .event = .update, .frame = &frame.frame, .owned = frame });
    try std.testing.expectEqual(@as(usize, 2), result.fired);
    try std.testing.expectEqual(@as(usize, 2), result.notices.len);
    try std.testing.expect(result.row == null);
}

test "SQL trigger dispatcher carries only BEFORE returned NEW and ignores AFTER assignments" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const layout = try frames.Layout.init(a, &.{.{ .name = "n", .path = "n", .type = .integer, .element_type = .int64 }}, 7, "rows", "public");
    const initial = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 10 })});
    const old = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 1 })});
    for ([_]definition.Timing{ .before, .after }) |timing| {
        var captured = try Test.captured(a);
        const routines_copy = try a.dupe(manifest.Routine, captured.routines);
        routines_copy[0].record.definition.body = "BEGIN NEW.n := NEW.n + 1; RETURN NEW; END";
        captured.routines = routines_copy;
        const entries = try a.alloc(@import("../system_catalog/triggers.zig").Entry, 2);
        entries[0] = captured.attachments.?.entries[0];
        entries[0].name = "a_touch";
        entries[0].timing = timing;
        entries[0].level = .row;
        entries[1] = entries[0];
        entries[1].id = 2;
        entries[1].name = "z_touch";
        captured.attachments.?.entries = entries;
        captured.attachments.?.next_id = 3;
        captured.table.attachment_digest = try manifest.attachmentDigest(captured.attachments.?);
        const owner = try Statement.init(a, captured, null, .{});
        const frame = try layout.frame(a, &initial, &old);
        const output = try owner.fire(.{ .timing = timing, .level = .row, .event = .update, .frame = &frame.frame, .owned = frame });
        try std.testing.expectEqual(@as(usize, 2), output.fired);
        try std.testing.expectEqual(@as(i64, if (timing == .before) 12 else 10), (try output.row.?.cell("n")).value.integer);
        try std.testing.expectEqual(@as(i64, 10), (try initial.cell("n")).value.integer);
        try std.testing.expectEqual(@as(i64, 1), (try old.cell("n")).value.integer);
    }
}

test "SQL trigger dispatch reuses bound bodies and TG variables for ten thousand unchanged rows" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var captured = try Test.captured(a);
    const bodies = try a.dupe(manifest.Routine, captured.routines);
    bodies[0].record.definition.body = "BEGIN RETURN NEW; END";
    captured.routines = bodies;
    const entries = try a.dupe(@import("../system_catalog/triggers.zig").Entry, captured.attachments.?.entries);
    entries[0].level = .row;
    captured.attachments.?.entries = entries;
    captured.table.attachment_digest = try manifest.attachmentDigest(captured.attachments.?);
    const layout = try frames.Layout.init(a, &.{.{ .name = "n", .path = "n", .type = .integer, .element_type = .int64 }}, 7, "rows", "public");
    const new = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 10 })});
    const old = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 1 })});
    const frame = try layout.frame(a, &new, &old);
    var counting = std.testing.FailingAllocator.init(a, .{});
    const owner = try Statement.init(counting.allocator(), captured, null, .{});
    const invocation: Dispatch = .{ .timing = .before, .level = .row, .event = .update, .frame = &frame.frame, .owned = frame };
    _ = try owner.fire(invocation);
    const allocations = counting.alloc_index;
    for (0..10_000) |_| {
        try frame.reset(&new, &old);
        const output = try owner.fire(invocation);
        try std.testing.expectEqual(&new, output.row.?);
    }
    try std.testing.expectEqual(allocations, counting.alloc_index);
}

test "SQL trigger AFTER row dispatcher admits one captured attachment for ordered FK interleaving" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var captured = try Test.captured(a);
    const entries = try a.alloc(@import("../system_catalog/triggers.zig").Entry, 2);
    entries[0] = captured.attachments.?.entries[0];
    entries[0].name = "AA_after";
    entries[0].timing = .after;
    entries[0].level = .row;
    entries[1] = entries[0];
    entries[1].id = 2;
    entries[1].name = "zz_after";
    captured.attachments.?.entries = entries;
    captured.attachments.?.next_id = 3;
    captured.table.attachment_digest = try manifest.attachmentDigest(captured.attachments.?);
    const owner = try Statement.init(a, captured, null, .{});
    const layout = try frames.Layout.init(a, &.{.{ .name = "n", .path = "n", .type = .integer, .element_type = .int64 }}, 7, "rows", "public");
    const row = try catalog.Row.fromDatums(a, "row", layout.row, &.{.json(.{ .integer = 1 })});
    const frame = try layout.frame(a, &row, &row);
    for ([_]u64{ 1, 2 }) |id| {
        const output = try owner.fire(.{ .attachment_id = id, .timing = .after, .level = .row, .event = .update, .frame = &frame.frame, .owned = frame });
        try std.testing.expectEqual(@as(usize, 1), output.fired);
        try std.testing.expectEqual(@as(usize, 1), output.notices.len);
    }
    try std.testing.expectError(error.InvalidSqlTriggerManifest, owner.fire(.{ .attachment_id = 3, .timing = .after, .level = .row, .event = .update, .frame = &frame.frame, .owned = frame }));
    try std.testing.expectError(error.InvalidSqlTriggerManifest, owner.fire(.{ .attachment_id = 1, .timing = .before, .level = .row, .event = .update, .frame = &frame.frame, .owned = frame }));
}

// Append to sql/trigger_runtime.zig so the existing SQL test root discovers it.
test "SQL trigger document candidates share native null provenance for OLD NEW and selected writes" {
    const prep = @import("trigger_preparation.zig");
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const schema_json =
        \\{"version":1,"enforce_types":true,"default_type":"doc","generated_columns":[{"column":"kind","expression":{"op":"jsonb_typeof","args":[{"op":"column","column":"payload"}]}}],"document_schemas":{"doc":{"schema":{"type":"object","properties":{"payload":{"type":"object","nullable":true},"scalar":{"type":"keyword","nullable":true},"kind":{"type":"keyword"}},"additionalProperties":true}}}}
    ;
    var view = try prep.captureView(a, schema_json);
    defer view.release();
    const owner = try prep.Owner.create(a, view, .{ .id = 7, .physical_name = "docs", .schema_json = schema_json, .schema_version = 1, .storage_mode = .document, .columns = &.{} }, null, .{});
    defer owner.deinit();
    const original = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"payload\":null,\"scalar\":null,\"kind\":\"null\"}", .{});
    const old = try owner.sourceRow("row", original, &.{}, 7, @splat(3));
    try std.testing.expect(!(try old.cell("payload")).sql_null);
    try std.testing.expect((try old.cell("scalar")).sql_null);
    var value = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"payload\":null,\"scalar\":null}", .{});
    var row = try owner.before(.{ .key = "row", .event = .update, .old = old, .after = &value });
    try std.testing.expect(!(try row.new.?.cell("payload")).sql_null);
    try std.testing.expect((try row.new.?.cell("scalar")).sql_null);
    try std.testing.expectEqualStrings("null", (try row.new.?.cell("kind")).value.string);
    try std.testing.expectEqual(@as(usize, 0), row.json_null_fields.len);
    _ = try owner.after(&row);
    var missing = try std.json.parseFromSliceLeaky(std.json.Value, a, "{}", .{});
    const absent = try owner.before(.{ .key = "second", .event = .insert, .after = &missing });
    try std.testing.expect((try absent.new.?.cell("payload")).sql_null);
    try std.testing.expect((try absent.new.?.cell("kind")).sql_null);
    try std.testing.expectEqual(@as(usize, 0), absent.json_null_fields.len);
}
