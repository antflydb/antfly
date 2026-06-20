// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const extension_domain = @import("../extensions/mod.zig");
const docstore_mod = @import("../storage/docstore.zig");
const relational_rows = @import("relational_rows.zig");
const relational_sql = @import("relational_sql.zig");
const runtime_schema = @import("../storage/schema.zig");
const sql_adapter = @import("sql_adapter/mod.zig");

const SpinMutex = struct {
    inner: std.Io.Mutex = .init,

    fn lock(self: *@This()) void {
        self.inner.lockUncancelable(std.Options.debug_io);
    }

    fn unlock(self: *@This()) void {
        self.inner.unlock(std.Options.debug_io);
    }
};

pub const RoutineRecord = struct {
    origin: RoutineOrigin = .catalog,
    kind: relational_sql.RoutineKind,
    name: []u8,
    argument_count: usize,
    returns_type: ?[]u8 = null,
    language: ?[]u8 = null,
    volatility: ?relational_sql.RoutineVolatility = null,
    security: ?relational_sql.RoutineSecurity = null,
    null_input: ?relational_sql.RoutineNullInput = null,
    parallel_safety: ?relational_sql.RoutineParallelSafety = null,
    leakproof: bool = false,
    window: bool = false,
    support_function: ?[]u8 = null,
    transform_types: [][]u8 = &.{},
    settings: []relational_sql.RoutineSetting = &.{},
    cost: ?[]u8 = null,
    rows: ?[]u8 = null,
    body: ?relational_sql.RoutineBodyPlan = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.returns_type) |value| alloc.free(value);
        if (self.language) |value| alloc.free(value);
        if (self.support_function) |value| alloc.free(value);
        freeOwnedStringSlice(alloc, self.transform_types);
        for (self.settings) |*setting| setting.deinit(alloc);
        if (self.settings.len > 0) alloc.free(self.settings);
        if (self.cost) |value| alloc.free(value);
        if (self.rows) |value| alloc.free(value);
        if (self.body) |*body| body.deinit(alloc);
        self.* = undefined;
    }
};

pub const RoutineOrigin = enum {
    catalog,
    extension_query_function,
};

pub const TriggerEvent = enum {
    insert,
    update,
    delete,
};

pub const TriggerRecord = struct {
    trigger_name: []u8,
    table_name: []u8,
    function_name: []u8,
    event: TriggerEvent,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.trigger_name);
        alloc.free(self.table_name);
        alloc.free(self.function_name);
        self.* = undefined;
    }
};

pub const OpenedStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedStore {
        const path_z = try alloc.dupeZ(u8, path);
        errdefer alloc.free(path_z);
        const docstore = try alloc.create(docstore_mod.DocStore);
        errdefer alloc.destroy(docstore);
        docstore.* = try docstore_mod.DocStore.open(alloc, path_z, .{});
        errdefer docstore.close();
        return .{
            .alloc = alloc,
            .path_z = path_z,
            .docstore = docstore,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }
};

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    mutex: SpinMutex = .{},
    routines: std.ArrayListUnmanaged(RoutineRecord) = .empty,
    triggers: std.ArrayListUnmanaged(TriggerRecord) = .empty,
    opened_store: ?*OpenedStore = null,

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.routines.items) |*routine| routine.deinit(self.alloc);
        self.routines.deinit(self.alloc);
        for (self.triggers.items) |*trigger| trigger.deinit(self.alloc);
        self.triggers.deinit(self.alloc);
        if (self.opened_store) |opened| {
            opened.deinit();
            self.alloc.destroy(opened);
        }
    }

    pub fn attachOpenedStore(self: *@This(), opened: *OpenedStore) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.opened_store != null) return error.InvalidRoutineStore;
        self.opened_store = opened;
        errdefer self.opened_store = null;
        try self.recoverPersistedRoutinesLocked(opened);
        try self.recoverPersistedTriggersLocked(opened);
    }

    pub fn apply(self: *@This(), plan: relational_sql.FunctionCatalogPlan) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        switch (plan) {
            .create => |create| try self.createLocked(create),
            .drop => |drop| try self.dropLocked(drop),
        }
    }

    pub fn routineCountForTest(self: *@This()) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.routines.items.len;
    }

    pub fn triggerCountForTest(self: *@This()) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.triggers.items.len;
    }

    pub fn applyTriggerDdlAlloc(self: *@This(), alloc: std.mem.Allocator, sql: []const u8) !bool {
        var parsed = parseTriggerDdlAlloc(alloc, sql) catch |err| switch (err) {
            error.NotTriggerDdl, error.UnsupportedSqlShape => return false,
            else => return err,
        };
        defer parsed.deinit(alloc);

        self.mutex.lock();
        defer self.mutex.unlock();
        switch (parsed) {
            .create => |create| try self.createTriggerLocked(create),
            .drop => |drop| try self.dropTriggerLocked(drop),
        }
        return true;
    }

    pub fn applyCatalogedTriggerDropDdlAlloc(self: *@This(), alloc: std.mem.Allocator, sql: []const u8) !bool {
        var parsed = parseTriggerDdlAlloc(alloc, sql) catch |err| switch (err) {
            error.NotTriggerDdl, error.UnsupportedSqlShape => return false,
            else => return err,
        };
        defer parsed.deinit(alloc);

        switch (parsed) {
            .create => return false,
            .drop => |drop| {
                self.mutex.lock();
                defer self.mutex.unlock();
                if (self.findTriggerIndexLocked(drop.table_name, drop.trigger_name) == null) return false;
                try self.dropTriggerLocked(drop);
                return true;
            },
        }
    }

    pub fn triggerFunctionForTableEventAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        table_name: []const u8,
        event: TriggerEvent,
    ) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.triggers.items) |trigger| {
            if (trigger.event == event and std.ascii.eqlIgnoreCase(trigger.table_name, table_name)) {
                return try alloc.dupe(u8, trigger.function_name);
            }
        }
        return null;
    }

    pub fn triggerHookForTableEvent(
        self: *@This(),
        table_name: []const u8,
        event: TriggerEvent,
    ) !?relational_sql.RoutineExecutionHook {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.triggers.items) |trigger| {
            if (trigger.event == event and std.ascii.eqlIgnoreCase(trigger.table_name, table_name)) {
                const routine = self.findRoutineLocked(.function, trigger.function_name, 0) orelse return error.RoutineNotFound;
                const body = routine.body orelse return error.RoutineBodyNotExecutable;
                if (body.kind != .plpgsql_trigger or body.expression != null) return error.RoutineBodyNotExecutable;
                return body.hook;
            }
        }
        return null;
    }

    pub fn replaceNativeQueryFunctionBindings(
        self: *@This(),
        bindings: []const extension_domain.QueryFunctionBinding,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.removeExtensionQueryFunctionsLocked();
        for (bindings) |binding| {
            try self.createNativeQueryFunctionLocked(binding);
        }
    }

    pub fn executeExpressionRoutineAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        routine_name: []const u8,
        argument_json: []const u8,
    ) ![]u8 {
        return try self.executeExpressionRoutineArgsAlloc(alloc, routine_name, &.{argument_json});
    }

    pub fn executeExpressionRoutineArgsAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        routine_name: []const u8,
        argument_json: []const []const u8,
    ) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const routine = self.findRoutineLocked(.function, routine_name, argument_json.len) orelse return error.RoutineNotFound;
        const body = routine.body orelse return error.RoutineBodyNotExecutable;
        if (body.kind != .sql_expression or body.hook != .expression) return error.RoutineBodyNotExecutable;
        const expression = body.expression orelse return error.RoutineBodyNotExecutable;
        const row_json = try routineArgumentObjectJsonAlloc(alloc, argument_json, routine.null_input);
        defer alloc.free(row_json);
        if (std.mem.eql(u8, row_json, "null")) return try alloc.dupe(u8, "null");
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
        defer parsed.deinit();
        return try relational_rows.expressionValueJsonAlloc(alloc, parsed.value, expression);
    }

    pub fn executeTriggerRoutineAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
        routine_name: []const u8,
        new_row_json: ?[]const u8,
        old_row_json: ?[]const u8,
    ) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const routine = self.findRoutineLocked(.function, routine_name, 0) orelse return error.RoutineNotFound;
        const body = routine.body orelse return error.RoutineBodyNotExecutable;
        if (body.kind != .plpgsql_trigger or body.expression != null) return error.RoutineBodyNotExecutable;
        if (body.hook == .trigger_return_null) return try alloc.dupe(u8, "null");
        const tuple_json = switch (body.hook) {
            .trigger_return_new => new_row_json orelse return error.RoutineTriggerTupleUnavailable,
            .trigger_return_old => old_row_json orelse return error.RoutineTriggerTupleUnavailable,
            else => return error.RoutineBodyNotExecutable,
        };
        return try routineTriggerTupleJsonAlloc(alloc, tuple_json);
    }

    pub fn listExpressionRoutineBindingsAlloc(
        self: *@This(),
        alloc: std.mem.Allocator,
    ) ![]relational_sql.RoutineExpressionBinding {
        self.mutex.lock();
        defer self.mutex.unlock();

        var bindings = std.ArrayListUnmanaged(relational_sql.RoutineExpressionBinding).empty;
        errdefer {
            for (bindings.items) |binding| {
                alloc.free(@constCast(binding.sql_name));
                runtime_schema.freeRelationalRowsExpression(alloc, binding.expression);
            }
            bindings.deinit(alloc);
        }

        for (self.routines.items) |routine| {
            if (routine.kind != .function) continue;
            const body = routine.body orelse continue;
            if (body.kind != .sql_expression or body.hook != .expression) continue;
            const expression_body = body.expression orelse continue;
            if (routine.argument_count > std.math.maxInt(u16)) return error.UnsupportedSqlShape;

            const sql_name = try alloc.dupe(u8, routine.name);
            errdefer alloc.free(sql_name);
            const expression = try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, expression_body);
            errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);

            try bindings.append(alloc, .{
                .sql_name = sql_name,
                .arity = @intCast(routine.argument_count),
                .expression = expression,
                .null_input = routine.null_input,
            });
        }

        return try bindings.toOwnedSlice(alloc);
    }

    fn createLocked(self: *@This(), plan: relational_sql.CreateRoutinePlan) !void {
        if (plan.body) |body| {
            switch (body.hook) {
                .expression => {
                    if (plan.kind != .function or body.kind != .sql_expression or body.expression == null) return error.UnsupportedSqlShape;
                },
                .trigger_return_new, .trigger_return_old, .trigger_return_null => {
                    if (plan.kind != .function or body.kind != .plpgsql_trigger or body.expression != null) return error.UnsupportedSqlShape;
                    const returns_type = plan.returns_type orelse return error.UnsupportedSqlShape;
                    if (!std.ascii.eqlIgnoreCase(returns_type, "trigger")) return error.UnsupportedSqlShape;
                },
                .procedure_noop => {
                    if (plan.kind != .procedure or body.kind != .plpgsql_procedure or body.expression != null) return error.UnsupportedSqlShape;
                    if (plan.returns_type != null) return error.UnsupportedSqlShape;
                },
            }
        }
        const existing_index = self.findRoutineIndexLocked(plan.kind, plan.routine_name, plan.argument_count);
        if (existing_index != null) {
            if (!plan.replace_existing) return error.RoutineAlreadyExists;
        }
        var record = try cloneCreateRoutineRecordAlloc(self.alloc, plan);
        errdefer record.deinit(self.alloc);
        try self.routines.ensureUnusedCapacity(self.alloc, 1);
        try self.persistCatalogRoutineLocked(record);
        if (existing_index) |existing| {
            var removed = self.routines.orderedRemove(existing);
            removed.deinit(self.alloc);
        }
        self.routines.appendAssumeCapacity(record);
    }

    fn createNativeQueryFunctionLocked(
        self: *@This(),
        binding: extension_domain.QueryFunctionBinding,
    ) !void {
        const expression_kind = binding.native_expression_kind;
        try extension_domain.validateQueryFunctionNativeExpressionArity(expression_kind, binding.arity);
        if (self.findRoutineIndexLocked(.function, binding.sql_name, binding.arity)) |_| return error.RoutineAlreadyExists;
        var record = try nativeQueryFunctionRecordAlloc(self.alloc, binding, expression_kind);
        errdefer record.deinit(self.alloc);
        try self.routines.append(self.alloc, record);
    }

    fn dropLocked(self: *@This(), plan: relational_sql.DropRoutinePlan) !void {
        if (plan.cascade) return error.UnsupportedSqlShape;
        if (self.routineReferencedByTriggerLocked(plan.kind, plan.routine_name, plan.argument_count)) return error.RoutineInUse;
        if (self.findRoutineIndexLocked(plan.kind, plan.routine_name, plan.argument_count)) |existing| {
            try self.deletePersistedCatalogRoutineLocked(self.routines.items[existing]);
            var removed = self.routines.orderedRemove(existing);
            removed.deinit(self.alloc);
            return;
        }
        if (!plan.if_exists) return error.RoutineNotFound;
    }

    fn createTriggerLocked(self: *@This(), plan: CreateTriggerPlan) !void {
        try self.validateTriggerFunctionLocked(plan.function_name, plan.event);
        const existing_index = self.findTriggerIndexLocked(plan.table_name, plan.trigger_name);
        if (existing_index != null and !plan.replace_existing) return error.TriggerAlreadyExists;
        var record = try cloneCreateTriggerRecordAlloc(self.alloc, plan);
        errdefer record.deinit(self.alloc);
        try self.triggers.ensureUnusedCapacity(self.alloc, 1);
        try self.persistTriggerLocked(record);
        if (existing_index) |existing| {
            var removed = self.triggers.orderedRemove(existing);
            removed.deinit(self.alloc);
        }
        self.triggers.appendAssumeCapacity(record);
    }

    fn dropTriggerLocked(self: *@This(), plan: DropTriggerPlan) !void {
        if (plan.cascade) return error.UnsupportedSqlShape;
        if (self.findTriggerIndexLocked(plan.table_name, plan.trigger_name)) |existing| {
            try self.deletePersistedTriggerLocked(self.triggers.items[existing]);
            var removed = self.triggers.orderedRemove(existing);
            removed.deinit(self.alloc);
            return;
        }
        if (!plan.if_exists) return error.TriggerNotFound;
    }

    fn removeExtensionQueryFunctionsLocked(self: *@This()) void {
        var i: usize = 0;
        while (i < self.routines.items.len) {
            if (self.routines.items[i].origin != .extension_query_function) {
                i += 1;
                continue;
            }
            var removed = self.routines.orderedRemove(i);
            removed.deinit(self.alloc);
        }
    }

    fn findRoutineLocked(
        self: *@This(),
        kind: relational_sql.RoutineKind,
        name: []const u8,
        argument_count: usize,
    ) ?*const RoutineRecord {
        const index = self.findRoutineIndexLocked(kind, name, argument_count) orelse return null;
        return &self.routines.items[index];
    }

    fn findRoutineIndexLocked(
        self: *@This(),
        kind: relational_sql.RoutineKind,
        name: []const u8,
        argument_count: usize,
    ) ?usize {
        for (self.routines.items, 0..) |routine, i| {
            if (routine.kind == kind and routine.argument_count == argument_count and std.ascii.eqlIgnoreCase(routine.name, name)) return i;
        }
        return null;
    }

    fn validateTriggerFunctionLocked(self: *@This(), function_name: []const u8, event: TriggerEvent) !void {
        const routine = self.findRoutineLocked(.function, function_name, 0) orelse return error.RoutineNotFound;
        const body = routine.body orelse return error.RoutineBodyNotExecutable;
        if (body.kind != .plpgsql_trigger or body.expression != null) return error.RoutineBodyNotExecutable;
        switch (body.hook) {
            .trigger_return_new => {
                if (event == .delete) return error.UnsupportedSqlShape;
            },
            .trigger_return_old => {
                if (event == .insert) return error.UnsupportedSqlShape;
            },
            .trigger_return_null => {},
            else => return error.RoutineBodyNotExecutable,
        }
    }

    fn findTriggerIndexLocked(self: *@This(), table_name: []const u8, trigger_name: []const u8) ?usize {
        for (self.triggers.items, 0..) |trigger, i| {
            if (std.ascii.eqlIgnoreCase(trigger.table_name, table_name) and
                std.ascii.eqlIgnoreCase(trigger.trigger_name, trigger_name)) return i;
        }
        return null;
    }

    fn routineReferencedByTriggerLocked(
        self: *@This(),
        kind: relational_sql.RoutineKind,
        routine_name: []const u8,
        argument_count: usize,
    ) bool {
        if (kind != .function or argument_count != 0) return false;
        for (self.triggers.items) |trigger| {
            if (std.ascii.eqlIgnoreCase(trigger.function_name, routine_name)) return true;
        }
        return false;
    }

    fn persistCatalogRoutineLocked(self: *@This(), record: RoutineRecord) !void {
        if (record.origin != .catalog) return;
        const opened = self.opened_store orelse return;
        const key = try routineRecordKeyAlloc(self.alloc, record.kind, record.name, record.argument_count);
        defer self.alloc.free(key);
        const encoded = try encodeRoutineRecordAlloc(self.alloc, record);
        defer self.alloc.free(encoded);
        try opened.docstore.put(key, encoded);
    }

    fn deletePersistedCatalogRoutineLocked(self: *@This(), record: RoutineRecord) !void {
        if (record.origin != .catalog) return;
        const opened = self.opened_store orelse return;
        const key = try routineRecordKeyAlloc(self.alloc, record.kind, record.name, record.argument_count);
        defer self.alloc.free(key);
        opened.docstore.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn recoverPersistedRoutinesLocked(self: *@This(), opened: *OpenedStore) !void {
        const results = try opened.docstore.scanPrefix(self.alloc, routine_key_prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, results);
        for (results) |kv| {
            var parsed = std.json.parseFromSlice(PersistedRoutineRecord, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidRoutineStore,
            };
            defer parsed.deinit();
            if (parsed.value.version != 1 or parsed.value.origin != .catalog) return error.InvalidRoutineStore;
            var record = clonePersistedRoutineRecordAlloc(self.alloc, parsed.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidRoutineStore,
            };
            errdefer record.deinit(self.alloc);
            if (self.findRoutineIndexLocked(record.kind, record.name, record.argument_count)) |existing| {
                var removed = self.routines.orderedRemove(existing);
                removed.deinit(self.alloc);
            }
            try self.routines.append(self.alloc, record);
        }
    }

    fn persistTriggerLocked(self: *@This(), record: TriggerRecord) !void {
        const opened = self.opened_store orelse return;
        const key = try triggerRecordKeyAlloc(self.alloc, record.table_name, record.trigger_name);
        defer self.alloc.free(key);
        const encoded = try encodeTriggerRecordAlloc(self.alloc, record);
        defer self.alloc.free(encoded);
        try opened.docstore.put(key, encoded);
    }

    fn deletePersistedTriggerLocked(self: *@This(), record: TriggerRecord) !void {
        const opened = self.opened_store orelse return;
        const key = try triggerRecordKeyAlloc(self.alloc, record.table_name, record.trigger_name);
        defer self.alloc.free(key);
        opened.docstore.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn recoverPersistedTriggersLocked(self: *@This(), opened: *OpenedStore) !void {
        const results = try opened.docstore.scanPrefix(self.alloc, trigger_key_prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, results);
        for (results) |kv| {
            var parsed = std.json.parseFromSlice(PersistedTriggerRecord, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidRoutineStore,
            };
            defer parsed.deinit();
            if (parsed.value.version != 1) return error.InvalidRoutineStore;
            var record = clonePersistedTriggerRecordAlloc(self.alloc, parsed.value) catch |err| switch (err) {
                error.OutOfMemory => return err,
            };
            errdefer record.deinit(self.alloc);
            try self.validateTriggerFunctionLocked(record.function_name, record.event);
            if (self.findTriggerIndexLocked(record.table_name, record.trigger_name)) |existing| {
                var removed = self.triggers.orderedRemove(existing);
                removed.deinit(self.alloc);
            }
            try self.triggers.append(self.alloc, record);
        }
    }
};

pub fn freeExpressionRoutineBindings(
    alloc: std.mem.Allocator,
    bindings: []const relational_sql.RoutineExpressionBinding,
) void {
    for (bindings) |binding| {
        alloc.free(@constCast(binding.sql_name));
        runtime_schema.freeRelationalRowsExpression(alloc, binding.expression);
    }
    if (bindings.len > 0) alloc.free(@constCast(bindings));
}

const TriggerDdlPlan = union(enum) {
    create: CreateTriggerPlan,
    drop: DropTriggerPlan,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*create| create.deinit(alloc),
            .drop => |*drop| drop.deinit(alloc),
        }
        self.* = undefined;
    }
};

const CreateTriggerPlan = struct {
    trigger_name: []u8,
    table_name: []u8,
    function_name: []u8,
    event: TriggerEvent,
    replace_existing: bool = false,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.trigger_name);
        alloc.free(self.table_name);
        alloc.free(self.function_name);
        self.* = undefined;
    }
};

const DropTriggerPlan = struct {
    trigger_name: []u8,
    table_name: []u8,
    if_exists: bool = false,
    cascade: bool = false,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.trigger_name);
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

const TriggerParser = struct {
    alloc: std.mem.Allocator,
    tokens: []const sql_adapter.Token,
    pos: usize = 0,

    fn parse(self: *@This()) !TriggerDdlPlan {
        if (self.matchKeyword("create")) {
            var replace_existing = false;
            if (self.matchKeyword("or")) {
                try self.expectKeyword("replace");
                replace_existing = true;
            }
            if (!self.matchKeyword("trigger")) return error.NotTriggerDdl;
            return .{ .create = try self.parseCreateTriggerTail(replace_existing) };
        }
        if (self.matchKeyword("drop")) {
            if (!self.matchKeyword("trigger")) return error.NotTriggerDdl;
            return .{ .drop = try self.parseDropTriggerTail() };
        }
        return error.NotTriggerDdl;
    }

    fn parseCreateTriggerTail(self: *@This(), replace_existing: bool) !CreateTriggerPlan {
        const trigger_name = try self.parseIdentifierOwned();
        var trigger_transferred = false;
        errdefer if (!trigger_transferred) self.alloc.free(trigger_name);

        try self.expectKeyword("before");
        const event = try self.parseTriggerEvent();
        try self.expectKeyword("on");
        const table_name = try self.parseObjectIdentifierOwned();
        var table_transferred = false;
        errdefer if (!table_transferred) self.alloc.free(table_name);

        if (self.matchKeyword("for")) {
            try self.expectKeyword("each");
            try self.expectKeyword("row");
        }

        try self.expectKeyword("execute");
        if (!(self.matchKeyword("function") or self.matchKeyword("procedure"))) return error.UnsupportedSqlShape;
        const function_name = try self.parseObjectIdentifierOwned();
        var function_transferred = false;
        errdefer if (!function_transferred) self.alloc.free(function_name);
        try self.expect(.lparen);
        try self.expect(.rparen);
        try self.expectStatementEnd();

        trigger_transferred = true;
        table_transferred = true;
        function_transferred = true;
        return .{
            .trigger_name = trigger_name,
            .table_name = table_name,
            .function_name = function_name,
            .event = event,
            .replace_existing = replace_existing,
        };
    }

    fn parseDropTriggerTail(self: *@This()) !DropTriggerPlan {
        var if_exists = false;
        if (self.matchKeyword("if")) {
            try self.expectKeyword("exists");
            if_exists = true;
        }
        const trigger_name = try self.parseIdentifierOwned();
        var trigger_transferred = false;
        errdefer if (!trigger_transferred) self.alloc.free(trigger_name);
        try self.expectKeyword("on");
        _ = self.matchKeyword("only");
        const table_name = try self.parseObjectIdentifierOwned();
        var table_transferred = false;
        errdefer if (!table_transferred) self.alloc.free(table_name);
        var cascade = false;
        if (self.matchKeyword("cascade")) {
            cascade = true;
        } else {
            _ = self.matchKeyword("restrict");
        }
        try self.expectStatementEnd();
        trigger_transferred = true;
        table_transferred = true;
        return .{
            .trigger_name = trigger_name,
            .table_name = table_name,
            .if_exists = if_exists,
            .cascade = cascade,
        };
    }

    fn parseTriggerEvent(self: *@This()) !TriggerEvent {
        if (self.matchKeyword("insert")) return .insert;
        if (self.matchKeyword("update")) {
            if (self.matchKeyword("of")) return error.UnsupportedSqlShape;
            return .update;
        }
        if (self.matchKeyword("delete")) return .delete;
        return error.UnsupportedSqlShape;
    }

    fn parseIdentifierOwned(self: *@This()) ![]u8 {
        if (self.pos >= self.tokens.len) return error.UnsupportedSqlShape;
        const token = self.tokens[self.pos];
        if (token.kind != .identifier) return error.UnsupportedSqlShape;
        self.pos += 1;
        return try self.alloc.dupe(u8, token.text);
    }

    fn parseObjectIdentifierOwned(self: *@This()) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        var first = true;
        while (true) {
            if (self.pos >= self.tokens.len) return error.UnsupportedSqlShape;
            const token = self.tokens[self.pos];
            if (token.kind != .identifier) return error.UnsupportedSqlShape;
            if (!first) try writer.writeByte('.');
            first = false;
            try writer.writeAll(token.text);
            self.pos += 1;
            if (self.pos + 1 > self.tokens.len) break;
            if (!std.mem.eql(u8, self.tokens[self.pos].text, ".")) break;
            self.pos += 1;
        }
        return try out.toOwnedSlice();
    }

    fn expect(self: *@This(), kind: sql_adapter.TokenKind) !void {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].kind != kind) return error.UnsupportedSqlShape;
        self.pos += 1;
    }

    fn matchKeyword(self: *@This(), keyword: []const u8) bool {
        if (self.pos >= self.tokens.len) return false;
        const token = self.tokens[self.pos];
        if (token.kind != .identifier or !std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
        self.pos += 1;
        return true;
    }

    fn expectKeyword(self: *@This(), keyword: []const u8) !void {
        if (!self.matchKeyword(keyword)) return error.UnsupportedSqlShape;
    }

    fn expectStatementEnd(self: *@This()) !void {
        if (self.pos < self.tokens.len and self.tokens[self.pos].kind == .semicolon) self.pos += 1;
        if (self.pos != self.tokens.len) return error.UnsupportedSqlShape;
    }
};

fn parseTriggerDdlAlloc(alloc: std.mem.Allocator, sql: []const u8) !TriggerDdlPlan {
    var tokens = try sql_adapter.tokenizeAlloc(alloc, sql);
    defer sql_adapter.freeTokens(alloc, &tokens);
    var parser = TriggerParser{ .alloc = alloc, .tokens = tokens.items };
    return try parser.parse();
}

fn cloneCreateTriggerRecordAlloc(alloc: std.mem.Allocator, plan: CreateTriggerPlan) !TriggerRecord {
    const trigger_name = try alloc.dupe(u8, plan.trigger_name);
    errdefer alloc.free(trigger_name);
    const table_name = try alloc.dupe(u8, plan.table_name);
    errdefer alloc.free(table_name);
    const function_name = try alloc.dupe(u8, plan.function_name);
    errdefer alloc.free(function_name);
    return .{
        .trigger_name = trigger_name,
        .table_name = table_name,
        .function_name = function_name,
        .event = plan.event,
    };
}

const PersistedTriggerRecord = struct {
    version: u32 = 1,
    trigger_name: []u8,
    table_name: []u8,
    function_name: []u8,
    event: TriggerEvent,
};

fn encodeTriggerRecordAlloc(alloc: std.mem.Allocator, record: TriggerRecord) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, PersistedTriggerRecord{
        .version = 1,
        .trigger_name = record.trigger_name,
        .table_name = record.table_name,
        .function_name = record.function_name,
        .event = record.event,
    }, .{ .emit_null_optional_fields = false });
}

fn clonePersistedTriggerRecordAlloc(alloc: std.mem.Allocator, persisted: PersistedTriggerRecord) !TriggerRecord {
    const trigger_name = try alloc.dupe(u8, persisted.trigger_name);
    errdefer alloc.free(trigger_name);
    const table_name = try alloc.dupe(u8, persisted.table_name);
    errdefer alloc.free(table_name);
    const function_name = try alloc.dupe(u8, persisted.function_name);
    errdefer alloc.free(function_name);
    return .{
        .trigger_name = trigger_name,
        .table_name = table_name,
        .function_name = function_name,
        .event = persisted.event,
    };
}

fn nativeQueryFunctionRecordAlloc(
    alloc: std.mem.Allocator,
    binding: extension_domain.QueryFunctionBinding,
    expression_kind: runtime_schema.RelationalRowsExpressionKind,
) !RoutineRecord {
    const name = try alloc.dupe(u8, binding.sql_name);
    errdefer alloc.free(name);
    const language = try alloc.dupe(u8, "native");
    errdefer alloc.free(language);
    const expression = try nativeQueryFunctionExpressionAlloc(alloc, expression_kind, binding.arity);
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
    return .{
        .origin = .extension_query_function,
        .kind = .function,
        .name = name,
        .argument_count = binding.arity,
        .language = language,
        .body = .{
            .kind = .sql_expression,
            .hook = .expression,
            .expression = expression,
        },
    };
}

fn nativeQueryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
) !runtime_schema.RelationalRowsExpression {
    if (arity == 0) return .{ .kind = expression_kind };
    const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, arity);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(operands);
    }
    for (operands, 0..) |*operand, i| {
        const field = try std.fmt.allocPrint(alloc, "arg{d}", .{i + 1});
        errdefer alloc.free(field);
        operand.* = .{ .kind = .field, .field = field };
        initialized += 1;
    }
    return .{ .kind = expression_kind, .operands = operands };
}

fn routineArgumentObjectJsonAlloc(
    alloc: std.mem.Allocator,
    argument_json: []const []const u8,
    null_input: ?relational_sql.RoutineNullInput,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    for (argument_json, 0..) |json, i| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, json, .{}) catch return error.InvalidRowsRequest;
        defer parsed.deinit();
        if (parsed.value == .null and null_input == .returns_null) {
            out.deinit();
            return try alloc.dupe(u8, "null");
        }
        if (i > 0) try writer.writeByte(',');
        try writer.print("\"arg{d}\":", .{i + 1});
        try std.json.Stringify.value(parsed.value, .{}, writer);
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn routineTriggerTupleJsonAlloc(alloc: std.mem.Allocator, tuple_json: []const u8) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, tuple_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
}

fn cloneCreateRoutineRecordAlloc(alloc: std.mem.Allocator, plan: relational_sql.CreateRoutinePlan) !RoutineRecord {
    const name = try alloc.dupe(u8, plan.routine_name);
    errdefer alloc.free(name);
    const returns_type = if (plan.returns_type) |value| try alloc.dupe(u8, value) else null;
    errdefer if (returns_type) |value| alloc.free(value);
    const language = if (plan.language) |value| try alloc.dupe(u8, value) else null;
    errdefer if (language) |value| alloc.free(value);
    const support_function = if (plan.support_function) |value| try alloc.dupe(u8, value) else null;
    errdefer if (support_function) |value| alloc.free(value);
    const transform_types = try cloneStringSliceAlloc(alloc, plan.transform_types);
    errdefer freeOwnedStringSlice(alloc, transform_types);
    const settings = try cloneRoutineSettingsAlloc(alloc, plan.settings);
    errdefer freeRoutineSettings(alloc, settings);
    const cost = if (plan.cost) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cost) |value| alloc.free(value);
    const rows = if (plan.rows) |value| try alloc.dupe(u8, value) else null;
    errdefer if (rows) |value| alloc.free(value);
    const body = if (plan.body) |value| try cloneRoutineBodyAlloc(alloc, value) else null;
    errdefer if (body) |*value| value.deinit(alloc);
    return .{
        .kind = plan.kind,
        .name = name,
        .argument_count = plan.argument_count,
        .returns_type = returns_type,
        .language = language,
        .volatility = plan.volatility,
        .security = plan.security,
        .null_input = plan.null_input,
        .parallel_safety = plan.parallel_safety,
        .leakproof = plan.leakproof,
        .window = plan.window,
        .support_function = support_function,
        .transform_types = transform_types,
        .settings = settings,
        .cost = cost,
        .rows = rows,
        .body = body,
    };
}

fn cloneStringSliceAlloc(alloc: std.mem.Allocator, source: []const []const u8) ![][]u8 {
    if (source.len == 0) return &.{};
    const out = try alloc.alloc([]u8, source.len);
    var copied: usize = 0;
    errdefer {
        for (out[0..copied]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (source, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        copied += 1;
    }
    return out;
}

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn cloneRoutineSettingsAlloc(alloc: std.mem.Allocator, source: []const relational_sql.RoutineSetting) ![]relational_sql.RoutineSetting {
    if (source.len == 0) return &.{};
    const out = try alloc.alloc(relational_sql.RoutineSetting, source.len);
    var copied: usize = 0;
    errdefer {
        for (out[0..copied]) |*setting| setting.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |setting, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, setting.name),
            .values = &.{},
            .from_current = setting.from_current,
        };
        errdefer out[i].deinit(alloc);
        out[i].values = try cloneStringSliceAlloc(alloc, setting.values);
        copied += 1;
    }
    return out;
}

fn freeRoutineSettings(alloc: std.mem.Allocator, settings: []relational_sql.RoutineSetting) void {
    for (settings) |*setting| setting.deinit(alloc);
    if (settings.len > 0) alloc.free(settings);
}

fn cloneRoutineBodyAlloc(alloc: std.mem.Allocator, body: relational_sql.RoutineBodyPlan) !relational_sql.RoutineBodyPlan {
    const expression = if (body.expression) |value| try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, value) else null;
    errdefer if (expression) |value| runtime_schema.freeRelationalRowsExpression(alloc, value);
    return .{
        .kind = body.kind,
        .hook = body.hook,
        .expression = expression,
    };
}

const PersistedRoutineRecord = struct {
    version: u32 = 1,
    origin: RoutineOrigin = .catalog,
    kind: relational_sql.RoutineKind,
    name: []u8,
    argument_count: usize,
    returns_type: ?[]u8 = null,
    language: ?[]u8 = null,
    volatility: ?relational_sql.RoutineVolatility = null,
    security: ?relational_sql.RoutineSecurity = null,
    null_input: ?relational_sql.RoutineNullInput = null,
    parallel_safety: ?relational_sql.RoutineParallelSafety = null,
    leakproof: bool = false,
    window: bool = false,
    support_function: ?[]u8 = null,
    transform_types: [][]u8 = &.{},
    settings: []relational_sql.RoutineSetting = &.{},
    cost: ?[]u8 = null,
    rows: ?[]u8 = null,
    body: ?relational_sql.RoutineBodyPlan = null,
};

fn encodeRoutineRecordAlloc(alloc: std.mem.Allocator, record: RoutineRecord) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, PersistedRoutineRecord{
        .version = 1,
        .origin = record.origin,
        .kind = record.kind,
        .name = record.name,
        .argument_count = record.argument_count,
        .returns_type = record.returns_type,
        .language = record.language,
        .volatility = record.volatility,
        .security = record.security,
        .null_input = record.null_input,
        .parallel_safety = record.parallel_safety,
        .leakproof = record.leakproof,
        .window = record.window,
        .support_function = record.support_function,
        .transform_types = record.transform_types,
        .settings = record.settings,
        .cost = record.cost,
        .rows = record.rows,
        .body = record.body,
    }, .{ .emit_null_optional_fields = false });
}

fn clonePersistedRoutineRecordAlloc(alloc: std.mem.Allocator, persisted: PersistedRoutineRecord) !RoutineRecord {
    const name = try alloc.dupe(u8, persisted.name);
    errdefer alloc.free(name);
    const returns_type = if (persisted.returns_type) |value| try alloc.dupe(u8, value) else null;
    errdefer if (returns_type) |value| alloc.free(value);
    const language = if (persisted.language) |value| try alloc.dupe(u8, value) else null;
    errdefer if (language) |value| alloc.free(value);
    const support_function = if (persisted.support_function) |value| try alloc.dupe(u8, value) else null;
    errdefer if (support_function) |value| alloc.free(value);
    const transform_types = try cloneStringSliceAlloc(alloc, persisted.transform_types);
    errdefer freeOwnedStringSlice(alloc, transform_types);
    const settings = try cloneRoutineSettingsAlloc(alloc, persisted.settings);
    errdefer freeRoutineSettings(alloc, settings);
    const cost = if (persisted.cost) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cost) |value| alloc.free(value);
    const rows = if (persisted.rows) |value| try alloc.dupe(u8, value) else null;
    errdefer if (rows) |value| alloc.free(value);
    const body = if (persisted.body) |value| try cloneRoutineBodyAlloc(alloc, value) else null;
    errdefer if (body) |*value| value.deinit(alloc);
    return .{
        .origin = persisted.origin,
        .kind = persisted.kind,
        .name = name,
        .argument_count = persisted.argument_count,
        .returns_type = returns_type,
        .language = language,
        .volatility = persisted.volatility,
        .security = persisted.security,
        .null_input = persisted.null_input,
        .parallel_safety = persisted.parallel_safety,
        .leakproof = persisted.leakproof,
        .window = persisted.window,
        .support_function = support_function,
        .transform_types = transform_types,
        .settings = settings,
        .cost = cost,
        .rows = rows,
        .body = body,
    };
}

fn routineRecordKeyAlloc(
    alloc: std.mem.Allocator,
    kind: relational_sql.RoutineKind,
    name: []const u8,
    argument_count: usize,
) ![]u8 {
    const Sha256 = std.crypto.hash.sha2.Sha256;
    var hasher = Sha256.init(.{});
    hasher.update("antfly.sql.routine.catalog.v1\x00");
    hasher.update(@tagName(kind));
    hasher.update("\x00");
    var lower_buf: [256]u8 = undefined;
    if (name.len <= lower_buf.len) {
        for (name, 0..) |c, i| lower_buf[i] = std.ascii.toLower(c);
        hasher.update(lower_buf[0..name.len]);
    } else {
        const lower = try alloc.alloc(u8, name.len);
        defer alloc.free(lower);
        for (name, 0..) |c, i| lower[i] = std.ascii.toLower(c);
        hasher.update(lower);
    }
    hasher.update("\x00");
    var arity_buf: [32]u8 = undefined;
    const arity = try std.fmt.bufPrint(&arity_buf, "{d}", .{argument_count});
    hasher.update(arity);
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ routine_key_prefix, std.fmt.bytesToHex(digest, .lower) });
}

const routine_key_prefix = "__api_sql_routines__:";
const trigger_key_prefix = "__api_sql_triggers__:";

fn triggerRecordKeyAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    trigger_name: []const u8,
) ![]u8 {
    const Sha256 = std.crypto.hash.sha2.Sha256;
    var hasher = Sha256.init(.{});
    hasher.update("antfly.sql.trigger.catalog.v1\x00");
    try hashLowerIdentifier(alloc, &hasher, table_name);
    hasher.update("\x00");
    try hashLowerIdentifier(alloc, &hasher, trigger_name);
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ trigger_key_prefix, std.fmt.bytesToHex(digest, .lower) });
}

fn hashLowerIdentifier(alloc: std.mem.Allocator, hasher: anytype, value: []const u8) !void {
    var lower_buf: [256]u8 = undefined;
    if (value.len <= lower_buf.len) {
        for (value, 0..) |c, i| lower_buf[i] = std.ascii.toLower(c);
        hasher.update(lower_buf[0..value.len]);
    } else {
        const lower = try alloc.alloc(u8, value.len);
        defer alloc.free(lower);
        for (value, 0..) |c, i| lower[i] = std.ascii.toLower(c);
        hasher.update(lower);
    }
}

test "sql routine runtime stores and executes safe expression bodies" {
    const alloc = std.testing.allocator;
    var plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql AS 'SELECT lower($1)';",
    );
    defer plan.deinit(alloc);

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.apply(switch (plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectEqual(@as(usize, 1), runtime.routineCountForTest());

    const out = try runtime.executeExpressionRoutineAlloc(alloc, "normalize_status", "\"ACTIVE\"");
    defer alloc.free(out);
    try std.testing.expectEqualStrings("\"active\"", out);
}

test "sql routine runtime persists catalog routines across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-routines", .{tmp.sub_path});
    defer alloc.free(path);

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);

        var plan = try relational_sql.lowerDdlPlanAlloc(
            alloc,
            "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql AS 'SELECT lower($1)';",
        );
        defer plan.deinit(alloc);
        try runtime.apply(switch (plan) {
            .function_catalog => |function_plan| function_plan,
            else => return error.TestUnexpectedResult,
        });

        const bindings = [_]extension_domain.QueryFunctionBinding{.{
            .extension_name = "pgcrypto",
            .object_name = "gen_random_uuid",
            .sql_name = "gen_random_uuid",
            .native_expression = "uuid_v4",
            .native_expression_kind = .uuid_v4,
            .arity = 0,
        }};
        try runtime.replaceNativeQueryFunctionBindings(&bindings);
        try std.testing.expectEqual(@as(usize, 2), runtime.routineCountForTest());
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);
        try std.testing.expectEqual(@as(usize, 1), runtime.routineCountForTest());

        const out = try runtime.executeExpressionRoutineAlloc(alloc, "normalize_status", "\"ACTIVE\"");
        defer alloc.free(out);
        try std.testing.expectEqualStrings("\"active\"", out);
        try std.testing.expectError(error.RoutineNotFound, runtime.executeExpressionRoutineArgsAlloc(alloc, "gen_random_uuid", &.{}));

        var drop_plan = try relational_sql.lowerDdlPlanAlloc(alloc, "DROP FUNCTION normalize_status(text);");
        defer drop_plan.deinit(alloc);
        try runtime.apply(switch (drop_plan) {
            .function_catalog => |function_plan| function_plan,
            else => return error.TestUnexpectedResult,
        });
        try std.testing.expectEqual(@as(usize, 0), runtime.routineCountForTest());
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);
        try std.testing.expectEqual(@as(usize, 0), runtime.routineCountForTest());
    }
}

test "sql routine runtime fails closed on corrupt durable catalog records" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-routines-corrupt", .{tmp.sub_path});
    defer alloc.free(path);

    const opened = try alloc.create(OpenedStore);
    errdefer alloc.destroy(opened);
    opened.* = try OpenedStore.open(alloc, path);
    errdefer opened.deinit();

    const key = try routineRecordKeyAlloc(alloc, .function, "broken", 0);
    defer alloc.free(key);
    try opened.docstore.put(key, "{not-json");

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try std.testing.expectError(error.InvalidRoutineStore, runtime.attachOpenedStore(opened));

    opened.deinit();
    alloc.destroy(opened);
}

test "sql routine runtime executes bounded multi argument expression bodies" {
    const alloc = std.testing.allocator;
    var plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION add_amounts(numeric, numeric) RETURNS numeric LANGUAGE sql AS 'SELECT $1 + $2';",
    );
    defer plan.deinit(alloc);

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.apply(switch (plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const out = try runtime.executeExpressionRoutineArgsAlloc(alloc, "add_amounts", &.{ "2", "3.5" });
    defer alloc.free(out);
    try std.testing.expectEqualStrings("5.5", out);

    var concat_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION status_label(text, text) RETURNS text LANGUAGE sql AS 'SELECT concat_ws('' '', $1, $2)';",
    );
    defer concat_plan.deinit(alloc);
    try runtime.apply(switch (concat_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    const label = try runtime.executeExpressionRoutineArgsAlloc(alloc, "status_label", &.{ "\"tenant\"", "\"active\"" });
    defer alloc.free(label);
    try std.testing.expectEqualStrings("\"tenant active\"", label);
}

test "sql routine runtime exports expression routine bindings for SQL lowering" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    var normalize_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql AS 'SELECT lower($1)';",
    );
    defer normalize_plan.deinit(alloc);
    try runtime.apply(switch (normalize_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    var label_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION status_label(text, text) RETURNS text LANGUAGE sql AS 'SELECT concat_ws('' '', $1, $2)';",
    );
    defer label_plan.deinit(alloc);
    try runtime.apply(switch (label_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    var strict_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION strict_normalize_status(text) RETURNS text LANGUAGE sql STRICT AS 'SELECT lower($1)';",
    );
    defer strict_plan.deinit(alloc);
    try runtime.apply(switch (strict_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const bindings = try runtime.listExpressionRoutineBindingsAlloc(alloc);
    defer freeExpressionRoutineBindings(alloc, bindings);
    try std.testing.expectEqual(@as(usize, 3), bindings.len);

    var saw_normalize = false;
    var saw_label = false;
    var saw_strict = false;
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.sql_name, "normalize_status")) {
            saw_normalize = true;
            try std.testing.expectEqual(@as(u16, 1), binding.arity);
            try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.lower, binding.expression.kind);
            try std.testing.expect(binding.null_input == null);
        } else if (std.mem.eql(u8, binding.sql_name, "status_label")) {
            saw_label = true;
            try std.testing.expectEqual(@as(u16, 2), binding.arity);
            try std.testing.expectEqual(runtime_schema.RelationalRowsExpressionKind.concat_ws, binding.expression.kind);
        } else if (std.mem.eql(u8, binding.sql_name, "strict_normalize_status")) {
            saw_strict = true;
            try std.testing.expectEqual(@as(u16, 1), binding.arity);
            try std.testing.expectEqual(relational_sql.RoutineNullInput.returns_null, binding.null_input.?);
        }
    }
    try std.testing.expect(saw_normalize);
    try std.testing.expect(saw_label);
    try std.testing.expect(saw_strict);
}

test "sql routine runtime executes nested safe expression bodies" {
    const alloc = std.testing.allocator;
    var plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION status_label_nested(text, text) RETURNS text LANGUAGE sql AS 'SELECT concat_ws('':'', lower($1), coalesce($2, ''missing''))';",
    );
    defer plan.deinit(alloc);

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.apply(switch (plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const missing = try runtime.executeExpressionRoutineArgsAlloc(alloc, "status_label_nested", &.{ "\"TENANT\"", "null" });
    defer alloc.free(missing);
    try std.testing.expectEqualStrings("\"tenant:missing\"", missing);

    const active = try runtime.executeExpressionRoutineArgsAlloc(alloc, "status_label_nested", &.{ "\"TENANT\"", "\"active\"" });
    defer alloc.free(active);
    try std.testing.expectEqualStrings("\"tenant:active\"", active);

    var clamp_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION clamp_amount(numeric, numeric, numeric) RETURNS numeric LANGUAGE sql AS 'SELECT least(greatest($1, $2), $3)';",
    );
    defer clamp_plan.deinit(alloc);
    try runtime.apply(switch (clamp_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const clamped = try runtime.executeExpressionRoutineArgsAlloc(alloc, "clamp_amount", &.{ "5", "10", "8" });
    defer alloc.free(clamped);
    try std.testing.expectEqualStrings("8", clamped);
}

test "sql routine runtime executes native row expression bodies and rejects ambient fields" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    var redact_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION redact_digits(text) RETURNS text LANGUAGE sql AS 'SELECT regexp_replace($1, ''[0-9]'', ''#'', ''g'')';",
    );
    defer redact_plan.deinit(alloc);
    try runtime.apply(switch (redact_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const redacted = try runtime.executeExpressionRoutineAlloc(alloc, "redact_digits", "\"acct-123\"");
    defer alloc.free(redacted);
    try std.testing.expectEqualStrings("\"acct-###\"", redacted);

    var replace_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION normalize_dash(text) RETURNS text LANGUAGE sql AS 'SELECT replace($1::text, ''-'', ''_'')';",
    );
    defer replace_plan.deinit(alloc);
    try runtime.apply(switch (replace_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const normalized = try runtime.executeExpressionRoutineAlloc(alloc, "normalize_dash", "\"a-b-c\"");
    defer alloc.free(normalized);
    try std.testing.expectEqualStrings("\"a_b_c\"", normalized);

    var cast_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION cast_amount(text) RETURNS numeric LANGUAGE sql AS 'SELECT cast($1 AS numeric) + 1';",
    );
    defer cast_plan.deinit(alloc);
    try runtime.apply(switch (cast_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const casted = try runtime.executeExpressionRoutineAlloc(alloc, "cast_amount", "\"41\"");
    defer alloc.free(casted);
    try std.testing.expectEqualStrings("42", casted);

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        relational_sql.lowerDdlPlanAlloc(alloc, "CREATE FUNCTION missing_arg(text) RETURNS text LANGUAGE sql AS 'SELECT lower($2)';"),
    );
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        relational_sql.lowerDdlPlanAlloc(alloc, "CREATE FUNCTION ambient_field(text) RETURNS text LANGUAGE sql AS 'SELECT lower(status)';"),
    );

    var trigger_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';",
    );
    defer trigger_plan.deinit(alloc);
    try runtime.apply(switch (trigger_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectEqual(@as(usize, 4), runtime.routineCountForTest());
    try std.testing.expectError(error.RoutineBodyNotExecutable, runtime.executeExpressionRoutineArgsAlloc(alloc, "audit_body", &.{}));
    const trigger_new = try runtime.executeTriggerRoutineAlloc(
        alloc,
        "audit_body",
        "{\"id\":\"u1\",\"status\":\"updated\"}",
        "{\"id\":\"u1\",\"status\":\"old\"}",
    );
    defer alloc.free(trigger_new);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"updated\"}", trigger_new);
    try std.testing.expectError(
        error.RoutineTriggerTupleUnavailable,
        runtime.executeTriggerRoutineAlloc(alloc, "audit_body", null, "{\"id\":\"u1\"}"),
    );

    var old_trigger_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION old_audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END';",
    );
    defer old_trigger_plan.deinit(alloc);
    try runtime.apply(switch (old_trigger_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectEqual(@as(usize, 5), runtime.routineCountForTest());
    try std.testing.expectError(error.RoutineBodyNotExecutable, runtime.executeExpressionRoutineArgsAlloc(alloc, "old_audit_body", &.{}));
    const trigger_old = try runtime.executeTriggerRoutineAlloc(
        alloc,
        "old_audit_body",
        "{\"id\":\"u1\",\"status\":\"updated\"}",
        "{\"id\":\"u1\",\"status\":\"old\"}",
    );
    defer alloc.free(trigger_old);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"old\"}", trigger_old);
    try std.testing.expectError(
        error.InvalidRowsRequest,
        runtime.executeTriggerRoutineAlloc(alloc, "old_audit_body", "{\"id\":\"u1\"}", "null"),
    );

    var null_trigger_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION skip_audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END';",
    );
    defer null_trigger_plan.deinit(alloc);
    try runtime.apply(switch (null_trigger_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectEqual(@as(usize, 6), runtime.routineCountForTest());
    const trigger_null = try runtime.executeTriggerRoutineAlloc(
        alloc,
        "skip_audit_body",
        "{\"id\":\"u1\",\"status\":\"updated\"}",
        "{\"id\":\"u1\",\"status\":\"old\"}",
    );
    defer alloc.free(trigger_null);
    try std.testing.expectEqualStrings("null", trigger_null);

    var noop_procedure_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql AS 'BEGIN NULL; END';",
    );
    defer noop_procedure_plan.deinit(alloc);
    try runtime.apply(switch (noop_procedure_plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectEqual(@as(usize, 7), runtime.routineCountForTest());
    try std.testing.expectError(error.RoutineNotFound, runtime.executeExpressionRoutineArgsAlloc(alloc, "rotate_usage", &.{}));
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        relational_sql.lowerDdlPlanAlloc(alloc, "CREATE FUNCTION audit_notice_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RAISE NOTICE ''audit''; RETURN NEW; END';"),
    );
}

test "sql routine runtime persists safe row trigger catalog records" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sql-routine-trigger-catalog", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        errdefer opened.deinit();
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);

        var trigger_fn_plan = try relational_sql.lowerDdlPlanAlloc(
            alloc,
            "CREATE FUNCTION audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';",
        );
        defer trigger_fn_plan.deinit(alloc);
        try runtime.apply(switch (trigger_fn_plan) {
            .function_catalog => |function| function,
            else => return error.TestUnexpectedResult,
        });

        try std.testing.expect(try runtime.applyTriggerDdlAlloc(
            alloc,
            "CREATE TRIGGER audit_insert BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_body();",
        ));
        try std.testing.expectEqual(@as(usize, 1), runtime.triggerCountForTest());
        const function_name = (try runtime.triggerFunctionForTableEventAlloc(alloc, "usage_records", .insert)) orelse return error.TestUnexpectedResult;
        defer alloc.free(function_name);
        try std.testing.expectEqualStrings("audit_body", function_name);
        try std.testing.expectError(error.RoutineInUse, runtime.apply(switch (trigger_fn_plan) {
            .function_catalog => |function| .{ .drop = .{
                .kind = function.create.kind,
                .routine_name = function.create.routine_name,
                .argument_count = function.create.argument_count,
                .if_exists = false,
                .cascade = false,
            } },
            else => return error.TestUnexpectedResult,
        }));
    }

    {
        var opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        errdefer opened.deinit();
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);
        try std.testing.expectEqual(@as(usize, 1), runtime.routineCountForTest());
        try std.testing.expectEqual(@as(usize, 1), runtime.triggerCountForTest());
        try std.testing.expect(try runtime.applyTriggerDdlAlloc(
            alloc,
            "DROP TRIGGER audit_insert ON usage_records;",
        ));
        try std.testing.expectEqual(@as(usize, 0), runtime.triggerCountForTest());
    }

    {
        var opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        errdefer opened.deinit();
        var runtime = Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.attachOpenedStore(opened);
        try std.testing.expectEqual(@as(usize, 0), runtime.triggerCountForTest());
    }
}

test "sql routine runtime validates trigger ddl against executable trigger bodies" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    try std.testing.expect(!(try runtime.applyTriggerDdlAlloc(alloc, "CREATE TABLE usage_records (id text);")));
    try std.testing.expectError(error.RoutineNotFound, runtime.applyTriggerDdlAlloc(
        alloc,
        "CREATE TRIGGER audit_insert BEFORE INSERT ON usage_records EXECUTE FUNCTION missing_trigger();",
    ));

    var old_trigger_plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION old_audit_body() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END';",
    );
    defer old_trigger_plan.deinit(alloc);
    try runtime.apply(switch (old_trigger_plan) {
        .function_catalog => |function| function,
        else => return error.TestUnexpectedResult,
    });
    try std.testing.expectError(error.UnsupportedSqlShape, runtime.applyTriggerDdlAlloc(
        alloc,
        "CREATE TRIGGER audit_insert BEFORE INSERT ON usage_records EXECUTE FUNCTION old_audit_body();",
    ));
    try std.testing.expect(try runtime.applyTriggerDdlAlloc(
        alloc,
        "CREATE TRIGGER audit_update BEFORE UPDATE ON usage_records EXECUTE FUNCTION old_audit_body();",
    ));
    try std.testing.expectEqual(@as(usize, 1), runtime.triggerCountForTest());
    try std.testing.expectError(error.TriggerAlreadyExists, runtime.applyTriggerDdlAlloc(
        alloc,
        "CREATE TRIGGER audit_update BEFORE UPDATE ON usage_records EXECUTE FUNCTION old_audit_body();",
    ));
    try std.testing.expect(try runtime.applyTriggerDdlAlloc(
        alloc,
        "CREATE OR REPLACE TRIGGER audit_update BEFORE UPDATE ON usage_records EXECUTE FUNCTION old_audit_body();",
    ));
    try std.testing.expectError(error.TriggerNotFound, runtime.applyTriggerDdlAlloc(
        alloc,
        "DROP TRIGGER missing_trigger ON usage_records;",
    ));
    try std.testing.expect(try runtime.applyTriggerDdlAlloc(
        alloc,
        "DROP TRIGGER IF EXISTS missing_trigger ON usage_records;",
    ));
}

test "sql routine runtime replaces ready extension query function bindings" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    const bindings = [_]extension_domain.QueryFunctionBinding{.{
        .extension_name = "pgcrypto",
        .object_name = "gen_random_uuid",
        .sql_name = "gen_random_uuid",
        .native_expression = "uuid_v4",
        .native_expression_kind = .uuid_v4,
        .arity = 0,
    }};

    try runtime.replaceNativeQueryFunctionBindings(&bindings);
    try std.testing.expectEqual(@as(usize, 1), runtime.routineCountForTest());
    const generated = try runtime.executeExpressionRoutineArgsAlloc(alloc, "gen_random_uuid", &.{});
    defer alloc.free(generated);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .string => |value| try std.testing.expect(value.len > 0),
        else => return error.TestUnexpectedResult,
    }

    try runtime.replaceNativeQueryFunctionBindings(&.{});
    try std.testing.expectEqual(@as(usize, 0), runtime.routineCountForTest());
    try std.testing.expectError(error.RoutineNotFound, runtime.executeExpressionRoutineArgsAlloc(alloc, "gen_random_uuid", &.{}));
}

test "sql routine runtime applies returns-null null-input policy before execution" {
    const alloc = std.testing.allocator;
    var plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION strict_normalize_status(text) RETURNS text LANGUAGE sql STRICT AS 'SELECT lower($1)';",
    );
    defer plan.deinit(alloc);

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.apply(switch (plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    const out = try runtime.executeExpressionRoutineAlloc(alloc, "strict_normalize_status", "null");
    defer alloc.free(out);
    try std.testing.expectEqualStrings("null", out);
}

test "sql routine runtime preserves typed catalog metadata" {
    const alloc = std.testing.allocator;
    var plan = try relational_sql.lowerDdlPlanAlloc(
        alloc,
        "CREATE FUNCTION normalize_status(text) RETURNS text LANGUAGE sql IMMUTABLE SECURITY DEFINER PARALLEL SAFE LEAKPROOF COST 3 SET search_path TO public AS 'SELECT lower($1)';",
    );
    defer plan.deinit(alloc);

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.apply(switch (plan) {
        .function_catalog => |function_plan| function_plan,
        else => return error.TestUnexpectedResult,
    });

    try std.testing.expectEqual(@as(usize, 1), runtime.routines.items.len);
    const routine = runtime.routines.items[0];
    try std.testing.expectEqual(relational_sql.RoutineVolatility.immutable, routine.volatility.?);
    try std.testing.expectEqual(relational_sql.RoutineSecurity.definer, routine.security.?);
    try std.testing.expectEqual(relational_sql.RoutineParallelSafety.safe, routine.parallel_safety.?);
    try std.testing.expect(routine.leakproof);
    try std.testing.expectEqualStrings("3", routine.cost.?);
    try std.testing.expectEqual(@as(usize, 1), routine.settings.len);
    try std.testing.expectEqualStrings("search_path", routine.settings[0].name);
    try std.testing.expectEqual(@as(usize, 1), routine.settings[0].values.len);
    try std.testing.expectEqualStrings("public", routine.settings[0].values[0]);
}
