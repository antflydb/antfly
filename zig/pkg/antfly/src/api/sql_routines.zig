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
const relational_rows = @import("relational_rows.zig");
const relational_sql = @import("relational_sql.zig");
const runtime_schema = @import("../storage/schema.zig");

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

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    mutex: SpinMutex = .{},
    routines: std.ArrayListUnmanaged(RoutineRecord) = .empty,

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.routines.items) |*routine| routine.deinit(self.alloc);
        self.routines.deinit(self.alloc);
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
        const row_json = try routineArgumentObjectJsonAlloc(alloc, argument_json, routine.null_input);
        defer alloc.free(row_json);
        if (std.mem.eql(u8, row_json, "null")) return try alloc.dupe(u8, "null");
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
        defer parsed.deinit();
        return try relational_rows.expressionValueJsonAlloc(alloc, parsed.value, body.expression);
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
            if (routine.argument_count > std.math.maxInt(u16)) return error.UnsupportedSqlShape;

            const sql_name = try alloc.dupe(u8, routine.name);
            errdefer alloc.free(sql_name);
            const expression = try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, body.expression);
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
        if (plan.kind == .procedure and plan.body != null) return error.UnsupportedSqlShape;
        if (plan.body) |body| {
            if (body.kind != .sql_expression or body.hook != .expression) return error.UnsupportedSqlShape;
        }
        if (self.findRoutineIndexLocked(plan.kind, plan.routine_name, plan.argument_count)) |existing| {
            if (!plan.replace_existing) return error.RoutineAlreadyExists;
            var removed = self.routines.orderedRemove(existing);
            removed.deinit(self.alloc);
        }
        var record = try cloneCreateRoutineRecordAlloc(self.alloc, plan);
        errdefer record.deinit(self.alloc);
        try self.routines.append(self.alloc, record);
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
        if (self.findRoutineIndexLocked(plan.kind, plan.routine_name, plan.argument_count)) |existing| {
            var removed = self.routines.orderedRemove(existing);
            removed.deinit(self.alloc);
            return;
        }
        if (!plan.if_exists) return error.RoutineNotFound;
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
    return .{
        .kind = body.kind,
        .hook = body.hook,
        .expression = try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, body.expression),
    };
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
