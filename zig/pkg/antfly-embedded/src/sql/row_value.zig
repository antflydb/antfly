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

//! Logical durable-expression operands, independent of ordered index keys.
//! Payloads borrow a pinned row/schema owner or the invocation's owned region.
//! JSON is an ingress/output representation, never a source of SQL identity.
const std = @import("std");
const layout = @import("../common/sql_array_layout.zig");
const json_order = @import("json_order.zig");
const Context = @import("numeric_value.zig").Context;

/// A present typed JSON value, including JSON null. SQL NULL is represented by
/// Value.null, never by this payload. Trees borrow a pinned ingress/schema/row
/// owner; neither strings nor exact numeric tokens acquire SQL scalar identity.
pub const Json = struct {
    value: std.json.Value,

    pub fn extract(self: Json, key: []const u8, work: *json_order.Budget) !?Json {
        return if (try @import("json_path.zig").extract(self.value, key, work)) |value| .{ .value = value } else null;
    }

    /// Materialize an independently owned tree in a caller-owned region.
    /// Partial allocations belong to that region on failure. Unlike decoding,
    /// no input token or container may remain attached to the pinned source.
    pub fn cloneLeaky(self: Json, alloc: std.mem.Allocator, work: *json_order.Budget) !Json {
        return .{ .value = try cloneNode(self.value, alloc, work, 0) };
    }

    fn cloneNode(value: std.json.Value, alloc: std.mem.Allocator, work: *json_order.Budget, depth: usize) anyerror!std.json.Value {
        try work.consume(1);
        if (depth > 64) return if (work.shared) |context| context.limit() else error.SqlProgramLimitExceeded;
        return switch (value) {
            inline .string, .number_string => |bytes, tag| blk: {
                try work.consume(bytes.len);
                break :blk @unionInit(std.json.Value, @tagName(tag), try alloc.dupe(u8, bytes));
            },
            .array => |items| blk: {
                var output = std.json.Array.init(alloc);
                try output.ensureTotalCapacity(items.items.len);
                for (items.items) |item| output.appendAssumeCapacity(try cloneNode(item, alloc, work, depth + 1));
                break :blk .{ .array = output };
            },
            .object => |items| blk: {
                var output: std.json.ObjectMap = .empty;
                try output.ensureTotalCapacity(alloc, @intCast(items.count()));
                for (items.keys(), items.values()) |key, item| {
                    try work.consume(key.len);
                    try output.put(alloc, try alloc.dupe(u8, key), try cloneNode(item, alloc, work, depth + 1));
                }
                break :blk .{ .object = output };
            },
            else => value,
        };
    }
};

/// Lazy decoded JSON cache for exactly one pinned physical row. Initialize at
/// its final address: the arena points at memory, and all consumers must finish
/// before resetRow/deinit or advancing the source row. No schema-width vector,
/// stringify/parse round trip, or per-expression source decoding is required.
pub const JsonRow = struct {
    memory: @import("memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    context: *Context,
    entries: std.AutoHashMapUnmanaged(u32, Entry),
    decodes: usize,
    admission: ?@import("memory_budget.zig").RegionAdmission = null,

    const Entry = struct { bytes: []const u8, json: *const Json };

    /// Backing must reclaim freed arena buffers. Request/transaction arenas
    /// use initRegion so row resets cannot refund retained parent capacity.
    pub fn init(self: *JsonRow, backing: std.mem.Allocator, limit: usize, context: *Context) void {
        self.* = .{
            .memory = .{ .backing = backing, .limit = limit },
            .arena = undefined,
            .context = context,
            .entries = .empty,
            .decodes = 0,
        };
        self.arena = .init(self.memory.allocator());
    }

    pub fn initRegion(self: *JsonRow, backing: std.mem.Allocator, limit: usize, context: *Context) void {
        self.init(backing, limit, context);
        self.memory.monotonic = true;
    }

    pub fn bindAdmission(self: *JsonRow, remaining: *usize) !void {
        try self.context.charge(0);
        if (self.admission != null) return error.InvalidSqlProgram;
        self.admission = @import("memory_budget.zig").RegionAdmission.init(&self.memory, remaining) catch return self.context.limit();
    }

    pub fn deinit(self: *JsonRow) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Invalidate every borrowed result before advancing the physical source.
    /// Retained capacity stays charged; row changes never reset CPU admission,
    /// sticky cancellation, or the maximum live decode-memory boundary.
    pub fn resetRow(self: *JsonRow) !void {
        try self.context.charge(0);
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        if (!self.arena.reset(.retain_capacity)) return self.failure(error.OutOfMemory);
        self.entries = .empty;
        self.decodes = 0;
    }

    /// Source bytes must belong to an authenticated, canonical physical JSON
    /// cell and remain immutable until row retirement. This bounded decoder
    /// does not replace physical validation or infer schema/NULL authority.
    pub fn decodePinned(self: *JsonRow, ordinal: u32, bytes: []const u8) !*const Json {
        try self.context.charge(1);
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        if (self.entries.get(ordinal)) |entry| {
            // An ordinal alone is not source authority. Changing a source
            // buffer without a row reset must not return a previous row's tree.
            if (entry.bytes.ptr != bytes.ptr or entry.bytes.len != bytes.len) return error.InvalidSqlProgram;
            return entry.json;
        }
        if (bytes.len > self.context.max_input_bytes) return self.context.limit();
        var work: json_order.Budget = .{ .shared = self.context };
        const value = json_order.parsePinnedTextLeaky(self.arena.allocator(), bytes, &work) catch |err| return self.failure(err);
        json_order.validateTextDomain(value, &work, 0) catch |err| return self.failure(err);
        const json = self.arena.allocator().create(Json) catch |err| return self.failure(err);
        json.* = .{ .value = value };
        self.entries.put(self.arena.allocator(), ordinal, .{ .bytes = bytes, .json = json }) catch |err| return self.failure(err);
        self.decodes += 1;
        return json;
    }

    /// Pin one already parsed, size-admitted typed input without reparsing or
    /// cloning its tree. Own the root header so root-map relocation/replacement
    /// cannot invalidate it; payloads borrow the immutable input region.
    pub fn pinValue(self: *JsonRow, value: std.json.Value) !*const Json {
        try self.context.charge(1);
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        var work: json_order.Budget = .{ .shared = self.context };
        json_order.validateTextDomain(value, &work, 0) catch |err| return self.failure(err);
        const root = self.arena.allocator().create(Json) catch |err| return self.failure(err);
        root.* = .{ .value = value };
        return root;
    }

    fn failure(self: *JsonRow, err: anyerror) anyerror {
        if (err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) return self.context.limit();
        return err;
    }
};

/// Row-local root JSON-null provenance. It is a set, not a substitute for
/// schema/physical validation: the preparer must authenticate every submitted
/// marker against its pinned schema and actual root datum before importing.
/// Initialize at its final address; snapshots borrow immutable owned keys and
/// must not escape this owner's lifetime.
pub const JsonNulls = struct {
    memory: @import("memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    context: *Context,
    fields: std.StringHashMapUnmanaged(void),
    admission: ?@import("memory_budget.zig").RegionAdmission = null,

    pub fn init(self: *JsonNulls, backing: std.mem.Allocator, limit: usize, context: *Context) void {
        self.* = .{
            .memory = .{ .backing = backing, .limit = limit },
            .arena = undefined,
            .context = context,
            .fields = .empty,
        };
        self.arena = .init(self.memory.allocator());
    }

    /// Parent arenas retain freed buffers; admission must not refund them.
    pub fn initRegion(self: *JsonNulls, backing: std.mem.Allocator, limit: usize, context: *Context) void {
        self.init(backing, limit, context);
        self.memory.monotonic = true;
    }

    pub fn bindAdmission(self: *JsonNulls, remaining: *usize) !void {
        try self.context.charge(0);
        if (self.admission != null) return error.InvalidSqlProgram;
        self.admission = @import("memory_budget.zig").RegionAdmission.init(&self.memory, remaining) catch return self.context.limit();
    }

    pub fn deinit(self: *JsonNulls) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Import authenticated source markers exactly once. Input names can be
    /// released immediately; the row owner never borrows caller metadata.
    pub fn import(self: *JsonNulls, names: []const []const u8) !void {
        try self.context.charge(names.len);
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        for (names) |name| {
            try self.context.charge(name.len);
            if (self.fields.contains(name)) return error.InvalidSqlProgram;
            try self.insertOwned(name);
        }
    }

    pub fn contains(self: *JsonNulls, name: []const u8) !bool {
        try self.context.charge(1);
        try self.context.charge(name.len);
        return self.fields.contains(name);
    }

    /// Only the typed result determines derived null semantics. A generated
    /// SQL NULL or nonnull JSON value removes any submitted marker; present
    /// JSON null adds it. Missing/default-skipped fields must not call this.
    pub fn assign(self: *JsonNulls, name: []const u8, value: Value) !void {
        try self.context.charge(1);
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        try self.context.charge(name.len);
        if (value != .null and value != .json) return error.InvalidSqlProgram;
        if (value == .json and value.json.value == .null) {
            if (!self.fields.contains(name)) try self.insertOwned(name);
        } else _ = self.fields.remove(name);
    }

    /// The returned set snapshot belongs to this row owner. Order is not
    /// semantic; physical encoding still uses immutable schema ordinals.
    pub fn snapshot(self: *JsonNulls) ![]const []const u8 {
        try self.context.charge(self.fields.count());
        if (self.admission) |*admission| admission.begin();
        defer if (self.admission) |*admission| admission.finish();
        if (self.fields.count() == 0) return &.{};
        const names = self.arena.allocator().alloc([]const u8, self.fields.count()) catch |err| return self.failure(err);
        var keys = self.fields.keyIterator();
        for (names) |*name| name.* = keys.next().?.*;
        return names;
    }

    fn insertOwned(self: *JsonNulls, name: []const u8) !void {
        // Clone first: allocation failure after map insertion must not publish
        // an uninitialized key into an owner that may be inspected or retried.
        const owned = self.arena.allocator().dupe(u8, name) catch |err| return self.failure(err);
        self.fields.put(self.arena.allocator(), owned, {}) catch |err| return self.failure(err);
    }

    fn failure(self: *JsonNulls, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.memory.isExhausted()) self.context.limit() else err;
    }
};

pub const Array = struct {
    element_type: layout.Kind,
    bytes: []const u8,

    /// Only canonical bytes admitted by the owning codec and protected by a
    /// pinned owner may enter. This extent check does not authenticate input.
    pub fn view(self: Array) !layout.View {
        return layout.View.openAuthenticated(self.element_type, self.bytes, .{});
    }
};

pub const Value = union(enum) {
    null,
    string: []const u8,
    blob: []const u8,
    boolean: bool,
    datetime: i128,
    integer: i64,
    number: f64,
    numeric: []const u8,
    sql_array: Array,
    /// A sparse owner allocates a JSON root only when actually referenced.
    /// Keep DOM container metadata out of every ordinary logical value slot.
    json: *const Json,

    /// Explicit transport-neutral scalar adaptation; never parse a value or
    /// infer its SQL type. The source supplies the exact scalar tag/payload.
    pub fn fromScalar(value: anytype) Value {
        return switch (value) {
            inline else => |payload, tag| @unionInit(Value, @tagName(tag), payload),
        };
    }

    /// Ordered-key domains remain independently versioned and scalar-only.
    /// Adding an expression value must not silently widen persistent indexes.
    pub fn scalar(self: Value, comptime Scalar: type) !Scalar {
        return switch (self) {
            .sql_array, .json => error.UnsupportedRelationalValueType,
            inline else => |payload, tag| @unionInit(Scalar, @tagName(tag), payload),
        };
    }
};

test "SQL JSON pinned DOM roots own headers without reparsing or cloning admitted input" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":[9007199254740993.2500,null]}", .{ .parse_numbers = false });
    defer parsed.deinit();
    var context: Context = .{ .alloc = a };
    var bytes: usize = 1024 * 1024;
    var owner: JsonRow = undefined;
    owner.init(a, bytes, &context);
    defer owner.deinit();
    try owner.bindAdmission(&bytes);
    const input = parsed.value.object.get("a").?.array.items[0].number_string;
    const pinned = try owner.pinValue(parsed.value);
    parsed.value = .null;
    const output = pinned.value.object.get("a").?.array.items[0].number_string;
    try std.testing.expectEqualStrings("9007199254740993.2500", output);
    try std.testing.expectEqual(@intFromPtr(input.ptr), @intFromPtr(output.ptr));
    try std.testing.expectEqual(@as(usize, 0), owner.decodes);
    try std.testing.expectEqual(@as(usize, 1024 * 1024) - bytes, owner.memory.footprint());
    const retained = owner.memory.footprint();
    try std.testing.expectError(error.SqlTypeMismatch, owner.pinValue(.{ .string = "\x00" }));
    try std.testing.expectEqual(retained, owner.memory.footprint());
    const Run = struct {
        fn run(alloc: std.mem.Allocator, source: std.json.Value) !void {
            var scope: Context = .{ .alloc = alloc };
            var roots: JsonRow = undefined;
            roots.init(alloc, 1024 * 1024, &scope);
            defer roots.deinit();
            const root = try roots.pinValue(source);
            try std.testing.expectEqual(@as(usize, 2), root.value.object.get("a").?.array.items.len);
        }
    };
    try Run.run(a, pinned.value);
    var stable = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{pinned.value});
}

test "SQL JSON owners share actual retained region admission without double charging or renewal" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var context: Context = .{ .alloc = a };
    var bytes: usize = 1024 * 1024;
    var row: JsonRow = undefined;
    row.initRegion(region.allocator(), bytes, &context);
    defer row.deinit();
    var nulls: JsonNulls = undefined;
    nulls.initRegion(region.allocator(), bytes, &context);
    defer nulls.deinit();
    // A single prepaid buffer lets retain_capacity reset reuse storage without
    // needing temporary headroom to consolidate several arena buffers.
    _ = try row.arena.allocator().alloc(u8, 8192);
    try row.bindAdmission(&bytes);
    try nulls.bindAdmission(&bytes);
    try std.testing.expectError(error.InvalidSqlProgram, row.bindAdmission(&bytes));
    try std.testing.expectError(error.InvalidSqlProgram, nulls.bindAdmission(&bytes));
    const source = "{\"a\":null}";
    _ = try row.decodePinned(0, source);
    try nulls.import(&.{"a"});
    try std.testing.expectEqual(@as(usize, 1024 * 1024) - bytes, row.memory.footprint() + nulls.memory.footprint());
    const retained = bytes;
    _ = try row.decodePinned(0, source);
    _ = try nulls.contains("a");
    try std.testing.expectEqual(retained, bytes);
    // Other programs may consume all unreserved headroom. Already admitted
    // capacity remains usable but neither region can obtain a fresh allowance.
    bytes = 0;
    try row.resetRow();
    _ = try row.decodePinned(1, source);
    try std.testing.expectEqual(@as(usize, 0), bytes);
    const wide: [16384]u8 = @splat('x');
    try std.testing.expectError(error.SqlProgramLimitExceeded, nulls.import(&.{&wide}));
    bytes = 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, row.decodePinned(1, source));
    try std.testing.expectError(error.SqlProgramLimitExceeded, nulls.snapshot());
}

test "SQL JSON late admission binds existing capacity once and refuses insufficient headroom" {
    const a = std.testing.allocator;
    var context: Context = .{ .alloc = a };
    var nulls: JsonNulls = undefined;
    nulls.init(a, 1024 * 1024, &context);
    defer nulls.deinit();
    try nulls.import(&.{"field"});
    const retained = nulls.memory.footprint();
    var bytes = retained;
    try nulls.bindAdmission(&bytes);
    try std.testing.expectEqual(@as(usize, 0), bytes);
    try std.testing.expect(try nulls.contains("field"));
    var second_context: Context = .{ .alloc = a };
    var row: JsonRow = undefined;
    row.init(a, 1024 * 1024, &second_context);
    defer row.deinit();
    _ = try row.decodePinned(0, "{}");
    var insufficient = row.memory.footprint() - 1;
    const before = insufficient;
    try std.testing.expectError(error.SqlProgramLimitExceeded, row.bindAdmission(&insufficient));
    try std.testing.expectEqual(before, insufficient);
    insufficient = 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, row.bindAdmission(&insufficient));
}

test "SQL JSON null provenance owns root names and derived typed results replace submitted markers" {
    const a = std.testing.allocator;
    var context: Context = .{ .alloc = a };
    var nulls: JsonNulls = undefined;
    nulls.init(a, 1024 * 1024, &context);
    defer nulls.deinit();
    var source = [_]u8{ 'b', 'a', 's', 'e' };
    try nulls.import(&.{&source});
    @memset(&source, 'x');
    try std.testing.expect(try nulls.contains("base"));
    try std.testing.expect(!try nulls.contains("xxxx"));
    try std.testing.expectError(error.InvalidSqlProgram, nulls.import(&.{"base"}));
    const json_null: Json = .{ .value = .null };
    try nulls.assign("generated", .{ .json = &json_null });
    try std.testing.expect(try nulls.contains("generated"));
    try nulls.assign("base", .null);
    try std.testing.expect(!try nulls.contains("base"));
    const nonnull: Json = .{ .value = .{ .object = .empty } };
    try nulls.assign("generated", .{ .json = &nonnull });
    try std.testing.expectEqual(@as(usize, 0), (try nulls.snapshot()).len);
    try nulls.assign("generated", .{ .json = &json_null });
    try nulls.assign("generated", .{ .json = &json_null });
    const names = try nulls.snapshot();
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("generated", names[0]);
    try std.testing.expectError(error.InvalidSqlProgram, nulls.assign("generated", .{ .integer = 1 }));
    try std.testing.expect(try nulls.contains("generated"));
    // Removing a marker cannot refund its retained arena/key capacity.
    const retained = nulls.memory.footprint();
    try nulls.assign("generated", .null);
    try std.testing.expectEqual(retained, nulls.memory.footprint());
    try std.testing.expectEqualStrings("generated", names[0]);
}

test "SQL JSON null provenance unwinds every allocation fault and retains sticky memory admission" {
    const Run = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var context: Context = .{ .alloc = alloc };
            var nulls: JsonNulls = undefined;
            nulls.init(alloc, 1024 * 1024, &context);
            defer nulls.deinit();
            const wide: [1024]u8 = @splat('x');
            try nulls.import(&.{ "a", "b", "c", &wide, "e", "f", "g", "h", "i" });
            const value: Json = .{ .value = .null };
            try nulls.assign("derived", .{ .json = &value });
            try nulls.assign("a", .null);
            try std.testing.expectEqual(@as(usize, 9), (try nulls.snapshot()).len);
            try std.testing.expect(!try nulls.contains("a"));
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    var context: Context = .{ .alloc = region.allocator() };
    var limited: JsonNulls = undefined;
    limited.initRegion(region.allocator(), 1, &context);
    defer limited.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.import(&.{"field"}));
    limited.memory.limit = 1024 * 1024;
    context.remaining = 8 * 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.import(&.{"field"}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.snapshot());
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.contains("field"));
}

test "SQL JSON null provenance shares every observed cancellation cut and work admission" {
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn poll(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const Run = struct {
        fn run(owner: *JsonNulls) !void {
            const wide: [1024]u8 = @splat('x');
            try owner.import(&.{ "base", &wide });
            const value: Json = .{ .value = .null };
            try owner.assign("derived", .{ .json = &value });
            _ = try owner.contains(&wide);
            _ = try owner.snapshot();
        }
    };
    const a = std.testing.allocator;
    var control: Control = .{};
    var context: Context = .{ .alloc = a, .checkpoint = Control.poll, .ptr = &control };
    var baseline: JsonNulls = undefined;
    baseline.init(a, 1024 * 1024, &context);
    defer baseline.deinit();
    try Run.run(&baseline);
    try std.testing.expect(control.polls >= 3);
    const work = 8 * 1024 * 1024 - context.remaining;
    for (1..control.polls + 1) |cut| {
        var canceled: Control = .{ .fail_at = cut };
        var scope: Context = .{ .alloc = a, .checkpoint = Control.poll, .ptr = &canceled };
        var owner: JsonNulls = undefined;
        owner.init(a, 1024 * 1024, &scope);
        defer owner.deinit();
        try std.testing.expectError(error.QueryCanceled, Run.run(&owner));
        scope.checkpoint = null;
        try std.testing.expectError(error.QueryCanceled, owner.snapshot());
    }
    var scope: Context = .{ .alloc = a, .remaining = work - 1 };
    var owner: JsonNulls = undefined;
    owner.init(a, 1024 * 1024, &scope);
    defer owner.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, Run.run(&owner));
    scope.remaining = work;
    try std.testing.expectError(error.SqlProgramLimitExceeded, owner.snapshot());
}

test "SQL row values preserve exact scalar operands across explicit key projection" {
    const Scalar = union(enum) {
        null,
        string: []const u8,
        blob: []const u8,
        boolean: bool,
        datetime: i128,
        integer: i64,
        number: f64,
        numeric: []const u8,
    };
    for ([_]Scalar{
        .null,
        .{ .string = "text" },
        .{ .blob = &.{ 0, 255 } },
        .{ .boolean = true },
        .{ .datetime = std.math.minInt(i128) },
        .{ .integer = std.math.maxInt(i64) },
        .{ .number = -0.0 },
        .{ .numeric = &.{ 1, 2, 3, 4 } },
    }) |value| try std.testing.expectEqualDeep(value, try Value.fromScalar(value).scalar(Scalar));
    var bytes: [layout.header_size]u8 = @splat(0);
    bytes[0] = layout.version;
    const array: Value = .{ .sql_array = .{ .element_type = .int64, .bytes = &bytes } };
    try std.testing.expectEqual(@as(u32, 0), (try array.sql_array.view()).count);
    try std.testing.expectError(error.UnsupportedRelationalValueType, array.scalar(Scalar));
    const json: Value = .{ .json = &.{ .value = .null } };
    try std.testing.expect(json != .null);
    try std.testing.expectEqual(std.json.Value.null, json.json.value);
    try std.testing.expectError(error.UnsupportedRelationalValueType, json.scalar(Scalar));
    const BeforeJson = union(enum) {
        null,
        string: []const u8,
        blob: []const u8,
        boolean: bool,
        datetime: i128,
        integer: i64,
        number: f64,
        numeric: []const u8,
        sql_array: Array,
    };
    try std.testing.expectEqual(@sizeOf(BeforeJson), @sizeOf(Value));
}

test "SQL row values JSON cache decodes once borrows exact tokens and resets source identity" {
    const a = std.testing.allocator;
    var context: Context = .{ .alloc = a };
    var row: JsonRow = undefined;
    row.init(a, 1024 * 1024, &context);
    defer row.deinit();
    const source = "{\"source\":\"escaped\\ntext\",\"n\":9007199254740993.2500,\"nil\":null}";
    const operand = try row.decodePinned(std.math.maxInt(u32), source);
    var work: json_order.Budget = .{ .shared = &context };
    const number = (try operand.extract("n", &work)).?;
    try std.testing.expectEqualStrings("9007199254740993.2500", number.value.number_string);
    const offset = std.mem.indexOf(u8, source, number.value.number_string).?;
    try std.testing.expectEqual(@intFromPtr(source.ptr + offset), @intFromPtr(number.value.number_string.ptr));
    try std.testing.expectEqualStrings("escaped\ntext", (try operand.extract("source", &work)).?.value.string);
    try std.testing.expectEqual(std.json.Value.null, (try operand.extract("nil", &work)).?.value);
    try std.testing.expect((try operand.extract("missing", &work)) == null);
    const live = row.memory.live;
    row.memory.limit = live; // Cache hits cannot obtain additional memory.
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |_| {
        const cached = try row.decodePinned(std.math.maxInt(u32), source);
        try std.testing.expectEqual(operand.value.object.values().ptr, cached.value.object.values().ptr);
    }
    std.debug.print("JSON row cache: reads=10000 decodes=1 cached_allocated_bytes=0 retained_bytes={} elapsed_ns={}\n", .{ live, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
    try std.testing.expectEqual(@as(usize, 1), row.decodes);
    try std.testing.expectEqual(live, row.memory.live);
    try std.testing.expectEqual(@as(usize, 1), row.entries.count());
    try std.testing.expectError(error.InvalidSqlProgram, row.decodePinned(std.math.maxInt(u32), "{\"source\":\"next\"}"));
    const before = context.remaining;
    try row.resetRow();
    try std.testing.expectEqual(before, context.remaining);
    const next = try row.decodePinned(std.math.maxInt(u32), "{\"source\":\"next\"}");
    try std.testing.expectEqualStrings("next", (try next.extract("source", &work)).?.value.string);
    try std.testing.expectEqual(@as(usize, 1), row.decodes);
}

test "SQL row values JSON cache unwinds every allocation cut and bounds live row memory" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(backing: std.mem.Allocator) !void {
            var context: Context = .{ .alloc = backing };
            var row: JsonRow = undefined;
            row.init(backing, 1024 * 1024, &context);
            defer row.deinit();
            const source = "{\"\\u0078\":[1,null,true,{},[\"a\\tb\"]]}";
            for (0..4) |_| {
                const operand = try row.decodePinned(7, source);
                try std.testing.expect(operand.value.object.contains("x"));
                _ = try row.decodePinned(7, source);
                try std.testing.expectEqual(@as(usize, 1), row.decodes);
                try row.resetRow();
            }
        }
    };
    try Run.run(a);
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Run.run, .{});
    var context: Context = .{ .alloc = a };
    var limited: JsonRow = undefined;
    limited.init(a, 0, &context);
    defer limited.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.decodePinned(1, "[1]"));
    limited.memory.limit = 1024 * 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.decodePinned(1, "[1]"));
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.resetRow());
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var region_context: Context = .{ .alloc = region.allocator() };
    var region_row: JsonRow = undefined;
    region_row.initRegion(region.allocator(), 1024 * 1024, &region_context);
    defer region_row.deinit();
    _ = try region_row.decodePinned(1, "[1,2,3]");
    const spent = region_row.memory.footprint();
    try region_row.resetRow();
    try std.testing.expect(region_row.memory.monotonic);
    try std.testing.expect(region_row.memory.footprint() >= spent);
    _ = try region_row.decodePinned(1, "[4]");
    try std.testing.expect(region_row.memory.footprint() >= spent);
}

test "SQL row values JSON cache retains cancellation and rejects malformed source before publication" {
    const a = std.testing.allocator;
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const wide: [1024]u8 = @splat('x');
    var source_buffer: [1100]u8 = undefined;
    const source = try std.fmt.bufPrint(&source_buffer, "{{\"source\":\"{s}\",\"array\":[1,null,true]}}", .{&wide});
    var observed: Control = .{};
    var context: Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &observed };
    var baseline: JsonRow = undefined;
    baseline.init(a, 1024 * 1024, &context);
    defer baseline.deinit();
    _ = try baseline.decodePinned(1, source);
    for (1..observed.polls + 1) |cut| {
        var control: Control = .{ .fail_at = cut };
        var canceled: Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &control };
        var row: JsonRow = undefined;
        row.init(a, 1024 * 1024, &canceled);
        defer row.deinit();
        try std.testing.expectError(error.QueryCanceled, row.decodePinned(1, source));
        try std.testing.expectError(error.QueryCanceled, row.decodePinned(1, source));
        try std.testing.expectError(error.QueryCanceled, row.resetRow());
        try std.testing.expectEqual(@as(usize, 0), row.entries.count());
    }
    for ([_][]const u8{ "{", "[1,]", "{\"x\":1,\"x\":2}", "\"\\u0000\"", "\"bad\\xescape\"" }) |invalid| {
        var fresh: Context = .{ .alloc = a };
        var row: JsonRow = undefined;
        row.init(a, 1024 * 1024, &fresh);
        defer row.deinit();
        _ = row.decodePinned(1, invalid) catch |err| {
            try std.testing.expect(err == error.SqlInvalidTextRepresentation or err == error.SqlTypeMismatch);
            try std.testing.expectEqual(@as(usize, 0), row.entries.count());
            continue;
        };
        return error.TestExpectedError;
    }
    var fresh: Context = .{ .alloc = a, .max_input_bytes = 1 };
    var row: JsonRow = undefined;
    row.init(a, 1024 * 1024, &fresh);
    defer row.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, row.decodePinned(1, "null"));
    fresh.max_input_bytes = 1024;
    try std.testing.expectError(error.SqlProgramLimitExceeded, row.decodePinned(1, "null"));
    var nested: [131]u8 = undefined;
    @memset(nested[0..65], '[');
    nested[65] = '0';
    @memset(nested[66..], ']');
    var depth_context: Context = .{ .alloc = a };
    var depth_row: JsonRow = undefined;
    depth_row.init(a, 1024 * 1024, &depth_context);
    defer depth_row.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, depth_row.decodePinned(0, &nested));
    try std.testing.expectEqual(@as(usize, 0), depth_row.entries.count());
    try std.testing.expectError(error.SqlProgramLimitExceeded, depth_row.decodePinned(0, "null"));
}

test "SQL row values JSON clone owns every token after its source is retired and unwinds faults" {
    const a = std.testing.allocator;
    const Run = struct {
        fn run(backing: std.mem.Allocator) !void {
            var output = std.heap.ArenaAllocator.init(backing);
            defer output.deinit();
            var context: Context = .{ .alloc = backing };
            var work: json_order.Budget = .{ .shared = &context };
            const cloned = blk: {
                var row: JsonRow = undefined;
                row.init(backing, 1024 * 1024, &context);
                defer row.deinit();
                const source = try backing.dupe(u8, "{\"s\":\"escaped\\ntext\",\"n\":9007199254740993.2500,\"a\":[null,{\"x\":\"plain\"}]}");
                defer backing.free(source);
                const input = try row.decodePinned(0, source);
                const result = try input.cloneLeaky(output.allocator(), &work);
                try std.testing.expect(input.value.object.values().ptr != result.value.object.values().ptr);
                try std.testing.expect(input.value.object.get("n").?.number_string.ptr != result.value.object.get("n").?.number_string.ptr);
                break :blk result;
            };
            try std.testing.expectEqualStrings("9007199254740993.2500", (try cloned.extract("n", &work)).?.value.number_string);
            try std.testing.expectEqualStrings("escaped\ntext", (try cloned.extract("s", &work)).?.value.string);
            const items = (try cloned.extract("a", &work)).?.value.array;
            try std.testing.expectEqual(output.allocator().ptr, items.allocator.ptr);
            try std.testing.expectEqual(std.json.Value.null, items.items[0]);
            try std.testing.expectEqualStrings("plain", items.items[1].object.get("x").?.string);
        }
    };
    try Run.run(a);
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Run.run, .{});
}

test "SQL row values JSON clone cancels every observed cut without retaining partial output" {
    const a = std.testing.allocator;
    const Control = struct {
        polls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.polls += 1;
            if (self.polls == self.fail_at) return error.QueryCanceled;
        }
    };
    const wide: [1024]u8 = @splat('x');
    var values = std.json.Array.init(a);
    defer values.deinit();
    try values.append(.{ .string = &wide });
    const input: Json = .{ .value = .{ .array = values } };
    var observed: Control = .{};
    var baseline_context: Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &observed };
    var baseline_work: json_order.Budget = .{ .shared = &baseline_context };
    var baseline_region = std.heap.ArenaAllocator.init(a);
    defer baseline_region.deinit();
    _ = try input.cloneLeaky(baseline_region.allocator(), &baseline_work);
    try std.testing.expect(observed.polls >= 2);
    for (1..observed.polls + 1) |cut| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        var control: Control = .{ .fail_at = cut };
        var context: Context = .{ .alloc = region.allocator(), .checkpoint = Control.check, .ptr = &control };
        var work: json_order.Budget = .{ .shared = &context };
        try std.testing.expectError(error.QueryCanceled, input.cloneLeaky(region.allocator(), &work));
        try std.testing.expectError(error.QueryCanceled, input.cloneLeaky(region.allocator(), &work));
    }
}
