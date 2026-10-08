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

//! Immutable typed NUMERIC schema literals. Compilation owns coefficients;
//! row checks borrow them and share the caller's sticky work/cancellation budget.
const std = @import("std");
const exact = @import("../sql/numeric_value.zig");
const storage = @import("../sql/numeric_storage.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const max_bytes = 4 * 1024 * 1024;
const keywords = [_][]const u8{ "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf" };

/// Stable-address request owner. Rows reuse bounded scratch, but never reset
/// work/cancellation admission between predicates or rows. Not thread safe.
pub const Execution = struct {
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    context: exact.Context,

    pub fn init(self: *Execution, alloc: A) void {
        self.memory = .{ .backing = alloc, .limit = max_bytes };
        self.arena = .init(self.memory.allocator());
        self.context = .{ .alloc = self.arena.allocator() };
    }

    pub fn deinit(self: *Execution) void {
        self.arena.deinit();
        std.debug.assert(self.memory.live == 0);
        self.* = undefined;
    }

    pub fn validateJson(self: *Execution, plan: *const Plan, value: Json) !void {
        return self.validate(plan, try self.prepareJson(value));
    }

    /// Borrowed until the next prepare or deinit. Composition predicates can
    /// share one parsed row value without resetting sticky work admission.
    pub fn prepareJson(self: *Execution, value: Json) !exact.Value {
        try self.context.charge(0);
        _ = self.arena.reset(.retain_capacity);
        return (storage.fromJson(&self.context, value) catch |err| return self.failure(err)).value;
    }

    pub fn validate(self: *Execution, plan: *const Plan, value: exact.Value) !void {
        plan.validate(&self.context, value) catch |err| return self.failure(err);
    }

    fn failure(self: *Execution, err: anyerror) anyerror {
        return switch (err) {
            error.OutOfMemory => if (self.memory.isExhausted()) self.context.limit() else err,
            error.SqlInvalidTextRepresentation, error.InvalidSqlNumber, error.SqlNumericOutOfRange => error.InvalidBatchRequest,
            else => err,
        };
    }
};

pub const Plan = struct {
    alloc: A,
    memory: @import("../sql/memory_budget.zig"),
    arena: std.heap.ArenaAllocator = undefined,
    bounds: [5]?exact.Value = @splat(null),
    has_const: bool = false,
    constant: ?exact.Value = null,
    has_enum: bool = false,
    enumeration: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    const Entry = struct { value: exact.Value, next: ?*Entry };

    pub fn create(alloc: A, object: std.json.ObjectMap) !?*Plan {
        var needed = object.contains("const");
        if (object.get("enum")) |value| needed = needed or value != .null;
        for (keywords) |key| if (object.get(key)) |value| {
            needed = needed or value != .null;
        };
        if (!needed) return null;
        const self = try alloc.create(Plan);
        self.* = .{ .alloc = alloc, .memory = .{ .backing = alloc, .limit = max_bytes } };
        self.arena = .init(self.memory.allocator());
        errdefer self.deinit();
        self.compile(object) catch |err| return switch (err) {
            error.OutOfMemory => if (self.memory.isExhausted()) error.RelationalExpressionBudgetExceeded else err,
            error.SqlProgramLimitExceeded => error.RelationalExpressionBudgetExceeded,
            error.Canceled => err,
            else => error.InvalidSchemaUpdateRequest,
        };
        return self;
    }

    pub fn deinit(self: *Plan) void {
        const alloc = self.alloc;
        self.arena.deinit();
        std.debug.assert(self.memory.live == 0);
        alloc.destroy(self);
    }

    fn compile(self: *Plan, object: std.json.ObjectMap) !void {
        const a = self.arena.allocator();
        var context: exact.Context = .{ .alloc = a };
        for (keywords, 0..) |key, i| if (object.get(key)) |value| {
            if (value == .null) continue;
            const parsed = try schemaNumber(&context, value);
            if (parsed.value.kind != .finite or (i == 4 and (parsed.value.negative or parsed.value.isZero()))) return error.InvalidSchemaUpdateRequest;
            self.bounds[i] = parsed.value;
        };
        if (object.get("const")) |value| {
            self.has_const = true;
            self.constant = try typedLiteral(&context, value);
        }
        if (object.get("enum")) |values| {
            if (values == .null) return;
            if (values != .array) return error.InvalidSchemaUpdateRequest;
            self.has_enum = true;
            for (values.array.items) |item| {
                try context.charge(1);
                const value = (try typedLiteral(&context, item)) orelse continue;
                const hash = try hashValue(&context, value);
                const slot = try self.enumeration.getOrPut(a, hash);
                if (!slot.found_existing) {
                    const entry = try a.create(Entry);
                    entry.* = .{ .value = value, .next = null };
                    slot.value_ptr.* = entry;
                } else {
                    var entry: ?*Entry = slot.value_ptr.*;
                    var duplicate = false;
                    while (entry) |current| : (entry = current.next) {
                        if (try exact.order(&context, value, current.value) == .eq) {
                            duplicate = true;
                            break;
                        }
                    }
                    if (duplicate) continue;
                    const next = try a.create(Entry);
                    next.* = .{ .value = value, .next = slot.value_ptr.* };
                    slot.value_ptr.* = next;
                }
            }
        }
    }

    pub fn validate(self: *const Plan, context: *exact.Context, value: exact.Value) !void {
        try context.charge(0);
        if (self.has_const) {
            const constant = self.constant orelse return error.InvalidBatchRequest;
            if (try exact.order(context, value, constant) != .eq) return error.InvalidBatchRequest;
        }
        if (self.has_enum) {
            var entry = self.enumeration.get(try hashValue(context, value));
            var found = false;
            while (entry) |current| : (entry = current.next) {
                if (try exact.order(context, value, current.value) == .eq) {
                    found = true;
                    break;
                }
            }
            if (!found) return error.InvalidBatchRequest;
        }
        for (self.bounds, 0..) |optional, i| if (optional) |bound| {
            if (i == 4) {
                var result = try exact.remainder(context, value, bound);
                defer result.deinit();
                if (!result.value.isZero()) return error.InvalidBatchRequest;
            } else {
                const order = try exact.order(context, value, bound);
                if (switch (i) {
                    0 => order == .lt,
                    1 => order == .gt,
                    2 => order != .gt,
                    3 => order != .lt,
                    else => unreachable,
                }) return error.InvalidBatchRequest;
            }
        };
    }
};

fn hashValue(context: *exact.Context, value: exact.Value) !u64 {
    var hash = std.hash.Wyhash.init(0x4e554d45524943);
    try exact.hash(context, value, &hash);
    return hash.final();
}

fn typedLiteral(context: *exact.Context, value: Json) !?exact.Value {
    if (value == .integer or value == .number_string or value == .float) return (try schemaNumber(context, value)).value;
    if (value == .string) {
        for ([_][]const u8{ "NaN", "+NaN", "-NaN", "Infinity", "+Infinity", "-Infinity", "inf", "+inf", "-inf" }) |special| {
            if (std.ascii.eqlIgnoreCase(value.string, special)) return (try exact.parse(context, value.string)).value;
        }
    }
    // JSON strings containing finite decimals remain strings in const/enum.
    return null;
}

fn schemaNumber(context: *exact.Context, value: Json) !exact.Owned {
    if (value == .float) {
        if (!std.math.isFinite(value.float)) return error.InvalidSchemaUpdateRequest;
        var text: [128]u8 = undefined;
        return exact.parse(context, try std.fmt.bufPrint(&text, "{e}", .{value.float}));
    }
    if (value != .integer and value != .number_string) return error.InvalidSchemaUpdateRequest;
    return storage.fromJson(context, value);
}
