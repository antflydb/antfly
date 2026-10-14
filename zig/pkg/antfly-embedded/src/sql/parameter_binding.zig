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

//! One statement's parameter identity, shared across independently lowered
//! plans. Binding may add constraints; preparation freezes the contract and
//! owns the decoded frame. Programs retain immutable slice contracts, never
//! pointers to temporary/moved Program structs. The enclosing bounded arena
//! owns this object and all registered program slices.
const std = @import("std");
const scalar = @import("scalar.zig");
const ast = @import("ast.zig");
const frames = @import("parameter_frame.zig");
const A = std.mem.Allocator;

pub const Invocation = struct {
    allocator: A,
    descriptors: []scalar.Type,
    fallbacks: []const ?ast.ColumnType = &.{},
    contracts: std.ArrayList(Contract) = .empty,
    frame: ?frames.Frame = null,
    phase: enum { binding, prepared, retired } = .binding,

    const Contract = struct { instructions: []const scalar.Instruction, descriptors: []const scalar.Type };

    pub fn initLeaky(a: A, count: usize) !*Invocation {
        if (count > 1024) return error.SqlProgramLimitExceeded;
        const self = try a.create(Invocation);
        const descriptors = try a.alloc(scalar.Type, count);
        @memset(descriptors, .{});
        self.* = .{ .allocator = a, .descriptors = descriptors };
        return self;
    }

    /// Coarse compatibility hints can fill a hole but never erase a precise
    /// width/element identity learned elsewhere in this statement.
    pub fn mergeCoarse(self: *Invocation, hints: []const ?ast.ColumnType) !void {
        if (self.phase == .retired) return error.InvalidSqlParameters;
        if (hints.len > self.descriptors.len) return error.InvalidSqlParameters;
        for (hints, self.descriptors[0..hints.len]) |hint, *descriptor| if (hint) |kind| {
            if (descriptor.kind) |prior| {
                if (prior != kind) return error.ConflictingSqlParameterTypes;
            } else {
                if (self.phase != .binding) return error.InvalidSqlParameters;
                descriptor.kind = kind;
            }
        };
    }

    pub fn mergeInferred(self: *Invocation, hints: []const scalar.Type) !void {
        if (self.phase == .retired) return error.InvalidSqlParameters;
        if (hints.len > self.descriptors.len) return error.InvalidSqlParameters;
        for (hints, self.descriptors[0..hints.len]) |hint, *descriptor| {
            try scalar.validateParameterType(hint);
            if (hint.kind == null) continue;
            if (descriptor.kind) |kind| {
                if (kind != hint.kind or (descriptor.element_type != null and hint.element_type != null and descriptor.element_type != hint.element_type) or descriptor.nullable != hint.nullable) return error.ConflictingSqlParameterTypes;
            }
            if (self.phase != .binding) {
                if (!std.meta.eql(descriptor.*, hint)) return error.ConflictingSqlParameterTypes;
            } else {
                descriptor.kind = hint.kind;
                if (hint.element_type != null) descriptor.element_type = hint.element_type;
                descriptor.nullable = hint.nullable;
            }
        }
    }

    pub fn infer(self: *Invocation, a: A, expression: *const ast.Scalar, columns: []const scalar.Column, coarse: []?ast.ColumnType, expected: ?scalar.Type, limits: scalar.BindLimits) !bool {
        if (self.phase != .binding) return error.InvalidSqlParameters;
        try self.mergeCoarse(coarse);
        var local_limits = limits;
        local_limits.invocation = null;
        const changed = try scalar.inferTypedParametersExpected(a, expression, columns, self.descriptors, expected, local_limits);
        for (coarse, self.descriptors[0..coarse.len]) |*kind, descriptor| kind.* = descriptor.kind;
        return changed;
    }

    /// Publish only referenced slots. Local programs may contain defaulted
    /// descriptors for holes used exclusively by a different plan.
    pub fn register(self: *Invocation, program: *scalar.Program) !void {
        if (self.phase == .retired) return error.InvalidSqlParameters;
        for (program.instructions) |instruction| {
            if (instruction.operation != .parameter) continue;
            const slot = instruction.operation.parameter;
            if (slot >= self.descriptors.len or slot >= program.parameter_descriptors.len) return error.InvalidSqlParameters;
            const descriptor = program.parameter_descriptors[slot];
            if (descriptor.kind == null) return error.UnknownSqlParameterType;
            try scalar.validateParameterType(descriptor);
            const prior = self.descriptors[slot];
            if (prior.kind != null and (prior.kind != descriptor.kind or (prior.element_type != null and prior.element_type != descriptor.element_type) or prior.nullable != descriptor.nullable)) return error.ConflictingSqlParameterTypes;
            if (self.frame != null) {
                if (!std.meta.eql(prior, descriptor)) return error.ConflictingSqlParameterTypes;
            } else self.descriptors[slot] = descriptor;
        }
        if (self.frame == null) try self.contracts.append(self.allocator, .{ .instructions = program.instructions, .descriptors = program.parameter_descriptors });
        program.invocation = self;
    }

    pub fn prepareJson(self: *Invocation, backing: A, inputs: []const std.json.Value, limits: frames.Limits) !void {
        if (self.phase != .binding) return error.InvalidSqlParameters;
        try self.freeze();
        var frame = try frames.Frame.prepareJson(backing, self.descriptors, inputs, limits);
        errdefer frame.deinit();
        try self.validateFrame(&frame);
        self.frame = frame;
        self.phase = .prepared;
    }

    fn freeze(self: *Invocation) !void {
        for (self.descriptors, 0..) |*descriptor, index| {
            // Unused positional holes have no SQL constraint. Preserve the
            // execution transport's logical type instead of forcing a numeric
            // unused argument through a text decoder. Describe stays unknown.
            if (descriptor.kind == null) descriptor.* = .{ .kind = if (index < self.fallbacks.len) self.fallbacks[index] orelse .string else .string };
            try scalar.validateParameterType(descriptor.*);
            if (descriptor.kind != .datetime and descriptor.element_type == null) descriptor.element_type = try scalar.parameterElementType(descriptor.*);
        }
    }

    /// Legacy scalar-only consumers borrow the already canonicalized values;
    /// no input payload is copied a second time for a cursor. SQL arrays have
    /// no JSON compatibility representation and are read via the typed frame.
    pub fn compatibilityValues(self: Invocation, a: A) ![]const std.json.Value {
        const frame = self.frame orelse return error.InvalidSqlParameters;
        const values = try a.alloc(std.json.Value, frame.values.len);
        for (frame.values, values) |datum, *value| value.* = if (datum.sql_null or datum.array != null) .null else datum.value;
        return values;
    }

    fn validateFrame(self: Invocation, frame: *const frames.Frame) !void {
        for (self.contracts.items) |contract| for (contract.instructions) |instruction| {
            if (instruction.operation != .parameter) continue;
            const slot = instruction.operation.parameter;
            if (slot >= frame.descriptors.len or slot >= contract.descriptors.len) return error.InvalidSqlParameters;
            if (!std.meta.eql(frame.descriptors[slot], contract.descriptors[slot])) return error.ConflictingSqlParameterTypes;
        };
    }

    pub fn deinitFrame(self: *Invocation) void {
        if (self.frame) |*frame| frame.deinit();
        self.frame = null;
        self.phase = .retired;
    }
};

test "SQL invocation contracts share precise slots and fence execution and retirement" {
    const a = std.testing.allocator;
    var owner: std.heap.ArenaAllocator = .init(a);
    defer owner.deinit();
    const invocation = try Invocation.initLeaky(owner.allocator(), 2);
    var first_sql = try @import("compiler.zig").compileScalar(a, "$1::integer", .{});
    defer first_sql.deinit();
    var second_sql = try @import("compiler.zig").compileScalar(a, "cardinality($2::bigint[])", .{});
    defer second_sql.deinit();
    var coarse: [2]?ast.ColumnType = @splat(null);
    const limits = scalar.BindLimits{ .invocation = invocation };
    try std.testing.expect(try scalar.inferParameters(a, first_sql.expression, &.{}, &coarse, null, limits));
    try std.testing.expect(try scalar.inferParameters(a, second_sql.expression, &.{}, &coarse, null, limits));
    var first = try scalar.bind(a, first_sql.expression, &.{}, &coarse, limits);
    defer first.deinit();
    var second = try scalar.bind(a, second_sql.expression, &.{}, &coarse, limits);
    defer second.deinit();
    var none: std.heap.FixedBufferAllocator = .init(&.{});
    try std.testing.expectError(error.InvalidSqlParameters, first.evaluate(none.allocator(), &.{}, &.{.{ .integer = 42 }}, .{}));
    try invocation.prepareJson(a, &.{ .{ .integer = 42 }, .{ .string = "[-2:0]={9007199254740993,NULL,2}" } }, .{});
    defer invocation.deinitFrame();
    const peak = invocation.frame.?.budget.peak;
    for (0..10000) |_| {
        try std.testing.expectEqual(@as(i64, 42), (try first.evaluate(none.allocator(), &.{}, &.{}, .{})).value.integer);
        try std.testing.expectEqual(@as(i64, 3), (try second.evaluate(none.allocator(), &.{}, &.{}, .{})).value.integer);
    }
    try std.testing.expectEqual(peak, invocation.frame.?.budget.peak);
    try std.testing.expectError(error.InvalidSqlParameters, invocation.prepareJson(a, &.{ .{ .integer = 1 }, .null }, .{}));
    try std.testing.expectError(error.ConflictingSqlParameterTypes, invocation.mergeInferred(&.{.{ .kind = .integer, .element_type = .int64 }}));
    invocation.deinitFrame();
    try std.testing.expectError(error.InvalidSqlParameters, first.evaluate(none.allocator(), &.{}, &.{.{ .integer = 1 }}, .{}));
    try std.testing.expectError(error.InvalidSqlParameters, invocation.mergeCoarse(&.{.integer}));
    try std.testing.expectError(error.InvalidSqlParameters, invocation.register(&first));
}
