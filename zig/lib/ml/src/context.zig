// Copyright 2026 Antfly, Inc.
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

/// Training context: scoped variable management, initialization, and
/// train/eval mode tracking. Inspired by GoMLX's Context.
///
/// A Context owns a flat map of named variables (weight tensors) with
/// hierarchical scope names separated by `/`. Models call `ctx.in("attn")`
/// to enter a sub-scope, then `ctx.variable("q_proj", shape, init)` to
/// declare a variable at `"attn/q_proj"`.
///
/// Variables are created lazily on first access and cached for reuse.
/// The Context tracks whether the model is in training or inference mode
/// so that ops like dropout can behave differently.

const std = @import("std");
const Shape = @import("graph/shape.zig").Shape;
const DType = @import("graph/shape.zig").DType;

/// Execution mode flags.
pub const Mode = enum {
    /// Full training: dropout enabled, batch norm uses batch stats.
    train,
    /// Evaluation: dropout disabled, batch norm uses running stats.
    eval,
    /// Inference-only: like eval, but graph may be further simplified
    /// (e.g. no gradient bookkeeping).
    inference,
};

/// How to initialize a new variable.
pub const Initializer = union(enum) {
    /// All zeros.
    zeros,
    /// All ones.
    ones,
    /// Fill with a constant value.
    constant: f64,
    /// Xavier/Glorot uniform: U(-limit, limit) where limit = sqrt(6 / (fan_in + fan_out)).
    xavier_uniform,
    /// Xavier/Glorot normal: N(0, std) where std = sqrt(2 / (fan_in + fan_out)).
    xavier_normal,
    /// Kaiming/He uniform: U(-limit, limit) where limit = sqrt(6 / fan_in).
    kaiming_uniform,
    /// Kaiming/He normal: N(0, std) where std = sqrt(2 / fan_in).
    kaiming_normal,
    /// Uniform in [-bound, bound].
    uniform: f64,
    /// Normal with given stddev.
    normal: f64,
};

/// A declared model variable (weight tensor).
pub const Variable = struct {
    /// Full scoped name, e.g. "model/layers/0/attn/q_proj".
    name: []const u8,
    /// Shape of the variable.
    shape: Shape,
    /// How this variable was initialized.
    initializer: Initializer,
    /// Whether this variable requires gradient computation.
    requires_grad: bool,
    /// Optional materialized data. Null until initialization runs.
    data: ?[]u8 = null,
};

/// Hierarchical variable scope and training state.
pub const Context = struct {
    allocator: std.mem.Allocator,
    mode: Mode = .inference,

    /// All declared variables, keyed by full scoped name.
    variables: std.StringHashMapUnmanaged(Variable) = .empty,
    /// Insertion-order list for deterministic iteration.
    variable_names: std.ArrayListUnmanaged([]const u8) = .empty,

    /// Current scope prefix stack. join with "/" to form full name.
    scope_stack: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn init(allocator: std.mem.Allocator) Context {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Context) void {
        // Free variable data
        var it = self.variables.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.data) |d| self.allocator.free(d);
            self.allocator.free(entry.value_ptr.name);
        }
        self.variables.deinit(self.allocator);
        self.variable_names.deinit(self.allocator);
        self.scope_stack.deinit(self.allocator);
    }

    /// Enter a sub-scope. Returns a child Context that shares state
    /// with the parent but has the scope pushed.
    pub fn in(self: *Context, name: []const u8) Scope {
        return .{ .ctx = self, .name = name, .entered = false };
    }

    /// Declare or retrieve a variable in the current scope.
    pub fn variable(
        self: *Context,
        name: []const u8,
        shape: Shape,
        initializer: Initializer,
    ) !*Variable {
        return self.variableOpts(name, shape, initializer, true);
    }

    /// Declare a variable that does not require gradients (e.g. embeddings
    /// used as lookup tables only).
    pub fn variableNoGrad(
        self: *Context,
        name: []const u8,
        shape: Shape,
        initializer: Initializer,
    ) !*Variable {
        return self.variableOpts(name, shape, initializer, false);
    }

    fn variableOpts(
        self: *Context,
        name: []const u8,
        shape: Shape,
        initializer: Initializer,
        requires_grad: bool,
    ) !*Variable {
        const full_name = try self.scopedName(name);

        const gop = try self.variables.getOrPut(self.allocator, full_name);
        if (gop.found_existing) {
            self.allocator.free(full_name);
            return gop.value_ptr;
        }

        gop.value_ptr.* = .{
            .name = full_name,
            .shape = shape,
            .initializer = initializer,
            .requires_grad = requires_grad,
        };
        try self.variable_names.append(self.allocator, full_name);
        return gop.value_ptr;
    }

    /// Build the full scoped name by joining scope_stack + name with "/".
    fn scopedName(self: *Context, name: []const u8) ![]const u8 {
        if (self.scope_stack.items.len == 0) {
            return try self.allocator.dupe(u8, name);
        }

        var total: usize = 0;
        for (self.scope_stack.items) |s| {
            total += s.len + 1; // +1 for "/"
        }
        total += name.len;

        const buf = try self.allocator.alloc(u8, total);
        var pos: usize = 0;
        for (self.scope_stack.items) |s| {
            @memcpy(buf[pos..][0..s.len], s);
            pos += s.len;
            buf[pos] = '/';
            pos += 1;
        }
        @memcpy(buf[pos..][0..name.len], name);
        return buf;
    }

    /// Number of declared variables.
    pub fn variableCount(self: *const Context) usize {
        return self.variable_names.items.len;
    }

    /// Iterate all variables in declaration order.
    pub fn variableIterator(self: *const Context) VariableIterator {
        return .{ .ctx = self, .index = 0 };
    }

    /// Return true when in training mode (dropout enabled, etc.).
    pub fn isTraining(self: *const Context) bool {
        return self.mode == .train;
    }

    /// Total number of scalar parameters across all variables.
    /// Returns null if any variable has dynamic (symbolic) dimensions.
    pub fn parameterCount(self: *const Context) ?i64 {
        var total: i64 = 0;
        for (self.variable_names.items) |vname| {
            const v = self.variables.get(vname) orelse continue;
            total += v.shape.numElements() orelse return null;
        }
        return total;
    }

    /// Total number of trainable scalar parameters (requires_grad == true).
    /// Returns null if any trainable variable has dynamic dimensions.
    pub fn trainableParameterCount(self: *const Context) ?i64 {
        var total: i64 = 0;
        for (self.variable_names.items) |vname| {
            const v = self.variables.get(vname) orelse continue;
            if (v.requires_grad) total += v.shape.numElements() orelse return null;
        }
        return total;
    }
};

/// RAII scope guard returned by `Context.in()`.
pub const Scope = struct {
    ctx: *Context,
    name: []const u8,
    entered: bool,

    pub fn enter(self: *Scope) !void {
        try self.ctx.scope_stack.append(self.ctx.allocator, self.name);
        self.entered = true;
    }

    pub fn exit(self: *Scope) void {
        if (self.entered) {
            _ = self.ctx.scope_stack.pop();
            self.entered = false;
        }
    }

    /// Convenience: enter scope, call function, exit scope.
    pub fn call(self: *Scope, comptime func: anytype, args: anytype) !@typeInfo(@TypeOf(func)).@"fn".return_type.? {
        try self.enter();
        defer self.exit();
        return @call(.auto, func, args);
    }

    /// Access the underlying context (for variable declarations).
    pub fn context(self: *Scope) *Context {
        return self.ctx;
    }
};

pub const VariableIterator = struct {
    ctx: *const Context,
    index: usize,

    pub fn next(self: *VariableIterator) ?*const Variable {
        if (self.index >= self.ctx.variable_names.items.len) return null;
        const name = self.ctx.variable_names.items[self.index];
        self.index += 1;
        return self.ctx.variables.getPtr(name);
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "context: basic variable declaration" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    const v = try ctx.variable("weight", Shape.init(.f32, &.{ 768, 768 }), .xavier_uniform);
    try std.testing.expectEqualStrings("weight", v.name);
    try std.testing.expect(v.requires_grad);
    try std.testing.expectEqual(@as(i64, 768 * 768), v.shape.numElements().?);
}

test "context: scoped naming" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    // Top-level
    _ = try ctx.variable("embed", Shape.init(.f32, &.{ 50257, 768 }), .{ .normal = 0.02 });

    // Nested scope: model/layers/0/attn/q_proj
    var model = ctx.in("model");
    try model.enter();
    defer model.exit();

    var layers = ctx.in("layers");
    try layers.enter();
    defer layers.exit();

    var l0 = ctx.in("0");
    try l0.enter();
    defer l0.exit();

    var attn = ctx.in("attn");
    try attn.enter();
    defer attn.exit();

    const q = try ctx.variable("q_proj", Shape.init(.f32, &.{ 768, 768 }), .xavier_uniform);
    try std.testing.expectEqualStrings("model/layers/0/attn/q_proj", q.name);

    const k = try ctx.variable("k_proj", Shape.init(.f32, &.{ 768, 768 }), .xavier_uniform);
    try std.testing.expectEqualStrings("model/layers/0/attn/k_proj", k.name);

    try std.testing.expectEqual(@as(usize, 3), ctx.variableCount());
}

test "context: variable deduplication" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    const v1 = try ctx.variable("w", Shape.init(.f32, &.{10}), .zeros);
    const v2 = try ctx.variable("w", Shape.init(.f32, &.{10}), .ones);

    // Same pointer — second call returns existing variable
    try std.testing.expectEqual(v1, v2);
    try std.testing.expectEqual(@as(usize, 1), ctx.variableCount());
}

test "context: mode flags" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    try std.testing.expect(!ctx.isTraining());

    ctx.mode = .train;
    try std.testing.expect(ctx.isTraining());

    ctx.mode = .eval;
    try std.testing.expect(!ctx.isTraining());
}

test "context: parameter counting" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    _ = try ctx.variable("w1", Shape.init(.f32, &.{ 10, 20 }), .zeros);
    _ = try ctx.variableNoGrad("embed", Shape.init(.f32, &.{ 1000, 64 }), .{ .normal = 0.02 });
    _ = try ctx.variable("w2", Shape.init(.f32, &.{ 20, 5 }), .zeros);

    // Total: 200 + 64000 + 100 = 64300
    try std.testing.expectEqual(@as(i64, 64300), ctx.parameterCount().?);
    // Trainable: 200 + 100 = 300 (embed excluded)
    try std.testing.expectEqual(@as(i64, 300), ctx.trainableParameterCount().?);
}

test "context: variable iterator" {
    var ctx = Context.init(std.testing.allocator);
    defer ctx.deinit();

    _ = try ctx.variable("a", Shape.init(.f32, &.{1}), .zeros);
    _ = try ctx.variable("b", Shape.init(.f32, &.{2}), .ones);
    _ = try ctx.variable("c", Shape.init(.f32, &.{3}), .xavier_uniform);

    var it = ctx.variableIterator();
    const v0 = it.next().?;
    try std.testing.expectEqualStrings("a", v0.name);
    const v1 = it.next().?;
    try std.testing.expectEqualStrings("b", v1.name);
    const v2 = it.next().?;
    try std.testing.expectEqualStrings("c", v2.name);
    try std.testing.expect(it.next() == null);
}
