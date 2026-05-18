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

const std = @import("std");
const ast = @import("ast.zig");
const Allocator = std.mem.Allocator;

// -----------------------------------------------------------------------
// Value types
// -----------------------------------------------------------------------

pub const ValueMap = std.StringArrayHashMapUnmanaged(Value);

pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    null,
    undefined,
    array: []const Value,
    map: ValueMap,
    safe_string: []const u8,

    pub fn isTruthy(self: Value) bool {
        return switch (self) {
            .null, .undefined => false,
            .boolean => |b| b,
            .integer => |i| i != 0,
            .float => |f| f != 0.0,
            .string => |s| s.len > 0,
            .safe_string => |s| s.len > 0,
            .array => |a| a.len > 0,
            .map => |m| m.count() > 0,
        };
    }

    pub fn toString(self: Value, arena: Allocator) ![]const u8 {
        return switch (self) {
            .string => |s| s,
            .safe_string => |s| s,
            .integer => |i| std.fmt.allocPrint(arena, "{d}", .{i}),
            .float => |f| std.fmt.allocPrint(arena, "{d}", .{f}),
            .boolean => |b| if (b) "true" else "false",
            .null, .undefined => "",
            .array => "[object Array]",
            .map => "[object Object]",
        };
    }

    pub fn isSafe(self: Value) bool {
        return self == .safe_string;
    }

    // Convenience constructors
    pub fn str(s: []const u8) Value {
        return .{ .string = s };
    }
    pub fn int(i: i64) Value {
        return .{ .integer = i };
    }
    pub fn flt(f: f64) Value {
        return .{ .float = f };
    }
    pub fn bln(b: bool) Value {
        return .{ .boolean = b };
    }
    pub fn safeStr(s: []const u8) Value {
        return .{ .safe_string = s };
    }

    /// Convert a Zig value to a handlebars Value.
    /// Supports: bool, integers, floats, []const u8, ?T, enums,
    /// slices/arrays of convertible types, and structs (→ map).
    pub fn from(arena: Allocator, val: anytype) Allocator.Error!Value {
        const T = @TypeOf(val);
        const info = @typeInfo(T);

        // Strings ([]const u8)
        if (T == []const u8) return .{ .string = val };
        if (T == Value) return val;

        // Optionals
        if (info == .optional) {
            if (val) |v| return from(arena, v) else return .null;
        }

        // Booleans
        if (T == bool) return .{ .boolean = val };

        // Integers
        if (info == .int or info == .comptime_int) return .{ .integer = @intCast(val) };

        // Floats
        if (info == .float or info == .comptime_float) return .{ .float = @floatCast(val) };

        // Enums → string
        if (info == .@"enum") return .{ .string = @tagName(val) };

        // Pointers to arrays/slices
        if (info == .pointer) {
            const child = info.pointer;
            // *const [N]u8 and similar — treat as string
            if (child.size == .one) {
                const ci = @typeInfo(child.child);
                if (ci == .array and ci.array.child == u8) {
                    return .{ .string = val };
                }
                // Dereference single-item pointers
                return from(arena, val.*);
            }
            // Slices
            if (child.size == .slice) {
                if (child.child == u8) return .{ .string = val };
                const items = try arena.alloc(Value, val.len);
                for (val, 0..) |item, i| {
                    items[i] = try from(arena, item);
                }
                return .{ .array = items };
            }
        }

        // Arrays
        if (info == .array) {
            if (info.array.child == u8) return .{ .string = &val };
            const items = try arena.alloc(Value, info.array.len);
            for (val, 0..) |item, i| {
                items[i] = try from(arena, item);
            }
            return .{ .array = items };
        }

        // Structs → map
        if (info == .@"struct") {
            var m: ValueMap = .{};
            inline for (info.@"struct".fields) |field| {
                const fv = @field(val, field.name);
                try m.put(arena, field.name, try from(arena, fv));
            }
            return .{ .map = m };
        }

        @compileError("unsupported type for Value.from: " ++ @typeName(T));
    }
};

// -----------------------------------------------------------------------
// Helper types
// -----------------------------------------------------------------------

pub const HelperContext = struct {
    params: []const Value,
    hash: ValueMap,
    arena: Allocator,
    userdata: ?*anyopaque = null, // caller-supplied context for closures
    // Block helper support (set when called as {{#helper}}...{{/helper}})
    eval: ?*Eval = null,
    fn_body: ?*const ast.Node = null,
    inverse_body: ?*const ast.Node = null,

    pub fn hashGet(self: *const HelperContext, key: []const u8) ?Value {
        return self.hash.get(key);
    }

    pub fn hashStr(self: *const HelperContext, key: []const u8) []const u8 {
        if (self.hash.get(key)) |v| {
            return switch (v) {
                .string => |s| s,
                .safe_string => |s| s,
                else => "",
            };
        }
        return "";
    }

    pub fn hashBool(self: *const HelperContext, key: []const u8) ?bool {
        if (self.hash.get(key)) |v| {
            return switch (v) {
                .boolean => |b| b,
                else => null,
            };
        }
        return null;
    }

    pub fn hashInt(self: *const HelperContext, key: []const u8) ?i64 {
        if (self.hash.get(key)) |v| {
            return switch (v) {
                .integer => |i| i,
                else => null,
            };
        }
        return null;
    }

    pub fn param(self: *const HelperContext, idx: usize) Value {
        if (idx < self.params.len) return self.params[idx];
        return .undefined;
    }

    /// Render the block's main body with the current context (like raymond's options.Fn())
    pub fn fnContent(self: *const HelperContext) anyerror![]const u8 {
        if (self.fn_body) |body| {
            if (self.eval) |e| {
                return e.evalProgram(&body.program);
            }
        }
        return "";
    }

    /// Render the block's main body with a specific context (like raymond's options.FnWith())
    pub fn fnWith(self: *const HelperContext, ctx: Value) anyerror![]const u8 {
        if (self.fn_body) |body| {
            if (self.eval) |e| {
                try e.contexts.append(e.arena, ctx);
                const result = try e.evalProgram(&body.program);
                _ = e.contexts.pop();
                return result;
            }
        }
        return "";
    }

    /// Render the block's inverse/else body (like raymond's options.Inverse())
    pub fn inverseContent(self: *const HelperContext) anyerror![]const u8 {
        if (self.inverse_body) |body| {
            if (self.eval) |e| {
                return e.evalProgram(&body.program);
            }
        }
        return "";
    }

    /// Render the block's main body with a data frame (like raymond's options.FnData())
    pub fn fnWithData(self: *const HelperContext, ctx: Value, frame: DataFrame) anyerror![]const u8 {
        if (self.fn_body) |body| {
            if (self.eval) |e| {
                try e.contexts.append(e.arena, ctx);
                try e.pushDataFrame(frame);
                const result = try e.evalProgram(&body.program);
                e.popDataFrame();
                _ = e.contexts.pop();
                return result;
            }
        }
        return "";
    }
};

pub const HelperFn = *const fn (ctx: HelperContext) anyerror!Value;

pub const Helper = struct {
    func: HelperFn,
    userdata: ?*anyopaque = null,

    pub fn from(func: HelperFn) Helper {
        return .{ .func = func };
    }

    pub fn withData(func: HelperFn, userdata: *anyopaque) Helper {
        return .{ .func = func, .userdata = userdata };
    }
};

pub const HelperMap = std.StringArrayHashMapUnmanaged(Helper);

/// Register a helper function (convenience for HelperMap.put with Helper.from)
pub fn putHelper(map: *HelperMap, arena: Allocator, name: []const u8, func: HelperFn) Allocator.Error!void {
    return map.put(arena, name, Helper.from(func));
}

// -----------------------------------------------------------------------
// Data frame (for @index, @key, @first, @last)
// -----------------------------------------------------------------------

pub const DataFrame = struct {
    index: ?usize = null,
    key: ?[]const u8 = null,
    first: ?bool = null,
    last: ?bool = null,
};

// -----------------------------------------------------------------------
// Evaluator
// -----------------------------------------------------------------------

pub const EvalError = error{
    OutOfMemory,
    HelperError,
    InvalidPath,
    EvalFailed,
};

pub const Eval = struct {
    arena: Allocator,
    root_context: Value,
    contexts: std.ArrayListUnmanaged(Value),
    data_frames: std.ArrayListUnmanaged(DataFrame),
    block_params: std.ArrayListUnmanaged(std.StringArrayHashMapUnmanaged(Value)),
    helpers: *const HelperMap,
    partials: *const std.StringArrayHashMapUnmanaged([]const u8),
    inline_partials: std.StringArrayHashMapUnmanaged([]const u8),
    partial_block_stack: std.ArrayListUnmanaged(*const ast.Node),

    pub fn init(
        arena: Allocator,
        context: Value,
        helpers: *const HelperMap,
        partials: *const std.StringArrayHashMapUnmanaged([]const u8),
    ) Eval {
        var contexts: std.ArrayListUnmanaged(Value) = .empty;
        contexts.append(arena, context) catch {};
        return .{
            .arena = arena,
            .root_context = context,
            .contexts = contexts,
            .data_frames = .empty,
            .block_params = .empty,
            .helpers = helpers,
            .partials = partials,
            .inline_partials = .{},
            .partial_block_stack = .empty,
        };
    }

    pub fn exec(self: *Eval, program: *const ast.Node) EvalError![]const u8 {
        return self.evalProgram(&program.program);
    }

    // ---------------------------------------------------------------
    // Core evaluation
    // ---------------------------------------------------------------

    fn evalProgram(self: *Eval, program: *const ast.Program) EvalError![]const u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        for (program.body, 0..) |stmt, i| {
            // Check if this node wants to strip preceding whitespace
            if (nodeStripBefore(stmt)) {
                trimTrailingWhitespace(&buf);
            }

            var s = try self.evalNode(stmt);

            // Check if previous node wants to strip following whitespace
            if (i > 0 and nodeStripAfter(program.body[i - 1])) {
                s = trimLeadingWhitespace(s);
            }

            try buf.appendSlice(self.arena, s);
        }
        return buf.items;
    }

    fn evalNode(self: *Eval, node: *const ast.Node) EvalError![]const u8 {
        return switch (node.*) {
            .content => |c| c.value,
            .comment => "",
            .mustache => |m| self.evalMustache(&m),
            .block => |b| self.evalBlock(&b),
            .partial => |p| self.evalPartial(&p),
            .partial_block => |pb| self.evalPartialBlock(&pb),
            .inline_partial => |ip| self.evalInlinePartial(&ip),
            .program => |p| self.evalProgram(&p),
            else => "",
        };
    }

    fn evalMustache(self: *Eval, mustache: *const ast.Mustache) EvalError![]const u8 {
        const value = try self.evalExpressionValue(&mustache.expression.expression);
        const text = try value.toString(self.arena);
        if (mustache.unescaped or value.isSafe()) {
            return text;
        }
        return htmlEscape(self.arena, text);
    }

    fn evalBlock(self: *Eval, block: *const ast.Block) EvalError![]const u8 {
        const expr = &block.expression.expression;
        const name = self.getPathName(expr.path);

        // Built-in block helpers
        if (name) |n| {
            if (std.mem.eql(u8, n, "if")) return self.evalIf(expr, block);
            if (std.mem.eql(u8, n, "unless")) return self.evalUnless(expr, block);
            if (std.mem.eql(u8, n, "each")) return self.evalEach(expr, block);
            if (std.mem.eql(u8, n, "with")) return self.evalWith(expr, block);
            if (std.mem.eql(u8, n, "equal")) return self.evalEqual(expr, block);
        }

        // Custom helper
        if (name) |n| {
            if (self.helpers.get(n)) |helper| {
                return self.callBlockHelper(helper, expr, block);
            }
        }

        // Implicit block: evaluate path as conditional/iterator
        const val = try self.evalExprParam(expr.path);
        return self.evalImplicitBlock(val, block);
    }

    fn evalPartial(self: *Eval, partial: *const ast.Partial) EvalError![]const u8 {
        // Resolve partial name (static path or dynamic sub-expression)
        const name: []const u8 = switch (partial.name.*) {
            .path => |p| blk: {
                if (p.parts.len == 0) break :blk "";
                if (p.data) {
                    // @data partial reference (e.g. @partial-block)
                    break :blk try std.fmt.allocPrint(self.arena, "@{s}", .{p.parts[0]});
                }
                break :blk p.parts[0];
            },
            .sub_expression => |s| blk: {
                const val = try self.evalExpressionValue(&s.expression.expression);
                break :blk try val.toString(self.arena);
            },
            else => return "",
        };

        // Check @partial-block
        if (std.mem.eql(u8, name, "@partial-block")) {
            if (self.partial_block_stack.items.len > 0) {
                const block_prog = self.partial_block_stack.items[self.partial_block_stack.items.len - 1];
                return self.evalProgram(&block_prog.program);
            }
            return "";
        }

        // Look up: inline partials first, then registered partials
        const source = self.inline_partials.get(name) orelse
            self.partials.get(name) orelse return "";

        const parser = @import("parser.zig");
        const node = parser.Parser.parse(source, self.arena) catch return "";

        // Determine partial context
        var pushed_context = false;
        if (partial.params.len > 0) {
            // {{> partial contextObj}} — use param as new context
            const ctx = try self.evalExprParam(partial.params[0]);
            try self.contexts.append(self.arena, ctx);
            pushed_context = true;
        } else if (partial.hash) |hash_node| {
            // {{> partial key=val}} — hash becomes new context
            const hash = try self.evalHash(hash_node);
            try self.contexts.append(self.arena, .{ .map = hash });
            pushed_context = true;
        }

        const result = try self.evalProgram(&node.program);

        if (pushed_context) _ = self.contexts.pop();

        // Apply indentation if present
        if (partial.indent.len > 0) {
            return indentLines(self.arena, result, partial.indent);
        }

        return result;
    }

    fn evalPartialBlock(self: *Eval, pb: *const ast.PartialBlock) EvalError![]const u8 {
        // Resolve name
        const name: []const u8 = switch (pb.name.*) {
            .path => |p| if (p.parts.len > 0) p.parts[0] else return "",
            .sub_expression => |s| blk: {
                const val = try self.evalExpressionValue(&s.expression.expression);
                break :blk try val.toString(self.arena);
            },
            else => return "",
        };

        // Push the default block content as @partial-block
        try self.partial_block_stack.append(self.arena, pb.program);

        // Context
        var pushed_context = false;
        if (pb.params.len > 0) {
            const ctx = try self.evalExprParam(pb.params[0]);
            try self.contexts.append(self.arena, ctx);
            pushed_context = true;
        } else if (pb.hash) |hash_node| {
            const hash = try self.evalHash(hash_node);
            try self.contexts.append(self.arena, .{ .map = hash });
            pushed_context = true;
        }

        // Look up and render the named partial
        const source = self.inline_partials.get(name) orelse
            self.partials.get(name) orelse {
            // No partial found — render default body
            if (pushed_context) _ = self.contexts.pop();
            _ = self.partial_block_stack.pop();
            return self.evalProgram(&pb.program.program);
        };

        const parser = @import("parser.zig");
        const node = parser.Parser.parse(source, self.arena) catch {
            if (pushed_context) _ = self.contexts.pop();
            _ = self.partial_block_stack.pop();
            return "";
        };

        const result = try self.evalProgram(&node.program);

        if (pushed_context) _ = self.contexts.pop();
        _ = self.partial_block_stack.pop();

        return result;
    }

    fn evalInlinePartial(self: *Eval, ip: *const ast.InlinePartial) EvalError![]const u8 {
        // Re-serialize the body back to source — inline partials are templates
        // For simplicity, evaluate the body and store as pre-rendered,
        // but Handlebars spec says they should be stored as template source.
        // We store the body as a program node and render it when referenced.
        // Actually, inline partials need to be stored as source text.
        // Since we have the AST, we'll render it as a one-shot and store.
        // Better: store the original source slice if we can, but we don't have it.
        // Simplest correct approach: render the body now and register as partial.
        const rendered = try self.evalProgram(&ip.program.program);
        try self.inline_partials.put(self.arena, ip.name, rendered);
        return "";
    }

    // ---------------------------------------------------------------
    // Built-in helpers
    // ---------------------------------------------------------------

    fn evalIf(self: *Eval, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        const condition = if (expr.params.len > 0)
            try self.evalExprParam(expr.params[0])
        else
            Value.undefined;

        if (condition.isTruthy()) {
            const result = try self.evalProgram(&block.program.program);
            return applyInnerStrip(result, block.open_strip.close, block.close_strip.open);
        } else if (block.inverse) |inv| {
            const result = try self.evalProgram(&inv.program);
            return applyInnerStrip(result, block.inverse_strip.close, block.close_strip.open);
        }
        return "";
    }

    fn evalUnless(self: *Eval, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        const condition = if (expr.params.len > 0)
            try self.evalExprParam(expr.params[0])
        else
            Value.undefined;

        if (!condition.isTruthy()) {
            const result = try self.evalProgram(&block.program.program);
            return applyInnerStrip(result, block.open_strip.close, block.close_strip.open);
        } else if (block.inverse) |inv| {
            const result = try self.evalProgram(&inv.program);
            return applyInnerStrip(result, block.inverse_strip.close, block.close_strip.open);
        }
        return "";
    }

    fn evalEach(self: *Eval, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        const target = if (expr.params.len > 0)
            try self.evalExprParam(expr.params[0])
        else
            Value.undefined;

        var buf: std.ArrayListUnmanaged(u8) = .empty;

        switch (target) {
            .array => |arr| {
                if (arr.len == 0) {
                    if (block.inverse) |inv| return self.evalProgram(&inv.program);
                    return "";
                }
                // Block params
                const bp_names = block.program.program.block_params;

                for (arr, 0..) |item, i| {
                    try self.pushDataFrame(.{
                        .index = i,
                        .first = i == 0,
                        .last = i == arr.len - 1,
                    });
                    if (bp_names.len > 0) {
                        var bp: std.StringArrayHashMapUnmanaged(Value) = .{};
                        try bp.put(self.arena, bp_names[0], item);
                        if (bp_names.len > 1) try bp.put(self.arena, bp_names[1], Value.int(@intCast(i)));
                        try self.block_params.append(self.arena, bp);
                    }
                    try self.contexts.append(self.arena, item);

                    var rendered = try self.evalProgram(&block.program.program);
                    rendered = applyInnerStrip(rendered, block.open_strip.close, block.close_strip.open);
                    try buf.appendSlice(self.arena, rendered);

                    _ = self.contexts.pop();
                    if (bp_names.len > 0) _ = self.block_params.pop();
                    self.popDataFrame();
                }
            },
            .map => |m| {
                if (m.count() == 0) {
                    if (block.inverse) |inv| return self.evalProgram(&inv.program);
                    return "";
                }
                const keys = m.keys();
                const values = m.values();
                const bp_names = block.program.program.block_params;

                for (keys, values, 0..) |k, v, i| {
                    try self.pushDataFrame(.{
                        .index = i,
                        .key = k,
                        .first = i == 0,
                        .last = i == keys.len - 1,
                    });
                    if (bp_names.len > 0) {
                        var bp: std.StringArrayHashMapUnmanaged(Value) = .{};
                        try bp.put(self.arena, bp_names[0], v);
                        if (bp_names.len > 1) try bp.put(self.arena, bp_names[1], Value.str(k));
                        try self.block_params.append(self.arena, bp);
                    }
                    try self.contexts.append(self.arena, v);

                    var rendered = try self.evalProgram(&block.program.program);
                    rendered = applyInnerStrip(rendered, block.open_strip.close, block.close_strip.open);
                    try buf.appendSlice(self.arena, rendered);

                    _ = self.contexts.pop();
                    if (bp_names.len > 0) _ = self.block_params.pop();
                    self.popDataFrame();
                }
            },
            else => {
                if (block.inverse) |inv| return self.evalProgram(&inv.program);
                return "";
            },
        }
        return buf.items;
    }

    fn evalWith(self: *Eval, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        const target = if (expr.params.len > 0)
            try self.evalExprParam(expr.params[0])
        else
            Value.undefined;

        if (target.isTruthy()) {
            try self.contexts.append(self.arena, target);

            // Block params: {{#with obj as |o|}}
            const bp_names = block.program.program.block_params;
            if (bp_names.len > 0) {
                var bp: std.StringArrayHashMapUnmanaged(Value) = .{};
                try bp.put(self.arena, bp_names[0], target);
                try self.block_params.append(self.arena, bp);
            }

            const result = try self.evalProgram(&block.program.program);

            if (bp_names.len > 0) _ = self.block_params.pop();
            _ = self.contexts.pop();
            return applyInnerStrip(result, block.open_strip.close, block.close_strip.open);
        } else if (block.inverse) |inv| {
            const result = try self.evalProgram(&inv.program);
            return applyInnerStrip(result, block.inverse_strip.close, block.close_strip.open);
        }
        return "";
    }

    fn evalEqual(self: *Eval, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        if (expr.params.len < 2) {
            if (block.inverse) |inv| return self.evalProgram(&inv.program);
            return "";
        }
        const a = try self.evalExprParam(expr.params[0]);
        const b = try self.evalExprParam(expr.params[1]);
        const a_str = try a.toString(self.arena);
        const b_str = try b.toString(self.arena);

        if (std.mem.eql(u8, a_str, b_str)) {
            return self.evalProgram(&block.program.program);
        } else if (block.inverse) |inv| {
            return self.evalProgram(&inv.program);
        }
        return "";
    }

    fn evalLog(self: *Eval, expr: *const ast.Expression) EvalError!Value {
        for (expr.params) |p| {
            const val = try self.evalExprParam(p);
            const text = try val.toString(self.arena);
            std.debug.print("{s} ", .{text});
        }
        std.debug.print("\n", .{});
        return Value.str("");
    }

    fn evalImplicitBlock(self: *Eval, val: Value, block: *const ast.Block) EvalError![]const u8 {
        switch (val) {
            .array => |arr| {
                // Iterate like each
                if (arr.len == 0) {
                    if (block.inverse) |inv| return self.evalProgram(&inv.program);
                    return "";
                }
                var buf: std.ArrayListUnmanaged(u8) = .empty;
                for (arr, 0..) |item, i| {
                    try self.pushDataFrame(.{ .index = i, .first = i == 0, .last = i == arr.len - 1 });
                    try self.contexts.append(self.arena, item);
                    try buf.appendSlice(self.arena, try self.evalProgram(&block.program.program));
                    _ = self.contexts.pop();
                    self.popDataFrame();
                }
                return buf.items;
            },
            else => {
                if (val.isTruthy()) {
                    try self.contexts.append(self.arena, val);
                    const result = try self.evalProgram(&block.program.program);
                    _ = self.contexts.pop();
                    return result;
                } else if (block.inverse) |inv| {
                    return self.evalProgram(&inv.program);
                }
                return "";
            },
        }
    }

    // ---------------------------------------------------------------
    // Expression evaluation
    // ---------------------------------------------------------------

    fn evalExpressionValue(self: *Eval, expr: *const ast.Expression) EvalError!Value {
        const name = self.getPathName(expr.path);

        // Check for registered helper (only for single-segment paths)
        if (name) |n| {
            if (self.helpers.get(n)) |helper| {
                return self.callHelper(helper, expr);
            }
            // Also treat as helper if it has params or hash but isn't a known path
            if (expr.params.len > 0 or expr.hash != null) {
                // Built-in value helpers
                if (std.mem.eql(u8, n, "lookup")) return self.evalLookup(expr);
                if (std.mem.eql(u8, n, "log")) return self.evalLog(expr);
            }
        }

        // Resolve as path
        return self.evalExprParam(expr.path);
    }

    fn evalExprParam(self: *Eval, node: *const ast.Node) EvalError!Value {
        return switch (node.*) {
            .path => |p| self.resolvePath(&p),
            .string_literal => |s| Value.str(s.value),
            .boolean_literal => |b| Value.bln(b.value),
            .number_literal => |n| if (n.is_int)
                Value.int(@intFromFloat(n.value))
            else
                Value.flt(n.value),
            .sub_expression => |s| self.evalExpressionValue(&s.expression.expression),
            else => .undefined,
        };
    }

    fn callHelper(self: *Eval, helper: Helper, expr: *const ast.Expression) EvalError!Value {
        var params: std.ArrayListUnmanaged(Value) = .empty;
        for (expr.params) |p| {
            try params.append(self.arena, try self.evalExprParam(p));
        }

        const hash = try self.evalHash(expr.hash);

        const ctx = HelperContext{
            .params = params.items,
            .hash = hash,
            .arena = self.arena,
            .userdata = helper.userdata,
        };

        return helper.func(ctx) catch .undefined;
    }

    fn callBlockHelper(self: *Eval, helper: Helper, expr: *const ast.Expression, block: *const ast.Block) EvalError![]const u8 {
        var params: std.ArrayListUnmanaged(Value) = .empty;
        for (expr.params) |p| {
            try params.append(self.arena, try self.evalExprParam(p));
        }

        const hash = try self.evalHash(expr.hash);

        const ctx = HelperContext{
            .params = params.items,
            .hash = hash,
            .arena = self.arena,
            .userdata = helper.userdata,
            .eval = self,
            .fn_body = block.program,
            .inverse_body = block.inverse,
        };

        const result = helper.func(ctx) catch Value.undefined;

        // If helper returned rendered content (string/safe_string), use directly.
        // This supports helpers that use fnContent/fnWith/inverseContent.
        // Otherwise fall back to truthiness-based block selection.
        switch (result) {
            .string, .safe_string => return try result.toString(self.arena),
            else => {
                if (result.isTruthy()) {
                    return self.evalProgram(&block.program.program);
                } else if (block.inverse) |inv| {
                    return self.evalProgram(&inv.program);
                }
                return "";
            },
        }
    }

    fn evalLookup(self: *Eval, expr: *const ast.Expression) EvalError!Value {
        if (expr.params.len < 2) return .undefined;
        const obj = try self.evalExprParam(expr.params[0]);
        const key = try self.evalExprParam(expr.params[1]);
        const key_str = try key.toString(self.arena);
        return resolveField(obj, key_str) orelse .undefined;
    }

    fn evalHash(self: *Eval, hash_node: ?*const ast.Node) EvalError!ValueMap {
        var hash: ValueMap = .{};
        if (hash_node) |h| {
            for (h.hash.pairs) |pair| {
                const hp = &pair.hash_pair;
                const val = try self.evalExprParam(hp.value);
                try hash.put(self.arena, hp.key, val);
            }
        }
        return hash;
    }

    // ---------------------------------------------------------------
    // Path resolution
    // ---------------------------------------------------------------

    fn resolvePath(self: *Eval, path: *const ast.Path) Value {
        // Data references (@index, @key, @root, etc.)
        if (path.data) {
            return self.resolveDataPath(path);
        }

        // "this" or "."
        if (path.parts.len == 0) {
            return self.currentContext(path.depth);
        }

        // Check block params first
        if (self.block_params.items.len > 0 and path.depth == 0) {
            var i = self.block_params.items.len;
            while (i > 0) {
                i -= 1;
                if (self.block_params.items[i].get(path.parts[0])) |bp_val| {
                    return resolveFieldPath(bp_val, path.parts[1..]);
                }
            }
        }

        // Explicit depth (../) - only try that context
        if (path.depth > 0) {
            const ctx = self.currentContext(path.depth);
            return resolveFieldPath(ctx, path.parts);
        }

        // Walk context stack looking for first part
        var depth: usize = 0;
        while (depth < self.contexts.items.len) : (depth += 1) {
            const ctx = self.currentContext(@intCast(depth));
            if (resolveField(ctx, path.parts[0])) |first_val| {
                if (first_val != .undefined) {
                    return resolveFieldPath(first_val, path.parts[1..]);
                }
            }
        }

        return .undefined;
    }

    fn resolveDataPath(self: *Eval, path: *const ast.Path) Value {
        if (path.parts.len == 0) return .undefined;

        const first = path.parts[0];

        // @root
        if (std.mem.eql(u8, first, "root")) {
            return resolveFieldPath(self.root_context, path.parts[1..]);
        }

        // @index, @key, @first, @last
        if (self.data_frames.items.len > 0) {
            const frame = &self.data_frames.items[self.data_frames.items.len - 1];

            if (std.mem.eql(u8, first, "index")) {
                if (frame.index) |idx| return Value.int(@intCast(idx));
            } else if (std.mem.eql(u8, first, "key")) {
                if (frame.key) |k| return Value.str(k);
            } else if (std.mem.eql(u8, first, "first")) {
                if (frame.first) |f| return Value.bln(f);
            } else if (std.mem.eql(u8, first, "last")) {
                if (frame.last) |l| return Value.bln(l);
            }
        }

        return .undefined;
    }

    fn currentContext(self: *Eval, depth: u32) Value {
        if (self.contexts.items.len == 0) return .undefined;
        const idx = self.contexts.items.len - 1;
        if (depth > idx) return .undefined;
        return self.contexts.items[idx - depth];
    }

    // ---------------------------------------------------------------
    // Stack management
    // ---------------------------------------------------------------

    fn pushDataFrame(self: *Eval, frame: DataFrame) EvalError!void {
        try self.data_frames.append(self.arena, frame);
    }

    fn popDataFrame(self: *Eval) void {
        if (self.data_frames.items.len > 0) _ = self.data_frames.pop();
    }

    // ---------------------------------------------------------------
    // Utilities
    // ---------------------------------------------------------------

    fn getPathName(self: *Eval, node: *const ast.Node) ?[]const u8 {
        _ = self;
        return switch (node.*) {
            .path => |p| if (p.parts.len == 1 and p.depth == 0) p.parts[0] else null,
            else => null,
        };
    }
};

// -----------------------------------------------------------------------
// Whitespace stripping
// -----------------------------------------------------------------------

/// Apply inner-block strip: open_strip.close strips leading WS from body,
/// close_strip.open strips trailing WS from body.
fn applyInnerStrip(s: []const u8, open_close: bool, close_open: bool) []const u8 {
    var result = s;
    if (open_close) {
        result = trimLeadingWhitespace(result);
    }
    if (close_open) {
        // Trim trailing whitespace
        var end = result.len;
        while (end > 0 and std.ascii.isWhitespace(result[end - 1])) end -= 1;
        result = result[0..end];
    }
    return result;
}

fn nodeStripBefore(node: *const ast.Node) bool {
    return switch (node.*) {
        .mustache => |m| m.strip.open,
        .block => |b| b.open_strip.open,
        .partial => |p| p.strip.open,
        .partial_block => |pb| pb.open_strip.open,
        .comment => |c| c.strip.open,
        else => false,
    };
}

fn nodeStripAfter(node: *const ast.Node) bool {
    return switch (node.*) {
        .mustache => |m| m.strip.close,
        .block => |b| b.close_strip.close,
        .partial => |p| p.strip.close,
        .partial_block => |pb| pb.close_strip.close,
        .comment => |c| c.strip.close,
        else => false,
    };
}

fn trimLeadingWhitespace(s: []const u8) []const u8 {
    var start: usize = 0;
    while (start < s.len and std.ascii.isWhitespace(s[start])) start += 1;
    return s[start..];
}

fn trimTrailingWhitespace(buf: *std.ArrayListUnmanaged(u8)) void {
    while (buf.items.len > 0 and std.ascii.isWhitespace(buf.items[buf.items.len - 1])) {
        _ = buf.pop();
    }
}

fn indentLines(arena: Allocator, input: []const u8, indent: []const u8) ![]const u8 {
    if (indent.len == 0 or input.len == 0) return input;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    var first = true;
    var iter = std.mem.splitScalar(u8, input, '\n');
    while (iter.next()) |line| {
        if (!first) try buf.append(arena, '\n');
        if (line.len > 0) {
            try buf.appendSlice(arena, indent);
            try buf.appendSlice(arena, line);
        }
        first = false;
    }
    return buf.items;
}

// -----------------------------------------------------------------------
// Field resolution (static)
// -----------------------------------------------------------------------

fn resolveField(val: Value, field: []const u8) ?Value {
    return switch (val) {
        .map => |m| m.get(field) orelse .undefined,
        .array => |arr| blk: {
            const idx = std.fmt.parseInt(usize, field, 10) catch break :blk Value.undefined;
            break :blk if (idx < arr.len) arr[idx] else Value.undefined;
        },
        else => .undefined,
    };
}

fn resolveFieldPath(val: Value, parts: []const []const u8) Value {
    var current = val;
    for (parts) |part| {
        current = resolveField(current, part) orelse return .undefined;
    }
    return current;
}

// -----------------------------------------------------------------------
// HTML escaping
// -----------------------------------------------------------------------

pub fn htmlEscape(arena: Allocator, input: []const u8) ![]const u8 {
    // Quick check if escaping needed
    var needs_escape = false;
    for (input) |ch| {
        if (ch == '&' or ch == '<' or ch == '>' or ch == '"' or ch == '\'' or ch == '`') {
            needs_escape = true;
            break;
        }
    }
    if (!needs_escape) return input;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    for (input) |ch| {
        switch (ch) {
            '&' => try buf.appendSlice(arena, "&amp;"),
            '<' => try buf.appendSlice(arena, "&lt;"),
            '>' => try buf.appendSlice(arena, "&gt;"),
            '"' => try buf.appendSlice(arena, "&quot;"),
            '\'' => try buf.appendSlice(arena, "&#x27;"),
            '`' => try buf.appendSlice(arena, "&#x60;"),
            else => try buf.append(arena, ch),
        }
    }
    return buf.items;
}

// -----------------------------------------------------------------------
// Value map builder
// -----------------------------------------------------------------------

pub fn mapFromEntries(arena: Allocator, entries: []const struct { []const u8, Value }) !Value {
    var m: ValueMap = .{};
    for (entries) |entry| {
        try m.put(arena, entry[0], entry[1]);
    }
    return .{ .map = m };
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

const Parser = @import("parser.zig").Parser;

fn testRender(arena: Allocator, template: []const u8, context: Value) ![]const u8 {
    const program = try Parser.parse(template, arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, context, &helpers, &partials);
    return eval_state.exec(program);
}

fn makeCtx(arena: Allocator, entries: anytype) !Value {
    var m: ValueMap = .{};
    inline for (@typeInfo(@TypeOf(entries)).@"struct".fields) |field| {
        const val = @field(entries, field.name);
        try m.put(arena, field.name, val);
    }
    return .{ .map = m };
}

test "eval simple variable" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const program = try Parser.parse("Hello, {{name}}!", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hello, World!", result);
}

test "eval dotted path" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user: ValueMap = .{};
    try user.put(arena, "name", Value.str("Alice"));
    try user.put(arena, "email", Value.str("alice@example.com"));

    var m: ValueMap = .{};
    try m.put(arena, "user", .{ .map = user });

    const program = try Parser.parse("{{user.name}} ({{user.email}})", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Alice (alice@example.com)", result);
}

test "eval if true" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "show", Value.bln(true));

    const program = try Parser.parse("{{#if show}}yes{{else}}no{{/if}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("yes", result);
}

test "eval if false" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "show", Value.bln(false));

    const program = try Parser.parse("{{#if show}}yes{{else}}no{{/if}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("no", result);
}

test "eval each array" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b"), Value.str("c") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const program = try Parser.parse("{{#each items}}{{this}},{{/each}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("a,b,c,", result);
}

test "eval each with @index" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("x"), Value.str("y") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const program = try Parser.parse("{{#each items}}{{@index}}:{{this}} {{/each}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("0:x 1:y ", result);
}

test "eval html escaping" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "html", Value.str("<b>bold</b>"));

    const program = try Parser.parse("{{html}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("&lt;b&gt;bold&lt;/b&gt;", result);
}

test "eval unescaped" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "html", Value.str("<b>bold</b>"));

    const program = try Parser.parse("{{{html}}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("<b>bold</b>", result);
}

test "eval missing variable" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};

    const program = try Parser.parse("Hello, {{name}}!", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hello, !", result);
}

test "eval unless" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "logged_in", Value.bln(false));

    const program = try Parser.parse("{{#unless logged_in}}Please log in{{/unless}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Please log in", result);
}

test "eval with" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var person: ValueMap = .{};
    try person.put(arena, "name", Value.str("Alice"));

    var m: ValueMap = .{};
    try m.put(arena, "person", .{ .map = person });

    const program = try Parser.parse("{{#with person}}{{name}}{{/with}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Alice", result);
}

test "eval integer value" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "count", Value.int(42));

    const program = try Parser.parse("Count: {{count}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Count: 42", result);
}

test "eval @root reference" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var inner: ValueMap = .{};
    try inner.put(arena, "x", Value.int(1));

    var m: ValueMap = .{};
    try m.put(arena, "title", Value.str("Root"));
    try m.put(arena, "inner", .{ .map = inner });

    const program = try Parser.parse("{{#with inner}}{{@root.title}}{{/with}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Root", result);
}

test "eval custom helper" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const eq = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const a = ctx.param(0);
            const b = ctx.param(1);
            const a_str = try a.toString(ctx.arena);
            const b_str = try b.toString(ctx.arena);
            return Value.bln(std.mem.eql(u8, a_str, b_str));
        }
    }.f;

    var helpers: HelperMap = .{};
    try helpers.put(arena, "eq", Helper.from(eq));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    var m: ValueMap = .{};
    try m.put(arena, "x", Value.str("hello"));

    const program = try Parser.parse("{{#if (eq x \"hello\")}}match{{else}}no{{/if}}", arena);
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("match", result);
}

test "eval safe_string bypasses escaping" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const media = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const url = ctx.hashStr("url");
            const result = try std.fmt.allocPrint(ctx.arena, "<<<media:{s}>>>", .{url});
            return Value.safeStr(result);
        }
    }.f;

    var helpers: HelperMap = .{};
    try helpers.put(arena, "media", Helper.from(media));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    var m: ValueMap = .{};
    try m.put(arena, "img", Value.str("data:image/png;base64,abc"));

    const program = try Parser.parse("{{media url=img}}", arena);
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    // safe_string should NOT be HTML-escaped
    try std.testing.expectEqualStrings("<<<media:data:image/png;base64,abc>>>", result);
}

test "eval each with nested objects" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user1: ValueMap = .{};
    try user1.put(arena, "name", Value.str("Alice"));
    var user2: ValueMap = .{};
    try user2.put(arena, "name", Value.str("Bob"));

    const items = [_]Value{ .{ .map = user1 }, .{ .map = user2 } };
    var m: ValueMap = .{};
    try m.put(arena, "users", .{ .array = &items });

    const program = try Parser.parse("{{#each users}}{{name}} {{/each}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Alice Bob ", result);
}

test "eval each map with @key" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var obj: ValueMap = .{};
    try obj.put(arena, "a", Value.str("1"));
    try obj.put(arena, "b", Value.str("2"));

    var m: ValueMap = .{};
    try m.put(arena, "obj", .{ .map = obj });

    const result = try testRender(arena,"{{#each obj}}{{@key}}={{this}} {{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("a=1 b=2 ", result);
}

test "eval each with @first and @last" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b"), Value.str("c") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena,"{{#each items}}{{#if @first}}[{{/if}}{{this}}{{#unless @last}},{{/unless}}{{#if @last}}]{{/if}}{{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("[a,b,c]", result);
}

test "eval array index access" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("alpha"), Value.str("beta"), Value.str("gamma") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena,"{{items.0}} and {{items.2}}", .{ .map = m });
    try std.testing.expectEqualStrings("alpha and gamma", result);
}

test "eval lookup helper" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var obj: ValueMap = .{};
    try obj.put(arena, "name", Value.str("found"));

    var m: ValueMap = .{};
    try m.put(arena, "obj", .{ .map = obj });
    try m.put(arena, "field", Value.str("name"));

    const result = try testRender(arena,"{{lookup obj field}}", .{ .map = m });
    try std.testing.expectEqualStrings("found", result);
}

test "eval implicit block truthy" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var user: ValueMap = .{};
    try user.put(arena, "name", Value.str("World"));

    var m: ValueMap = .{};
    try m.put(arena, "user", .{ .map = user });

    const result = try testRender(arena,"{{#user}}Hello {{name}}{{/user}}", .{ .map = m });
    try std.testing.expectEqualStrings("Hello World", result);
}

test "eval implicit block falsy" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "show", Value.bln(false));

    const result = try testRender(arena,"{{#show}}yes{{else}}no{{/show}}", .{ .map = m });
    try std.testing.expectEqualStrings("no", result);
}

test "eval else if chaining" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "a", Value.bln(false));
    try m.put(arena, "b", Value.bln(true));

    const result = try testRender(arena,"{{#if a}}A{{else if b}}B{{else}}C{{/if}}", .{ .map = m });
    try std.testing.expectEqualStrings("B", result);
}

test "eval else if chaining fallthrough" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "a", Value.bln(false));
    try m.put(arena, "b", Value.bln(false));

    const result = try testRender(arena,"{{#if a}}A{{else if b}}B{{else}}C{{/if}}", .{ .map = m });
    try std.testing.expectEqualStrings("C", result);
}

test "eval ampersand unescaped" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "html", Value.str("<b>bold</b>"));

    const result = try testRender(arena,"{{&html}}", .{ .map = m });
    try std.testing.expectEqualStrings("<b>bold</b>", result);
}

test "eval partial" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const program = try Parser.parse("Hello {{> greeting}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "greeting", "{{name}}!");

    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);
    try std.testing.expectEqualStrings("Hello World!", result);
}

test "eval equal block helper" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "status", Value.str("active"));

    const result = try testRender(arena,"{{#equal status \"active\"}}yes{{else}}no{{/equal}}", .{ .map = m });
    try std.testing.expectEqualStrings("yes", result);
}

test "eval equal block helper mismatch" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "status", Value.str("inactive"));

    const result = try testRender(arena,"{{#equal status \"active\"}}yes{{else}}no{{/equal}}", .{ .map = m });
    try std.testing.expectEqualStrings("no", result);
}

test "eval parent ref in each" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b") };
    var m: ValueMap = .{};
    try m.put(arena, "title", Value.str("List"));
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena,"{{#each items}}{{../title}}: {{this}} {{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("List: a List: b ", result);
}

test "eval each empty array inverse" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{};
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena,"{{#each items}}{{this}}{{else}}empty{{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("empty", result);
}

test "eval with else" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const result = try testRender(arena, "{{#with missing}}found{{else}}not found{{/with}}", .{ .map = m });
    try std.testing.expectEqualStrings("not found", result);
}

test "eval block params in each" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("x"), Value.str("y") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena,"{{#each items as |item idx|}}{{idx}}:{{item}} {{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("0:x 1:y ", result);
}

test "eval comment produces no output" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const program = try Parser.parse("before{{! comment }}after", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("beforeafter", result);
}

// -----------------------------------------------------------------------
// Whitespace control (~) tests
// -----------------------------------------------------------------------

test "eval whitespace strip before mustache" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const program = try Parser.parse("Hello   {{~name}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("HelloWorld", result);
}

test "eval whitespace strip after mustache" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("Hello"));

    const program = try Parser.parse("{{name~}}   World", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("HelloWorld", result);
}

test "eval whitespace strip both sides" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("X"));

    const program = try Parser.parse("A   {{~name~}}   B", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("AXB", result);
}

test "eval whitespace strip on block outer" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "show", .{ .boolean = true });
    try m.put(arena, "name", Value.str("X"));

    // Test outer strip: {{~#if strips before, {{~/if~}} strips after
    const program = try Parser.parse("A   {{~#if show}}{{name}}{{~/if~}}   B", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("AXB", result);
}

// -----------------------------------------------------------------------
// Bracket notation tests
// -----------------------------------------------------------------------

test "eval bracket notation for special keys" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "special-key", Value.str("works"));

    const result = try testRender(arena, "{{[special-key]}}", .{ .map = m });
    try std.testing.expectEqualStrings("works", result);
}

test "eval bracket notation nested path" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var inner: ValueMap = .{};
    try inner.put(arena, "my-val", Value.str("hello"));
    var m: ValueMap = .{};
    try m.put(arena, "obj", .{ .map = inner });

    const result = try testRender(arena, "{{obj.[my-val]}}", .{ .map = m });
    try std.testing.expectEqualStrings("hello", result);
}

// -----------------------------------------------------------------------
// Partial context and hash tests
// -----------------------------------------------------------------------

test "eval partial with context param" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var person: ValueMap = .{};
    try person.put(arena, "name", Value.str("Alice"));
    var m: ValueMap = .{};
    try m.put(arena, "person", .{ .map = person });

    const program = try Parser.parse("{{> greeting person}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "greeting", "Hello, {{name}}!");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hello, Alice!", result);
}

test "eval partial with hash params as context" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const program = try Parser.parse("{{> greeting name=\"Bob\"}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "greeting", "Hello, {{name}}!");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hello, Bob!", result);
}

// -----------------------------------------------------------------------
// Dynamic partial tests
// -----------------------------------------------------------------------

test "eval dynamic partial via sub-expression" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "tmplName", Value.str("greeting"));

    const lookup = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const name = ctx.param(0);
            return switch (name) {
                .string => |s| Value.str(s),
                else => Value.str(""),
            };
        }
    }.f;

    const program = try Parser.parse("{{> (lookup tmplName)}}", arena);
    var helpers: HelperMap = .{};
    try helpers.put(arena, "lookup", Helper.from(lookup));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "greeting", "Hi there!");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hi there!", result);
}

// -----------------------------------------------------------------------
// Partial indentation tests
// -----------------------------------------------------------------------

test "eval indentLines utility" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const result = try indentLines(arena, "line1\nline2\nline3", "  ");
    try std.testing.expectEqualStrings("  line1\n  line2\n  line3", result);

    // Empty lines should not get indent
    const result2 = try indentLines(arena, "a\n\nb", "  ");
    try std.testing.expectEqualStrings("  a\n\n  b", result2);

    // Empty input
    const result3 = try indentLines(arena, "", "  ");
    try std.testing.expectEqualStrings("", result3);
}

// -----------------------------------------------------------------------
// Block helper fnContent/fnWith/inverseContent tests
// -----------------------------------------------------------------------

test "eval block helper fnContent" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const bold = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const body = try ctx.fnContent();
            const result = try std.fmt.allocPrint(ctx.arena, "<b>{s}</b>", .{body});
            return .{ .safe_string = result };
        }
    }.f;

    const program = try Parser.parse("{{#bold}}Hello {{name}}{{/bold}}", arena);
    var helpers: HelperMap = .{};
    try helpers.put(arena, "bold", Helper.from(bold));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("<b>Hello World</b>", result);
}

test "eval block helper fnWith custom context" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};

    const withName = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            var inner: ValueMap = .{};
            try inner.put(ctx.arena, "name", Value.str("injected"));
            const body = try ctx.fnWith(.{ .map = inner });
            return .{ .safe_string = body };
        }
    }.f;

    const program = try Parser.parse("{{#withName}}name={{name}}{{/withName}}", arena);
    var helpers: HelperMap = .{};
    try helpers.put(arena, "withName", Helper.from(withName));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("name=injected", result);
}

test "eval block helper inverseContent" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};

    const myunless = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const cond = ctx.param(0);
            if (!cond.isTruthy()) {
                const body = try ctx.fnContent();
                return .{ .safe_string = body };
            } else {
                const inv = try ctx.inverseContent();
                return .{ .safe_string = inv };
            }
        }
    }.f;

    const program = try Parser.parse("{{#myunless true}}main{{else}}inverse{{/myunless}}", arena);
    var helpers: HelperMap = .{};
    try helpers.put(arena, "myunless", Helper.from(myunless));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("inverse", result);
}

test "eval block helper fnWithData" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};

    const mydata = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            var inner: ValueMap = .{};
            try inner.put(ctx.arena, "val", Value.str("ok"));
            const frame = DataFrame{ .key = "mykey" };
            const body = try ctx.fnWithData(.{ .map = inner }, frame);
            return .{ .safe_string = body };
        }
    }.f;

    const program = try Parser.parse("{{#mydata}}{{val}}-{{@key}}{{/mydata}}", arena);
    var helpers: HelperMap = .{};
    try helpers.put(arena, "mydata", Helper.from(mydata));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("ok-mykey", result);
}

// -----------------------------------------------------------------------
// Value.from() tests
// -----------------------------------------------------------------------

test "Value.from string" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, "hello");
    try std.testing.expectEqualStrings("hello", v.string);
}

test "Value.from bool" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, true);
    try std.testing.expectEqual(true, v.boolean);
}

test "Value.from integer" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, @as(i64, 42));
    try std.testing.expectEqual(@as(i64, 42), v.integer);
}

test "Value.from float" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, @as(f64, 3.14));
    try std.testing.expectEqual(@as(f64, 3.14), v.float);
}

test "Value.from optional null" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, @as(?[]const u8, null));
    try std.testing.expectEqual(Value.null, v);
}

test "Value.from optional value" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, @as(?[]const u8, "hello"));
    try std.testing.expectEqualStrings("hello", v.string);
}

test "Value.from struct" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, .{
        .name = "Alice",
        .age = @as(i64, 30),
        .active = true,
    });
    try std.testing.expect(v == .map);
    try std.testing.expectEqualStrings("Alice", v.map.get("name").?.string);
    try std.testing.expectEqual(@as(i64, 30), v.map.get("age").?.integer);
    try std.testing.expectEqual(true, v.map.get("active").?.boolean);
}

test "Value.from nested struct" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const v = try Value.from(arena, .{
        .user = .{
            .name = "Bob",
            .email = "bob@example.com",
        },
    });
    const user = v.map.get("user").?.map;
    try std.testing.expectEqualStrings("Bob", user.get("name").?.string);
    try std.testing.expectEqualStrings("bob@example.com", user.get("email").?.string);
}

test "Value.from slice" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_][]const u8{ "a", "b", "c" };
    const v = try Value.from(arena, @as([]const []const u8, &items));
    try std.testing.expectEqual(@as(usize, 3), v.array.len);
    try std.testing.expectEqualStrings("a", v.array[0].string);
    try std.testing.expectEqualStrings("c", v.array[2].string);
}

test "Value.from renders in template" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const ctx = try Value.from(arena, .{
        .name = "World",
        .count = @as(i64, 3),
    });

    const result = try testRender(arena, "Hello {{name}}, you have {{count}} items", ctx);
    try std.testing.expectEqualStrings("Hello World, you have 3 items", result);
}

test "Value.from struct with array renders in each" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("x"), Value.str("y") };
    const ctx = try Value.from(arena, .{
        .items = @as([]const Value, &items),
    });

    const result = try testRender(arena, "{{#each items}}{{this}}{{/each}}", ctx);
    try std.testing.expectEqualStrings("xy", result);
}

// -----------------------------------------------------------------------
// with block params test
// -----------------------------------------------------------------------

test "eval with block params" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var person: ValueMap = .{};
    try person.put(arena, "name", Value.str("Alice"));
    try person.put(arena, "age", Value.int(30));
    var m: ValueMap = .{};
    try m.put(arena, "person", .{ .map = person });

    const result = try testRender(arena, "{{#with person as |p|}}{{p.name}} is {{p.age}}{{/with}}", .{ .map = m });
    try std.testing.expectEqualStrings("Alice is 30", result);
}

// -----------------------------------------------------------------------
// Helper closure/userdata test
// -----------------------------------------------------------------------

test "eval helper with userdata closure" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var prefix: []const u8 = "PREFIX";

    const myHelper = struct {
        fn f(ctx: HelperContext) anyerror!Value {
            const p: *[]const u8 = @ptrCast(@alignCast(ctx.userdata.?));
            const val = try ctx.param(0).toString(ctx.arena);
            return Value.str(try std.fmt.allocPrint(ctx.arena, "{s}:{s}", .{ p.*, val }));
        }
    }.f;

    var helpers: HelperMap = .{};
    try helpers.put(arena, "prefixed", Helper.withData(myHelper, @ptrCast(&prefix)));
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("hello"));

    const program = try Parser.parse("{{prefixed name}}", arena);
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("PREFIX:hello", result);
}

// -----------------------------------------------------------------------
// Inner-block whitespace strip tests
// -----------------------------------------------------------------------

test "eval inner strip on if block" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "show", .{ .boolean = true });

    // {{#if show~}} strips leading WS inside block, {{~/if}} strips trailing WS inside
    const program = try Parser.parse("{{#if show~}}  \n  content  \n  {{~/if}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("content", result);
}

test "eval inner strip on each block" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b") };
    var m: ValueMap = .{};
    try m.put(arena, "items", .{ .array = &items });

    // open_strip.close strips leading WS, close_strip.open strips trailing WS per iteration
    const result = try testRender(arena, "{{#each items~}} {{this}} {{~/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("ab", result);

    // Only strip one side
    const result2 = try testRender(arena, "{{#each items~}} {{this}},{{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("a,b,", result2);
}

// -----------------------------------------------------------------------
// Standalone partial indent tests
// -----------------------------------------------------------------------

test "eval standalone partial indent" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    const program = try Parser.parse("begin\n  {{> mypartial}}\nend", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "mypartial", "line1\nline2");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("begin\n  line1\n  line2\nend", result);
}

test "eval non-standalone partial no indent" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    // Not standalone — text before partial on same line
    const program = try Parser.parse("x {{> mypartial}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "mypartial", "hello");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("x hello", result);
}

// -----------------------------------------------------------------------
// Inline partial tests
// -----------------------------------------------------------------------

test "eval inline partial" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "name", Value.str("World"));

    const tmpl = "{{#*inline \"greeting\"}}Hello, {{name}}!{{/inline}}{{> greeting}}";
    const program = try Parser.parse(tmpl, arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("Hello, World!", result);
}

test "eval inline partial overrides registered partial" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};

    const tmpl = "{{#*inline \"msg\"}}inline version{{/inline}}{{> msg}}";
    const program = try Parser.parse(tmpl, arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "msg", "registered version");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("inline version", result);
}

// -----------------------------------------------------------------------
// Partial block tests
// -----------------------------------------------------------------------

test "eval partial block with found partial" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var m: ValueMap = .{};
    try m.put(arena, "title", Value.str("My Page"));

    // layout partial references @partial-block for the caller's content
    const program = try Parser.parse("{{#> layout}}page content{{/layout}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    try partials.put(arena, "layout", "<h1>{{title}}</h1><main>{{> @partial-block}}</main>");
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("<h1>My Page</h1><main>page content</main>", result);
}

test "eval partial block fallback when partial missing" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const m: ValueMap = .{};
    // No "missing" partial registered — should render default body
    const program = try Parser.parse("{{#> missing}}fallback content{{/missing}}", arena);
    var helpers: HelperMap = .{};
    var partials: std.StringArrayHashMapUnmanaged([]const u8) = .{};
    var eval_state = Eval.init(arena, .{ .map = m }, &helpers, &partials);
    const result = try eval_state.exec(program);

    try std.testing.expectEqualStrings("fallback content", result);
}

// -----------------------------------------------------------------------
// @root data variable tests
// -----------------------------------------------------------------------

test "eval @root in nested context" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var inner: ValueMap = .{};
    try inner.put(arena, "val", Value.str("inner"));
    var m: ValueMap = .{};
    try m.put(arena, "title", Value.str("Root Title"));
    try m.put(arena, "obj", .{ .map = inner });

    const result = try testRender(arena, "{{#with obj}}{{@root.title}}-{{val}}{{/with}}", .{ .map = m });
    try std.testing.expectEqualStrings("Root Title-inner", result);
}

test "eval @root in each" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const items = [_]Value{ Value.str("x") };
    var m: ValueMap = .{};
    try m.put(arena, "sep", Value.str("|"));
    try m.put(arena, "items", .{ .array = &items });

    const result = try testRender(arena, "{{#each items}}{{@root.sep}}{{this}}{{/each}}", .{ .map = m });
    try std.testing.expectEqualStrings("|x", result);
}
