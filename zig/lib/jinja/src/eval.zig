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

// Jinja2 evaluator — renders AST with a context.
//
// Evaluates the HuggingFace chat template subset: variables, string concat,
// conditionals, for loops, filters, subscript/dot access, etc.

const std = @import("std");
const ast = @import("ast.zig");
const parser_mod = @import("parser.zig");

// --- Value types ---

pub const ValueMap = std.StringArrayHashMapUnmanaged(Value);

pub const MacroValue = struct {
    name: []const u8,
    params: []const ast.MacroParam,
    body: []const *ast.Node,
};

pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    none,
    list: []const Value,
    map: ValueMap,
    macro: MacroValue,
    undefined,

    pub fn isTruthy(self: Value) bool {
        return switch (self) {
            .string => |s| s.len > 0,
            .integer => |i| i != 0,
            .float => |f| f != 0.0,
            .boolean => |b| b,
            .none, .undefined => false,
            .list => |l| l.len > 0,
            .map => |m| m.count() > 0,
            .macro => true,
        };
    }

    pub fn eql(a: Value, b: Value) bool {
        const a_tag = std.meta.activeTag(a);
        const b_tag = std.meta.activeTag(b);
        if (a_tag != b_tag) {
            // Allow integer/float comparison
            if (a_tag == .integer and b_tag == .float) return @as(f64, @floatFromInt(a.integer)) == b.float;
            if (a_tag == .float and b_tag == .integer) return a.float == @as(f64, @floatFromInt(b.integer));
            return false;
        }
        return switch (a) {
            .string => |s| std.mem.eql(u8, s, b.string),
            .integer => |i| i == b.integer,
            .float => |f| f == b.float,
            .boolean => |bl| bl == b.boolean,
            .none => true,
            .undefined => true,
            .list, .map, .macro => false,
        };
    }

    pub fn str(s: []const u8) Value {
        return .{ .string = s };
    }

    pub fn int(i: i64) Value {
        return .{ .integer = i };
    }

    pub fn bln(b: bool) Value {
        return .{ .boolean = b };
    }
};

// --- Evaluator ---

pub const Eval = struct {
    arena: std.mem.Allocator,
    scopes: [16]?*ValueMap,
    scope_depth: usize,
    output: std.ArrayListUnmanaged(u8),
    /// Pending whitespace strip: if true, the next text node has leading whitespace stripped.
    strip_next: bool,
    /// Fence position: stripTrailingWhitespace will not strip past this point.
    /// Updated after expression output to protect rendered content from being stripped.
    strip_fence: usize,

    pub fn init(arena: std.mem.Allocator, context: *ValueMap) Eval {
        var e = Eval{
            .arena = arena,
            .scopes = .{null} ** 16,
            .scope_depth = 1,
            .output = std.ArrayListUnmanaged(u8).empty,
            .strip_next = false,
            .strip_fence = 0,
        };
        e.scopes[0] = context;
        return e;
    }

    pub fn exec(self: *Eval, nodes: []const *ast.Node) ![]const u8 {
        for (nodes) |node| {
            try self.execNode(node);
        }
        return self.output.toOwnedSlice(self.arena);
    }

    fn execNode(self: *Eval, node: *const ast.Node) !void {
        switch (node.*) {
            .text => |text| {
                var t = text;
                if (self.strip_next) {
                    self.strip_next = false;
                    t = std.mem.trimStart(u8, t, &.{ ' ', '\t', '\n', '\r' });
                }
                try self.output.appendSlice(self.arena, t);
            },
            .output => |out| {
                if (out.strip_left) {
                    self.stripTrailingWhitespace();
                }
                const val = try self.evalExpr(out.expr);
                const s = try self.valueToString(val);
                try self.output.appendSlice(self.arena, s);
                // Protect expression output from being stripped by subsequent {%-
                self.strip_fence = self.output.items.len;
                if (out.strip_right) {
                    self.strip_next = true;
                }
            },
            .if_stmt => |if_stmt| {
                // Always apply the opening if-tag's strip_left
                if (if_stmt.branches.len > 0 and if_stmt.branches[0].open_strip_left) {
                    self.stripTrailingWhitespace();
                }
                var matched = false;
                for (if_stmt.branches) |branch| {
                    const should_exec = if (branch.condition) |cond|
                        (try self.evalExpr(cond)).isTruthy()
                    else
                        true; // else branch
                    if (should_exec) {
                        if (branch.open_strip_left) self.stripTrailingWhitespace();
                        if (branch.open_strip_right) self.strip_next = true;
                        for (branch.body) |child| try self.execNode(child);
                        if (branch.close_strip_left) self.stripTrailingWhitespace();
                        if (branch.close_strip_right) self.strip_next = true;
                        matched = true;
                        break;
                    }
                }
                if (!matched and if_stmt.branches.len > 0) {
                    const last = if_stmt.branches[if_stmt.branches.len - 1];
                    if (last.close_strip_left) self.stripTrailingWhitespace();
                    if (last.close_strip_right) self.strip_next = true;
                }
            },
            .for_stmt => |for_stmt| {
                if (for_stmt.open_strip_left) self.stripTrailingWhitespace();
                if (for_stmt.open_strip_right) self.strip_next = true;

                const iterable = try self.evalExpr(for_stmt.iterable);
                const items = switch (iterable) {
                    .list => |l| l,
                    else => {
                        if (for_stmt.close_strip_left) self.stripTrailingWhitespace();
                        if (for_stmt.close_strip_right) self.strip_next = true;
                        return;
                    },
                };

                if (items.len == 0) {
                    for (for_stmt.else_body) |child| try self.execNode(child);
                    if (for_stmt.close_strip_left) self.stripTrailingWhitespace();
                    if (for_stmt.close_strip_right) self.strip_next = true;
                    return;
                }

                // Push scope for loop variable + loop object
                var scope = ValueMap{};
                self.pushScope(&scope);
                defer self.popScope();

                for (items, 0..) |item, i| {
                    // Tuple unpacking: for key, value in list_of_pairs
                    if (for_stmt.value_name) |val_name| {
                        switch (item) {
                            .list => |pair| {
                                if (pair.len >= 1) scope.put(self.arena, for_stmt.var_name, pair[0]) catch return;
                                if (pair.len >= 2) scope.put(self.arena, val_name, pair[1]) catch return;
                            },
                            else => {
                                scope.put(self.arena, for_stmt.var_name, item) catch return;
                                scope.put(self.arena, val_name, .undefined) catch return;
                            },
                        }
                    } else {
                        scope.put(self.arena, for_stmt.var_name, item) catch return;
                    }

                    // Build loop object
                    var loop_map = ValueMap{};
                    loop_map.put(self.arena, "index", Value.int(@intCast(i + 1))) catch return;
                    loop_map.put(self.arena, "index0", Value.int(@intCast(i))) catch return;
                    loop_map.put(self.arena, "first", Value.bln(i == 0)) catch return;
                    loop_map.put(self.arena, "last", Value.bln(i == items.len - 1)) catch return;
                    loop_map.put(self.arena, "length", Value.int(@intCast(items.len))) catch return;
                    loop_map.put(self.arena, "revindex", Value.int(@intCast(items.len - i))) catch return;
                    loop_map.put(self.arena, "revindex0", Value.int(@intCast(items.len - i - 1))) catch return;
                    scope.put(self.arena, "loop", .{ .map = loop_map }) catch return;

                    for (for_stmt.body) |child| try self.execNode(child);
                }
                if (for_stmt.close_strip_left) self.stripTrailingWhitespace();
                if (for_stmt.close_strip_right) self.strip_next = true;
            },
            .set_stmt => |set| {
                if (set.strip_left) self.stripTrailingWhitespace();
                const val = try self.evalExpr(set.value);
                if (set.attr) |attr| {
                    // Dotted assignment: set ns.attr = value
                    if (self.lookupMutablePtr(set.name)) |target_ptr| {
                        if (target_ptr.* == .map) {
                            try target_ptr.map.put(self.arena, attr, val);
                        }
                    }
                } else {
                    // Simple assignment: set in the nearest scope
                    if (self.scope_depth > 0) {
                        if (self.scopes[self.scope_depth - 1]) |scope| {
                            try scope.put(self.arena, set.name, val);
                        }
                    }
                }
                if (set.strip_right) self.strip_next = true;
            },
            .macro_stmt => |mac| {
                if (mac.strip_left) self.stripTrailingWhitespace();
                // Register macro in current scope
                const macro_val = Value{ .macro = .{
                    .name = mac.name,
                    .params = mac.params,
                    .body = mac.body,
                } };
                if (self.scope_depth > 0) {
                    if (self.scopes[self.scope_depth - 1]) |scope| {
                        try scope.put(self.arena, mac.name, macro_val);
                    }
                }
                if (mac.close_strip_right) self.strip_next = true;
            },
        }
    }

    fn evalExpr(self: *Eval, expr: *const ast.Expr) !Value {
        switch (expr.*) {
            .literal => |lit| return switch (lit) {
                .string => |s| Value.str(try self.processEscapes(s)),
                .integer => |i| Value.int(i),
                .float => |f| .{ .float = f },
                .boolean => |b| Value.bln(b),
                .none => .none,
            },
            .name => |name| return self.lookup(name),
            .get_attr => |ga| {
                const obj = try self.evalExpr(ga.obj);
                return self.getAttr(obj, ga.attr);
            },
            .get_item => |gi| {
                const obj = try self.evalExpr(gi.obj);
                const key = try self.evalExpr(gi.key);
                return self.getItem(obj, key);
            },
            .slice => |sl| {
                const obj = try self.evalExpr(sl.obj);
                return self.evalSlice(obj, sl);
            },
            .bin_op => |bo| {
                const left = try self.evalExpr(bo.left);
                const right = try self.evalExpr(bo.right);
                return self.evalBinOp(bo.op, left, right);
            },
            .unary_op => |uo| {
                const val = try self.evalExpr(uo.operand);
                return switch (uo.op) {
                    .not => Value.bln(!val.isTruthy()),
                    .neg => switch (val) {
                        .integer => |i| Value.int(-i),
                        .float => |f| .{ .float = -f },
                        else => .undefined,
                    },
                    .is_defined => Value.bln(val != .undefined),
                    .is_not_defined => Value.bln(val == .undefined),
                    .is_string => Value.bln(val == .string),
                    .is_iterable => Value.bln(switch (val) {
                        .list, .map, .string => true,
                        else => false,
                    }),
                    .is_mapping => Value.bln(val == .map),
                    .is_sequence => Value.bln(val == .list or val == .string),
                    .is_none => Value.bln(val == .none),
                    .is_not_none => Value.bln(val != .none),
                    .is_boolean => Value.bln(val == .boolean),
                };
            },
            .call => |c| {
                // Method calls: obj.method(args)
                if (c.func.* == .get_attr) {
                    const obj = try self.evalExpr(c.func.get_attr.obj);
                    return try self.evalMethodCall(obj, c.func.get_attr.attr, c.args);
                }

                // Evaluate the function expression
                const func_val = try self.evalExpr(c.func);

                // Macro calls
                if (func_val == .macro) {
                    return try self.callMacro(func_val.macro, c.args, c.kwargs);
                }

                // raise_exception and other unknown functions are no-ops
                return .undefined;
            },
            .filter => |f| {
                const val = try self.evalExpr(f.expr);
                return self.applyFilter(val, f.name, f.args);
            },
            .list => |items| {
                const vals = try self.arena.alloc(Value, items.len);
                for (items, 0..) |item, i| {
                    vals[i] = try self.evalExpr(item);
                }
                return .{ .list = vals };
            },
            .dict => |pairs| {
                var map = ValueMap{};
                for (pairs) |pair| {
                    const key = try self.evalExpr(pair.key);
                    const value = try self.evalExpr(pair.value);
                    if (key == .string) {
                        try map.put(self.arena, key.string, value);
                    }
                }
                return .{ .map = map };
            },
            .inline_if => |iif| {
                const cond = try self.evalExpr(iif.condition);
                if (cond.isTruthy()) {
                    return self.evalExpr(iif.true_expr);
                } else if (iif.false_expr) |fe| {
                    return self.evalExpr(fe);
                }
                return .undefined;
            },
        }
    }

    fn evalBinOp(self: *Eval, op: ast.BinOp, left: Value, right: Value) !Value {
        switch (op) {
            .eq => return Value.bln(left.eql(right)),
            .ne => return Value.bln(!left.eql(right)),
            .@"and" => return if (left.isTruthy()) right else left,
            .@"or" => return if (left.isTruthy()) left else right,
            .in_op => return Value.bln(self.evalIn(left, right)),
            .not_in => return Value.bln(!self.evalIn(left, right)),
            .lt, .gt, .le, .ge => {
                const cmp = self.compareValues(left, right) orelse return .undefined;
                return Value.bln(switch (op) {
                    .lt => cmp < 0,
                    .gt => cmp > 0,
                    .le => cmp <= 0,
                    .ge => cmp >= 0,
                    else => unreachable,
                });
            },
            .add => {
                // String concatenation or numeric addition
                if (left == .string or right == .string) {
                    const ls = try self.valueToString(left);
                    const rs = try self.valueToString(right);
                    const result = try std.fmt.allocPrint(self.arena, "{s}{s}", .{ ls, rs });
                    return Value.str(result);
                }
                return self.numericOp(left, right, .add);
            },
            .sub => return self.numericOp(left, right, .sub),
            .mul => return self.numericOp(left, right, .mul),
            .div => return self.numericOp(left, right, .div),
            .mod => return self.numericOp(left, right, .mod),
            .concat => {
                const ls = try self.valueToString(left);
                const rs = try self.valueToString(right);
                const result = try std.fmt.allocPrint(self.arena, "{s}{s}", .{ ls, rs });
                return Value.str(result);
            },
        }
    }

    fn numericOp(_: *Eval, left: Value, right: Value, op: ast.BinOp) !Value {
        // Integer math
        if (left == .integer and right == .integer) {
            const a = left.integer;
            const b = right.integer;
            return Value.int(switch (op) {
                .add => a + b,
                .sub => a - b,
                .mul => a * b,
                .div => if (b != 0) @divTrunc(a, b) else 0,
                .mod => if (b != 0) @mod(a, b) else 0,
                else => 0,
            });
        }
        return .undefined;
    }

    /// Evaluate the "in" containment test: left in right.
    /// Supports: value in list, substring in string, key in map.
    fn evalIn(_: *Eval, left: Value, right: Value) bool {
        switch (right) {
            .list => |l| {
                for (l) |item| {
                    if (left.eql(item)) return true;
                }
                return false;
            },
            .string => |s| {
                if (left == .string) {
                    return std.mem.indexOf(u8, s, left.string) != null;
                }
                return false;
            },
            .map => |m| {
                if (left == .string) {
                    return m.get(left.string) != null;
                }
                return false;
            },
            else => return false,
        }
    }

    fn compareValues(_: *Eval, a: Value, b: Value) ?i32 {
        if (a == .integer and b == .integer) {
            if (a.integer < b.integer) return -1;
            if (a.integer > b.integer) return 1;
            return 0;
        }
        if (a == .string and b == .string) {
            const order = std.mem.order(u8, a.string, b.string);
            return switch (order) {
                .lt => @as(i32, -1),
                .gt => @as(i32, 1),
                .eq => @as(i32, 0),
            };
        }
        return null;
    }

    fn getAttr(_: *Eval, obj: Value, attr: []const u8) Value {
        return switch (obj) {
            .map => |m| m.get(attr) orelse .undefined,
            .string => |s| {
                if (std.mem.eql(u8, attr, "length")) return Value.int(@intCast(s.len));
                return .undefined;
            },
            .list => |l| {
                if (std.mem.eql(u8, attr, "length")) return Value.int(@intCast(l.len));
                return .undefined;
            },
            else => .undefined,
        };
    }

    fn getItem(self: *Eval, obj: Value, key: Value) Value {
        switch (obj) {
            .map => |m| {
                if (key == .string) return m.get(key.string) orelse .undefined;
                return .undefined;
            },
            .list => |l| {
                if (key == .integer) {
                    const idx = key.integer;
                    const uidx: usize = if (idx < 0)
                        if (@as(usize, @intCast(-idx)) <= l.len) l.len - @as(usize, @intCast(-idx)) else return .undefined
                    else
                        @intCast(idx);
                    if (uidx < l.len) return l[uidx];
                }
                return .undefined;
            },
            .string => |s| {
                if (key == .integer) {
                    const idx = key.integer;
                    const uidx: usize = if (idx < 0)
                        if (@as(usize, @intCast(-idx)) <= s.len) s.len - @as(usize, @intCast(-idx)) else return .undefined
                    else
                        @intCast(idx);
                    if (uidx < s.len) {
                        const c = self.arena.alloc(u8, 1) catch return .undefined;
                        c[0] = s[uidx];
                        return Value.str(c);
                    }
                }
                return .undefined;
            },
            else => return .undefined,
        }
    }

    fn evalSlice(self: *Eval, obj: Value, sl: ast.Slice) anyerror!Value {
        const items = switch (obj) {
            .list => |l| l,
            else => return .undefined,
        };

        const len: i64 = @intCast(items.len);
        var start: i64 = 0;
        var stop: i64 = len;

        if (sl.start) |s| {
            const sv = try self.evalExpr(s);
            if (sv == .integer) start = sv.integer;
        }
        if (sl.stop) |s| {
            const sv = try self.evalExpr(s);
            if (sv == .integer) stop = sv.integer;
        }

        // Normalize negative indices
        if (start < 0) start = @max(0, len + start);
        if (stop < 0) stop = @max(0, len + stop);
        start = @min(start, len);
        stop = @min(stop, len);
        if (start >= stop) return .{ .list = &.{} };

        return .{ .list = items[@intCast(start)..@intCast(stop)] };
    }

    fn applyFilter(self: *Eval, val: Value, name: []const u8, args: []const *ast.Expr) !Value {
        if (std.mem.eql(u8, name, "trim")) {
            if (val == .string) {
                return Value.str(std.mem.trim(u8, val.string, &.{ ' ', '\t', '\n', '\r' }));
            }
        }
        if (std.mem.eql(u8, name, "length")) {
            return switch (val) {
                .string => |s| Value.int(@intCast(s.len)),
                .list => |l| Value.int(@intCast(l.len)),
                .map => |m| Value.int(@intCast(m.count())),
                else => Value.int(0),
            };
        }
        if (std.mem.eql(u8, name, "lower")) {
            if (val == .string) {
                const s = try self.arena.dupe(u8, val.string);
                for (s) |*c| c.* = std.ascii.toLower(c.*);
                return Value.str(s);
            }
        }
        if (std.mem.eql(u8, name, "upper")) {
            if (val == .string) {
                const s = try self.arena.dupe(u8, val.string);
                for (s) |*c| c.* = std.ascii.toUpper(c.*);
                return Value.str(s);
            }
        }
        if (std.mem.eql(u8, name, "default") or std.mem.eql(u8, name, "d")) {
            if (val == .undefined or val == .none) {
                if (args.len > 0) return self.evalExpr(args[0]);
                return Value.str("");
            }
            return val;
        }
        if (std.mem.eql(u8, name, "first")) {
            if (val == .list and val.list.len > 0) return val.list[0];
        }
        if (std.mem.eql(u8, name, "last")) {
            if (val == .list and val.list.len > 0) return val.list[val.list.len - 1];
        }
        if (std.mem.eql(u8, name, "dictsort")) {
            if (val == .map) {
                const keys = val.map.keys();
                const values = val.map.values();
                // Build list of [key, value] pairs, sorted by key
                const pair_list = try self.arena.alloc(Value, keys.len);
                // Create index array for sorting
                const indices = try self.arena.alloc(usize, keys.len);
                for (0..keys.len) |i| indices[i] = i;
                // Sort indices by key
                std.mem.sort(usize, indices, keys, struct {
                    fn cmp(k: []const []const u8, a: usize, b: usize) bool {
                        return std.mem.order(u8, k[a], k[b]) == .lt;
                    }
                }.cmp);
                for (indices, 0..) |sorted_i, out_i| {
                    const pair = try self.arena.alloc(Value, 2);
                    pair[0] = Value.str(keys[sorted_i]);
                    pair[1] = values[sorted_i];
                    pair_list[out_i] = .{ .list = pair };
                }
                return .{ .list = pair_list };
            }
        }
        if (std.mem.eql(u8, name, "map")) {
            if (val == .list and args.len > 0) {
                const filter_name_val = try self.evalExpr(args[0]);
                if (filter_name_val == .string) {
                    const mapped = try self.arena.alloc(Value, val.list.len);
                    for (val.list, 0..) |item, i| {
                        mapped[i] = try self.applyFilter(item, filter_name_val.string, &.{});
                    }
                    return .{ .list = mapped };
                }
            }
        }
        if (std.mem.eql(u8, name, "list")) {
            return val; // already a list or pass through
        }
        if (std.mem.eql(u8, name, "join")) {
            if (val == .list) {
                var sep: []const u8 = "";
                if (args.len > 0) {
                    const sep_val = try self.evalExpr(args[0]);
                    if (sep_val == .string) sep = sep_val.string;
                }
                var result = std.ArrayListUnmanaged(u8).empty;
                for (val.list, 0..) |item, i| {
                    if (i > 0) try result.appendSlice(self.arena, sep);
                    try result.appendSlice(self.arena, try self.valueToString(item));
                }
                return Value.str(try result.toOwnedSlice(self.arena));
            }
        }
        return val; // unknown filter — pass through
    }

    // --- Macro calls ---

    fn callMacro(self: *Eval, mac: MacroValue, args: []const *ast.Expr, kwargs: []const ast.KwArg) anyerror!Value {
        var scope = ValueMap{};

        // Bind parameters: positional args first, then kwargs, then defaults
        for (mac.params, 0..) |param, pi| {
            if (pi < args.len) {
                try scope.put(self.arena, param.name, try self.evalExpr(args[pi]));
            } else {
                // Check kwargs
                var found = false;
                for (kwargs) |kw| {
                    if (std.mem.eql(u8, kw.key, param.name)) {
                        try scope.put(self.arena, param.name, try self.evalExpr(kw.value));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    if (param.default) |def| {
                        try scope.put(self.arena, param.name, try self.evalExpr(def));
                    } else {
                        try scope.put(self.arena, param.name, .undefined);
                    }
                }
            }
        }

        // Save output state — macro captures its own output
        const saved_output = self.output;
        const saved_fence = self.strip_fence;
        const saved_strip = self.strip_next;
        self.output = std.ArrayListUnmanaged(u8).empty;
        self.strip_fence = 0;
        self.strip_next = false;

        self.pushScope(&scope);
        for (mac.body) |node| try self.execNode(node);
        self.popScope();

        const result = try self.output.toOwnedSlice(self.arena);
        self.output = saved_output;
        self.strip_fence = saved_fence;
        self.strip_next = saved_strip;

        return Value.str(result);
    }

    // --- Method calls ---

    fn evalMethodCall(self: *Eval, obj: Value, method: []const u8, args: []const *ast.Expr) anyerror!Value {
        if (obj == .string) {
            if (std.mem.eql(u8, method, "split")) {
                if (args.len > 0) {
                    const sep_val = try self.evalExpr(args[0]);
                    if (sep_val == .string) {
                        return self.stringSplit(obj.string, sep_val.string);
                    }
                }
            }
        }
        return .undefined;
    }

    fn stringSplit(self: *Eval, s: []const u8, sep: []const u8) !Value {
        var parts = std.ArrayListUnmanaged(Value).empty;
        if (sep.len == 0) {
            try parts.append(self.arena, Value.str(s));
            return .{ .list = try parts.toOwnedSlice(self.arena) };
        }
        var start: usize = 0;
        var pos: usize = 0;
        while (pos + sep.len <= s.len) {
            if (std.mem.eql(u8, s[pos .. pos + sep.len], sep)) {
                try parts.append(self.arena, Value.str(s[start..pos]));
                pos += sep.len;
                start = pos;
            } else {
                pos += 1;
            }
        }
        try parts.append(self.arena, Value.str(s[start..]));
        return .{ .list = try parts.toOwnedSlice(self.arena) };
    }

    // --- Scope management ---

    fn lookup(self: *Eval, name: []const u8) Value {
        // Search scopes from innermost to outermost
        var i: usize = self.scope_depth;
        while (i > 0) {
            i -= 1;
            if (self.scopes[i]) |scope| {
                if (scope.get(name)) |val| return val;
            }
        }
        return .undefined;
    }

    /// Like lookup but returns a mutable pointer to the Value in the scope.
    /// Used for dotted assignment (set ns.attr = value) to modify maps in place.
    fn lookupMutablePtr(self: *Eval, name: []const u8) ?*Value {
        var i: usize = self.scope_depth;
        while (i > 0) {
            i -= 1;
            if (self.scopes[i]) |scope| {
                if (scope.getPtr(name)) |ptr| return ptr;
            }
        }
        return null;
    }

    fn pushScope(self: *Eval, scope: *ValueMap) void {
        if (self.scope_depth < self.scopes.len) {
            self.scopes[self.scope_depth] = scope;
            self.scope_depth += 1;
        }
    }

    fn popScope(self: *Eval) void {
        if (self.scope_depth > 0) {
            self.scope_depth -= 1;
            self.scopes[self.scope_depth] = null;
        }
    }

    // --- Output helpers ---

    fn valueToString(self: *Eval, val: Value) ![]const u8 {
        return switch (val) {
            .string => |s| s,
            .integer => |i| try std.fmt.allocPrint(self.arena, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(self.arena, "{d}", .{f}),
            .boolean => |b| if (b) "True" else "False",
            .none => "",
            .undefined => "",
            .list => "[list]",
            .map => "[map]",
            .macro => "",
        };
    }

    fn stripTrailingWhitespace(self: *Eval) void {
        // Only strip whitespace from template text, not from expression output.
        // strip_fence marks the end of the last expression output.
        while (self.output.items.len > self.strip_fence) {
            const last = self.output.items[self.output.items.len - 1];
            if (last == ' ' or last == '\t' or last == '\n' or last == '\r') {
                self.output.items.len -= 1;
            } else break;
        }
    }

    /// Process escape sequences in string literals (\\n → \n, \\t → \t, etc.)
    fn processEscapes(self: *Eval, s: []const u8) ![]const u8 {
        // Fast path: no escapes
        if (std.mem.indexOfScalar(u8, s, '\\') == null) return s;

        var result = std.ArrayListUnmanaged(u8).empty;
        var i: usize = 0;
        while (i < s.len) {
            if (s[i] == '\\' and i + 1 < s.len) {
                switch (s[i + 1]) {
                    'n' => try result.append(self.arena, '\n'),
                    't' => try result.append(self.arena, '\t'),
                    'r' => try result.append(self.arena, '\r'),
                    '\\' => try result.append(self.arena, '\\'),
                    '\'' => try result.append(self.arena, '\''),
                    '"' => try result.append(self.arena, '"'),
                    else => {
                        try result.append(self.arena, s[i]);
                        try result.append(self.arena, s[i + 1]);
                    },
                }
                i += 2;
            } else {
                try result.append(self.arena, s[i]);
                i += 1;
            }
        }
        return result.toOwnedSlice(self.arena);
    }
};

// --- Tests ---

test "eval simple variable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "name", Value.str("World"));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("Hello {{ name }}!", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("Hello World!", result);
}

test "eval if/else" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.bln(true));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{% if x %}yes{% else %}no{% endif %}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("yes", result);
}

test "eval for loop" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b"), Value.str("c") };
    var ctx = ValueMap{};
    try ctx.put(a, "items", .{ .list = &items });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{% for x in items %}{{ x }}{% endfor %}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("abc", result);
}

test "eval string concatenation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "name", Value.str("World"));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{{ 'Hello ' + name }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("Hello World", result);
}

test "eval dot access" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var msg = ValueMap{};
    try msg.put(a, "role", Value.str("user"));
    try msg.put(a, "content", Value.str("hello"));

    var ctx = ValueMap{};
    try ctx.put(a, "msg", .{ .map = msg });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{{ msg.role }}: {{ msg.content }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("user: hello", result);
}

test "eval subscript" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var msg = ValueMap{};
    try msg.put(a, "role", Value.str("user"));
    const msgs = [_]Value{.{ .map = msg }};

    var ctx = ValueMap{};
    try ctx.put(a, "messages", .{ .list = &msgs });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{{ messages[0][\"role\"] }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("user", result);
}

test "eval loop variables" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b") };
    var ctx = ValueMap{};
    try ctx.put(a, "items", .{ .list = &items });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{% for x in items %}{{ loop.index0 }}{% endfor %}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("01", result);
}

test "eval set statement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{% set x = 'hello' %}{{ x }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("hello", result);
}

test "eval whitespace control" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.str("hello"));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("  {{- x -}}  ", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("hello", result);
}

test "eval if strip whitespace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.bln(true));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("  {%- if x -%}  yes  {%- endif -%}  ", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("yes", result);
}

test "eval for strip whitespace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b") };
    var ctx = ValueMap{};
    try ctx.put(a, "items", .{ .list = &items });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("  {%- for x in items -%}{{- x -}}{%- endfor -%}  ", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("ab", result);
}

test "eval set strip whitespace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("  {%- set x = 'hello' -%}  {{ x }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("hello", result);
}

test "eval if/elif strip whitespace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.bln(false));
    try ctx.put(a, "y", Value.bln(true));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("before{%- if x -%}A{%- elif y -%}B{%- endif -%}after", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("beforeBafter", result);
}

test "eval if false strip whitespace no output" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.bln(false));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("before {%- if x -%} body {%- endif -%} after", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("beforeafter", result);
}

test "eval escape sequences" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{{ 'hello\\nworld' }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("hello\nworld", result);
}

test "eval filter trim" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.str("  hello  "));

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{{ x | trim }}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("hello", result);
}

test "eval raise_exception is silent" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var ctx = ValueMap{};
    try ctx.put(a, "x", Value.bln(true));

    const parser = @import("parser.zig");
    // raise_exception in a conditional branch should produce no output
    const nodes = try parser.Parser.parse("{%- if x -%}{{ raise_exception('bad') }}{%- endif -%}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("", result);
}

test "eval slice" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const items = [_]Value{ Value.str("a"), Value.str("b"), Value.str("c") };
    var ctx = ValueMap{};
    try ctx.put(a, "items", .{ .list = &items });

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse("{% for x in items[1:] %}{{ x }}{% endfor %}", a);
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("bc", result);
}

test "eval is defined and is not defined" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const parser = @import("parser.zig");
    const nodes = try parser.Parser.parse(
        "{% if present is defined %}A{% endif %}{% if missing is not defined %}B{% endif %}",
        a,
    );
    var ctx = ValueMap{};
    try ctx.put(a, "present", Value.int(1));
    var eval = Eval.init(a, &ctx);
    const result = try eval.exec(nodes);
    try std.testing.expectEqualStrings("AB", result);
}
