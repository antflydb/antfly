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

//! Tile kernels (prototype): describe a kernel once in Zig, lower it to CPU
//! (`@Vector`, compiled in), Metal (MSL text) and CUDA (C++ text).
//!
//! The first primitive is `RowReduce`: walk one f32 row in fixed-size blocks,
//! evaluate per-element masks, and fold the element into a set of reduction
//! channels (argmax with lowest-index ties, online log-sum-exp). This is the
//! shape of logits post-processing (Whisper's timestamp statistics, argmax
//! with suppression, top-1 sampling) and of row norms.
//!
//! Everything here is comptime data. Lowerings walk it:
//! - `cpu.zig` builds a vectorized Zig function at comptime.
//! - `msl.zig` and `cuda.zig` render source text at comptime.
//! - `reference` below is the scalar oracle every lowering is tested against.

const std = @import("std");

pub const no_index: u32 = 0xffff_ffff;
pub const neg_inf = -std.math.inf(f32);

pub const CmpOp = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,

    pub fn symbol(op: CmpOp) []const u8 {
        return switch (op) {
            .eq => "==",
            .ne => "!=",
            .lt => "<",
            .le => "<=",
            .gt => ">",
            .ge => ">=",
        };
    }
};

/// Per-element expression. `index`, `param` and `lit` are u32; everything
/// else is bool. Build with the helpers (`index()`, `param("x").ge(...)`).
pub const Expr = union(enum) {
    /// Element index within the row.
    index,
    /// A u32 field of the params struct (after any `Param.override`).
    param: []const u8,
    lit: u32,
    truth: bool,
    /// A `Let` declared on the kernel, evaluated once per element.
    ref: []const u8,
    cmp: Cmp,
    all: []const Expr,
    any: []const Expr,
    not: *const Expr,
    nonzero: *const Expr,
    pick: Pick,
    /// True when the element index does not appear in the named `i32_list`
    /// binding. Negative list entries never match.
    not_listed: []const u8,

    pub const Cmp = struct { op: CmpOp, a: *const Expr, b: *const Expr };
    pub const Pick = struct { cond: *const Expr, then: *const Expr, otherwise: *const Expr };

    fn box(comptime e: Expr) *const Expr {
        return &struct {
            const value = e;
        }.value;
    }

    fn compare(comptime op: CmpOp, comptime a: Expr, comptime b: Expr) Expr {
        return .{ .cmp = .{ .op = op, .a = box(a), .b = box(b) } };
    }

    pub fn eq(comptime a: Expr, comptime b: Expr) Expr {
        return compare(.eq, a, b);
    }
    pub fn lt(comptime a: Expr, comptime b: Expr) Expr {
        return compare(.lt, a, b);
    }
    pub fn ge(comptime a: Expr, comptime b: Expr) Expr {
        return compare(.ge, a, b);
    }
    pub fn andAlso(comptime a: Expr, comptime b: Expr) Expr {
        return .{ .all = &.{ a, b } };
    }
    pub fn orElse(comptime a: Expr, comptime b: Expr) Expr {
        return .{ .any = &.{ a, b } };
    }
    pub fn negate(comptime a: Expr) Expr {
        return .{ .not = box(a) };
    }
    pub fn isSet(comptime a: Expr) Expr {
        return .{ .nonzero = box(a) };
    }
    pub fn then(comptime cond: Expr, comptime a: Expr, comptime b: Expr) Expr {
        return .{ .pick = .{ .cond = box(cond), .then = box(a), .otherwise = box(b) } };
    }
};

pub fn index() Expr {
    return .index;
}
pub fn param(comptime name: []const u8) Expr {
    return .{ .param = name };
}
pub fn lit(comptime value: u32) Expr {
    return .{ .lit = value };
}
pub fn ref(comptime name: []const u8) Expr {
    return .{ .ref = name };
}
pub fn notListed(comptime list: []const u8) Expr {
    return .{ .not_listed = list };
}
pub const always: Expr = .{ .truth = true };

/// A u32 field of the uniform params struct, in ABI order.
pub const Param = struct {
    name: []const u8,
    override: ?Override = null,

    /// When `(params[flag] & mask) != 0` the value is read from
    /// `buffer[word]` instead (device-side state such as a grammar window).
    pub const Override = struct { flag: []const u8, mask: u32, buffer: []const u8, word: u32 };
};

/// A kernel argument, in GPU binding order.
pub const Binding = union(enum) {
    /// The f32 row being reduced.
    values: []const u8,
    /// Read-only i32 list with its length in a param (`not_listed`).
    i32_list: struct { name: []const u8, count: []const u8 },
    /// Read-only u32 words (`Param.override` sources).
    u32_words: []const u8,
    /// f32 output: one `partial_stride` record per block.
    partials: []const u8,
    /// The uniform params struct.
    params,
};

pub const Reducer = enum {
    /// Record (value, index); ties keep the lower index. Index `no_index`
    /// means no element passed the mask.
    argmax,
    /// Record (max, sum) with sum = Σ exp(v - max). -inf contributes nothing.
    logsumexp,
};

pub const Channel = struct { name: []const u8, reduce: Reducer, where: Expr };

/// A named bool shared by several channels, evaluated once per element.
pub const Let = struct { name: []const u8, expr: Expr };

pub const RowReduce = struct {
    /// Kernel symbol in generated sources.
    name: []const u8,
    /// Params struct name in generated sources.
    params_type: []const u8,
    params: []const Param,
    bindings: []const Binding,
    /// Param holding the row length.
    extent: []const u8,
    /// Elements per partial record (one GPU threadgroup each).
    block: u32,
    /// GPU threads per block.
    threads: u32,
    /// f32 slots per partial record; at least 2 per channel, rest zero.
    partial_stride: u32,
    lets: []const Let = &.{},
    channels: []const Channel,

    pub fn validate(comptime k: RowReduce) void {
        if (k.partial_stride < 2 * k.channels.len) @compileError(k.name ++ ": partial_stride too small");
        if (k.block % k.threads != 0) @compileError(k.name ++ ": block must be a multiple of threads");
        if (!std.math.isPowerOfTwo(k.threads)) @compileError(k.name ++ ": threads must be a power of two");
        _ = k.paramIndex(k.extent);
        for (k.params) |p| if (p.override) |o| {
            _ = k.paramIndex(o.flag);
            _ = k.wordsIndex(o.buffer);
        };
        for (k.lets) |l| k.checkExpr(l.expr);
        for (k.channels) |c| k.checkExpr(c.where);
    }

    fn checkExpr(comptime k: RowReduce, comptime e: Expr) void {
        switch (e) {
            .index, .lit, .truth => {},
            .param => |name| _ = k.paramIndex(name),
            .ref => |name| _ = k.letIndex(name),
            .cmp => |c| {
                k.checkExpr(c.a.*);
                k.checkExpr(c.b.*);
            },
            .all, .any => |xs| for (xs) |x| k.checkExpr(x),
            .not, .nonzero => |x| k.checkExpr(x.*),
            .pick => |p| {
                k.checkExpr(p.cond.*);
                k.checkExpr(p.then.*);
                k.checkExpr(p.otherwise.*);
            },
            .not_listed => |name| _ = k.listIndex(name),
        }
    }

    pub fn paramIndex(comptime k: RowReduce, comptime name: []const u8) usize {
        for (k.params, 0..) |p, i| if (std.mem.eql(u8, p.name, name)) return i;
        @compileError(k.name ++ ": unknown param " ++ name);
    }

    pub fn letIndex(comptime k: RowReduce, comptime name: []const u8) usize {
        for (k.lets, 0..) |l, i| if (std.mem.eql(u8, l.name, name)) return i;
        @compileError(k.name ++ ": unknown let " ++ name);
    }

    /// Position among the `i32_list` bindings.
    pub fn listIndex(comptime k: RowReduce, comptime name: []const u8) usize {
        var n: usize = 0;
        for (k.bindings) |b| switch (b) {
            .i32_list => |l| {
                if (std.mem.eql(u8, l.name, name)) return n;
                n += 1;
            },
            else => {},
        };
        @compileError(k.name ++ ": unknown list " ++ name);
    }

    /// Position among the `u32_words` bindings.
    pub fn wordsIndex(comptime k: RowReduce, comptime name: []const u8) usize {
        var n: usize = 0;
        for (k.bindings) |b| switch (b) {
            .u32_words => |w| {
                if (std.mem.eql(u8, w, name)) return n;
                n += 1;
            },
            else => {},
        };
        @compileError(k.name ++ ": unknown words buffer " ++ name);
    }

    pub fn listCount(comptime k: RowReduce) usize {
        var n: usize = 0;
        for (k.bindings) |b| if (b == .i32_list) {
            n += 1;
        };
        return n;
    }

    pub fn wordsCount(comptime k: RowReduce) usize {
        var n: usize = 0;
        for (k.bindings) |b| if (b == .u32_words) {
            n += 1;
        };
        return n;
    }

    /// Host-side inputs for the CPU lowering and the reference.
    pub fn Args(comptime k: RowReduce) type {
        return struct {
            values: []const f32,
            /// One u32 per `params` entry, in order.
            params: [k.params.len]u32,
            lists: [k.listCount()][]const i32 = undefined,
            words: [k.wordsCount()][]const u32 = undefined,
        };
    }

    /// Params after applying overrides.
    pub fn resolveParams(comptime k: RowReduce, args: *const k.Args()) [k.params.len]u32 {
        var out = args.params;
        inline for (k.params, 0..) |p, i| if (p.override) |o| {
            if ((args.params[comptime k.paramIndex(o.flag)] & o.mask) != 0) out[i] = args.words[comptime k.wordsIndex(o.buffer)][o.word];
        };
        return out;
    }

    pub fn Result(comptime k: RowReduce) type {
        return [k.channels.len]State;
    }
};

/// One channel's running state: argmax (value, index) or logsumexp (max, sum).
pub const State = struct {
    a: f32,
    b: f32,

    pub fn identity(reducer: Reducer) State {
        return switch (reducer) {
            .argmax => .{ .a = neg_inf, .b = @bitCast(no_index) },
            .logsumexp => .{ .a = neg_inf, .b = 0 },
        };
    }

    pub fn id(s: State) u32 {
        return @bitCast(s.b);
    }

    pub fn push(s: *State, reducer: Reducer, v: f32, i: u32) void {
        switch (reducer) {
            .argmax => argmaxPush(s, v, i),
            .logsumexp => lsePush(s, v),
        }
    }

    pub fn merge(s: *State, reducer: Reducer, o: State) void {
        switch (reducer) {
            .argmax => if (o.id() != no_index) argmaxPush(s, o.a, o.id()),
            .logsumexp => lseMerge(s, o),
        }
    }

    fn argmaxPush(s: *State, v: f32, i: u32) void {
        if (s.id() == no_index or v > s.a or (v == s.a and i < s.id())) {
            s.a = v;
            s.b = @bitCast(i);
        }
    }

    fn lsePush(s: *State, v: f32) void {
        if (v == neg_inf) return;
        if (v > s.a) {
            s.b = s.b * @exp(s.a - v) + 1;
            s.a = v;
        } else {
            s.b += @exp(v - s.a);
        }
    }

    fn lseMerge(s: *State, o: State) void {
        if (o.b <= 0) return;
        if (s.b <= 0) {
            s.* = o;
            return;
        }
        if (o.a > s.a) {
            s.b = s.b * @exp(s.a - o.a) + o.b;
            s.a = o.a;
        } else {
            s.b += o.b * @exp(o.a - s.a);
        }
    }
};

/// Pack channel states into a partial record (`partial_stride` floats).
pub fn writeRecord(comptime k: RowReduce, states: *const k.Result(), out: []f32) void {
    @memset(out[0..k.partial_stride], 0);
    for (states, 0..) |s, c| {
        out[2 * c] = s.a;
        out[2 * c + 1] = s.b;
    }
}

/// Merge partial records (as the GPU writes them) into channel states.
pub fn combineRecords(comptime k: RowReduce, records: []const f32) k.Result() {
    var out: k.Result() = undefined;
    inline for (k.channels, 0..) |c, i| out[i] = State.identity(c.reduce);
    var r: usize = 0;
    while (r + k.partial_stride <= records.len) : (r += k.partial_stride) {
        inline for (k.channels, 0..) |c, i| {
            out[i].merge(c.reduce, .{ .a = records[r + 2 * i], .b = records[r + 2 * i + 1] });
        }
    }
    return out;
}

/// Scalar oracle: one element at a time, in index order. Log-sum-exp sums
/// accumulate in f64 (as `whisper_timestamps.hostLogitsStats` does) so the
/// oracle is more accurate than any f32 lowering it checks.
pub fn reference(comptime k: RowReduce, args: k.Args()) k.Result() {
    comptime k.validate();
    const params = k.resolveParams(&args);
    var out: k.Result() = undefined;
    var sums: [k.channels.len]f64 = @splat(0);
    inline for (k.channels, 0..) |c, i| out[i] = State.identity(c.reduce);
    const n = @min(args.values.len, params[comptime k.paramIndex(k.extent)]);
    for (args.values[0..n], 0..) |v, idx| {
        const i: u32 = @intCast(idx);
        var lets: [k.lets.len]bool = undefined;
        inline for (k.lets, 0..) |l, li| lets[li] = evalBool(k, l.expr, i, &params, &args, &lets);
        inline for (k.channels, 0..) |c, ci| {
            if (evalBool(k, c.where, i, &params, &args, &lets)) switch (c.reduce) {
                .argmax => out[ci].push(.argmax, v, i),
                .logsumexp => if (v != neg_inf) {
                    if (v > out[ci].a) {
                        sums[ci] = sums[ci] * @exp(@as(f64, out[ci].a) - v) + 1;
                        out[ci].a = v;
                    } else {
                        sums[ci] += @exp(@as(f64, v) - out[ci].a);
                    }
                },
            };
        }
    }
    inline for (k.channels, 0..) |c, ci| {
        if (c.reduce == .logsumexp) out[ci].b = @floatCast(sums[ci]);
    }
    return out;
}

fn evalU32(comptime k: RowReduce, comptime e: Expr, i: u32, params: *const [k.params.len]u32) u32 {
    return switch (e) {
        .index => i,
        .param => |name| params[comptime k.paramIndex(name)],
        .lit => |v| v,
        else => @compileError("expected a u32 expression"),
    };
}

fn evalBool(comptime k: RowReduce, comptime e: Expr, i: u32, params: *const [k.params.len]u32, args: *const k.Args(), lets: []const bool) bool {
    return switch (e) {
        .truth => |t| t,
        .ref => |name| lets[comptime k.letIndex(name)],
        .cmp => |c| {
            const a = evalU32(k, c.a.*, i, params);
            const b = evalU32(k, c.b.*, i, params);
            return switch (c.op) {
                .eq => a == b,
                .ne => a != b,
                .lt => a < b,
                .le => a <= b,
                .gt => a > b,
                .ge => a >= b,
            };
        },
        .all => |xs| {
            inline for (xs) |x| if (!evalBool(k, x, i, params, args, lets)) return false;
            return true;
        },
        .any => |xs| {
            inline for (xs) |x| if (evalBool(k, x, i, params, args, lets)) return true;
            return false;
        },
        .not => |x| !evalBool(k, x.*, i, params, args, lets),
        .nonzero => |x| evalU32(k, x.*, i, params) != 0,
        .pick => |p| if (evalBool(k, p.cond.*, i, params, args, lets))
            evalBool(k, p.then.*, i, params, args, lets)
        else
            evalBool(k, p.otherwise.*, i, params, args, lets),
        .not_listed => |name| {
            for (args.lists[comptime k.listIndex(name)]) |t| {
                if (t >= 0 and @as(u32, @intCast(t)) == i) return false;
            }
            return true;
        },
        .index, .param, .lit => @compileError("expected a bool expression"),
    };
}
