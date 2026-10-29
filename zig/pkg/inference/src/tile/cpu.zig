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

//! CPU lowering: a tile `RowReduce` becomes a Zig function over
//! `@Vector(width, f32)`. Nothing is rendered as text; the kernel
//! description is walked at comptime and the masks, reductions and lane
//! merges are specialized into straight-line vector code.

const std = @import("std");
const tile = @import("tile.zig");

const State = tile.State;
const no_index = tile.no_index;
const neg_inf = tile.neg_inf;

/// Vector exp. `@exp` on a `@Vector` lowers to one libm call per lane, which
/// made the vector kernel slower than scalar code; this is a Cephes-style
/// polynomial (about 1 ulp) that stays in vector registers. Inputs below
/// -87.3 (including -inf) return 0.
pub fn vexp(comptime width: comptime_int, x: @Vector(width, f32)) @Vector(width, f32) {
    const V = @Vector(width, f32);
    const I = @Vector(width, i32);
    const xc = @min(@max(x, @as(V, @splat(-87.3))), @as(V, @splat(88.37)));
    const n = @round(xc * @as(V, @splat(1.44269504088896341)));
    const r = xc - n * @as(V, @splat(0.693359375)) - n * @as(V, @splat(-2.12194440e-4));
    var p: V = @splat(1.9875691500e-4);
    p = p * r + @as(V, @splat(1.3981999507e-3));
    p = p * r + @as(V, @splat(8.3334519073e-3));
    p = p * r + @as(V, @splat(4.1665795894e-2));
    p = p * r + @as(V, @splat(1.6666665459e-1));
    p = p * r + @as(V, @splat(5.0000001201e-1));
    const y = p * r * r + r + @as(V, @splat(1));
    const ni: I = @intFromFloat(n);
    const scale: V = @bitCast((ni + @as(I, @splat(127))) << @splat(23));
    return @select(f32, x < @as(V, @splat(-87.3)), @as(V, @splat(0)), y * scale);
}

pub fn RowReduce(comptime k: tile.RowReduce, comptime width: comptime_int) type {
    comptime k.validate();
    return struct {
        const V = @Vector(width, f32);
        const U = @Vector(width, u32);
        const B = @Vector(width, bool);
        const Args = k.Args();

        const lane_offsets: U = blk: {
            var out: [width]u32 = undefined;
            for (&out, 0..) |*o, i| o.* = i;
            break :blk out;
        };

        /// Per-lane running state for one channel.
        const Lanes = struct {
            a: V,
            b: V,
            i: U,
        };

        const Ctx = struct {
            idx: U,
            params: *const [k.params.len]u32,
            args: *const Args,
            lets: [k.lets.len]B = undefined,
        };

        /// Reduce the whole row; same result as `tile.reference` up to
        /// float summation order.
        pub fn run(args: *const Args) k.Result() {
            const params = k.resolveParams(args);
            const n = @min(args.values.len, params[comptime k.paramIndex(k.extent)]);
            return reduceRange(args, &params, 0, n);
        }

        /// Write one `partial_stride` record per `block` elements, as the
        /// GPU lowerings do. `out.len` must be at least blocks * stride.
        pub fn partials(args: *const Args, out: []f32) usize {
            const params = k.resolveParams(args);
            const n = @min(args.values.len, params[comptime k.paramIndex(k.extent)]);
            const blocks = (n + k.block - 1) / k.block;
            for (0..blocks) |b| {
                const start = b * k.block;
                const states = reduceRange(args, &params, start, @min(start + k.block, n));
                tile.writeRecord(k, &states, out[b * k.partial_stride ..][0..k.partial_stride]);
            }
            return blocks;
        }

        fn reduceRange(args: *const Args, params: *const [k.params.len]u32, start: usize, end: usize) k.Result() {
            var lanes: [k.channels.len]Lanes = undefined;
            inline for (k.channels, 0..) |c, ci| {
                const id = State.identity(c.reduce);
                lanes[ci] = .{ .a = @splat(id.a), .b = @splat(id.b), .i = @splat(no_index) };
            }
            var pos = start;
            while (pos + width <= end) : (pos += width) {
                const v: V = args.values[pos..][0..width].*;
                step(&lanes, args, params, v, @intCast(pos), @splat(true));
            }
            if (pos < end) {
                var tail: [width]f32 = @splat(0);
                @memcpy(tail[0 .. end - pos], args.values[pos..end]);
                const in_range = lane_offsets < @as(U, @splat(@intCast(end - pos)));
                step(&lanes, args, params, tail, @intCast(pos), in_range);
            }
            var out: k.Result() = undefined;
            inline for (k.channels, 0..) |c, ci| out[ci] = mergeLanes(c.reduce, lanes[ci]);
            return out;
        }

        inline fn step(lanes: *[k.channels.len]Lanes, args: *const Args, params: *const [k.params.len]u32, v: V, base: u32, in_range: B) void {
            var ctx: Ctx = .{ .idx = @as(U, @splat(base)) + lane_offsets, .params = params, .args = args };
            inline for (k.lets, 0..) |l, li| ctx.lets[li] = evalBool(l.expr, &ctx);
            inline for (k.channels, 0..) |c, ci| {
                const mask = andB(in_range, evalBool(c.where, &ctx));
                pushLanes(c.reduce, &lanes[ci], v, ctx.idx, mask);
            }
        }

        inline fn pushLanes(comptime reducer: tile.Reducer, s: *Lanes, v: V, idx: U, mask: B) void {
            switch (reducer) {
                .argmax => {
                    // Lanes see increasing indices, so a tie keeps the earlier one.
                    const empty = s.i == @as(U, @splat(no_index));
                    const take = andB(mask, orB(empty, v > s.a));
                    s.a = @select(f32, take, v, s.a);
                    s.i = @select(u32, take, idx, s.i);
                },
                .logsumexp => {
                    // One exp per element: exp(-|v - max|) rescales the old
                    // sum when v is the new max, or is v's own term otherwise.
                    const live = andB(mask, v != @as(V, @splat(neg_inf)));
                    const grows = v > s.a;
                    const d = vexp(width, -@abs(v - s.a));
                    const sum = @select(f32, grows, s.b * d + @as(V, @splat(1)), s.b + d);
                    s.a = @select(f32, andB(live, grows), v, s.a);
                    s.b = @select(f32, live, sum, s.b);
                },
            }
        }

        fn mergeLanes(comptime reducer: tile.Reducer, s: Lanes) State {
            var out = State.identity(reducer);
            inline for (0..width) |l| {
                const lane: State = switch (reducer) {
                    .argmax => .{ .a = s.a[l], .b = @bitCast(s.i[l]) },
                    .logsumexp => .{ .a = s.a[l], .b = s.b[l] },
                };
                out.merge(reducer, lane);
            }
            return out;
        }

        inline fn andB(a: B, b: B) B {
            return @select(bool, a, b, @as(B, @splat(false)));
        }

        inline fn orB(a: B, b: B) B {
            return @select(bool, a, @as(B, @splat(true)), b);
        }

        inline fn evalU32(comptime e: tile.Expr, ctx: *const Ctx) U {
            return switch (e) {
                .index => ctx.idx,
                .param => |name| @splat(ctx.params[comptime k.paramIndex(name)]),
                .lit => |v| @splat(v),
                else => @compileError("expected a u32 expression"),
            };
        }

        inline fn evalBool(comptime e: tile.Expr, ctx: *const Ctx) B {
            return switch (e) {
                .truth => |t| @splat(t),
                .ref => |name| ctx.lets[comptime k.letIndex(name)],
                .cmp => |c| blk: {
                    const a = evalU32(c.a.*, ctx);
                    const b = evalU32(c.b.*, ctx);
                    break :blk switch (c.op) {
                        .eq => a == b,
                        .ne => a != b,
                        .lt => a < b,
                        .le => a <= b,
                        .gt => a > b,
                        .ge => a >= b,
                    };
                },
                .all => |xs| blk: {
                    var acc: B = @splat(true);
                    inline for (xs) |x| acc = andB(acc, evalBool(x, ctx));
                    break :blk acc;
                },
                .any => |xs| blk: {
                    var acc: B = @splat(false);
                    inline for (xs) |x| acc = orB(acc, evalBool(x, ctx));
                    break :blk acc;
                },
                .not => |x| @select(bool, evalBool(x.*, ctx), @as(B, @splat(false)), @as(B, @splat(true))),
                .nonzero => |x| evalU32(x.*, ctx) != @as(U, @splat(0)),
                .pick => |p| @select(bool, evalBool(p.cond.*, ctx), evalBool(p.then.*, ctx), evalBool(p.otherwise.*, ctx)),
                .not_listed => |name| blk: {
                    var hit: B = @splat(false);
                    for (ctx.args.lists[comptime k.listIndex(name)]) |t| {
                        if (t < 0) continue;
                        hit = orB(hit, ctx.idx == @as(U, @splat(@intCast(t))));
                    }
                    break :blk @select(bool, hit, @as(B, @splat(false)), @as(B, @splat(true)));
                },
                .index, .param, .lit => @compileError("expected a bool expression"),
            };
        }
    };
}
