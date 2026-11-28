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

//! Shared audio encoder block order and value lifetimes. Precision-specific
//! stages retain their own projection and normalization implementations.

pub const Norm = enum { ffn_pre, ffn_post, attention_pre, attention_post, convolution_pre, convolution_post, output };
pub const Projection = enum { ffn_up, ffn_down, query, key, value, attention_out, convolution_in, convolution_out };
pub const Unary = enum { bound, scale_residual, silu, glu, depthwise };

/// Physical work queried from the same backend that executes the block.
pub const Physical = union(enum) {
    admit: struct { source_bf16: bool },
    f32_plan: @import("audio_schedule.zig").AudioRecipe,
    f32_norm: struct { name: []const u8, rows: usize, width: usize },
    linear: struct { name: []const u8, rows: usize, width: usize, columns: usize, source_bf16: bool },
    norm: struct { name: []const u8, rows: usize, width: usize, segments: []const usize },
    attention: struct { rows: usize, heads: usize, head_dim: usize, chunk: usize, context: usize, left: usize, segments: []const usize },
};

fn boundedNorm(ctx: anytype, input: @TypeOf(ctx.*).Value, kind: Norm, second: bool) !@TypeOf(ctx.*).Value {
    if (@hasDecl(@TypeOf(ctx.*), "normalizeBounded")) return ctx.normalizeBounded(kind, second, input);
    const bounded = try ctx.unary(.bound, input);
    defer ctx.release(bounded);
    return ctx.norm(kind, second, bounded);
}

pub fn FfnPost(comptime Value: type) type {
    return union(enum) { normalized: Value, fused: Value };
}

fn feedForward(ctx: anytype, input: @TypeOf(ctx.*).Value, second: bool) !@TypeOf(ctx.*).Value {
    const Context = @TypeOf(ctx.*);
    const scaled = scale: {
        const post = normalize: {
            const down = project: {
                const up = activate: {
                    if (@hasDecl(Context, "projectFfnUp")) break :activate try ctx.projectFfnUp(input, second);
                    const pre = try boundedNorm(ctx, input, .ffn_pre, second);
                    defer ctx.release(pre);
                    break :activate try ctx.linear(.ffn_up, second, pre);
                };
                defer ctx.release(up);
                break :project try ctx.linear(.ffn_down, second, up);
            };
            defer ctx.release(down);
            if (@hasDecl(Context, "finishFfnNorm")) switch (try ctx.finishFfnNorm(input, down, second)) {
                .normalized => |value| break :normalize value,
                .fused => |value| return value,
            };
            break :normalize try boundedNorm(ctx, down, .ffn_post, second);
        };
        defer ctx.release(post);
        break :scale try ctx.unary(.scale_residual, post);
    };
    defer ctx.release(scaled);
    return ctx.add(input, scaled);
}

/// Each component owns its slice or the result of its output clamp.
pub fn splitPackedQkv(ctx: anytype, packed_rows: @TypeOf(ctx.*).Value, width: usize, specs: anytype) ![3]@TypeOf(ctx.*).Value {
    var results: [3]@TypeOf(ctx.*).Value = undefined;
    var made: usize = 0;
    errdefer for (results[0..made]) |value| ctx.release(value);
    for (specs, &results, 0..) |spec, *result, index| {
        const slice = try ctx.sliceQkv(packed_rows, index * width, width);
        result.* = if (spec.clipsOutput()) clipped: {
            defer ctx.release(slice);
            break :clipped try ctx.clampQkv(slice, width, spec);
        } else slice;
        made += 1;
    }
    return results;
}

fn attend(ctx: anytype, input: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const relative = try ctx.relativeKey();
    defer ctx.releaseRelativeKey(relative);
    const Context = @TypeOf(ctx.*);
    const attended = attention: {
        if (@hasDecl(Context, "packedAttention")) {
            if (try ctx.packedAttention(input, relative)) |result| break :attention result;
        }
        const q = try ctx.linear(.query, false, input);
        defer ctx.release(q);
        const k = try ctx.linear(.key, false, input);
        defer ctx.release(k);
        const v = try ctx.linear(.value, false, input);
        defer ctx.release(v);
        break :attention try ctx.attention(q, k, v, relative);
    };
    defer ctx.release(attended);
    if (@hasDecl(Context, "afterAttention")) ctx.afterAttention();
    return ctx.linear(.attention_out, false, attended);
}

fn convolve(ctx: anytype, input: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const pw2 = project: {
        const activated = activate: {
            const normed = normalize: {
                const convolved = stencil: {
                    const glu = gate: {
                        const pw1 = project_input: {
                            const normed = try ctx.norm(.convolution_pre, false, input);
                            defer ctx.release(normed);
                            break :project_input try ctx.linear(.convolution_in, false, normed);
                        };
                        defer ctx.release(pw1);
                        break :gate try ctx.unary(.glu, pw1);
                    };
                    defer ctx.release(glu);
                    break :stencil try ctx.unary(.depthwise, glu);
                };
                defer ctx.release(convolved);
                break :normalize try boundedNorm(ctx, convolved, .convolution_post, false);
            };
            defer ctx.release(normed);
            break :activate try ctx.unary(.silu, normed);
        };
        defer ctx.release(activated);
        break :project try ctx.linear(.convolution_out, false, activated);
    };
    defer ctx.release(pw2);
    return ctx.add(input, pw2);
}

pub fn run(ctx: anytype, input: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const Context = @TypeOf(ctx.*);
    const custom = @hasDecl(Context, "attentionResidual");
    const ff2 = feed_forward: {
        const convolution = conv: {
            const residual = attention: {
                const ff1 = try feedForward(ctx, input, false);
                defer ctx.release(ff1);
                if (@hasDecl(Context, "afterFeedForward")) try ctx.afterFeedForward(ff1, false);
                const attended = project: {
                    const normed = if (custom) try ctx.normalize(.attention_pre, ff1) else try boundedNorm(ctx, ff1, .attention_pre, false);
                    defer ctx.release(normed);
                    break :project try attend(ctx, normed);
                };
                defer ctx.release(attended);
                if (custom) break :attention try ctx.attentionResidual(ff1, attended);
                const normed = try boundedNorm(ctx, attended, .attention_post, false);
                defer ctx.release(normed);
                break :attention try ctx.add(ff1, normed);
            };
            defer ctx.release(residual);
            if (@hasDecl(Context, "beforeConvolution")) ctx.beforeConvolution();
            break :conv try convolve(ctx, residual);
        };
        defer ctx.release(convolution);
        if (@hasDecl(Context, "afterConvolution")) try ctx.afterConvolution(convolution);
        break :feed_forward try feedForward(ctx, convolution, true);
    };
    defer ctx.release(ff2);
    if (@hasDecl(Context, "afterFeedForward")) try ctx.afterFeedForward(ff2, true);
    return if (custom) ctx.normalize(.output, ff2) else boundedNorm(ctx, ff2, .output, false);
}
