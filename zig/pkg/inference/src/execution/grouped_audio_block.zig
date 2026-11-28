//! Shared operation order and value lifetimes for a grouped source-precision
//! audio encoder block. Planning and execution use the same block body.

pub const Norm = enum { ffn_pre, ffn_post, attention_pre, attention_post, convolution_pre, convolution_post, output };
pub const Projection = enum { ffn_up, ffn_down, query, key, value, attention_out, convolution_in, convolution_out };
pub const Unary = enum { bound, scale_residual, silu, glu, depthwise };

/// Physical work queried from the same backend that executes the block.
pub const Physical = union(enum) {
    admit: void,
    linear: struct { name: []const u8, rows: usize, width: usize, columns: usize },
    norm: struct { name: []const u8, rows: usize, width: usize, segments: []const usize },
    attention: struct { rows: usize, heads: usize, head_dim: usize, chunk: usize, context: usize, left: usize, segments: []const usize },
};

fn boundedNorm(ctx: anytype, input: @TypeOf(ctx.*).Value, kind: Norm, second: bool) !@TypeOf(ctx.*).Value {
    const bounded = try ctx.unary(.bound, input);
    defer ctx.release(bounded);
    return ctx.norm(kind, second, bounded);
}

fn feedForward(ctx: anytype, input: @TypeOf(ctx.*).Value, second: bool) !@TypeOf(ctx.*).Value {
    const scaled = scale: {
        const post = normalize: {
            const down = project: {
                const up = activate: {
                    const pre = try boundedNorm(ctx, input, .ffn_pre, second);
                    defer ctx.release(pre);
                    break :activate try ctx.linear(.ffn_up, second, pre);
                };
                defer ctx.release(up);
                break :project try ctx.linear(.ffn_down, second, up);
            };
            defer ctx.release(down);
            break :normalize try boundedNorm(ctx, down, .ffn_post, second);
        };
        defer ctx.release(post);
        break :scale try ctx.unary(.scale_residual, post);
    };
    defer ctx.release(scaled);
    return ctx.add(input, scaled);
}

fn attend(ctx: anytype, input: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const attended = attention: {
        const q = try ctx.linear(.query, false, input);
        defer ctx.release(q);
        const k = try ctx.linear(.key, false, input);
        defer ctx.release(k);
        const v = try ctx.linear(.value, false, input);
        defer ctx.release(v);
        break :attention try ctx.attention(q, k, v);
    };
    defer ctx.release(attended);
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
    const ff2 = feed_forward: {
        const convolution = conv: {
            const residual = attention: {
                const ff1 = try feedForward(ctx, input, false);
                defer ctx.release(ff1);
                const attended = project: {
                    const normed = try boundedNorm(ctx, ff1, .attention_pre, false);
                    defer ctx.release(normed);
                    break :project try attend(ctx, normed);
                };
                defer ctx.release(attended);
                const normed = try boundedNorm(ctx, attended, .attention_post, false);
                defer ctx.release(normed);
                break :attention try ctx.add(ff1, normed);
            };
            defer ctx.release(residual);
            break :conv try convolve(ctx, residual);
        };
        defer ctx.release(convolution);
        break :feed_forward try feedForward(ctx, convolution, true);
    };
    defer ctx.release(ff2);
    return boundedNorm(ctx, ff2, .output, false);
}
