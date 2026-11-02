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

//! Shared BERT encoder semantics for backend execution and differentiable IR.
//! Contexts may implement a complete block as an opaque prepared region. The
//! fallback defines the same residuals, activation and normalization once.
pub const Embedding = enum { word, position, token_type };
pub fn embeddingName(comptime role: Embedding) []const u8 {
    return "embeddings." ++ switch (role) {
        .word => "word_embeddings.weight",
        .position => "position_embeddings.weight",
        .token_type => "token_type_embeddings.weight",
    };
}

pub fn embeddings(ctx: anytype) !@TypeOf(ctx.*).Value {
    var result = (try ctx.lookup(.word)).?;
    defer ctx.free(result);
    const positions = (try ctx.lookup(.position)).?;
    defer ctx.free(positions);
    const with_positions = try ctx.add(result, positions);
    ctx.free(result);
    result = with_positions;
    if (try ctx.lookup(.token_type)) |types| {
        defer ctx.free(types);
        const with_types = try ctx.add(result, types);
        ctx.free(result);
        result = with_types;
    }
    return ctx.normalize(result);
}

pub const Linear = enum(usize) { q, k, v, attention_output, intermediate, output };
pub fn linearBase(comptime role: Linear) []const u8 {
    return switch (role) {
        .q => "attention.self.query",
        .k => "attention.self.key",
        .v => "attention.self.value",
        .attention_output => "attention.output.dense",
        .intermediate => "intermediate.dense",
        .output => "output.dense",
    };
}
pub const LinearSpec = struct {
    kind: Linear,
    weight: []const u8,
    bias: []const u8,
    input_intermediate: bool = false,
    output_intermediate: bool = false,
};
fn linearSpec(comptime role: Linear) LinearSpec {
    return .{ .kind = role, .weight = linearBase(role) ++ ".weight", .bias = linearBase(role) ++ ".bias", .input_intermediate = role == .output, .output_intermediate = role == .intermediate };
}
pub const linear_specs = [_]LinearSpec{ linearSpec(.q), linearSpec(.k), linearSpec(.v), linearSpec(.attention_output), linearSpec(.intermediate), linearSpec(.output) };

pub fn Qkv(comptime Value: type) type {
    return struct { first: Value, second: Value, third: Value };
}

fn queryKeyValue(ctx: anytype, hidden: @TypeOf(ctx.*).Value) !Qkv(@TypeOf(ctx.*).Value) {
    // Packed/prepared QKV remains an opaque implementation candidate.
    if (try ctx.fusedQueryKeyValue(hidden)) |qkv| return .{ .first = qkv.first, .second = qkv.second, .third = qkv.third };
    const q = try ctx.linear(.q, hidden);
    errdefer ctx.free(q);
    const k = try ctx.linear(.k, hidden);
    errdefer ctx.free(k);
    const v = try ctx.linear(.v, hidden);
    return .{ .first = q, .second = k, .third = v };
}

pub fn encoderLayer(ctx: anytype, hidden: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const qkv = try queryKeyValue(ctx, hidden);
    defer ctx.free(qkv.first);
    defer ctx.free(qkv.second);
    defer ctx.free(qkv.third);
    const attention = try ctx.attention(qkv.first, qkv.second, qkv.third);
    defer ctx.free(attention);
    return encoderOutput(ctx, hidden, attention);
}

pub const Norm = enum { attention_output, output };

pub fn encoderOutput(ctx: anytype, hidden: @TypeOf(ctx.*).Value, attention: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    const normed = try attentionOutput(ctx, hidden, attention);
    defer ctx.free(normed);
    if (try ctx.fusedFeedForward(normed)) |output| return output;
    const residual = blk: {
        if (try ctx.fusedFeedForwardResidual(normed)) |output| break :blk output;
        const intermediate = try ctx.linear(.intermediate, normed);
        defer ctx.free(intermediate);
        const activated = try ctx.activate(intermediate);
        defer ctx.free(activated);
        const projected = try ctx.linear(.output, activated);
        defer ctx.free(projected);
        break :blk try ctx.add(projected, normed);
    };
    defer ctx.free(residual);
    return ctx.norm(.output, residual);
}

fn attentionOutput(ctx: anytype, hidden: @TypeOf(ctx.*).Value, attention: @TypeOf(ctx.*).Value) !@TypeOf(ctx.*).Value {
    if (try ctx.fusedAttentionOutput(hidden, attention)) |output| return output;
    const projected = try ctx.linear(.attention_output, attention);
    defer ctx.free(projected);
    const residual = try ctx.add(projected, hidden);
    defer ctx.free(residual);
    return ctx.norm(.attention_output, residual);
}
