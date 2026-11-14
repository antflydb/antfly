// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const program = @import("../../native_program.zig");
const tile = @import("antfly_tile");
const f = tile.fexpr;
const expr = tile.native_math.cudaExpression;
const query_recipe = f.acc.times(.bias).times(.gate);
const key_recipe = f.acc.times(.bias);
const cap_recipe = f.acc.plus(.bias).dividedBy(.gate).tanhOf().times(.gate);
const publication_recipe = f.acc.roundBf16();
pub const source_0 = program.render(
    \\// Caller-funded source F32 GEMMs share the existing local-relative owner.
    \\__device__ __forceinline__ float antfly_local_relative_bf16_value(float x) {
    \\    return __bfloat162float(__float2bfloat16_rn(x));
    \\}
    \\extern "C" __global__ void antfly_local_relative_source_pack_f32(
    \\    float* query, float* packed_query, float* key, float* value,
    \\    const float* q, const float* k, const float* v, const float* scales,
    \\    unsigned rows, unsigned heads, unsigned dim, unsigned chunk, unsigned context,
    \\    unsigned relative, unsigned blocks, float query_scale, float key_scale) {
    \\    const size_t i=(size_t)blockIdx.x*blockDim.x+threadIdx.x;
    \\    const size_t hidden=(size_t)heads*dim, padded=(size_t)blocks*chunk;
    \\    if(i<padded*hidden) {
    \\        const unsigned row=i/hidden,head=(i/dim)%heads,d=i%dim;
    \\        const float input=row<rows?q[i]:0.f;
    \\        const float scaled={{query_scale}};
    \\        query[i]=scaled;
    \\        if(blocks>1)packed_query[((size_t)head*blocks+row/chunk)*chunk*dim+(row%chunk)*dim+d]=scaled;
    \\    }
    \\    if(i<(size_t)blocks*context*hidden) {
    \\        const unsigned block=i/((size_t)context*hidden),c=(i/hidden)%context,head=(i/dim)%heads,d=i%dim;
    \\        const long long row=(long long)block*chunk+c-(relative-1);
    \\        const bool present=row>=0&&row<rows;
    \\        const size_t source=present?(size_t)row*hidden+head*dim+d:0;
    \\        const float key_input=present?k[source]:0.f;
    \\        const float key_value={{key_scale}};
    \\        const float plain_value=present?v[source]:0.f;
    \\        const size_t ki=blocks==1?i:(((size_t)head*blocks+block)*dim+d)*context+c;
    \\        const size_t vi=blocks==1?i:(((size_t)head*blocks+block)*context+c)*dim+d;
    \\        key[ki]=key_value;value[vi]=plain_value;
    \\    }
    \\}
    \\extern "C" __global__ void antfly_local_relative_source_softmax_f32(
    \\    float* scores,const float* relative_scores,const float* valid,
    \\    unsigned rows,unsigned heads,unsigned chunk,unsigned context,unsigned relative,unsigned blocks,
    \\    float cap,float invalid_value,unsigned exclude_farthest) {
    \\    const unsigned lane=threadIdx.x,padded=blocks*chunk;
    \\    const unsigned head=blockIdx.x/padded,query=blockIdx.x%padded;
    \\    if(head>=heads||lane>=32)return;
    \\    const unsigned block=query/chunk,offset=query%chunk;
    \\    const size_t score_index=(((size_t)head*blocks+block)*chunk+offset)*context+lane;
    \\    float score=-INFINITY;
    \\    if(lane<context) {
    \\        // Literal pad(...W+1-L)/flatten/truncate/view relative shift.
    \\        const unsigned flat=offset*context+lane,position=flat%(context+1);
    \\        const float relative_value=position<relative?relative_scores[((size_t)head*padded+block*chunk+flat/(context+1))*relative+position]:0.f;
    \\        const float content=scores[score_index];
    \\        score={{cap}};
    \\        const long long key=(long long)block*chunk+lane-(relative-1);
    \\        if(query>=rows||key<0||key>=rows||key>query||query-key>relative-1-exclude_farthest||valid[key]==0.f)score=invalid_value;
    \\    }
    \\    float maximum=score;
    \\    for(int offset=16;offset>0;offset>>=1)maximum=fmaxf(maximum,__shfl_xor_sync(0xffffffffu,maximum,offset));
    \\    float probability=lane<context?expf(__fsub_rn(score,maximum)):0.f;
    \\    float denominator=probability;
    \\    for(int offset=16;offset>0;offset>>=1)denominator=__fadd_rn(denominator,__shfl_xor_sync(0xffffffffu,denominator,offset));
    \\    if(lane<context)scores[score_index]=__fdiv_rn(probability,denominator);
    \\}
    \\extern "C" __global__ void antfly_local_relative_source_publish_bf16_values_f32(
    \\    float* out,const float* input,unsigned rows,unsigned heads,unsigned dim,unsigned chunk,unsigned blocks) {
    \\    const size_t i=(size_t)blockIdx.x*blockDim.x+threadIdx.x,hidden=(size_t)heads*dim;
    \\    if(i>=(size_t)rows*hidden)return;
    \\    const unsigned row=i/hidden,head=(i/dim)%heads,d=i%dim;
    \\    const size_t source=blocks==1?i:(((size_t)head*blocks+row/chunk)*chunk+row%chunk)*dim+d;
    \\    const float value=input[source];out[i]={{publication}};
    \\}
    \\
, .{
    .query_scale = expr(query_recipe, "input", "query_scale", "scales[d]", ""),
    .key_scale = expr(key_recipe, "key_input", "key_scale", "", ""),
    .cap = expr(cap_recipe, "content", "relative_value", "cap", ""),
    .publication = f.Emission{ .expr = publication_recipe, .source = .{ .round_bf16 = "antfly_local_relative_bf16_value" }, .inputs = .{ .value = "value", .bias = "", .gate = "", .residual = "" } },
});

test "source local relative recipes preserve F32 scale and BF16 publication boundaries" {
    const std = @import("std");
    @setFloatMode(.strict);
    const qs: f32 = @bitCast(@as(u32, 0x3e0293ee));
    const cases = [_]struct { q: f32, scale: f32, expected: u32 }{
        .{ .q = 1.0078125, .scale = 0.6875, .expected = 0x3db4f27e },
        .{ .q = -1.015625, .scale = 1.3125, .expected = 0xbe2e0fb1 },
        .{ .q = 255, .scale = 0.5, .expected = 0x4182115a },
        .{ .q = 0.00390625, .scale = 2, .expected = 0x3a8293ee },
    };
    for (cases) |c| {
        const actual = query_recipe.evaluate(tile.native_math.ScalarMath, f.Inputs(f32){ .value = c.q, .bias = qs, .gate = c.scale, .residual = 0 });
        try std.testing.expectEqual(c.expected, @as(u32, @bitCast(actual)));
    }
    for ([_][2]f32{ .{ 1.00390625, 1 }, .{ 1.01171875, 1.015625 }, .{ -1.00390625, -1 } }) |c| {
        const actual = publication_recipe.evaluate(tile.native_math.ScalarMath, f.Inputs(f32){ .value = c[0], .bias = 0, .gate = 0, .residual = 0 });
        try std.testing.expectEqual(c[1], actual);
    }
}
