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

//! Shared complex arithmetic for CPU SIMD FFT and native GPU butterflies.
//! Separate-product profiles bind ADD/SUB/MUL explicitly; ordinary profiles
//! retain the backend compiler's existing contraction policy.
const f = @import("fexpr.zig");
const math = @import("native_math.zig");
pub const real_product = f.operand("real").times(f.operand("wr")).minus(f.operand("imag").times(f.operand("wi")));
pub const imag_product = f.operand("real").times(f.operand("wi")).plus(f.operand("imag").times(f.operand("wr")));

pub inline fn product(real: anytype, imag: @TypeOf(real), wr: @TypeOf(real), wi: @TypeOf(real)) struct { real: @TypeOf(real), imag: @TypeOf(real) } {
    const inputs = .{ .value = real, .real = real, .imag = imag, .wr = wr, .wi = wi };
    return .{ .real = real_product.evaluate(math.ScalarMath, inputs), .imag = imag_product.evaluate(math.ScalarMath, inputs) };
}

pub const power = f.operand("real").times(f.operand("real")).plus(f.operand("imag").times(f.operand("imag")));
pub inline fn magnitudeSquared(real: anytype, imag: @TypeOf(real)) @TypeOf(real) {
    return power.evaluate(math.ScalarMath, .{ .value = real, .real = real, .imag = imag });
}
pub const Fft512 = struct {
    pub const size = 512;
    pub const frame_samples = 320;
    pub const bins = size / 2 + 1;
    pub const threads = size / 2;
    pub const index_bits = @import("std").math.log2_int(u32, size);
    pub const reverse_shift: u5 = @intCast(32 - @as(u32, index_bits));
    pub const header = @import("std").fmt.comptimePrint(
        "#define ANTFLY_FFT512_SIZE {d}u\n#define ANTFLY_FFT512_FRAME_SAMPLES {d}u\n#define ANTFLY_FFT512_BINS {d}u\n#define ANTFLY_FFT512_THREADS {d}u\n",
        .{ size, frame_samples, bins, threads },
    );
};

pub const Butterfly = struct {
    pub const Native = struct {
        twiddles: []const u8,
        integer: []const u8 = "uint",
        lane: []const u8 = "tid",
        constructor: []const u8 = "float2",
        barrier: []const u8 = "threadgroup_barrier(mem_flags::mem_threadgroup);",
        source: f.FExpr.Source = .{ .compact = true },
        split_components: bool = false,
    };
    pub fn native(comptime n: Native) []const u8 {
        const fmt = @import("std").fmt.comptimePrint;
        const inputs = .{ .real = "y.x", .imag = "y.y", .wr = n.twiddles ++ "[2*w]", .wi = n.twiddles ++ "[2*w+1]" };
        const real = real_product.renderNative(n.source, inputs, null);
        const imag = imag_product.renderNative(n.source, inputs, null);
        const product_source = if (n.split_components) fmt("float re={s};float im={s};", .{ real, imag }) else fmt("float2 v={s}({s},{s});", .{ n.constructor, real, imag });
        const r = if (n.split_components) "re" else "v.x";
        const i = if (n.split_components) "im" else "v.y";
        const plus = f.operand("left").plus(f.operand("right"));
        const minus = f.operand("left").minus(f.operand("right"));
        const stores = if (!n.split_components and n.source.add == null and n.source.sub == null)
            "values[a]=x+v;values[b]=x-v;"
        else
            fmt("values[a]={s}({s},{s});values[b]={s}({s},{s});", .{
                n.constructor, plus.renderNative(n.source, .{ .left = "x.x", .right = r }, null),  plus.renderNative(n.source, .{ .left = "x.y", .right = i }, null),
                n.constructor, minus.renderNative(n.source, .{ .left = "x.x", .right = r }, null), minus.renderNative(n.source, .{ .left = "x.y", .right = i }, null),
            });
        const span = if (n.split_components) "half" else "span";
        return fmt("for({s} len=2;len<={d};len*=2) {{ {s} {s}=len/2,j={s}%{s},a=({s}/{s})*len+j,b=a+{s},w=j*({d}/len);float2 x=values[a],y=values[b];{s}{s}{s} }}", .{
            n.integer, Fft512.size, n.integer, span, n.lane, span, n.lane, span, span, Fft512.size, product_source, stores, n.barrier,
        });
    }
};

/// Sparse mel filters must traverse the entire spectrum when any magnitude is
/// nonfinite: zero weights still multiply those values, preserving NaN policy.
pub const MelProjection = struct {
    features: u32 = 128,
    bins: u32 = Fft512.bins,
    descriptor_base: u32 = Fft512.size + Fft512.frame_samples,
    pub const Native = struct {
        integer: []const u8 = "uint",
        frame: []const u8 = "frame",
        log: []const u8 = "log",
        source: f.FExpr.Source,
    };
    pub fn native(comptime p: MelProjection, comptime n: Native) []const u8 {
        const accumulation = f.operand("sum").plus(f.operand("magnitude").times(f.operand("weight"))).renderNative(n.source, .{ .sum = "sum", .magnitude = "mag[k]", .weight = "weight" }, null);
        return @import("source_template.zig").render(
            \\if(tid<{{features}}) { {{u}} first=({{u}})plan[{{first}}+tid*3],end=({{u}})plan[{{end}}+tid*3],off=({{u}})plan[{{offset}}+tid*3];float sum=0.f;for({{u}} k=nonfinite ? 0 : first;k<(nonfinite ? {{bins}} : end);k++) { float weight=k>=first && k<end ? plan[off+k-first] : 0.f; sum={{accumulation}}; }out[{{frame}}*{{features}}+tid]={{log}}(sum+floor)*mask[frame]; }
            \\
        , .{ .features = @import("std").fmt.comptimePrint("{d}", .{p.features}), .bins = @import("std").fmt.comptimePrint("{d}", .{p.bins}), .u = n.integer, .first = @import("std").fmt.comptimePrint("{d}", .{p.descriptor_base}), .end = @import("std").fmt.comptimePrint("{d}", .{p.descriptor_base + 1}), .offset = @import("std").fmt.comptimePrint("{d}", .{p.descriptor_base + 2}), .accumulation = accumulation, .frame = n.frame, .log = n.log });
    }
};

/// Serial channel normalization is a centered two-pass schedule. Its explicit
/// arithmetic bindings keep CUDA separate-product rounding and Metal native
/// contraction, including their historical ReLU operand order.
pub const ChannelNorm = struct {
    pub const Native = struct {
        integer: []const u8,
        source: f.FExpr.Source,
        maximum: []const u8,
        sqrt: []const u8,
        index: []const u8,
        zero_first: bool = false,
    };
    pub fn native(comptime n: Native) []const u8 {
        const div = f.operand("left").dividedBy(f.operand("right"));
        const sub = f.operand("left").minus(f.operand("right"));
        const add = f.operand("left").plus(f.operand("right"));
        const mul = f.operand("left").times(f.operand("right"));
        const scaled = sub.times(f.operand("scale")).times(f.operand("weight"));
        return @import("source_template.zig").render(
            \\float mean=0.0f;
            \\for({{u}} c=0;c<width;c++) mean={{sum}};
            \\mean={{mean}};
            \\float variance=0.0f;
            \\for({{u}} c=0;c<width;c++) { float d={{difference}};variance={{variance_sum}}; }
            \\variance={{variance}};
            \\float scale={{scale}};
            \\for({{u}} c=0;c<width;c++) { {{index}} i=base+c*stride;output[i]={{relu}}; }
            \\
        , .{
            .u = n.integer,
            .index = n.index,
            .sum = add.renderNative(n.source, .{ .left = "mean", .right = "input[base+c*stride]" }, null),
            .mean = div.renderNative(n.source, .{ .left = "mean", .right = "(float)width" }, null),
            .difference = sub.renderNative(n.source, .{ .left = "input[base+c*stride]", .right = "mean" }, null),
            .variance_sum = add.renderNative(n.source, .{ .left = "variance", .right = mul.renderNative(n.source, .{ .left = "d", .right = "d" }, null) }, null),
            .variance = div.renderNative(n.source, .{ .left = "variance", .right = "(float)width" }, null),
            .scale = div.renderNative(n.source, .{ .left = "1.0f", .right = n.sqrt ++ "(" ++ add.renderNative(n.source, .{ .left = "variance", .right = "eps" }, null) ++ ")" }, null),
            .relu = n.maximum ++ "(" ++ (if (n.zero_first) "0.0f," else "") ++ scaled.renderNative(n.source, .{ .left = "input[i]", .right = "mean", .scale = "scale", .weight = "weight[c]" }, null) ++ (if (n.zero_first) "" else ",0.0f") ++ ")",
        });
    }
};

/// Windowing, magnitude, log/mask and interleaved channel permutation. The
/// centered normalization helper is shared with the standalone channel export.
pub const Frontend = struct {
    pub const Native = struct {
        integer: []const u8,
        wide: []const u8,
        prefix: []const u8 = "",
        source: f.FExpr.Source,
        sqrt: []const u8,
        log: []const u8,
        normalize: []const u8,
    };
    pub fn native(comptime n: Native) []const u8 {
        const mul = f.operand("left").times(f.operand("right"));
        const add = f.operand("left").plus(f.operand("right"));
        const p = n.prefix;
        return @import("source_template.zig").render(
            \\{{wide}} total=({{wide}}){{p}}rows*{{p}}width*({{p}}mode==4 ? {{p}}hop : 1)*{{p}}batch;
            \\if(({{wide}})i>=total) return;
            \\if({{p}}mode==0) out[i]={{window}};
            \\else if({{p}}mode==1) { float re=input[2*i],im=input[2*i+1];out[i]={{sqrt}}({{power}}); }
            \\else if({{p}}mode==2) out[i]={{log_mask}};
            \\else if({{p}}mode==3) {
            \\    if(({{wide}})i>=({{wide}}){{p}}rows*{{p}}batch) return;
            \\    {{u}} b=i/{{p}}rows,pos=i%{{p}}rows;
            \\    {{wide}} base=({{wide}})b*{{p}}rows*{{p}}width;
            \\    {{normalize}}(input,aux,out,base+pos,{{p}}rows,{{p}}width,{{p}}floor);
            \\} else {
            \\    {{u}} per={{p}}rows*{{p}}width*{{p}}hop,b=i/per,j=i%per;
            \\    out[i]=input[({{wide}})b*per+((j%{{p}}hop)*{{p}}rows+j/({{p}}width*{{p}}hop))*{{p}}width+(j/{{p}}hop)%{{p}}width];
            \\}
            \\
        , .{
            .u = n.integer,
            .wide = n.wide,
            .p = p,
            .sqrt = n.sqrt,
            .normalize = n.normalize,
            .window = mul.renderNative(n.source, .{ .left = "input[(i/" ++ p ++ "width)*" ++ p ++ "hop+i%" ++ p ++ "width]", .right = "aux[i%" ++ p ++ "width]" }, null),
            .power = power.renderNative(n.source, .{ .real = "re", .imag = "im" }, null),
            .log_mask = mul.renderNative(n.source, .{ .left = n.log ++ "(" ++ add.renderNative(n.source, .{ .left = "input[i]", .right = p ++ "floor" }, null) ++ ")", .right = "aux[i/" ++ p ++ "width]" }, null),
        });
    }
};
