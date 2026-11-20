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

//! Direct F32 convolution. Channel/kernel traversal, padding and accumulation
//! are shared; tiled/MMA and vendor convolution retain their own schedules.
const render = @import("source_template.zig").render;

pub const Direct = struct {
    rank: enum { one, two },
    pub const Native = struct {
        integer: []const u8,
        prefix: []const u8 = "",
        index: []const u8,
        output: []const u8,
        optional_bias: bool = false,
        guard_groups: bool = false,
        unsigned_bounds: bool = false,
    };
    pub fn native(comptime p: Direct, comptime n: Native) []const u8 {
        return render(two, .{
            .u = n.integer,
            .p = n.prefix,
            .i = n.index,
            .output = n.output,
            .optional_bias = n.optional_bias,
            .guard_groups = n.guard_groups and p.rank == .two,
            .grouped = p.rank == .two,
            .unsigned_bounds = n.unsigned_bounds,
            .height = n.prefix ++ (if (p.rank == .one) "time_steps" else "height"),
            .width = if (p.rank == .one) "1u" else n.prefix ++ "width",
            .out_h = n.prefix ++ (if (p.rank == .one) "out_time" else "out_h"),
            .out_w = if (p.rank == .one) "1u" else n.prefix ++ "out_w",
            .kernel_h = n.prefix ++ (if (p.rank == .one) "kernel_size" else "kernel_h"),
            .kernel_w = if (p.rank == .one) "1u" else n.prefix ++ "kernel_w",
            .stride_h = n.prefix ++ (if (p.rank == .one) "stride" else "stride_h"),
            .stride_w = if (p.rank == .one) "1u" else n.prefix ++ "stride_w",
            .padding_h = n.prefix ++ (if (p.rank == .one) "padding" else "padding_h"),
            .padding_w = if (p.rank == .one) "0u" else n.prefix ++ "padding_w",
            .groups = if (p.rank == .one) "1u" else n.prefix ++ "groups",
        });
    }
};

const two =
    \\    {{u}} total = {{p}}batch * {{p}}out_channels * {{out_h}} * {{out_w}};
    \\    if ({{i}} >= total{{#guard_groups}} || {{groups}} == 0u{{/guard_groups}}) return;
    \\    {{u}} ox = {{i}} % {{out_w}};
    \\    {{u}} tmp = {{i}} / {{out_w}};
    \\    {{u}} oy = tmp % {{out_h}};
    \\    tmp /= {{out_h}};
    \\    {{u}} oc = tmp % {{p}}out_channels;
    \\    {{u}} b = tmp / {{p}}out_channels;
    \\    {{u}} in_per_group = {{p}}in_channels / {{groups}};
    \\{{#grouped}}    {{u}} out_per_group = {{p}}out_channels / {{groups}};
    \\    {{u}} group = oc / out_per_group;
    \\{{/grouped}}{{^grouped}}    {{u}} group = 0u;
    \\{{/grouped}}
    \\    float acc = {{#optional_bias}}{{p}}has_bias != 0u ? {{/optional_bias}}bias[oc]{{#optional_bias}} : 0.0f{{/optional_bias}};
    \\    for ({{u}} ig = 0; ig < in_per_group; ++ig) {
    \\        {{u}} ic = group * in_per_group + ig;
    \\        for ({{u}} ky = 0; ky < {{kernel_h}}; ++ky) {
    \\            int iy = (int)(oy * {{stride_h}} + ky) - (int){{padding_h}};
    \\            if (iy < 0 || {{#unsigned_bounds}}({{u}})iy >= {{height}}{{/unsigned_bounds}}{{^unsigned_bounds}}iy >= (int){{height}}{{/unsigned_bounds}}) continue;
    \\            for ({{u}} kx = 0; kx < {{kernel_w}}; ++kx) {
    \\                int ix = (int)(ox * {{stride_w}} + kx) - (int){{padding_w}};
    \\                if (ix < 0 || {{#unsigned_bounds}}({{u}})ix >= {{width}}{{/unsigned_bounds}}{{^unsigned_bounds}}ix >= (int){{width}}{{/unsigned_bounds}}) continue;
    \\                {{u}} x_idx = ((b * {{p}}in_channels + ic) * {{height}} + ({{u}})iy) * {{width}} + ({{u}})ix;
    \\                {{u}} w_idx = (((oc * in_per_group + ig) * {{kernel_h}} + ky) * {{kernel_w}}) + kx;
    \\                acc += input[x_idx] * weight[w_idx];
    \\            }
    \\        }
    \\    }
    \\    {{output}}[{{i}}] = acc;
    \\
;

/// Fixed 3x3, stride-two, one-pixel padding shared by the qualified scalar and
/// matrix staging schedules. Cast placement keeps the old U32 coordinate math.
pub const Stencil3x3Stride2 = struct {
    pub fn coordinates(comptime position: []const u8, comptime reduction: []const u8, comptime width: []const u8) []const u8 {
        return "int iy=(int)((" ++ position ++ "/" ++ width ++ ")*2+" ++ reduction ++ "/3%3)-1,ix=(int)((" ++ position ++ "%" ++ width ++ ")*2+" ++ reduction ++ "%3)-1;\n";
    }
};

/// Scalar shared-memory 16x16 output tile. Hardware MMA is a distinct schedule;
/// its staging can reuse the stencil without changing fragment ownership.
pub const Stencil3x3Stride2Tile16 = struct {
    pub const tile = 16;
    pub const threads = [3]u32{ tile, tile, 1 };
    pub const scratch_bytes = 2 * tile * tile * @sizeOf(f32);
    pub const header = @import("std").fmt.comptimePrint(
        "#define ANTFLY_CONV3_S2_TILE16_THREADS_X {d}u\n#define ANTFLY_CONV3_S2_TILE16_THREADS_Y {d}u\n",
        .{ threads[0], threads[1] },
    );
    pub const Native = struct {
        integer: []const u8,
        shared: []const u8,
        barrier: []const u8,
        thread: []const u8,
        group: []const u8,
        prefix: []const u8 = "",
        optional_bias: bool = false,
        // CUDA widens batch/channel bases, retaining U32 coordinate products.
        wide_base: bool = false,
        fused: bool = false,
        bounded_tail: bool = false,
    };
    pub fn native(comptime n: Native) []const u8 {
        const f = @import("fexpr.zig");
        const ic = n.prefix ++ "in_channels";
        const oc = n.prefix ++ "out_channels";
        const height = n.prefix ++ "height";
        const width = n.prefix ++ "width";
        const oh = n.prefix ++ "out_h";
        const ow = n.prefix ++ "out_w";
        const product = f.FExpr.mulAdd(f.operand("left"), f.operand("right"), f.operand("sum"));
        const update = if (n.fused) "acc=" ++ product.renderNative(@import("native_math.zig").cuda, .{ .left = "a[y][k]", .right = "b[k][x]", .sum = "acc" }, null) ++ ";" else "acc+=a[y][k]*b[k][x];";
        return render(
            \\{{shared}} float a[16][16],b[16][16];
            \\{{u}} x={{tx}},y={{ty}},channel={{gy}}*16+y,pos={{gx}}*16+x,batch={{gz}};
            \\float acc={{bias_check}}channel<{{oc}} ? bias[channel] : 0.f;
            \\for({{u}} base=0;base<{{ic}}*9;base+=16) {
            \\    {{u}} ak=base+x,bk=base+y;
            \\    a[y][x]=(channel<{{oc}} && ak<{{ic}}*9) ? weight[{{wide}}channel*{{ic}}*9+ak] : 0.f;
            \\    {{coordinates}}
            \\    b[y][x]=(pos<{{oh}}*{{ow}} && bk<{{ic}}*9 && iy>=0 && iy<(int){{height}} && ix>=0 && ix<(int){{width}}) ? input[{{input_index}}] : 0.f;
            \\    {{barrier}}
            \\{{#fused}}    #pragma unroll
            \\{{/fused}}    for({{u}} k=0;k<{{extent}};k++) {{update}}
            \\    {{barrier}}
            \\}
            \\if(channel<{{oc}} && pos<{{oh}}*{{ow}}) out[(({{wide}}batch*{{oc}}+channel)*{{oh}}*{{ow}})+pos]=acc;
            \\
        , .{
            .shared = n.shared,
            .u = n.integer,
            .tx = n.thread ++ ".x",
            .ty = n.thread ++ ".y",
            .gx = n.group ++ ".x",
            .gy = n.group ++ ".y",
            .gz = n.group ++ ".z",
            .ic = ic,
            .oc = oc,
            .height = height,
            .width = width,
            .oh = oh,
            .ow = ow,
            .bias_check = if (n.optional_bias) n.prefix ++ "has_bias!=0 && " else "",
            .wide = if (n.wide_base) "(size_t)" else "",
            .coordinates = Stencil3x3Stride2.coordinates("pos", "bk", ow),
            .barrier = n.barrier,
            .fused = n.fused,
            .update = update,
            .extent = if (n.bounded_tail) "min(16u," ++ ic ++ "*9-base)" else "16",
            .input_index = if (n.wide_base) "((size_t)batch*" ++ ic ++ "+bk/9)*" ++ height ++ "*" ++ width ++ "+(unsigned)iy*" ++ width ++ "+(unsigned)ix" else "((batch*" ++ ic ++ "+bk/9)*" ++ height ++ "+uint(iy))*" ++ width ++ "+uint(ix)",
        });
    }
};
