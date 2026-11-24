// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fixed-geometry visual backbone and 2x2 adapter of the released checkpoint.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const buffer = @import("../ops/cuda/buffer.zig");
const Context = @import("../ops/cuda/context.zig").CudaContext;
const Blas = @import("../ops/cuda/cublaslt.zig").CublasLt;
const Arena = @import("../ops/cuda/pinned_input_arena.zig").Arena;
const embedded = @import("../ops/cuda/embedded_module.zig");
const entries = @import("../tile/cuda_standalone.zig");
const launch_abi = @import("../ops/cuda/launch_abi.zig");
const MmapRegion = @import("../util/c_file.zig").MmapRegion;
const Program = embedded.EmbeddedModule(struct {
    pub const image = embedded.profile(entries.patch_transformer).image;
}, entries.patch_transformer_entries);
const DeviceBuffer = buffer.DeviceBuffer;

const width = 1024;
const inner = 4096;
const text_width = 2304;
const patch_features = 20 * 20 * 3;
const layers = 23;
const eps = 1e-5;
const threads = 128;

const Layer = enum { ln1, bias1, ln2, bias2, scale1, scale2, q, q_bias, k, k_bias, v, v_bias, out, out_bias, up, up_bias, down, down_bias };
const Weight = enum { patch, pos, cls, cls_pos, pre, pre_bias, in, out };
const Temp = enum { rgb, patches, patch_embed, embed, hidden, normed, q, k, v, attention, update, wide, merged, blas };

fn Buffers(comptime T: type) type {
    return [@typeInfo(T).@"enum".field_names.len]DeviceBuffer;
}

const Tensor = struct { name: []const u8, shape: []const i64 };

fn upload(ctx: *Context, source: *const checkpoint.Checkpoint, arena: *Arena, path: []const u8, shape: []const i64) !DeviceBuffer {
    const tensor = source.reader.header.tensors.get(path) orelse return error.MissingInkling2Weight;
    if (tensor.dtype != .bf16 or tensor.encoding != .dense or !std.mem.eql(i64, tensor.shape, shape)) return error.InvalidInkling2Weight;
    return arena.uploadWeight(ctx, try source.reader.rawTensorBytes(path));
}

pub const Vision = struct {
    ctx: *Context,
    weights: Buffers(Weight) = @splat(.{}),
    layer_weights: [layers]Buffers(Layer) = @splat(@splat(.{})),
    program: Program = .{},

    pub fn load(ctx: *Context, source: *const checkpoint.Checkpoint) !Vision {
        var vision: Vision = .{ .ctx = ctx };
        errdefer vision.deinit();
        var arena: Arena = .{};
        defer arena.deinit(ctx);
        const mapping = if (source.reader.mmap_region) |region| region.data else null;
        var upload_read = MmapRegion.SequentialRead.begin(mapping, source.reader.file_bytes);
        defer upload_read.deinit();
        const model_tensors: std.EnumArray(Weight, Tensor) = .init(.{
            .patch = .{ .name = "backbone.embeddings.patch_embedding.weight", .shape = &.{ width, patch_features } },
            .pos = .{ .name = "backbone.embeddings.position_embedding", .shape = &.{ 32 * 32, width } },
            .cls = .{ .name = "backbone.embeddings.class_embedding", .shape = &.{width} },
            .cls_pos = .{ .name = "backbone.embeddings.cls_position_embedding", .shape = &.{width} },
            .pre = .{ .name = "backbone.ln_pre.weight", .shape = &.{width} },
            .pre_bias = .{ .name = "backbone.ln_pre.bias", .shape = &.{width} },
            .in = .{ .name = "proj_in.weight", .shape = &.{ inner, width * 4 } },
            .out = .{ .name = "proj_out.weight", .shape = &.{ text_width, inner } },
        });
        const layer_tensors: std.EnumArray(Layer, Tensor) = .init(.{
            .ln1 = .{ .name = "ln_1.weight", .shape = &.{width} },
            .bias1 = .{ .name = "ln_1.bias", .shape = &.{width} },
            .ln2 = .{ .name = "ln_2.weight", .shape = &.{width} },
            .bias2 = .{ .name = "ln_2.bias", .shape = &.{width} },
            .scale1 = .{ .name = "ls_1.gamma", .shape = &.{width} },
            .scale2 = .{ .name = "ls_2.gamma", .shape = &.{width} },
            .q = .{ .name = "self_attn.q_proj.weight", .shape = &.{ width, width } },
            .q_bias = .{ .name = "self_attn.q_proj.bias", .shape = &.{width} },
            .k = .{ .name = "self_attn.k_proj.weight", .shape = &.{ width, width } },
            .k_bias = .{ .name = "self_attn.k_proj.bias", .shape = &.{width} },
            .v = .{ .name = "self_attn.v_proj.weight", .shape = &.{ width, width } },
            .v_bias = .{ .name = "self_attn.v_proj.bias", .shape = &.{width} },
            .out = .{ .name = "self_attn.out_proj.weight", .shape = &.{ width, width } },
            .out_bias = .{ .name = "self_attn.out_proj.bias", .shape = &.{width} },
            .up = .{ .name = "mlp.c_fc.weight", .shape = &.{ inner, width } },
            .up_bias = .{ .name = "mlp.c_fc.bias", .shape = &.{inner} },
            .down = .{ .name = "mlp.c_proj.weight", .shape = &.{ width, inner } },
            .down_bias = .{ .name = "mlp.c_proj.bias", .shape = &.{width} },
        });
        var name_buf: [128]u8 = undefined;
        inline for (std.enums.values(Weight)) |key| {
            const tensor = model_tensors.get(key);
            const name = try std.fmt.bufPrint(&name_buf, "model.visual.{s}", .{tensor.name});
            vision.weights[@backingInt(key)] = try upload(ctx, source, &arena, name, tensor.shape);
        }
        for (&vision.layer_weights, 0..) |*storage, i| {
            inline for (std.enums.values(Layer)) |key| {
                const tensor = layer_tensors.get(key);
                const name = try std.fmt.bufPrint(&name_buf, "model.visual.backbone.layers.{d}.{s}", .{ i, tensor.name });
                storage[@backingInt(key)] = try upload(ctx, source, &arena, name, tensor.shape);
            }
        }
        try ctx.synchronize();
        return vision;
    }

    pub fn deinit(self: *Vision) void {
        self.program.deinit(self.ctx);
        for (&self.layer_weights) |*layer| for (layer) |*weight| weight.free(self.ctx);
        for (&self.weights) |*weight| weight.free(self.ctx);
    }

    fn launch(self: *Vision, comptime entry: Program.Entry, x: u32, y: u32, args: anytype) !void {
        var abi = launch_abi.addresses(args);
        try self.program.launch(self.ctx, entry, x, y, threads, &abi);
    }

    fn layerNorm(self: *Vision, output: DeviceBuffer, input: DeviceBuffer, gamma: DeviceBuffer, beta: DeviceBuffer, rows: u32) !void {
        var pointers = [_]u64{ output.ptr, input.ptr, gamma.ptr, beta.ptr };
        var epsilon: f32 = eps;
        try self.launch(.norm, rows, 1, .{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &epsilon });
    }

    fn gelu(self: *Vision, storage: DeviceBuffer, values: u32) !void {
        var ptr = storage.ptr;
        var count = values;
        try self.launch(.gelu, blocks(values), 1, .{ &ptr, &count });
    }

    fn residual(self: *Vision, hidden: DeviceBuffer, update: DeviceBuffer, gamma: DeviceBuffer, values: u32) !void {
        var pointers = [_]u64{ hidden.ptr, update.ptr, gamma.ptr };
        var count = values;
        try self.launch(.residual, blocks(values), 1, .{ &pointers[0], &pointers[1], &pointers[2], &count });
    }

    pub fn encodeRgb(self: *Vision, blas: *Blas, rgb: []const u8, image_x: u32, image_y: u32) !Embeddings {
        if (image_x == 0 or image_y == 0 or image_x > 4096 or image_y > 4096 or rgb.len != @as(usize, image_x) * image_y * 3) return error.InvalidInkling2Image;
        const ctx = self.ctx;
        var gx: u32 = 2 * ((image_x + 39) / 40);
        var gy: u32 = 2 * ((image_y + 39) / 40);
        const patch_rows = gx * gy;
        var token_rows = patch_rows + 1;
        const merged_rows = patch_rows / 4;
        var arena: Buffers(Temp) = @splat(.{});
        defer for (&arena) |*item| item.free(ctx);
        var pinned = try buffer.HostBuffer.alloc(ctx, rgb.len);
        defer pinned.free(ctx);
        var result: Embeddings = .{ .device = try DeviceBuffer.allocWithPurpose(ctx, @as(usize, merged_rows) * text_width * 2, .command_workspace), .rows = merged_rows };
        errdefer result.deinit(ctx);
        errdefer ctx.synchronize() catch {};
        const narrow_bytes = @as(usize, token_rows) * width * 2;
        const workspace_bytes: std.EnumArray(Temp, usize) = .init(.{
            .rgb = rgb.len,
            .patches = @as(usize, patch_rows) * patch_features * 2,
            .patch_embed = @as(usize, patch_rows) * width * 2,
            .embed = narrow_bytes,
            .hidden = narrow_bytes,
            .normed = narrow_bytes,
            .q = narrow_bytes,
            .k = narrow_bytes,
            .v = narrow_bytes,
            .attention = narrow_bytes,
            .update = narrow_bytes,
            .wide = @as(usize, token_rows) * inner * 2,
            .merged = @as(usize, merged_rows) * width * 4 * 2,
            .blas = 8 * 1024 * 1024,
        });
        inline for (std.enums.values(Temp)) |key| arena[@backingInt(key)] = try DeviceBuffer.allocWithPurpose(ctx, workspace_bytes.get(key), .command_workspace);
        const workspace = arena[@backingInt(Temp.blas)];
        const hidden = arena[@backingInt(Temp.hidden)];
        const normed = arena[@backingInt(Temp.normed)];
        const update = arena[@backingInt(Temp.update)];
        const wide = arena[@backingInt(Temp.wide)];
        try self.program.init(ctx);
        @memcpy(pinned.bytes(), rgb);
        try arena[@backingInt(Temp.rgb)].copyFromHost(ctx, pinned.constBytes());
        var x = image_x;
        var y = image_y;
        var p = [_]u64{ arena[@backingInt(Temp.patches)].ptr, arena[@backingInt(Temp.rgb)].ptr };
        try self.launch(.patches, patch_rows, 1, .{ &p[0], &p[1], &x, &y, &gx });
        const patch_embed = arena[@backingInt(Temp.patch_embed)];
        try blas.matmulBf16WeightBf16Out(ctx, patch_embed, arena[@backingInt(Temp.patches)], self.weights[@backingInt(Weight.patch)], workspace, patch_rows, patch_features, width);
        const embed = arena[@backingInt(Temp.embed)];
        var positions = [_]u64{ embed.ptr, patch_embed.ptr, self.weights[@backingInt(Weight.pos)].ptr, self.weights[@backingInt(Weight.cls)].ptr, self.weights[@backingInt(Weight.cls_pos)].ptr };
        try self.launch(.positions, token_rows, 1, .{ &positions[0], &positions[1], &positions[2], &positions[3], &positions[4], &gx, &gy });
        try self.layerNorm(hidden, embed, self.weights[@backingInt(Weight.pre)], self.weights[@backingInt(Weight.pre_bias)], token_rows);
        const attention = arena[@backingInt(Temp.attention)];
        const q = arena[@backingInt(Temp.q)];
        const k = arena[@backingInt(Temp.k)];
        const v = arena[@backingInt(Temp.v)];
        for (&self.layer_weights) |*l| {
            try self.layerNorm(normed, hidden, l[@backingInt(Layer.ln1)], l[@backingInt(Layer.bias1)], token_rows);
            for ([_]DeviceBuffer{ q, k, v }, [_]Layer{ .q, .k, .v }, [_]Layer{ .q_bias, .k_bias, .v_bias }) |dst, w, bias| {
                try blas.matmulBf16WeightBiasBf16Out(ctx, dst, normed, l[@backingInt(w)], l[@backingInt(bias)], workspace, token_rows, width, width);
            }
            var qkv = [_]u64{ attention.ptr, q.ptr, k.ptr, v.ptr };
            try self.launch(.rotary, token_rows, 1, .{ &qkv[1], &qkv[2], &gx });
            try self.launch(.attention, token_rows, 4, .{ &qkv[0], &qkv[1], &qkv[2], &qkv[3], &token_rows });
            try blas.matmulBf16WeightBiasBf16Out(ctx, update, attention, l[@backingInt(Layer.out)], l[@backingInt(Layer.out_bias)], workspace, token_rows, width, width);
            try self.residual(hidden, update, l[@backingInt(Layer.scale1)], token_rows * width);
            try self.layerNorm(normed, hidden, l[@backingInt(Layer.ln2)], l[@backingInt(Layer.bias2)], token_rows);
            try blas.matmulBf16WeightBiasBf16Out(ctx, wide, normed, l[@backingInt(Layer.up)], l[@backingInt(Layer.up_bias)], workspace, token_rows, width, inner);
            try self.gelu(wide, token_rows * inner);
            try blas.matmulBf16WeightBiasBf16Out(ctx, update, wide, l[@backingInt(Layer.down)], l[@backingInt(Layer.down_bias)], workspace, token_rows, inner, width);
            try self.residual(hidden, update, l[@backingInt(Layer.scale2)], token_rows * width);
        }
        const merged = arena[@backingInt(Temp.merged)];
        var merge = [_]u64{ merged.ptr, hidden.ptr };
        try self.launch(.merge, merged_rows, 1, .{ &merge[0], &merge[1], &gx });
        try blas.matmulBf16WeightBf16Out(ctx, wide, merged, self.weights[@backingInt(Weight.in)], workspace, merged_rows, width * 4, inner);
        try self.gelu(wide, merged_rows * inner);
        try blas.matmulBf16WeightBf16Out(ctx, result.device, wide, self.weights[@backingInt(Weight.out)], workspace, merged_rows, inner, text_width);
        try ctx.synchronize();
        return result;
    }
};

fn blocks(values: u32) u32 {
    return (values - 1) / threads + 1;
}

pub const Embeddings = struct {
    device: DeviceBuffer,
    rows: u32,

    pub fn row(self: Embeddings, index: u32) !DeviceBuffer {
        if (index >= self.rows) return error.InvalidInkling2ImageRow;
        const bytes = text_width * 2;
        return .{ .ptr = self.device.ptr + @as(usize, index) * bytes, .len = bytes, .borrowed = true };
    }

    pub fn deinit(self: *Embeddings, ctx: *Context) void {
        self.device.free(ctx);
    }
};

test "CUDA BF16 patch norm reads BF16 inputs, scales and biases" {
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var vision: Vision = .{ .ctx = &ctx };
    defer vision.deinit();
    try vision.program.init(&ctx);
    const row_bytes = width * 2;
    var host = try buffer.HostBuffer.alloc(&ctx, 4 * row_bytes);
    defer host.free(&ctx);
    var backing = try DeviceBuffer.alloc(&ctx, host.len);
    defer backing.free(&ctx);
    var device: [4]DeviceBuffer = undefined;
    for (&device, 0..) |*view, index| view.* = .{ .ptr = backing.ptr + index * row_bytes, .len = row_bytes, .borrowed = true };
    const signal = [_]u16{ 0xbf80, 0x3f80 };
    const gamma = [_]u16{ 0x3f80, 0x4000 };
    for (0..width) |i| {
        std.mem.writeInt(u16, host.bytes()[2 * i ..][0..2], signal[i % 2], .little);
        std.mem.writeInt(u16, host.bytes()[row_bytes + 2 * i ..][0..2], gamma[i % 2], .little);
        std.mem.writeInt(u16, host.bytes()[2 * row_bytes + 2 * i ..][0..2], 0x3f00, .little);
    }
    try backing.copyFromHost(&ctx, host.constBytes()[0 .. 3 * row_bytes]);
    try vision.layerNorm(device[3], device[0], device[1], device[2], 1);
    try device[3].copyToHost(&ctx, host.bytes()[3 * row_bytes ..]);
    try ctx.synchronize();
    const expected = [_]u16{ 0xbf00, 0x4020 };
    for (0..width) |i| {
        try std.testing.expectEqual(expected[i % 2], std.mem.readInt(u16, host.constBytes()[3 * row_bytes + 2 * i ..][0..2], .little));
    }
}
