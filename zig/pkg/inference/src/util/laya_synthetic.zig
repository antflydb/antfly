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

//! Fixture-free Laya test support: a whitespace word-hash tokenizer and a
//! small seeded ModernBERT + decision-head checkpoint in upstream tensor names.
//! Tests that need PyTorch agreement use the ANTFLY_LAYA_* fixtures instead.
const std = @import("std");
const tokenizer_mod = @import("inference_tokenizer");
const checkpoint = @import("../finetune/safetensors_checkpoint.zig");

pub const vocab_size = 64;
pub const hidden = 64;
pub const intermediate = 96;
pub const layers = 3;
pub const n_act = 2;

/// `[PAD]=0 [UNK]=1 [CLS]=2 [SEP]=3 [MASK]=4`; lowercase words hash to 5..63.
pub const WordTokenizer = struct {
    const special = tokenizer_mod.SpecialTokens{ .pad_id = 0, .unk_id = 1, .cls_id = 2, .sep_id = 3, .mask_id = 4 };

    pub fn tokenizer(self: *WordTokenizer) tokenizer_mod.Tokenizer {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn word(text: []const u8) i32 {
        var h: u32 = 2166136261;
        for (text) |c| h = (h ^ std.ascii.toLower(c)) *% 16777619;
        return @intCast(5 + h % (vocab_size - 5));
    }
    fn encode(_: *anyopaque, a: std.mem.Allocator, text: []const u8) anyerror![]i32 {
        var out: std.ArrayListUnmanaged(i32) = .empty;
        errdefer out.deinit(a);
        var words = std.mem.tokenizeAny(u8, text, " \t\r\n:,?.");
        while (words.next()) |w| try out.append(a, word(w));
        return out.toOwnedSlice(a);
    }
    fn encodeInto(ptr: *anyopaque, a: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(ptr, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }
    fn encodeForModel(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: usize) anyerror!tokenizer_mod.EncodeResult {
        return error.Unsupported;
    }
    fn encodeGeneration(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: usize, _: bool) anyerror!tokenizer_mod.EncodeResult {
        return error.Unsupported;
    }
    fn decode(_: *anyopaque, _: std.mem.Allocator, _: []const i32) anyerror![]u8 {
        return error.Unsupported;
    }
    fn specialTokens(_: *anyopaque) tokenizer_mod.SpecialTokens {
        return special;
    }
    fn vocabSize(_: *anyopaque) usize {
        return vocab_size;
    }
    fn deinit(_: *anyopaque) void {}
    const vtable = tokenizer_mod.Tokenizer.VTable{
        .encode = encode,
        .encodeInto = encodeInto,
        .encodeForModel = encodeForModel,
        .encodeGeneration = encodeGeneration,
        .decode = decode,
        .specialTokens = specialTokens,
        .vocabSize = vocabSize,
        .deinit = deinit,
    };
};

/// Write `config.json`, `model_manifest.json`, and `model.safetensors` under
/// `dir`. `packing` is the JSON value of `laya.packing`, or null.
pub fn writeModel(a: std.mem.Allocator, io: std.Io, dir: []const u8, packing: ?[]const u8, max_len: usize, seed: u64) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const config = try std.fmt.allocPrint(s,
        \\{{"model_type":"modernbert","architectures":["ModernBertModel"],"vocab_size":{d},"hidden_size":{d},"num_hidden_layers":{d},"num_attention_heads":2,"intermediate_size":{d},"max_position_embeddings":512,"local_attention":8,"global_attn_every_n_layers":3,"layer_norm_eps":1e-5,"pad_token_id":0,"cls_token_id":2,"sep_token_id":3,
        \\"laya":{{"head_layers":2,"max_len":{d},"head_max_len":48,"act_costs":{{"escalate":0.5}},"mask_token":"[MASK]"{s}{s}}}}}
    , .{ vocab_size, hidden, layers, intermediate, max_len, if (packing != null) ",\"packing\":" else "", packing orelse "" });
    const manifest =
        \\{"type":"classifier","tasks":["extract"],"capabilities":["classification","typed_decisions"],"inputs":["text"],"source":{"repository":"synthetic-laya","revision":"seeded"}}
    ;
    var d = try std.Io.Dir.cwd().openDir(io, dir, .{});
    defer d.close(io);
    try d.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try d.writeFile(io, .{ .sub_path = "model_manifest.json", .data = manifest });
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var tensors: std.ArrayListUnmanaged(checkpoint.NamedTensor) = .empty;
    const Add = struct {
        fn tensor(list: *std.ArrayListUnmanaged(checkpoint.NamedTensor), alloc: std.mem.Allocator, r: std.Random, name: []const u8, shape: []const usize, center: f32, scale: f32) !void {
            var n: usize = 1;
            for (shape) |dim| n *= dim;
            const values = try alloc.alloc(f32, n);
            for (values) |*v| v.* = center + scale * r.floatNorm(f32);
            try list.append(alloc, .{ .name = try alloc.dupe(u8, name), .data = values, .shape = try alloc.dupe(usize, shape) });
        }
    };
    const d_: usize = hidden;
    try Add.tensor(&tensors, s, random, "encoder.embeddings.tok_embeddings.weight", &.{ vocab_size, d_ }, 0, 0.5);
    try Add.tensor(&tensors, s, random, "encoder.embeddings.norm.weight", &.{d_}, 1, 0.05);
    try Add.tensor(&tensors, s, random, "encoder.final_norm.weight", &.{d_}, 1, 0.05);
    for (0..layers) |layer| {
        var name: [96]u8 = undefined;
        if (layer > 0) try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn_norm.weight", .{layer}), &.{d_}, 1, 0.05);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp_norm.weight", .{layer}), &.{d_}, 1, 0.05);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wqkv.weight", .{layer}), &.{ 3 * d_, d_ }, 0, 0.15);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.attn.Wo.weight", .{layer}), &.{ d_, d_ }, 0, 0.1);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wi.weight", .{layer}), &.{ 2 * intermediate, d_ }, 0, 0.1);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "encoder.layers.{d}.mlp.Wo.weight", .{layer}), &.{ d_, intermediate }, 0, 0.1);
    }
    try Add.tensor(&tensors, s, random, "type_emb.weight", &.{ 3, d_ }, 0, 0.5);
    for (0..2) |layer| {
        var name: [96]u8 = undefined;
        const prefix = try std.fmt.allocPrint(s, "head.layers.{d}", .{layer});
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.self_attn.in_proj_weight", .{prefix}), &.{ 3 * d_, d_ }, 0, 0.15);
        try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.self_attn.in_proj_bias", .{prefix}), &.{3 * d_}, 0, 0.05);
        inline for (.{ .{ "self_attn.out_proj", d_, d_ }, .{ "linear1", d_, 4 * d_ }, .{ "linear2", 4 * d_, d_ } }) |spec| {
            try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.{s}.weight", .{ prefix, spec[0] }), &.{ spec[2], spec[1] }, 0, 0.1);
            try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.{s}.bias", .{ prefix, spec[0] }), &.{spec[2]}, 0, 0.05);
        }
        inline for (.{ "norm1", "norm2" }) |norm| {
            try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.{s}.weight", .{ prefix, norm }), &.{d_}, 1, 0.05);
            try Add.tensor(&tensors, s, random, try std.fmt.bufPrint(&name, "{s}.{s}.bias", .{ prefix, norm }), &.{d_}, 0, 0.05);
        }
    }
    try Add.tensor(&tensors, s, random, "scorer.0.weight", &.{d_}, 1, 0.05);
    try Add.tensor(&tensors, s, random, "scorer.0.bias", &.{d_}, 0, 0.05);
    try Add.tensor(&tensors, s, random, "scorer.1.weight", &.{ d_, d_ }, 0, 0.2);
    try Add.tensor(&tensors, s, random, "scorer.1.bias", &.{d_}, 0, 0.05);
    try Add.tensor(&tensors, s, random, "scorer.3.weight", &.{ 1, d_ }, 0, 0.5);
    try Add.tensor(&tensors, s, random, "scorer.3.bias", &.{1}, 0, 0.05);
    try Add.tensor(&tensors, s, random, "act_head.0.weight", &.{ 256, d_ + 4 }, 0, 0.1);
    try Add.tensor(&tensors, s, random, "act_head.0.bias", &.{256}, 0, 0.05);
    try Add.tensor(&tensors, s, random, "act_head.2.weight", &.{ n_act, 256 }, 0, 0.1);
    try Add.tensor(&tensors, s, random, "act_head.2.bias", &.{n_act}, 0, 0.05);
    const weights = try std.fs.path.join(s, &.{ dir, "model.safetensors" });
    try checkpoint.saveControlled(s, weights, tensors.items, null, false);
}
