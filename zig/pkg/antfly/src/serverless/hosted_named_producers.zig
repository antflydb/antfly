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

//! Named enrichment producers of the hosted (wasm) runtime (#1791). Dense
//! vectors use the host's embedding task; producer-backed assets use the
//! host's `asset` inference task:
//!
//!   request  {"task":"asset","name":..,"producer":{type,config},"content_type":..,"source":..}
//!   reply    {"output": "<text>"}   (JSON content types hold JSON text)
//!
//! A host that cannot serve a request fails it; the document is retried on a
//! later cycle. Sparse vectors have no hosted task and stay pending.
const std = @import("std");
const Reply = @import("objectstore").wasm_host.Reply;
const named_outputs = @import("enrichment/named_outputs.zig");
const hosted_embedding = @import("hosted_embedding.zig");
const EnrichmentConfig = @import("catalog/named_enrichments.zig").EnrichmentConfig;
extern "antfly_host" fn infer_call([*]const u8, usize, [*]const u8, usize, *Reply) u32;

pub const Hosted = struct {
    embedding: *hosted_embedding.Context,
    /// Dimensions for declarations without `expected_dims`.
    default_dims: u32,

    pub fn producers(self: *Hosted) named_outputs.Producers {
        return .{ .ctx = self, .embed = embed, .asset = asset };
    }

    fn embed(ctx: *anyopaque, a: std.mem.Allocator, cfg: EnrichmentConfig, texts: []const []const u8) anyerror![][]f32 {
        const self: *Hosted = @ptrCast(@alignCast(ctx));
        const dims = if (cfg.expected_dims != 0) cfg.expected_dims else self.default_dims;
        const vectors = try hosted_embedding.Dense.batch(self.embedding, a, cfg.name, texts, dims);
        defer a.free(vectors);
        const out = try a.alloc([]f32, vectors.len);
        for (out, vectors) |*owned, vector| owned.* = @constCast(vector);
        return out;
    }

    fn asset(_: *anyopaque, a: std.mem.Allocator, cfg: EnrichmentConfig, source: []const u8, _: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) anyerror!std.json.Value {
        try cancellation.check();
        const producer: std.json.Value = if (cfg.producer_json.len == 0) .null else try std.json.parseFromSliceLeaky(std.json.Value, a, cfg.producer_json, .{});
        const request = try std.json.Stringify.valueAlloc(a, .{ .task = "asset", .name = cfg.name, .producer = producer, .content_type = cfg.content_type, .source = source }, .{});
        defer a.free(request);
        var reply: Reply = .{};
        const status = infer_call(request.ptr, request.len, "".ptr, 0, &reply);
        const header: []u8 = if (reply.json_len == 0) &.{} else @as([*]u8, @ptrFromInt(reply.json_ptr))[0..reply.json_len];
        defer std.heap.wasm_allocator.free(header);
        if (reply.body_len != 0) std.heap.wasm_allocator.free(@as([*]u8, @ptrFromInt(reply.body_ptr))[0..reply.body_len]);
        if (status != 0) return error.HostedAssetFailed;
        const parsed = try std.json.parseFromSlice(struct { output: []const u8 }, a, header, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        return named_outputs.outputValue(a, cfg.content_type, try a.dupe(u8, parsed.value.output));
    }
};
