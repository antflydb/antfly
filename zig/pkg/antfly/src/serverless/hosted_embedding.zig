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

const std = @import("std");
const Reply = @import("objectstore").wasm_host.Reply;
extern "antfly_host" fn infer_call([*]const u8, usize, [*]const u8, usize, *Reply) u32;
pub const Context = @import("hosted_embedding_checkpoint.zig").Context;
pub const Dense = struct {
    pub fn embed(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, text: []const u8, dims: u32) ![]f32 {
        const vectors = try batch(ptr, a, name, &.{text}, dims);
        defer a.free(vectors);
        return @constCast(vectors[0]);
    }
    pub fn batch(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8, dims: u32) ![]const []const f32 {
        if (dims == 0 or dims > 1536 or texts.len == 0) return error.HostedEmbeddingBatchExceeded;
        const window: usize = @min(64, (256 * 1024) / (@as(usize, dims) * 16));
        // Bound each provider response and retain native document-level source
        // validation. Checkpointed passes can yield after complete windows;
        // only the full embedding set reaches the authoritative WAL.
        const context: *Context = @ptrCast(@alignCast(ptr));
        if (texts.len > (if (context.enabled) @as(usize, 4096) else window * 8) or
            texts.len * @as(usize, dims) * 4 > 8 * 1024 * 1024) return error.HostedEmbeddingBatchExceeded;
        const vectors = try a.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| a.free(vector);
            a.free(vectors);
        }
        while (initialized < texts.len) {
            const end = @min(texts.len, initialized + window);
            const input = if (context.enabled) try context.inputDigest(a, texts[initialized..end], dims) else @as([32]u8, @splat(0));
            const cached = if (context.enabled) try context.load(a, input, end - initialized, dims) else null;
            const part = cached orelse compute: {
                if (context.provider_calls >= context.max_provider_calls) return if (context.enabled) error.HostedEmbeddingYield else error.HostedEmbeddingBatchExceeded;
                context.provider_calls += 1;
                const result = try batchWindow(a, texts[initialized..end], dims);
                errdefer {
                    for (result) |vector| a.free(vector);
                    a.free(result);
                }
                if (context.enabled) try context.save(a, input, result, dims);
                break :compute result;
            };
            defer a.free(part);
            @memcpy(vectors[initialized..end], part);
            initialized = end;
        }
        return vectors;
    }
    fn batchWindow(a: std.mem.Allocator, texts: []const []const u8, dims: u32) ![]const []const f32 {
        const request = try std.json.Stringify.valueAlloc(a, .{ .task = "embedding", .texts = texts, .dimensions = dims }, .{});
        defer a.free(request);
        var reply: Reply = .{};
        const status = infer_call(request.ptr, request.len, "".ptr, 0, &reply);
        const header: []u8 = if (reply.json_len == 0) &.{} else @as([*]u8, @ptrFromInt(reply.json_ptr))[0..reply.json_len];
        defer std.heap.wasm_allocator.free(header);
        if (reply.body_len != 0) std.heap.wasm_allocator.free(@as([*]u8, @ptrFromInt(reply.body_ptr))[0..reply.body_len]);
        if (status != 0) return error.HostedEmbeddingFailed;
        const result = try std.json.parseFromSlice(struct { data: []const []const f32 }, a, header, .{ .ignore_unknown_fields = true });
        defer result.deinit();
        if (result.value.data.len != texts.len) return error.HostedEmbeddingDimensionsMismatch;
        const vectors = try a.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| a.free(vector);
            a.free(vectors);
        }
        for (result.value.data, vectors) |vector, *owned| {
            if (vector.len != dims) return error.HostedEmbeddingDimensionsMismatch;
            for (vector) |value| if (!std.math.isFinite(value)) return error.HostedEmbeddingInvalid;
            owned.* = try a.dupe(f32, vector);
            initialized += 1;
        }
        return vectors;
    }
};
