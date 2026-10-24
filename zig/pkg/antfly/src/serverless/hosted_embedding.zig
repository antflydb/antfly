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
pub const Dense = struct {
    pub fn embed(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, text: []const u8, dims: u32) ![]f32 {
        const vectors = try batch(ptr, a, name, &.{text}, dims);
        defer a.free(vectors);
        return @constCast(vectors[0]);
    }
    pub fn batch(_: *anyopaque, a: std.mem.Allocator, _: []const u8, texts: []const []const u8, dims: u32) ![]const []const f32 {
        if (dims == 0 or dims > 1536 or texts.len == 0) return error.HostedEmbeddingBatchExceeded;
        const window: usize = @min(64, (256 * 1024) / (@as(usize, dims) * 16));
        // The invocation's existing eight-call allowance bounds the complete
        // document. Split provider responses without changing native progress
        // or publishing any partially embedded document.
        if (texts.len > window * 8) return error.HostedEmbeddingBatchExceeded;
        const vectors = try a.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| a.free(vector);
            a.free(vectors);
        }
        while (initialized < texts.len) {
            const end = @min(texts.len, initialized + window);
            const part = try batchWindow(a, texts[initialized..end], dims);
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
