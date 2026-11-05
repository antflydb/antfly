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

//! Read-pool broker ownership and caller-local classification presentation.
const std = @import("std");
const span = @import("extractors/gliner_span_v2_executor.zig");
const wire = @import("extractors/extraction_v2.zig");
const processor = @import("pipelines/gliner_boundary_processor.zig");

test {
    _ = @import("server/executor_microbatch.zig");
    _ = @import("server/read_pool_microbatch.zig");
    _ = @import("server/write_pool_microbatch.zig");
    _ = @import("server/tensor_microbatch.zig");
    _ = @import("architectures/whisper.zig");
    _ = @import("pipelines/text_batch_admission.zig");
    _ = @import("pipelines/batch_execution.zig");
    _ = @import("pipelines/embedding.zig");
}

test "read pool microbatch presents independent schemas without reentering the backend" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"classifications":[{"name":"intent","labels":["sales","support"]}]},"inputs":[{"id":"first","content":"Help"},{"id":"second","content":"Buy","schema":{"classifications":[{"name":"route","labels":["red","green","blue"]}]}}]}
    , .{});
    defer request.deinit();
    var samples: [1]processor.Sample = undefined;
    var batches: [1]processor.PreparedBatch = undefined;
    batches[0].samples = &samples;
    var ranges = [_]span.TaskRange{.{ .start = 0, .end = 1 }};
    var items = [_]span.PreparedItem{
        .{ .batches = &batches, .ranges = &ranges, .prompt_tokens = 7 },
        .{ .batches = &batches, .ranges = &ranges, .prompt_tokens = 11 },
    };
    const plan = span.Plan{ .items = &items, .prompt_tokens = 18 };
    var first = [_]f64{ 0, 8 };
    var second = [_]f64{ 0, 7, 0 };
    var rows1 = [_][]f64{&first};
    var rows2 = [_][]f64{&second};
    const logits = [_]span.ItemLogits{
        .{ .rows = &rows1, .prompt_tokens = 7, .sequences = 1 },
        .{ .rows = &rows2, .prompt_tokens = 11, .sequences = 1 },
    };
    const result = try span.executePlanned(null, a, .{ .deberta = .{} }, &request, &plan, .{ .classification_results = &logits });
    defer a.free(result);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqualStrings("first", data[0].object.get("id").?.string);
    try std.testing.expectEqualStrings("support", data[0].object.get("classifications").?.array.items[0].object.get("label").?.string);
    try std.testing.expectEqualStrings("green", data[1].object.get("classifications").?.array.items[0].object.get("label").?.string);
    try std.testing.expectEqual(@as(usize, 2), logits[0].rows[0].len);
    try std.testing.expectError(error.InvalidExtractionInput, span.executePlanned(null, a, .{ .deberta = .{} }, &request, &plan, .{ .classification_results = logits[0..1] }));
    items[0].ranges = &.{};
    try std.testing.expect(!span.canBatchClassifications(&request, &plan));
}

const MaskedEncoder = struct {
    pub fn inputInfo(_: @This()) []const @import("backends/tensor.zig").TensorInfo {
        return &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        };
    }
};

test "read pool microbatch document buckets bound input padding and backend tile rounding" {
    const bucket = @import("pipelines/batch_execution.zig").maskedSequenceBucket;
    for (1..8193) |tokens| {
        const width = bucket(MaskedEncoder{}, tokens, 8192);
        try std.testing.expect(width >= tokens and width * 4 <= tokens * 5);
        const rows = @min(@as(usize, 4), 8192 / width);
        const physical_width = if (width >= 128) std.mem.alignForward(usize, width, 64) else width;
        try std.testing.expect(rows >= 1 and rows * physical_width <= 8192);
    }
    try std.testing.expectEqual(@as(usize, 2048), bucket(MaskedEncoder{}, 2041, 8192));
    try std.testing.expectEqual(@as(usize, 4096), bucket(MaskedEncoder{}, 4091, 8192));
    try std.testing.expectEqual(@as(usize, 8192), bucket(MaskedEncoder{}, 8191, 8192));
}

test "read pool microbatch document rows share bounded forwards across heterogeneous arrays" {
    const broker_mod = @import("server/executor_microbatch.zig");
    const bucket = @import("pipelines/batch_execution.zig").maskedSequenceBucket;
    var broker = broker_mod.Broker.init(std.testing.allocator);
    defer broker.deinit();
    const Executor = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn execute(raw: *anyopaque, items: []const broker_mod.ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            var padded: usize = 0;
            for (items) |item| padded += bucket(MaskedEncoder{}, item.payloadAs(usize).*, 8192);
            std.debug.assert(items.len <= 4 and padded <= 8192);
            for (items) |item| item.slot.setValue(usize, item.payloadAs(usize).*, if (items.len > 1) .native_batch else .serial);
        }
    };
    var executor = Executor{};
    const tokens = [_]usize{ 2040, 4090, 8190, 2041, 4091, 2042, 2043 };
    var shapes: [tokens.len]broker_mod.Shape = undefined;
    for (tokens, &shapes) |count, *shape| {
        const width = bucket(MaskedEncoder{}, count, 8192);
        shape.* = .{ .geometry_key = width, .tokens = width };
    }
    const ids = @as([tokens.len]broker_mod.Identity, @splat(.{}));
    const results = try broker.submitBatchControlled(usize, usize, std.testing.io, std.testing.allocator, .{ .model = "document-encoder", .task = .embed, .resource_class = .gpu }, .{ .mode = .native, .preferred_items = 4, .max_items = 4, .max_tokens = 8192 }, &shapes, &ids, null, .{}, &tokens, &executor, Executor.execute);
    defer std.testing.allocator.free(results);
    try std.testing.expectEqual(@as(usize, 5), executor.calls.load(.monotonic));
    for (tokens, results) |expected, actual| try std.testing.expectEqual(expected, actual.result.value);
    try std.testing.expectEqual(results[0].execution_id, results[3].execution_id);
    // The generic broker retains four-ticket waves for bounded host ownership.
    try std.testing.expectEqual(results[5].execution_id, results[6].execution_id);
    try std.testing.expect(results[0].execution_id != results[5].execution_id);
    try std.testing.expect(results[1].execution_id != results[4].execution_id);
}
