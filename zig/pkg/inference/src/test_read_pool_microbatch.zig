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
