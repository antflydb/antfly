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
const ml = @import("ml");
const program = @import("program.zig");
const executable = @import("executable.zig");
const native = @import("../ops/native_compute.zig");

test "prepared PJRT preserves exact integer state outputs through its native signature" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.i64, &.{2});
    const input = try builder.parameter("counter", shape);
    const one = try builder.tensorConstBytes(std.mem.sliceAsBytes(&[_]i64{ 1, 1 }), shape);
    const output = try builder.add(input, one);
    try graph.markOutput(output);
    var owned = try program.ModelProgram.init(a, &graph, &.{ .{ .name = "counter", .node_id = input, .kind = .state_input }, .{ .name = "next", .node_id = output, .kind = .state_output } });
    defer owned.deinit();
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    var prepared = try executable.prepareWithOptions(a, &owned, &backend, .compiled_required, .{ .compiled = .{ .cb = &backend, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } });
    defer prepared.deinit();
    const values = [_]i64{ 9007199254740993, -9007199254740993 };
    const value = (try backend.fromConstantBytes(std.mem.sliceAsBytes(&values), .i64, &.{2})).?;
    defer backend.free(value);
    const inputs = [_]@import("../graph/interpreter.zig").RuntimeInput{.{ .node_id = owned.bindings[0].node_id, .value = value }};
    var result = try prepared.run(a, .{ .runtime_inputs = &inputs, .output_placement = .producer, .strict_integer_constants = true });
    defer result.deinit(&prepared.runtime);
    const actual = (try result.backendFor(&prepared.runtime, 0).exportTensorData(result.outputs[0], a)).?;
    defer a.free(actual.payload.bytes);
    try std.testing.expectEqual(.i64, actual.dtype);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&[_]i64{ 9007199254740994, -9007199254740992 }), actual.payload.bytes);
}
