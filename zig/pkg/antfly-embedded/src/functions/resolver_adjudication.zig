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

//! Validate resolver policy through the same frozen typed-decision contract used
//! by materialized enrichments, before accepting any durable resolver mutation.
const std = @import("std");
const policy = @import("antfly_resolver").adjudication;

pub fn validate(a: std.mem.Allocator, raw: []const u8) !void {
    const parsed = try policy.Config.parse(a, raw);
    defer parsed.deinit();
    const specification = try parsed.value.producerSpecification(a);
    defer a.free(specification);
    const validated = @import("materialization.zig").parse(a, specification) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidAdjudicationConfig;
    defer validated.deinit();
}

/// The standard lookup projection keeps asset payloads under .value.
/// Return only the requested asset from the public projection envelope.
pub fn projectedDescription(a: std.mem.Allocator, raw: []const u8, artifact: []const u8) !?[]u8 {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidAdjudicationDescriptions;
    const artifacts = parsed.value.object.get("_artifacts") orelse return null;
    if (artifacts != .object) return error.InvalidAdjudicationDescriptions;
    const entry = artifacts.object.get(artifact) orelse return null;
    if (entry != .object) return error.InvalidAdjudicationDescriptions;
    const value = entry.object.get("value") orelse return null;
    if (value != .string) return error.InvalidAdjudicationDescriptions;
    return try std.json.Stringify.valueAlloc(a, value, .{});
}

test "resolver adjudication validates frozen typed specifications and rejects unknown policies" {
    const a = std.testing.allocator;
    const valid =
        \\{"decider":{"provider":"antfly","model":"fixture"},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}
    ;
    try validate(a, valid);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, valid, .{});
    try value.object.put(scratch, "arbitrary_script", .{ .string = "bad" });
    try std.testing.expectError(error.UnknownField, validate(a, try std.json.Stringify.valueAlloc(scratch, value, .{})));
    _ = value.object.swapRemove("arbitrary_script");
    try value.object.getPtr("decider").?.object.put(scratch, "decision_method", .{ .string = "embedding_similarity" });
    try std.testing.expectError(error.InvalidAdjudicationConfig, validate(a, try std.json.Stringify.valueAlloc(scratch, value, .{})));
    _ = value.object.getPtr("decider").?.object.swapRemove("decision_method");
    _ = value.object.getPtr("decider").?.object.swapRemove("model");
    try std.testing.expectError(error.InvalidAdjudicationConfig, validate(a, try std.json.Stringify.valueAlloc(scratch, value, .{})));
    try value.object.getPtr("decider").?.object.put(scratch, "model", .{ .string = "fixture" });
    try value.object.getPtr("accept").?.object.put(scratch, "choice", .{ .string = "not-a-choice" });
    try std.testing.expectError(error.InvalidAdjudicationConfig, validate(a, try std.json.Stringify.valueAlloc(scratch, value, .{})));
    const projected = (try projectedDescription(a, "{\"_artifacts\":{\"summary\":{\"kind\":\"asset\",\"value\":\"A database\"}}}", "summary")).?;
    defer a.free(projected);
    try std.testing.expectEqualStrings("\"A database\"", projected);
}
