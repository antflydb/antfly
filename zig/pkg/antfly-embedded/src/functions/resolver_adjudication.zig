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
    // Generated and cascade specifications clone the decider while lowering.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const specification = try parsed.value.producerSpecification(arena.allocator());
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

/// Share the ordinary producer's partial-result contract across local and
/// distributed candidate adapters. A sibling failure cannot discard evidence.
pub fn executeBatch(a: std.mem.Allocator, producer: @import("../storage/db/enrichment/asset_producer.zig").Producer, context: @import("../storage/db/enrichment/asset_producer.zig").InvocationContext, requests: []const @import("antfly_resolver").adjudication.Request) ![]@import("antfly_resolver").adjudication.Result {
    const asset = @import("../storage/db/enrichment/asset_producer.zig");
    const inputs = try a.alloc(asset.Request, requests.len);
    defer a.free(inputs);
    var scoped = context;
    const now = @import("antfly_platform").time.monotonicNs();
    for (inputs, requests, 0..) |*input, request, i| {
        const deadline = now + @as(u64, request.timeout_ms) * std.time.ns_per_ms;
        scoped.deadline_ns = if (scoped.deadline_ns) |caller| @min(caller, deadline) else deadline;
        input.* = .{ .producer_type = .decision, .config_json = request.specification_json, .source_text = request.input, .source_fingerprint = request.source_fingerprint, .item_id = try std.fmt.allocPrint(a, "{d}", .{i}) };
    }
    var batch = try producer.withInvocationContext(scoped).produceBatchReported(a, inputs);
    defer batch.deinit(a);
    if (batch.items.len != requests.len) return error.InvalidAdjudicationOutput;
    const results = try a.alloc(@import("antfly_resolver").adjudication.Result, requests.len);
    for (batch.items, results) |item, *result| result.* = switch (item.result) {
        .value => |value| .{ .value = try a.dupe(u8, value) },
        .item_error => |failure| .{ .failure = @errorName(failure.cause) },
    };
    return results;
}

pub fn modelRevision(a: std.mem.Allocator, producer: @import("../storage/db/enrichment/asset_producer.zig").Producer, context: @import("../storage/db/enrichment/asset_producer.zig").InvocationContext, specification: []const u8, timeout_ms: u32) !?[32]u8 {
    var scoped = context;
    const deadline = @import("antfly_platform").time.monotonicNs() + @as(u64, timeout_ms) * std.time.ns_per_ms;
    scoped.deadline_ns = if (context.deadline_ns) |caller| @min(caller, deadline) else deadline;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    // Ordinary producer specs need no extra prompt/schema parse. Escaped keys
    // still take the parsed path for the discovery-only envelope.
    const envelope: std.json.Value = if (std.mem.indexOf(u8, specification, "decision_revision_probe") != null or std.mem.indexOfScalar(u8, specification, '\\') != null)
        try std.json.parseFromSliceLeaky(std.json.Value, scratch, specification, .{})
    else
        .null;
    var require_revision = false;
    if (envelope == .object) if (envelope.object.get("decision_revision_probe")) |raw| {
        _ = try policy.Config.parseRevisionRequest(scratch, raw);
        // Keep the original Decisions route intact. The producer adapter owns
        // linked-vs-HTTP routing, alias verification and credential scope.
        require_revision = true;
    };
    const capabilities = (try producer.withInvocationContext(scoped).capabilitiesForRequests(a, &.{.{ .producer_type = .decision, .config_json = specification, .source_text = "" }})) orelse {
        if (require_revision) return error.CapabilityDiscoveryUnavailable;
        return null;
    };
    if (require_revision and capabilities.model_revision == null) return error.CapabilityDiscoveryUnavailable;
    return capabilities.model_revision;
}

test "resolver adjudication validates cascade deciders through the frozen decision contract" {
    const a = std.testing.allocator;
    const valid =
        \\{"decider":{"decision_method":"cascade","cascade":{"stages":[{"name":"fast","decider":{"provider":"jev"}},{"name":"strong","decider":{"provider":"antfly","model":"fixture"},"when":{"question":"same","choice":"same","probability_between":[0.3,0.9]}}]}},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}
    ;
    try validate(a, valid);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    // Stage configs are complete deciders; a missing model is rejected.
    const invalid_stage = try std.mem.replaceOwned(u8, scratch, valid, "{\"provider\":\"antfly\",\"model\":\"fixture\"}", "{\"provider\":\"antfly\"}");
    try std.testing.expectError(error.InvalidAdjudicationConfig, validate(a, invalid_stage));
    // A cascade owns no transport of its own.
    const transport = try std.mem.replaceOwned(u8, scratch, valid, "\"decision_method\":\"cascade\",", "\"decision_method\":\"cascade\",\"model\":\"m\",");
    try std.testing.expectError(error.InvalidAdjudicationConfig, validate(a, transport));
    // Provenance identity follows the cascade definition, not its credentials.
    const materialization = @import("materialization.zig");
    const hash = struct {
        fn of(alloc: std.mem.Allocator, raw: []const u8) ![]const u8 {
            const parsed = try policy.Config.parse(alloc, raw);
            defer parsed.deinit();
            const spec = try materialization.parse(alloc, try parsed.value.producerSpecification(alloc));
            defer spec.deinit();
            return (try materialization.provenance(alloc, spec.value, null)).object.get("specification_hash").?.string;
        }
    }.of;
    const base = try hash(scratch, valid);
    const rebanded = try hash(scratch, try std.mem.replaceOwned(u8, scratch, valid, "[0.3,0.9]", "[0.2,0.9]"));
    const keyed = try hash(scratch, try std.mem.replaceOwned(u8, scratch, valid, "{\"provider\":\"jev\"}", "{\"provider\":\"jev\",\"api_key\":\"k\"}"));
    try std.testing.expect(!std.mem.eql(u8, base, rebanded));
    try std.testing.expectEqualStrings(base, keyed);
}
