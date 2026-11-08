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

//! The serving boundary requires an authoritative index-publication witness as
//! well as successful inspection. An artifact body cannot authorize itself.
//! Storage adapters must capture source, artifact and witness in one read cut.
const std = @import("std");
const contract = @import("contract.zig");
const A = std.mem.Allocator;
pub const Publication = struct {
    generation: u64,
    policy_version: [64]u8,
    source_fingerprint: [64]u8,
    projection_fingerprint: [64]u8,
    state: enum { pending, failed, published },
};

pub fn project(a: A, policy: contract.Policy, generation: u64, source: []const u8, artifact: ?[]const u8, publication: ?Publication) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    return projectPrepared(a, try contract.PreparedPolicy.init(arena.allocator(), policy), generation, source, artifact, publication);
}

/// The prepared binding must be captured from the same immutable policy cut as
/// the caller's publication/read fences. Artifact JSON cannot supply it.
pub fn projectPrepared(a: A, prepared: contract.PreparedPolicy, generation: u64, source: []const u8, artifact: ?[]const u8, publication: ?Publication) ![]u8 {
    const policy = prepared.policy;
    if (policy.mode == .monitor) return a.dupe(u8, source);
    const bytes = artifact orelse return error.PrivacyPending;
    const witness = publication orelse return error.PrivacyPublicationPending;
    if (witness.state == .failed) return error.PrivacyInspectionFailed;
    if (witness.state != .published) return error.PrivacyPublicationPending;
    const version = prepared.version;
    if (generation == 0 or witness.generation != generation or !std.mem.eql(u8, &version, &witness.policy_version) or
        !std.mem.eql(u8, &contract.fingerprint(bytes), &witness.projection_fingerprint)) return error.PrivacyStale;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const original = try std.json.parseFromSlice(std.json.Value, scratch, source, .{ .parse_numbers = false });
    if (original.value != .object) return error.InvalidPrivacyDocument;
    // Producer source selection serializes the complete document before
    // inspection. Reproduce that exact selection rather than hashing a
    // field projection or a caller's selected output fields.
    const canonical_source = try std.json.Stringify.valueAlloc(scratch, original.value, .{});
    const source_digest = contract.fingerprint(canonical_source);
    if (!std.mem.eql(u8, &source_digest, &witness.source_fingerprint)) return error.PrivacyStale;
    const projected = try std.json.parseFromSlice(std.json.Value, scratch, bytes, .{ .parse_numbers = false });
    if (projected.value != .object) return error.InvalidPrivacyDocument;
    var sanitized = projected.value;
    const metadata = sanitized.object.get("_privacy") orelse return error.InvalidPrivacyReceipt;
    if (metadata != .object) return error.InvalidPrivacyReceipt;
    const recorded_version = metadata.object.get("policy_version") orelse return error.InvalidPrivacyReceipt;
    const recorded_source = metadata.object.get("source_fingerprint") orelse return error.InvalidPrivacyReceipt;
    if (recorded_version != .string or recorded_source != .string or !std.mem.eql(u8, recorded_version.string, &version) or
        !std.mem.eql(u8, recorded_source.string, &source_digest)) return error.PrivacyStale;
    const fields = metadata.object.get("fields") orelse return error.InvalidPrivacyReceipt;
    if (fields != .array or fields.array.items.len != policy.fields.len) return error.InvalidPrivacyReceipt;
    for (policy.fields) |field| {
        const original_value = original.value.object.get(field) orelse .null;
        const original_text = switch (original_value) {
            .null => "",
            .string => |text| text,
            else => return error.InvalidPrivacyFieldType,
        };
        const matching = for (fields.array.items) |entry| {
            if (entry != .object) return error.InvalidPrivacyReceipt;
            const name = entry.object.get("field") orelse return error.InvalidPrivacyReceipt;
            if (name != .string) return error.InvalidPrivacyReceipt;
            if (std.mem.eql(u8, name.string, field)) break entry;
        } else return error.InvalidPrivacyReceipt;
        const receipt_value = matching.object.get("receipt") orelse return error.InvalidPrivacyReceipt;
        const receipt = try std.json.parseFromValue(contract.Receipt, scratch, receipt_value, .{});
        if (!receipt.value.current(version, original_text, prepared.revision)) return error.PrivacyStale;
        const text = matching.object.get("text") orelse return error.InvalidPrivacyReceipt;
        const actual = sanitized.object.get(field) orelse .null;
        if (original_value == .string and (text != .string or actual != .string or !std.mem.eql(u8, text.string, actual.string))) return error.InvalidPrivacyReceipt;
    }
    _ = sanitized.object.swapRemove("_privacy");
    return std.json.Stringify.valueAlloc(a, sanitized, .{});
}

test "privacy serving rejects stale generations and requires publication independently of artifact bytes" {
    const a = std.testing.allocator;
    const document = @import("document.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const policy = contract.Policy{ .mode = .redact_for_search_and_ai, .fields = &.{"body"} };
    const producer = try document.resolve(arena.allocator(), policy, true);
    const config = try std.json.Stringify.valueAlloc(arena.allocator(), producer.config, .{});
    const NoFindings = struct {
        fn call(_: *anyopaque, out: A, _: contract.Producer, _: []const u8) ![]u8 {
            return out.dupe(u8, "{\"findings\":[],\"long_document\":{\"window_count\":1}}");
        }
        fn check(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    const source = "{\"body\":\"hello\"}";
    const bytes = try document.produce(a, config, source, .{ .ptr = &context, .produce = NoFindings.call, .check = NoFindings.check });
    defer a.free(bytes);
    var publication = Publication{ .generation = 1, .policy_version = try policy.identity(a), .source_fingerprint = contract.fingerprint(source), .projection_fingerprint = contract.fingerprint(bytes), .state = .pending };
    try std.testing.expectError(error.PrivacyPending, project(a, policy, 1, source, null, null));
    try std.testing.expectError(error.PrivacyPublicationPending, project(a, policy, 1, source, bytes, null));
    try std.testing.expectError(error.PrivacyPublicationPending, project(a, policy, 1, source, bytes, publication));
    publication.state = .published;
    const visible = try project(a, policy, 1, source, bytes, publication);
    defer a.free(visible);
    try std.testing.expectEqualStrings(source, visible);
    try std.testing.expectError(error.PrivacyStale, project(a, policy, 2, source, bytes, publication));
    try std.testing.expectError(error.PrivacyStale, project(a, policy, 1, "{\"body\":\"edited\"}", bytes, publication));
    publication.state = .failed;
    try std.testing.expectError(error.PrivacyInspectionFailed, project(a, policy, 1, source, bytes, publication));
}

test "privacy prepared serving and status keep row memory independent of policy descriptions" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const description = try scratch.alloc(u8, 128 * 1024);
    @memset(description, 'x');
    const policy = contract.Policy{ .mode = .redact_for_search_and_ai, .fields = &.{"absent"}, .category_groups = &.{}, .categories = &.{.{ .name = "email", .description = description }} };
    const prepared = try contract.PreparedPolicy.init(scratch, policy);
    const document = @import("document.zig");
    const resolved = try document.resolve(scratch, policy, true);
    const config = try std.json.Stringify.valueAlloc(scratch, resolved.config, .{});
    const Detector = struct {
        fn call(_: *anyopaque, _: A, _: contract.Producer, _: []const u8) ![]u8 {
            return error.UnexpectedInference;
        }
        fn check(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    const source = "{\"ordinal\":1}";
    const artifact = try document.produce(scratch, config, source, .{ .ptr = &context, .produce = Detector.call, .check = Detector.check });
    const publication = Publication{ .generation = 1, .policy_version = prepared.version, .source_fingerprint = contract.fingerprint(source), .projection_fingerprint = contract.fingerprint(artifact), .state = .published };
    var budget = @import("antfly_inference_work").BoundedInvocationAllocator.init(a, 32 * 1024);
    const visible = try projectPrepared(budget.allocator(), prepared, 1, source, artifact, publication);
    defer budget.allocator().free(visible);
    try std.testing.expectEqualStrings(source, visible);
    var observation = try @import("status.zig").inspectPrepared(budget.allocator(), prepared, source, artifact);
    defer observation.deinit(budget.allocator());
    try std.testing.expect(observation.current);
    try std.testing.expectError(error.PrivacyStale, projectPrepared(budget.allocator(), prepared, 1, "{\"ordinal\":2}", artifact, publication));
}
