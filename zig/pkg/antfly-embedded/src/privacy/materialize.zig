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

//! Policy-managed artifacts use ordinary extractor/generator execution. The
//! producer envelope carries private, immutable validation instructions; the
//! storage materializer validates output before publishing any derived bytes.
const std = @import("std");
const contract = @import("contract.zig");
const inspection = @import("inspection.zig");
pub const Marker = struct {
    policy_version: [64]u8,
    field: []const u8,
    categories: []const contract.Category,
    replacement: contract.Replacement,
    producer_revision: []const u8,
    require_window_receipt: bool = false,
};

/// Null means an ordinary artifact. A malformed privacy marker fails rather
/// than downgrading to an unvalidated ordinary producer result.
pub fn output(a: std.mem.Allocator, source: []const u8, producer_json: []const u8, produced: []const u8) !?[]u8 {
    if (producer_json.len == 0) return null;
    var config = try std.json.parseFromSlice(std.json.Value, a, producer_json, .{});
    defer config.deinit();
    if (config.value != .object) return error.InvalidPrivacyProducer;
    const raw_marker = config.value.object.get("privacy") orelse return null;
    var marker = try std.json.parseFromValue(Marker, a, raw_marker, .{});
    defer marker.deinit();
    return try outputPrepared(a, source, marker.value, produced);
}

/// Managed document execution already owns a validated immutable marker. Avoid
/// serializing and parsing policy descriptions again for every selected field.
pub fn outputPrepared(a: std.mem.Allocator, source: []const u8, marker: Marker, produced: []const u8) ![]u8 {
    var tree = try std.json.parseFromSlice(std.json.Value, a, produced, .{});
    defer tree.deinit();
    if (tree.value != .object) return error.InvalidPrivacyOutput;
    const findings = try inspection.normalize(a, marker.field, source, tree.value, marker.categories);
    defer a.free(findings);
    var window_count: u32 = 1;
    if (tree.value.object.get("long_document")) |document| {
        if (document != .object) return error.InvalidPrivacyWindowReceipt;
        const count = document.object.get("window_count") orelse return error.InvalidPrivacyWindowReceipt;
        if (count != .integer or count.integer <= 0 or count.integer > 65536) return error.InvalidPrivacyWindowReceipt;
        window_count = @intCast(count.integer);
    } else if (marker.require_window_receipt) return error.InvalidPrivacyWindowReceipt;
    const text = try contract.redact(a, source, findings, marker.replacement);
    defer a.free(text);
    const receipt = contract.Receipt{
        .policy_version = marker.policy_version,
        .source_fingerprint = contract.fingerprint(source),
        .producer_revision = marker.producer_revision,
        .required_windows = window_count,
        .inspected_windows = window_count,
        .state = .inspected,
    };
    return try std.json.Stringify.valueAlloc(a, .{ .field = marker.field, .text = text, .findings = findings, .receipt = receipt }, .{});
}

test "privacy materialization drops matched values, rebases no coordinates, and does not certify publication" {
    const a = std.testing.allocator;
    const version = contract.fingerprint("policy");
    const config = try std.json.Stringify.valueAlloc(a, .{ .type = "extractor", .privacy = Marker{
        .policy_version = version,
        .field = "body",
        .categories = &.{.{ .name = "email", .threshold = 0.5 }},
        .replacement = .category,
        .producer_revision = "v1",
        .require_window_receipt = true,
    } }, .{});
    defer a.free(config);
    const produced = "{\"entities\":{\"email\":[{\"text\":\"a@b.co\",\"start\":0,\"end\":6,\"confidence\":0.9}]},\"long_document\":{\"window_count\":2}}";
    const result = (try output(a, "a@b.co hello", config, produced)).?;
    defer a.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "a@b.co") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "[EMAIL] hello") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"state\":\"inspected\"") != null);
    try std.testing.expectError(error.InvalidPrivacyWindowReceipt, output(a, "hello", config, "{\"entities\":{}}"));
}
