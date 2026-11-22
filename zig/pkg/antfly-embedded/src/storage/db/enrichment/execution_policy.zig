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
const Allocator = std.mem.Allocator;

pub const ExecutionPolicy = struct {
    max_output_tokens_per_item: ?u32 = null,
    batch_items: ?usize = null,
    batch_bytes: ?usize = null,
    max_document_pages: ?usize = null,
};

pub fn parseExecutionPolicyJson(alloc: Allocator, execution_json: []const u8) !ExecutionPolicy {
    if (execution_json.len == 0) return .{};
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, execution_json, .{});
    defer parsed.deinit();
    return try parseExecutionPolicyValue(parsed.value);
}

pub fn parseExecutionPolicyValue(value: std.json.Value) !ExecutionPolicy {
    if (value != .object) return error.InvalidEnrichmentExecutionConfig;
    var out = ExecutionPolicy{};
    var iter = value.object.iterator();
    while (iter.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "max_output_tokens_per_item")) {
            if (entry.value_ptr.* == .null) continue;
            out.max_output_tokens_per_item = std.math.cast(u32, try parsePositiveExecutionInteger(entry.value_ptr.*)) orelse return error.InvalidEnrichmentExecutionConfig;
        } else if (std.mem.eql(u8, entry.key_ptr.*, "batch_items")) {
            if (entry.value_ptr.* == .null) continue;
            out.batch_items = try parsePositiveExecutionInteger(entry.value_ptr.*);
        } else if (std.mem.eql(u8, entry.key_ptr.*, "batch_bytes")) {
            if (entry.value_ptr.* == .null) continue;
            out.batch_bytes = try parsePositiveExecutionInteger(entry.value_ptr.*);
        } else if (std.mem.eql(u8, entry.key_ptr.*, "max_document_pages")) {
            if (entry.value_ptr.* == .null) continue;
            out.max_document_pages = try parsePositiveExecutionInteger(entry.value_ptr.*);
        } else {
            return error.InvalidEnrichmentExecutionConfig;
        }
    }
    return out;
}

fn parsePositiveExecutionInteger(value: std.json.Value) !usize {
    const raw = switch (value) {
        .integer => |n| n,
        else => return error.InvalidEnrichmentExecutionConfig,
    };
    if (raw <= 0) return error.InvalidEnrichmentExecutionConfig;
    return std.math.cast(usize, raw) orelse error.InvalidEnrichmentExecutionConfig;
}

pub fn executionBatchItemsOrDefault(alloc: Allocator, execution_json: []const u8, default_value: usize) usize {
    const policy = parseExecutionPolicyJson(alloc, execution_json) catch return default_value;
    return policy.batch_items orelse default_value;
}

pub fn executionBatchBytesOrDefault(alloc: Allocator, execution_json: []const u8, default_value: usize) usize {
    const policy = parseExecutionPolicyJson(alloc, execution_json) catch return default_value;
    return policy.batch_bytes orelse default_value;
}

test "execution policy admits a positive PDF document page ceiling" {
    const policy = try parseExecutionPolicyJson(
        std.testing.allocator,
        "{\"batch_items\":4,\"batch_bytes\":1024,\"max_document_pages\":200}",
    );
    try std.testing.expectEqual(@as(?usize, 200), policy.max_document_pages);
    try std.testing.expectError(
        error.InvalidEnrichmentExecutionConfig,
        parseExecutionPolicyJson(std.testing.allocator, "{\"max_document_pages\":0}"),
    );
}

/// Materialize an invocation-only generation budget. The caller retains the
/// original producer JSON for fingerprints; this allocation is never a recipe.
pub fn withGenerationBudgetAlloc(alloc: Allocator, config_json: []const u8, execution_json: []const u8) !?[]u8 {
    const budget = (try parseExecutionPolicyJson(alloc, execution_json)).max_output_tokens_per_item orelse return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEnrichmentExecutionConfig;
    try parsed.value.object.put(parsed.arena.allocator(), "max_tokens", .{ .integer = budget });
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

test "execution output budget overrides only invocation configuration" {
    const a = std.testing.allocator;
    const recipe = "{\"provider\":\"antfly\",\"model\":\"reader@revision\",\"max_tokens\":512}";
    const invocation = (try withGenerationBudgetAlloc(a, recipe, "{\"max_output_tokens_per_item\":8192}")).?;
    defer a.free(invocation);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, invocation, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 8192), parsed.value.object.get("max_tokens").?.integer);
    try std.testing.expect(std.mem.indexOf(u8, recipe, "512") != null);
    try std.testing.expectEqual(@as(?[]u8, null), try withGenerationBudgetAlloc(a, recipe, "{}"));
    inline for (.{ "{\"max_output_tokens_per_item\":0}", "{\"max_output_tokens_per_item\":-1}", "{\"max_output_tokens_per_item\":4294967296}" }) |invalid|
        try std.testing.expectError(error.InvalidEnrichmentExecutionConfig, parseExecutionPolicyJson(a, invalid));
}
