// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Metadata contract for one native checkpoint followed by ordinary siblings.
const std = @import("std");
const api = @import("inference_api");
pub const max_items = 16;

/// The caller's arena owns dynamic response values through batch serialization.
/// Preserve opaque provider metadata and schema numbers without f64 rounding.
pub fn parseResponse(arena: std.mem.Allocator, raw: []const u8) !api.GenerateResponse {
    const value = try @import("antfly-json").parseValueLosslessLeaky(arena, raw);
    return std.json.parseFromValueLeaky(api.GenerateResponse, arena, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

pub fn validate(batch: api.GenerateBatchRequest) !void {
    if (batch.requests.len < 2 or batch.requests.len > max_items or
        (batch.mode != null and batch.mode.? != .sync)) return error.InvalidNativeCheckpointBatch;
    const model = batch.requests[0].body.model;
    if (model.len == 0) return error.InvalidNativeCheckpointBatch;
    for (batch.requests, 0..) |item, index| {
        const body = item.body;
        if (item.custom_id.len == 0 or item.custom_id.len > 128 or
            !std.mem.eql(u8, model, body.model) or body.backend != batch.requests[0].body.backend or
            body.compiled_target != null or body.messages.len == 0 or
            (body.stream orelse false) or body.mode == null or !std.mem.eql(u8, body.mode.?, "eager") or
            body.max_tokens == null or body.max_tokens.? < 1 or body.max_tokens.? > 256 or
            (body.prompt_cache orelse false) or body.context_management != null or
            body.context_policy != null or body.kv_cache != null or body.cache_compaction_ratio != null or
            body.prompt_cache_key != null or body.draft_model != null)
            return error.InvalidNativeCheckpointBatch;
        for (batch.requests[0..index]) |prior|
            if (std.mem.eql(u8, prior.custom_id, item.custom_id)) return error.InvalidNativeCheckpointBatch;
    }
}

test "context native checkpoint mixed batch preserves opaque response numbers and owns response bytes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const raw = try a.dupe(u8,
        \\{"id":"fixture","object":"chat.completion","created":1,"model":"fixture","usage":null,"choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"kept","extra_content":{"numbers":[1.0000000000000001,1e-400,1e100000,18446744073709551615]}}}]}
    );
    defer a.free(raw);
    const response = try parseResponse(arena.allocator(), raw);
    @memset(raw, 'x');
    const encoded = try std.json.Stringify.valueAlloc(a, response, .{});
    defer a.free(encoded);
    for ([_][]const u8{ "kept", "1.0000000000000001", "1e-400", "1e100000", "18446744073709551615" }) |expected|
        try std.testing.expect(std.mem.indexOf(u8, encoded, expected) != null);
}

test "context native checkpoint mixed batch validates all metadata before execution" {
    const a = std.testing.allocator;
    const raw =
        \\{"mode":"sync","requests":[
        \\{"custom_id":"checkpoint","body":{"model":"fixture","messages":[{"role":"user","content":"native"}],"mode":"eager","max_tokens":8,"prompt_cache":false}},
        \\{"custom_id":"ordinary","body":{"model":"fixture","messages":[{"role":"user","content":"exact sibling"}],"mode":"eager","max_tokens":16,"prompt_cache":false}}]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateBatchRequest, a, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try validate(parsed.value);
    const requests = @constCast(parsed.value.requests);
    const original = requests[1];
    requests[1].custom_id = requests[0].custom_id;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    requests[1].body.model = "different-model";
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    requests[1].body.backend = .native;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    requests[1].body.stream = true;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    requests[1].body.prompt_cache = null;
    try validate(parsed.value);
    requests[1] = original;
    requests[1].body.prompt_cache = true;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    requests[1].body.max_tokens = 257;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(parsed.value));
    requests[1] = original;
    var singleton = parsed.value;
    singleton.requests = singleton.requests[0..1];
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(singleton));
    var items: [17]api.GenerateBatchRequestItem = undefined;
    var ids: [17][8]u8 = undefined;
    for (&items, &ids, 0..) |*item, *id, index| {
        item.* = original;
        item.custom_id = try std.fmt.bufPrint(id, "item-{d}", .{index});
    }
    var boundary = parsed.value;
    boundary.requests = items[0..16];
    try validate(boundary);
    boundary.requests = &items;
    try std.testing.expectError(error.InvalidNativeCheckpointBatch, validate(boundary));
}
