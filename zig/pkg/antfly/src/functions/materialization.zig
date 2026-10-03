// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Versioned, immutable decision specifications stored by asset enrichment.
const std = @import("std");
const d = @import("decisions.zig");
pub const Specification = struct {
    version: []const u8,
    decider: d.DeciderConfig,
    questions: d.Json,
};
pub fn parse(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Specification) {
    const parsed = try std.json.parseFromSlice(Specification, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    _ = try d.text(.{ .string = parsed.value.version });
    try parsed.value.decider.validate();
    try d.validateQuestions(parsed.value.questions, d.capabilities(parsed.value.decider.provider));
    return parsed;
}
pub fn provenance(a: std.mem.Allocator, spec: Specification, source_fingerprint: ?[]const u8) !d.Json {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .version = spec.version, .provider = spec.decider.provider, .model = spec.decider.modelName(), .url = spec.decider.baseUrl(), .questions = spec.questions }, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    var value = d.jsonObject();
    try d.put(a, &value, "version", .{ .string = spec.version });
    try d.put(a, &value, "provider", .{ .string = @tagName(spec.decider.provider) });
    try d.put(a, &value, "specification_hash", .{ .string = try std.fmt.allocPrint(a, "{x}", .{&hash}) });
    try d.put(a, &value, "source_fingerprint", if (source_fingerprint) |fingerprint| .{ .string = fingerprint } else .null);
    return value;
}
