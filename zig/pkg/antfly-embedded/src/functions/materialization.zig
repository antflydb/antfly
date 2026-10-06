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
