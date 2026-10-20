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
    var parsed = try std.json.parseFromSlice(Specification, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    _ = try d.text(.{ .string = parsed.value.version });
    try parsed.value.decider.validate();
    parsed.value.questions = if (parsed.value.decider.decision_method == .generated) try d.generatedQuestions(parsed.arena.allocator(), parsed.value.questions, parsed.value.decider.instructions) else try d.publicQuestions(parsed.arena.allocator(), parsed.value.questions);
    try d.validateQuestions(parsed.value.questions, parsed.value.decider.resolvedCapabilities());
    if (parsed.value.decider.generator) |generator| if (generator.provider != .antfly) {
        const c = @import("antfly_structured");
        try c.validateProviderSchema(generator, try c.hostedCapabilities(generator).select(parsed.value.decider.output, parsed.value.decider.probabilities), try @import("generated.zig").questionSchema(parsed.arena.allocator(), parsed.value.questions));
    };
    return parsed;
}
pub fn provenance(a: std.mem.Allocator, spec: Specification, source_fingerprint: ?[]const u8) !d.Json {
    return provenanceWithRevision(a, spec, source_fingerprint, null);
}
pub fn provenanceWithRevision(a: std.mem.Allocator, spec: Specification, source_fingerprint: ?[]const u8, revision: ?d.Json) !d.Json {
    var generator = spec.decider.generator;
    if (generator) |*config| config.api_key = null;
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .model_revision = revision, .generator = generator, .instructions = spec.decider.instructions, .output = spec.decider.output, .probabilities = spec.decider.probabilities, .batch = spec.decider.batch, .version = spec.version, .provider = spec.decider.provider, .decision_method = spec.decider.decision_method, .model = spec.decider.modelName(), .model_identity = spec.decider.model_identity, .embedding_options = spec.decider.embedding_options, .url = spec.decider.baseUrl(), .questions = spec.questions }, .{});
    defer a.free(bytes);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    var value = d.jsonObject();
    if (revision) |actual| try d.put(a, &value, "model_revision", actual);
    try d.put(a, &value, "version", .{ .string = spec.version });
    try d.put(a, &value, "provider", .{ .string = if (spec.decider.generator) |cfg| @tagName(cfg.provider) else @tagName(spec.decider.provider) });
    try d.put(a, &value, "specification_hash", .{ .string = try std.fmt.allocPrint(a, "{x}", .{&hash}) });
    try d.put(a, &value, "source_fingerprint", if (source_fingerprint) |fingerprint| .{ .string = fingerprint } else .null);
    return value;
}
