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

//! Predeclared study sources. Only these compiled bytes can construct a native
//! study branch. A retention artifact binds their hashes; it cannot supply new
//! instructions or alter the generation algorithm.
const std = @import("std");
const profiles = @import("../runtime/kv/compaction_profile.zig");
const protocol = @import("context_query_protocol.zig");

pub const Kind = enum { fit, gate };
pub const response_tokens: u32 = 256;
pub const generation_contract =
    "{\"schema_version\":2,\"mode\":\"greedy_fixed_length_ignore_eos\",\"response_tokens\":256," ++
    "\"selection\":\"argmax_first_index\",\"construction\":\"loaded_chat_template_extend_empty_assistant_user_v1\"," ++
    "\"evaluation_used_for_selection\":false}";

pub fn template(kind: Kind) []const u8 {
    return switch (kind) {
        .fit => "Review the supplied reference in its original order. Reproduce its important facts, exact names, dates, numbers, identifiers, relationships, and concrete examples. Cover details throughout the reference rather than only its beginning. Preserve explicit conditions and limits. Do not invent information.",
        .gate => "Using only the supplied reference, make a factual study guide. Describe its main subjects and the supporting evidence from different sections. Include exact terminology, quantities, named entities, exceptions, and procedural steps where present. State when requested details are absent. Do not invent information.",
    };
}

pub fn authority(kind: Kind) profiles.QueryStudyAuthority {
    var result = profiles.QueryStudyAuthority{
        .template_sha256 = undefined,
        .generation_contract_sha256 = undefined,
        .response_tokens = response_tokens,
        .generation_mode = .greedy_fixed_length_ignore_eos,
    };
    std.crypto.hash.sha2.Sha256.hash(template(kind), &result.template_sha256, .{});
    std.crypto.hash.sha2.Sha256.hash(generation_contract, &result.generation_contract_sha256, .{});
    return result;
}

pub fn validate(kind: Kind, expected: profiles.QueryStudyAuthority) !void {
    if (!std.meta.eql(authority(kind), expected)) return error.CompactionQueryProtocolMismatch;
    try protocol.validateStudyBytes(expected, template(kind), generation_contract);
}

test "context compiled studies bind templates response length and generation mode" {
    const fit = authority(.fit);
    const gate = authority(.gate);
    try validate(.fit, fit);
    try validate(.gate, gate);
    try std.testing.expect(!std.mem.eql(u8, &fit.template_sha256, &gate.template_sha256));
    try std.testing.expectEqualSlices(u8, &fit.generation_contract_sha256, &gate.generation_contract_sha256);
    try std.testing.expectError(error.CompactionQueryProtocolMismatch, validate(.fit, gate));
    var changed = fit;
    changed.response_tokens += 1;
    try std.testing.expectError(error.CompactionQueryProtocolMismatch, validate(.fit, changed));
}
