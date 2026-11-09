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

//! Storage-authored local publication receipts. Provider bytes and skip state
//! cannot certify a parent revision. Ordered owners use causal proofs instead.
const std = @import("std");
const A = std.mem.Allocator;
const prefix = "\x00\x00__artifact_local_publication:";
pub fn key(a: A, artifact: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ prefix, artifact });
}
pub fn recipe(entry: anytype) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    inline for (.{ "producer_json", "source_field", "source_template", "source_artifact_name", "content_type", "neighbor_context_json", "prior_context_json", "group_source_json", "execution_json" }) |field| {
        const value = @field(entry, field);
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, value.len, .little);
        h.update(&length);
        h.update(value);
    }
    return h.finalResult();
}
pub fn requestRecipe(request: anytype) [32]u8 {
    return recipe(.{ .producer_json = request.producer_json, .source_field = request.source_field, .source_template = request.source_template, .source_artifact_name = request.upstream_artifact_name, .content_type = request.content_type, .neighbor_context_json = request.neighbor_context_json, .prior_context_json = request.prior_context_json, .group_source_json = request.group_source_json, .execution_json = request.execution_json });
}
pub fn encode(definition: [32]u8, parent: []const u8, output: []const u8) [96]u8 {
    var result: [96]u8 = undefined;
    @memcpy(result[0..32], &definition);
    std.crypto.hash.sha2.Sha256.hash(parent, result[32..64], .{});
    std.crypto.hash.sha2.Sha256.hash(output, result[64..96], .{});
    return result;
}
pub fn require(a: A, txn: anytype, artifact: []const u8, definition: [32]u8, parent: []const u8, output: []const u8) !void {
    const selected = try key(a, artifact);
    defer a.free(selected);
    const actual = txn.get(selected) catch |err| if (err == error.NotFound) return error.ArtifactPublicationPending else return err;
    if (!std.mem.eql(u8, actual, &encode(definition, parent, output))) return error.EnrichmentSourceChanged;
}
