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
const keys = @import("../internal_keys.zig");
const compact_prefix = prefix ++ "v2:";
pub fn legacyKey(a: A, artifact: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ prefix, artifact });
}
pub fn key(a: A, artifact: []const u8) ![]u8 {
    return keys.documentMetadataKeyAlloc(a, prefix, compact_prefix, artifact);
}
pub fn legacyRecipe(entry: anytype) [32]u8 {
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
/// Keep the framed legacy shape with an empty operational slot, preserving
/// the common pre-policy identity while all future policies share one recipe.
pub fn recipe(entry: anytype) [32]u8 {
    return legacyRecipe(.{ .producer_json = entry.producer_json, .source_field = entry.source_field, .source_template = entry.source_template, .source_artifact_name = entry.source_artifact_name, .content_type = entry.content_type, .neighbor_context_json = entry.neighbor_context_json, .prior_context_json = entry.prior_context_json, .group_source_json = entry.group_source_json, .execution_json = "" });
}

const compatibility_prefix = "\x00\x00__artifact_recipe_compatibility:";
fn compatibilityKey(a: A, digest: [32]u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ compatibility_prefix, &digest });
}

/// Record O(producers), never O(documents), compatibility before replacing a
/// catalog. The alias and replacement commit in the same transaction. Aliases
/// authenticate only recipe identity; parent and output digests remain exact.
pub fn stageCatalogCompatibility(a: A, txn: anytype) !void {
    const raw = txn.get("\x00\x00__metadata__:enrichments") catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const parsed = try std.json.parseFromSlice([]@import("catalog/enrichment_catalog.zig").EnrichmentConfig, a, raw, .{});
    defer parsed.deinit();
    for (parsed.value) |entry| {
        if (entry.kind != .asset or entry.execution_json.len == 0) continue;
        const old = legacyRecipe(entry);
        const current = recipe(entry);
        const name = try compatibilityKey(a, old);
        defer a.free(name);
        const previous = txn.get(name) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |value| {
            if (!std.mem.eql(u8, value, &current)) return error.EnrichmentSourceChanged;
        } else try txn.put(name, &current);
    }
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
    return requireCompatible(a, txn, artifact, definition, null, parent, output);
}

pub fn requireCompatible(a: A, txn: anytype, artifact: []const u8, definition: [32]u8, legacy: ?[32]u8, parent: []const u8, output: []const u8) !void {
    const actual = keys.readDocumentMetadata(a, txn, prefix, compact_prefix, artifact) catch |err| if (err == error.NotFound) return error.ArtifactPublicationPending else return err;
    if (actual.len != 96) return error.EnrichmentSourceChanged;
    const expected = encode(definition, parent, output);
    if (!std.mem.eql(u8, actual[32..], expected[32..])) return error.EnrichmentSourceChanged;
    if (std.mem.eql(u8, actual[0..32], &definition)) return;
    if (legacy) |old| if (std.mem.eql(u8, actual[0..32], &old)) return;
    const name = try compatibilityKey(a, actual[0..32].*);
    defer a.free(name);
    const mapped = txn.get(name) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
    if (!std.mem.eql(u8, mapped, &definition)) return error.EnrichmentSourceChanged;
}
