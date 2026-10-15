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

//! Bounded owner-local request to order an already-installed producer catalog.
//! Queue admission is never a publication proof; receivers use Raft receipts.
const std = @import("std");
const inventory = @import("artifact_inventory.zig");
const prefix = "ACAT1";
pub const max_bytes = 6 * 1024 * 1024;
pub fn isProposal(bytes: []const u8) bool {
    return std.mem.startsWith(u8, bytes, prefix);
}
pub fn encode(alloc: std.mem.Allocator, command: inventory.Command) ![]u8 {
    try command.validate();
    if (command.binding.effect_protocol != 15) return error.InvalidArtifactCatalogCommand;
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    if (json.len > max_bytes - prefix.len) return error.InvalidArtifactCatalogCommand;
    return std.mem.concat(alloc, u8, &.{ prefix, json });
}
pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(inventory.Command) {
    if (!isProposal(bytes) or bytes.len > max_bytes) return error.InvalidArtifactCatalogCommand;
    var parsed = try std.json.parseFromSlice(inventory.Command, alloc, bytes[prefix.len..], .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    if (parsed.value.binding.effect_protocol != 15) return error.InvalidArtifactCatalogCommand;
    return parsed;
}
test "producer catalog proposal validates bounded authenticated definitions" {
    const a = std.testing.allocator;
    const catalogs: inventory.Catalogs = .{ .indexes = &.{ 0, 255, 128, 34, 92 }, .enrichments = "[]", .resolvers = "[]" };
    // Invalid index catalogs must fail semantic validation before queue admission.
    const command: inventory.Command = .{ .namespace = @splat(1), .catalogs = catalogs, .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = @splat(1), .effect_protocol = 15 } };
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, encode(a, command));
    const valid: inventory.Catalogs = .{};
    const good: inventory.Command = .{ .namespace = @splat(1), .catalogs = valid, .binding = .{ .epoch = 1, .digest = valid.digest(), .semantic_digest = try valid.semanticDigest(a), .effect_protocol = 15 } };
    const bytes = try encode(a, good);
    defer a.free(bytes);
    var roundtrip = try decode(a, bytes);
    defer roundtrip.deinit();
    try std.testing.expectEqualDeep(good, roundtrip.value);
    try std.testing.expectError(error.MissingField, decode(a, "ACAT1{}"));
}
