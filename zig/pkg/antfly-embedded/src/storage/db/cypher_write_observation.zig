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

//! Private owner observations used to validate a Cypher write cut at prepare.
const std = @import("std");
const types = @import("types.zig");
const tracking = @import("../range_protection.zig");
const route = @import("../../metadata/catalog_route_contract.zig");
pub const Owner = struct {
    table: []const u8,
    fence: route.CatalogRouteFence,
    predicates: []const types.TransactionVersionPredicate,
    proofs: []const tracking.Proof,
};
pub const Cut = struct {
    predicates: []const types.TransactionVersionPredicate,
    proofs: []const tracking.Proof,
};
/// Range guards belong to the retained cut before any row is consumed.
/// SQL graph reads need these guards without materializing write predicates.
pub fn captureRanges(comptime Snapshot: type, self: *Snapshot, a: std.mem.Allocator) !Cut {
    const view = try self.openWriteObservation(a);
    defer view.deinit();
    const proofs = try view.rangeProofs(a);
    if (proofs.len == 0) return error.SqlRangeTrackingRequired;
    return .{ .predicates = &.{}, .proofs = proofs };
}
pub fn capture(comptime Snapshot: type, self: *Snapshot, a: std.mem.Allocator) !Cut {
    const view = try self.openWriteObservation(a);
    defer view.deinit();
    var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
    while (true) {
        var page = try view.next(a, 256);
        defer page.deinit();
        for (page.rows) |row| {
            if (predicates.items.len == 65536) return error.CypherAnchorBudgetExceeded;
            try predicates.append(a, .{ .key = try a.dupe(u8, row.id), .expected_version = row.version, .expected_content_digest = row.expected_content_digest });
        }
        if (page.after == null) break;
    }
    const proofs = try view.rangeProofs(a);
    if (proofs.len == 0) return error.SqlRangeTrackingRequired;
    return .{ .predicates = try predicates.toOwnedSlice(a), .proofs = proofs };
}
