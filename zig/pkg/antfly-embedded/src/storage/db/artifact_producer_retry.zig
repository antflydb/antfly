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

//! Anti-entropy for pending required streams, independent of upload lifetime.
//! Verified output is skipped; queue admission and transport receipts are not
//! completion evidence. Bounded pages retain the ordinary producer machinery.
const std = @import("std");
const dispatch = @import("artifact_producer_dispatch.zig");
const Plan = @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot;
pub const interval_ns = 5 * std.time.ns_per_min;

pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, plan: *const Plan, first: u32, document: []const u8, buffer: *dispatch.Buffer) !dispatch.Page {
    var verifier: @import("artifact_completion_progress.zig").StreamVerifier = .{ .plan = plan };
    return prepareWithVerifier(alloc, txn, root, plan, first, document, buffer, &verifier);
}

fn prepareWithVerifier(alloc: std.mem.Allocator, txn: anytype, root: u128, plan: *const Plan, first: u32, document: []const u8, buffer: *dispatch.Buffer, verifier: anytype) !dispatch.Page {
    const compiled = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    // Charge visited templates, not just emitted requests, so long accepted
    // prefixes cannot turn one maintenance slice into an unbounded proof scan.
    const page = try dispatch.prepare(plan.generated_templates, first, document, buffer);
    var count: usize = 0;
    var visited: u32 = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    for (page.items, first..) |request, ordinal| {
        if (visited != 0 and @import("antfly_platform").time.monotonicNs() >= deadline) break;
        const node = try compiled.provider(ordinal);
        visited += 1;
        if (try verifier.verify(alloc, txn, root, document, node)) |found| {
            var witness = found;
            witness.deinit();
            continue;
        }
        buffer[count] = request;
        count += 1;
    }
    const next = first + visited;
    return .{ .items = buffer[0..count], .progress = .{ .next_template = next, .complete = next == plan.generated_templates.len } };
}

test "ordered artifact inventory retry pages skip verified streams without losing their cursor" {
    const alloc = std.testing.allocator;
    var templates = [_]@import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest{
        .{ .kind = .asset, .index_name = "", .artifact_name = "accepted", .doc_key = "", .source_field = "body" },
        .{ .kind = .asset, .index_name = "", .artifact_name = "pending", .doc_key = "", .source_field = "body" },
        .{ .kind = .asset, .index_name = "", .artifact_name = "also-accepted", .doc_key = "", .source_field = "body" },
    };
    var plan: Plan = .{ .alloc = alloc, .generation = 1, .dense_fields = &.{}, .sparse_fields = &.{}, .graph_fields = &.{}, .generated_templates = &templates, .chunk_dependents = &.{}, .completion_plan = try @import("artifact_completion_plan.zig").Plan.init(alloc, .{}, &templates) };
    defer plan.completion_plan.?.deinit();
    const Verifier = struct {
        calls: usize = 0,
        const Witness = struct {
            pub fn deinit(_: *@This()) void {}
        };
        pub fn verify(self: *@This(), _: std.mem.Allocator, _: void, _: u128, _: []const u8, node: *const @import("artifact_completion_plan.zig").Node) !?Witness {
            self.calls += 1;
            return if (std.mem.eql(u8, node.artifact, "pending")) null else Witness{};
        }
    };
    var verifier: Verifier = .{};
    var buffer: dispatch.Buffer = undefined;
    const document: [12000]u8 = @splat('d');
    for (0..templates.len) |i| {
        const page = try prepareWithVerifier(alloc, {}, 1, &plan, @intCast(i), &document, &buffer, &verifier);
        try std.testing.expectEqual(i + 1, page.progress.next_template);
        try std.testing.expectEqual(i == 2, page.progress.complete);
        try std.testing.expectEqual(@as(usize, if (i == 1) 1 else 0), page.items.len);
        if (page.items.len != 0) {
            try std.testing.expectEqualStrings("pending", page.items[0].artifact_name);
            try std.testing.expectEqualStrings(&document, page.items[0].doc_key);
        }
    }
    try std.testing.expectEqual(@as(usize, 3), verifier.calls);
}
