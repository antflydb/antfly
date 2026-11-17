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

//! Qualified registry reads through the real native opaque owner boundary.
const std = @import("std");
const wire = @import("antfly_local_sources").storage_source_scope_control;
const metadata = @import("../metadata/api.zig");
const completion = @import("antfly_local_sources").storage_source_scope_retirement;
pub fn verify(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner")) !void {
    const query: wire.Query = .{ .version = 1, .owner = source_owner, .domain = @splat(71), .minimum_applied_index = 1 };
    const observed = try readQualified(a, owner, fence, query);
    try observed.validate(query);
    try std.testing.expect(observed.record == null);
    try std.testing.expect(!observed.header.strict);
    try std.testing.expectEqual(@as(u16, 0), observed.header.domains);
    try std.testing.expectEqual(@as(u64, 0), observed.header.metadata_floor);
    const again = try readQualified(a, owner, fence, query);
    try std.testing.expectEqualDeep(observed.header, again.header);
    var changed = query;
    changed.owner.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, owner.sourceScopeObservationRouted(a, fence, "docs", changed));
    try verifyReadFence(a, owner, fence, query, observed.cut.index);
}
fn verifyReadFence(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, query: wire.Query, index: u64) !void {
    var future = query;
    future.minimum_applied_index = index + 1000000;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, readQualified(a, owner, fence, future));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    try std.testing.expectError(error.Canceled, owner.sourceScopeObservationRouted(a, cancelled_fence, "docs", query));
}

fn readQualified(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, query: wire.Query) !wire.Observation {
    for (0..1000) |_| {
        return owner.sourceScopeObservationRouted(a, fence, "docs", query) catch |err| switch (err) {
            // The real serving barrier uses tryLock. Contention cannot establish
            // a read cut; wait only for that transient failure, not future cuts.
            error.ReadUnavailable => {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
    }
    return error.TestReadFenceUnavailable;
}

pub fn verifyRouted(a: std.mem.Allocator, port: @import("source_scope_operations.zig").Port, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner")) !void {
    const query: wire.Query = .{ .version = 1, .owner = source_owner, .domain = @splat(71), .minimum_applied_index = 1 };
    const observed = try port.read(a, fence, "docs", query, .{});
    try observed.validate(query);
    try std.testing.expect(observed.record == null);
    try std.testing.expect(!observed.header.strict);
    const again = try port.read(a, fence, "docs", query, .{});
    try std.testing.expectEqualDeep(observed.header, again.header);
    var cancelled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, port.read(a, fence, "docs", query, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
}
