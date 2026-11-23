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

//! Real native/Raft fixture assertions kept outside the large runtime owner.
const std = @import("std");
const operations = @import("source_checkpoint_operations.zig");
const wire = operations.wire;
const metadata = @import("../metadata/api.zig");
pub fn verify(a: std.mem.Allocator, port: operations.Port, fence: metadata.CatalogRouteFence, request: wire.Request) !void {
    const observed = try port.read(a, fence, "docs", request, .{});
    try observed.validate(request);
    try std.testing.expectEqual(.committed, observed.disposition);
    try std.testing.expectEqual(@as(u64, 2), (try observed.head.?.decode()).sequence);
    try std.testing.expectEqual(@as(u64, 1), (try observed.receipt.?.decode()).sequence);
    try std.testing.expectEqual(request.transaction_id, (try observed.receipt.?.decode()).transaction_id);
    const again = try port.read(a, fence, "docs", request, .{});
    try std.testing.expectEqualDeep(observed.head, again.head);
    try std.testing.expectEqualDeep(observed.receipt, again.receipt);
    var cancelled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, port.read(a, fence, "docs", request, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
    var next = request;
    next.command.sequence = 3;
    next.command.expected_checkpoint = "source:2";
    const absent = try port.read(a, fence, "docs", next, .{});
    try std.testing.expectEqual(.absent, absent.disposition);
    try std.testing.expect(absent.receipt == null);
}
