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

//! Native lease boundaries for an optimistic hosted publication transaction.
const std = @import("std");
const objects = @import("objectstore");
const leases = @import("build/object_work_lease_store.zig");
pub const Lease = struct { owner: []const u8, token: []const u8, expires: []const u8 };
pub fn run(a: std.mem.Allocator, client: objects.Client, bucket: []const u8, prefix: []const u8, namespace: []const u8, action: enum { acquire, validate, release }, owner: []const u8, lease: ?Lease, now: u64) ![]u8 {
    if (owner.len == 0 or owner.len > 64) return error.InvalidPublicationOwner;
    const path = try std.fmt.allocPrint(a, "{s}/progress", .{prefix});
    defer a.free(path);
    var store = try leases.ObjectWorkLeaseStore.initWithClient(a, client, bucket, path);
    defer store.deinit();
    if (action == .acquire) {
        const acquired = (try store.acquire(namespace, owner, now, 30 * std.time.ns_per_s)) orelse return error.WorkLeaseLost;
        const token = try std.fmt.allocPrint(a, "{d}", .{acquired.fencing_token});
        defer a.free(token);
        const expires = try std.fmt.allocPrint(a, "{d}", .{acquired.expires_at_unix_ns});
        defer a.free(expires);
        return std.json.Stringify.valueAlloc(a, Lease{ .owner = owner, .token = token, .expires = expires }, .{});
    }
    const held = lease orelse return error.PublicationLeaseRequired;
    if (!std.mem.eql(u8, owner, held.owner)) return error.WorkLeaseLost;
    const token = try std.fmt.parseInt(u64, held.token, 10);
    if (action == .validate) {
        try store.validate(namespace, owner, token, now);
        return a.dupe(u8, "{\"valid\":true}");
    }
    return std.json.Stringify.valueAlloc(a, .{ .released = try store.release(namespace, owner, token) }, .{});
}
