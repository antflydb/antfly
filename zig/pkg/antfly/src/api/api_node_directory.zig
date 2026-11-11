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

//! API issuers are not necessarily shard owners. Metadata peers advertise
//! orchestration endpoints; data APIs advertise healthy registered stores.
const std = @import("std");

pub fn resolve(a: std.mem.Allocator, node: u64, metadata_peers: anytype, stores: anytype) !?[]u8 {
    if (node == 0) return error.InvalidArgument;
    for (metadata_peers) |peer| {
        if (peer.node_id != node) continue;
        if (peer.orchestration_url) |uri| if (uri.len != 0) return try a.dupe(u8, uri);
        return null;
    }
    for (stores) |store| {
        if (store.node_id == node and store.live and std.mem.eql(u8, store.health_class, "healthy") and store.api_url.len != 0)
            return try a.dupe(u8, store.api_url);
    }
    return null;
}

/// Private body-authorized point lookup. Metadata-only peers resolve without
/// copying the storage catalog, and the same request controls cover all work.
pub fn readJson(svc: anytype, a: std.mem.Allocator, node: u64, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
    if (!context.setting_admin) return error.Forbidden;
    if (node == 0) return error.InvalidArgument;
    try context.ensureActive();
    try svc.ensureLinearizableReadWithContext(context);
    const Store = @import("../metadata/table_manager.zig").StoreRecord;
    if (try resolve(a, node, svc.reallocation_protocol_peers, &[_]Store{})) |uri| {
        defer a.free(uri);
        try context.ensureActive();
        return std.json.Stringify.valueAlloc(a, uri, .{});
    }
    const stores = try svc.listProjectedStores(a);
    defer svc.freeProjectedStores(a, stores);
    const uri = try resolve(a, node, svc.reallocation_protocol_peers, stores);
    defer if (uri) |value| a.free(value);
    try context.ensureActive();
    return std.json.Stringify.valueAlloc(a, uri, .{});
}

test "retained owner native controls API directory resolves metadata-only and data issuers independently of placements" {
    const a = std.testing.allocator;
    const Peer = struct { node_id: u64, orchestration_url: ?[]const u8 };
    const Store = @import("../metadata/table_manager.zig").StoreRecord;
    const peers = [_]Peer{ .{ .node_id = 1, .orchestration_url = "http://metadata" }, .{ .node_id = 2, .orchestration_url = null } };
    const stores = [_]Store{
        .{ .store_id = 10, .node_id = 3, .api_url = "http://data" },
        .{ .store_id = 11, .node_id = 4, .api_url = "http://offline", .live = false },
    };
    const metadata = (try resolve(a, 1, &peers, &stores)).?;
    defer a.free(metadata);
    try std.testing.expectEqualStrings("http://metadata", metadata);
    const data = (try resolve(a, 3, &peers, &stores)).?;
    defer a.free(data);
    try std.testing.expectEqualStrings("http://data", data);
    for ([_]u64{ 2, 4, 99 }) |node| try std.testing.expectEqual(@as(?[]u8, null), try resolve(a, node, &peers, &stores));
}

test "retained owner native controls API directory private metadata lookup avoids storage catalog and requires authority" {
    const a = std.testing.allocator;
    const operation = @import("antfly_local_sources").api_operation;
    const Store = @import("../metadata/table_manager.zig").StoreRecord;
    const Service = struct {
        reallocation_protocol_peers: []const struct { node_id: u64, orchestration_url: ?[]const u8 } = &.{.{ .node_id = 3, .orchestration_url = "http://metadata" }},
        reads: usize = 0,
        store_reads: usize = 0,
        pub fn ensureLinearizableReadWithContext(self: *@This(), context: operation.RequestContext) !void {
            try context.ensureActive();
            self.reads += 1;
        }
        pub fn listProjectedStores(self: *@This(), _: std.mem.Allocator) ![]Store {
            self.store_reads += 1;
            return @constCast(&[_]Store{});
        }
        pub fn freeProjectedStores(_: *@This(), _: std.mem.Allocator, _: []Store) void {}
    };
    var service: Service = .{};
    const json = try readJson(&service, a, 3, .{ .setting_admin = true });
    defer a.free(json);
    try std.testing.expectEqualStrings("\"http://metadata\"", json);
    try std.testing.expectEqual(@as(usize, 1), service.reads);
    try std.testing.expectEqual(@as(usize, 0), service.store_reads);
    try std.testing.expectError(error.Forbidden, readJson(&service, a, 3, .{}));
    try std.testing.expectError(error.InvalidArgument, readJson(&service, a, 0, .{ .setting_admin = true }));
    const absent = try readJson(&service, a, 99, .{ .setting_admin = true });
    defer a.free(absent);
    try std.testing.expectEqualStrings("null", absent);
    try std.testing.expectEqual(@as(usize, 1), service.store_reads);
}
