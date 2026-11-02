// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Minimal views over the durable store/placement prefixes used by routing.
//! Bootstrap strings are skipped by length; they never become routing memory.
const std = @import("std");
const read = @import("antfly_local_sources").common_read_admission;
const contract = @import("storage/raft_apply_contract.zig").ControlRoutingProjection;
const Reader = struct {
    bytes: []const u8,
    pos: usize = 0,
    fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len - self.pos) return error.InvalidMetadataTransitionEncoding;
        const value = self.bytes[self.pos..][0..n];
        self.pos += n;
        return value;
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn string(self: *@This()) ![]const u8 {
        return self.take(try self.int(u32));
    }
};
pub fn copy(a: std.mem.Allocator, bytes: []const u8, control: read.Port) ![]u8 {
    try control.checkpoint();
    const value = try a.alloc(u8, bytes.len);
    errdefer a.free(value);
    var pos: usize = 0;
    while (pos < bytes.len) {
        try control.checkpoint();
        const end = @min(bytes.len, pos + 4096);
        @memcpy(value[pos..end], bytes[pos..end]);
        pos = end;
    }
    try control.checkpoint();
    return value;
}
pub fn storeHeader(a: std.mem.Allocator, bytes: []const u8, control: read.Port) !contract.Store {
    try control.checkpoint();
    var r: Reader = .{ .bytes = bytes };
    const store_id = try r.int(u64);
    const node_id = try r.int(u64);
    const api = try r.string();
    _ = try r.string(); // Raft endpoint.
    _ = try r.string(); // Role.
    const health = try r.string();
    _ = try r.string(); // Failure domain.
    const live = try r.int(u8);
    if (live > 1) return error.InvalidMetadataTransitionEncoding;
    const api_url = try copy(a, api, control);
    errdefer a.free(api_url);
    const health_class = try copy(a, health, control);
    errdefer a.free(health_class);
    return .{ .store_id = store_id, .node_id = node_id, .api_url = api_url, .health_class = health_class, .live = live != 0 };
}
pub fn placement(bytes: []const u8, control: read.Port) !contract.Placement {
    try control.checkpoint();
    var r: Reader = .{ .bytes = bytes };
    const group = try r.int(u64);
    _ = try r.int(u64); // Replica ID.
    const node = try r.int(u64);
    _ = try r.int(u64); // Metadata version.
    _ = std.enums.fromInt(@import("../raft/storage/catalog.zig").ReplicaBootstrapMode, try r.int(u8)) orelse return error.InvalidMetadataTransitionEncoding;
    const peers = try r.int(u32);
    if (peers > (r.bytes.len - r.pos) / 8) return error.InvalidMetadataTransitionEncoding;
    _ = try r.take(@as(usize, peers) * 8);
    var state: @FieldType(contract.Placement, "serving_state") = .serving;
    if (r.pos < bytes.len) _ = try r.int(u64); // Store ID.
    if (r.pos < bytes.len) switch (try r.int(u8)) {
        0 => {},
        4 => if (try r.int(u64) == 0) return error.InvalidMetadataTransitionEncoding,
        1, 3 => |tag| {
            _ = try r.int(u64);
            _ = try r.int(u64);
            _ = try r.string();
            _ = try r.string();
            if (tag == 3) _ = try r.int(u8);
        },
        2 => {
            for (0..5) |_| _ = try r.string();
            _ = try r.int(u64);
            _ = try r.string();
        },
        else => return error.InvalidMetadataTransitionEncoding,
    };
    if (r.pos < bytes.len) {
        state = std.enums.fromInt(@FieldType(contract.Placement, "serving_state"), try r.int(u8)) orelse return error.InvalidMetadataTransitionEncoding;
        _ = try r.take(7 * 8); // Relocation watermarks do not select endpoints.
    }
    try control.checkpoint();
    return .{ .group_id = group, .node_id = node, .serving_state = state };
}
