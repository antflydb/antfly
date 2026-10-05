// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Immutable backup pin identity and control messages, with no filesystem or
//! physical database dependency. The seal owner implements these commands.
const topology = @import("relational_integrity_topology_contract.zig");
const json = @import("relational_integrity_json.zig");

pub const Handle = struct {
    fence: topology.Fence,
    digest: [32]u8,
    pub fn jsonStringify(self: Handle, stream: anytype) !void {
        try json.write(self, stream);
    }
};
pub const Request = union(enum) {
    seal: struct { id: []const u8, fence: topology.Fence },
    release: Handle,
    cancel: topology.Fence,
    pub fn jsonStringify(self: Request, stream: anytype) !void {
        try json.write(self, stream);
    }
};
