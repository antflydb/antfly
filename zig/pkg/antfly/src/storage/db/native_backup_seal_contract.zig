// Copyright 2026 Antfly, Inc.
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
