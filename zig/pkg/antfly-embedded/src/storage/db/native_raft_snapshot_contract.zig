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

//! Pure frozen-image provenance shared by the native writer and restore
//! producer. No DB implementation, file owner, key, or publication authority.
const std = @import("std");
const schema_contract = @import("relational_schema_owner_contract.zig");
pub const Origin = @import("native_backup_origin.zig").Origin;
pub const InstalledOrigin = @import("native_backup_origin.zig").Installation;
pub const header_size = 80;

pub const Identity = struct {
    group_id: u64,
    namespace: [24]u8,
    through_index: u64,
    native_term: u64,
    native_index: u64,
    origin: ?Origin = null,
    schema_header: ?schema_contract.Header = null,

    pub fn validate(self: Identity) !void {
        if (self.group_id == 0 or self.native_index > self.through_index or
            (self.native_index == 0) != (self.native_term == 0)) return error.InvalidSnapshot;
        if (self.origin) |origin| {
            try origin.requireRaftTransfer(self.namespace, self.through_index);
            if ((if (origin.applied) |cut| cut.index else 0) != self.native_index or
                (if (origin.applied) |cut| cut.term else 0) != self.native_term) return error.InvalidSnapshot;
            if ((self.schema_header != null) != (origin.schema_header != null)) return error.InvalidSnapshot;
            if (self.schema_header) |header| {
                try header.validate();
                if (header.binding.authority != .raft or header.updated != .raft or
                    header.binding.fence.owner_group_id != self.group_id or
                    !std.mem.eql(u8, &@import("online_source_contract.zig").namespaceBytes(header.binding.fence.namespace), &self.namespace) or
                    header.updated.raft.index > self.native_index or header.updated.raft.term > self.native_term) return error.InvalidSnapshot;
                var buffer: [schema_contract.max_header + 44]u8 = undefined;
                var fixed = std.heap.FixedBufferAllocator.init(&buffer);
                if (!std.mem.eql(u8, &schema_contract.hash(try header.encode(fixed.allocator())), &origin.schema_header.?)) return error.InvalidSnapshot;
            }
        } else if (self.schema_header != null) return error.InvalidSnapshot;
    }
};

/// Actual completed native stream, not evidence that an outer artifact was
/// durably published. A producer must join that separate lifecycle decision.
pub const StreamReceipt = struct { identity: Identity, footer_digest: [32]u8 };

/// Application metadata copied from the actual writer, never a durability
/// decision. The enclosing payload envelope binds it to the complete payload;
/// a grant producer must additionally join the committed replica checkpoint.
pub const MaterializationWitness = struct {
    kind: enum { native_replica_stream } = .native_replica_stream,
    version: u8 = 1,
    native_offset: u64,
    native_size: u64,
    stream: StreamReceipt,

    pub fn validate(self: @This(), payload_size: u64) !void {
        if (self.version != 1 or self.native_offset > payload_size or self.native_size < header_size + 32 or
            self.native_size != payload_size - self.native_offset or self.stream.identity.origin == null or
            std.mem.allEqual(u8, &self.stream.footer_digest, 0)) return error.InvalidSnapshot;
        try self.stream.identity.validate();
    }

    pub fn requireFor(self: @This(), group_id: u64, index: u64, term: u64, payload_size: u64) !void {
        try self.validate(payload_size);
        if (group_id != self.stream.identity.group_id or index == 0 or term == 0 or
            index != self.stream.identity.through_index or self.stream.identity.native_term > term) return error.InvalidSnapshot;
    }
};
