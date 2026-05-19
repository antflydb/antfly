// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const raft_engine = @import("raft_engine");
const applied_sink_mod = @import("applied_sink.zig");
const mod = @import("mod.zig");

pub const MetadataStateMachine = struct {
    alloc: std.mem.Allocator,
    applied_sink: applied_sink_mod.AppliedIndexSink,
    snapshot_builder: ?mod.SnapshotBuilder = null,
    delegate: ?raft_engine.runtime.storage_iface.StateMachine = null,

    pub fn stateMachine(self: *MetadataStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .apply_ready = applyReady,
            },
        };
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *MetadataStateMachine = @ptrCast(@alignCast(ptr));
        if (self.snapshot_builder) |snapshot_builder| {
            const payload = try mod.encodeCommittedEntries(self.alloc, committed_entries);
            defer self.alloc.free(payload);
            try snapshot_builder.applyBatch(.{
                .group_id = group_id,
                .commit_index = if (committed_entries.len > 0) committed_entries[committed_entries.len - 1].index else 0,
                .entries_bytes = payload,
            });
        }
        if (committed_entries.len > 0) {
            try self.applied_sink.setAppliedIndex(group_id, committed_entries[committed_entries.len - 1].index);
        }
        if (self.delegate) |delegate| {
            try delegate.applyReady(group_id, committed_entries, read_states);
        }
    }
};
