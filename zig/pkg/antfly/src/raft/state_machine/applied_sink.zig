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

const raft_engine = @import("raft_engine");
const std = @import("std");
pub const NativeProgress = raft_engine.runtime.completion_admission_iface.Progress;

/// This only validates framing and entry identity. Native ownership and the
/// durable contiguous-predecessor invariant must already be verified by the
/// retained group provider; an arbitrary matching hash is not authority.
pub fn validateNativeProgress(progress: NativeProgress, entry: raft_engine.core.Entry) !void {
    if (progress.term == 0 or progress.index == 0 or progress.term != entry.term or progress.index != entry.index)
        return error.CompletionAdmissionUnavailable;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(entry.data, &digest, .{});
    if (!std.mem.eql(u8, &digest, &progress.payload_digest)) return error.CompletionAdmissionUnavailable;
}

pub const AppliedIndexSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        set_applied_index: *const fn (ptr: *anyopaque, group_id: raft_engine.core.types.GroupId, index: raft_engine.core.types.Index) anyerror!void,
        /// Exact empty normal entries have no application state to publish.
        /// Qualified direct-WAL providers can authenticate them without a
        /// second durable mutation; other providers retain ordinary handling.
        set_durable_noop: ?*const fn (*anyopaque, u64, raft_engine.core.Entry) anyerror!void = null,
        set_native_applied: ?*const fn (*anyopaque, u64, NativeProgress, raft_engine.core.Entry) anyerror!void = null,
    };

    pub fn setAppliedIndex(self: AppliedIndexSink, group_id: raft_engine.core.types.GroupId, index: raft_engine.core.types.Index) !void {
        return try self.vtable.set_applied_index(self.ptr, group_id, index);
    }
    pub fn supportsDurableNoop(self: AppliedIndexSink) bool {
        return self.vtable.set_durable_noop != null;
    }
    pub fn setDurableNoop(self: AppliedIndexSink, group_id: u64, entry: raft_engine.core.Entry) !void {
        if (entry.entry_type != .normal or entry.data.len != 0 or entry.term == 0 or entry.index == 0)
            return error.CompletionAdmissionUnavailable;
        const callback = self.vtable.set_durable_noop orelse return error.CompletionAdmissionUnavailable;
        try callback(self.ptr, group_id, entry);
    }
    pub fn setNativeApplied(self: AppliedIndexSink, group_id: u64, progress: NativeProgress, entry: raft_engine.core.Entry) !void {
        if (progress.term == 0 or progress.index == 0 or entry.index == 0 or entry.index > progress.index)
            return error.CompletionAdmissionUnavailable;
        if (progress.index == entry.index) try validateNativeProgress(progress, entry);
        // A retry may be covered by a newer permanent receipt. The concrete
        // sink must authenticate both entries against its durable Raft log.
        const callback = self.vtable.set_native_applied orelse return error.CompletionAdmissionUnavailable;
        try callback(self.ptr, group_id, progress, entry);
    }
};

pub fn noopAppliedIndexSink() AppliedIndexSink {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .set_applied_index = setAppliedIndexNoop,
        },
    };
}

fn setAppliedIndexNoop(_: *anyopaque, _: raft_engine.core.types.GroupId, _: raft_engine.core.types.Index) !void {}
