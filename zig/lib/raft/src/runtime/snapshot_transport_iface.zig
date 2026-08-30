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

const std = @import("std");
const core = @import("../core/mod.zig");

/// Durable representation used to select the artifact decoder. `unknown` is
/// reserved for catalogs written before the format was persisted; transports
/// must use a compatibility-safe discovery order for those records rather than
/// infer the artifact format from the peer's current capabilities.
pub const SnapshotArtifactFormat = enum(u8) {
    unknown = 0,
    legacy_envelope_v1 = 1,
    chunked_manifest_v2 = 2,
};

pub const SnapshotLocator = struct {
    snapshot_id: []const u8,
    uri: []const u8 = &.{},
    format: SnapshotArtifactFormat = .unknown,
};

pub const SnapshotSendRequest = struct {
    group_id: core.types.GroupId,
    /// Runtime generation for this local group. Asynchronous completion
    /// records use this to fence responses from retired replicas.
    incarnation: u64 = 0,
    from: core.types.NodeId = 0,
    to: core.types.NodeId,
    term: core.types.Term = 0,
    attempt_generation: u64 = 0,
    snapshot: core.types.Snapshot,
    locator: ?SnapshotLocator = null,
};

/// Result exposed to the runtime after dispatch through the transport's
/// structurally valid sender mode. `delivered` is produced only by the
/// synchronous adapter; asynchronous callbacks cannot claim delivery without
/// publishing an exact terminal completion.
pub const SnapshotSubmitResult = union(enum) {
    delivered,
    accepted,
    duplicate,
    retry_later: struct {
        retry_after_ms: u64 = 1,
    },
};

/// Submission is an ownership boundary. `accepted` (or `duplicate` for an
/// already-owned exact attempt) obligates the asynchronous sender to publish
/// one terminal completion. `retry_later` guarantees that it retained nothing.
pub const AsyncSnapshotSubmitResult = union(enum) {
    accepted,
    duplicate,
    retry_later: struct {
        retry_after_ms: u64 = 1,
    },
};

pub const SnapshotCompletionStatus = enum {
    delivered,
    failed,
};

pub const SnapshotCompletion = struct {
    group_id: core.types.GroupId,
    incarnation: u64,
    from: core.types.NodeId,
    to: core.types.NodeId,
    term: core.types.Term,
    attempt_generation: u64,
    snapshot_index: core.types.Index,
    snapshot_term: core.types.Term,
    status: SnapshotCompletionStatus,

    pub fn key(self: SnapshotCompletion) SnapshotAttemptKey {
        return .{
            .group_id = self.group_id,
            .incarnation = self.incarnation,
            .from = self.from,
            .to = self.to,
            .term = self.term,
            .attempt_generation = self.attempt_generation,
            .snapshot_index = self.snapshot_index,
            .snapshot_term = self.snapshot_term,
        };
    }
};

/// Exact local identity of one transport-owned snapshot publication. It is
/// deliberately independent of payload storage so runtimes can retain bounded
/// liveness state after the transport takes ownership of large snapshot bytes.
pub const SnapshotAttemptKey = struct {
    group_id: core.types.GroupId,
    incarnation: u64,
    from: core.types.NodeId,
    to: core.types.NodeId,
    term: core.types.Term,
    attempt_generation: u64,
    snapshot_index: core.types.Index,
    snapshot_term: core.types.Term,

    pub fn fromRequest(req: SnapshotSendRequest) SnapshotAttemptKey {
        return .{
            .group_id = req.group_id,
            .incarnation = req.incarnation,
            .from = req.from,
            .to = req.to,
            .term = req.term,
            .attempt_generation = req.attempt_generation,
            .snapshot_index = req.snapshot.metadata.index,
            .snapshot_term = req.snapshot.metadata.term,
        };
    }
};

pub const SnapshotFetchRequest = struct {
    group_id: core.types.GroupId,
    from: core.types.NodeId,
    term: core.types.Term = 0,
    locator: SnapshotLocator,
    /// Set only on the receiver callback after its byte admission hook has
    /// accepted this payload. Transports must relinquish the reservation with
    /// the same ownership semantics as the snapshot itself.
    admission_reserved: bool = false,
    admitted_snapshot_bytes: usize = 0,
};

pub const SnapshotReceiver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        admit_snapshot: ?*const fn (
            ptr: *anyopaque,
            req: SnapshotFetchRequest,
            data_len: usize,
        ) anyerror!void = null,
        cancel_snapshot_admission: ?*const fn (
            ptr: *anyopaque,
            data_len: usize,
        ) void = null,
        /// Takes ownership of `snapshot` when invoked, whether the callback
        /// succeeds or fails. This keeps transport error paths from guessing
        /// whether a partially admitted Raft message still owns its buffers.
        receive_snapshot: *const fn (
            ptr: *anyopaque,
            req: SnapshotFetchRequest,
            snapshot: core.types.Snapshot,
        ) anyerror!void,
        receive_locator: ?*const fn (ptr: *anyopaque, req: SnapshotFetchRequest) anyerror!void = null,
    };

    pub fn admitSnapshot(self: SnapshotReceiver, req: SnapshotFetchRequest, data_len: usize) !bool {
        const admit = self.vtable.admit_snapshot orelse return false;
        if (self.vtable.cancel_snapshot_admission == null) return error.InvalidSnapshotReceiver;
        try admit(self.ptr, req, data_len);
        return true;
    }

    pub fn cancelSnapshotAdmission(self: SnapshotReceiver, data_len: usize) void {
        const cancel = self.vtable.cancel_snapshot_admission orelse return;
        cancel(self.ptr, data_len);
    }

    pub fn receiveSnapshot(self: SnapshotReceiver, req: SnapshotFetchRequest, snapshot: core.types.Snapshot) !void {
        return try self.vtable.receive_snapshot(self.ptr, req, snapshot);
    }

    pub fn receiveLocator(self: SnapshotReceiver, req: SnapshotFetchRequest) !void {
        if (self.vtable.receive_locator) |receive_locator| {
            return try receive_locator(self.ptr, req);
        }
    }
};

pub const SnapshotTransport = struct {
    ptr: *anyopaque,
    sender: Sender,
    vtable: *const VTable,

    pub const SynchronousSender = *const fn (
        ptr: *anyopaque,
        req: SnapshotSendRequest,
    ) anyerror!void;

    pub const AsynchronousSender = struct {
        submit_snapshot: *const fn (
            ptr: *anyopaque,
            req: SnapshotSendRequest,
        ) anyerror!AsyncSnapshotSubmitResult,
        drain_completions: *const fn (
            ptr: *anyopaque,
            out: []SnapshotCompletion,
        ) usize,
        /// Idempotently relinquishes transport ownership of an exact attempt.
        /// A racing terminal completion may win; runtimes fence it by key.
        cancel_submission: *const fn (
            ptr: *anyopaque,
            key: SnapshotAttemptKey,
        ) void,
    };

    /// The tagged sender makes the asynchronous ownership contract total:
    /// callers cannot provide submission without also providing completion
    /// draining, while synchronous implementations need no synthetic queue.
    pub const Sender = union(enum) {
        synchronous: SynchronousSender,
        asynchronous: AsynchronousSender,
    };

    pub const VTable = struct {
        fetch_snapshot: ?*const fn (ptr: *anyopaque, req: SnapshotFetchRequest, receiver: SnapshotReceiver) anyerror!void = null,
        cancel_snapshot: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, snapshot_id: []const u8) anyerror!void = null,
    };

    pub fn sendSnapshot(self: SnapshotTransport, req: SnapshotSendRequest) !void {
        return switch (self.sender) {
            .synchronous => |send_snapshot| try send_snapshot(self.ptr, req),
            .asynchronous => error.AsynchronousSnapshotTransportRequiresSubmission,
        };
    }

    pub fn submitSnapshot(self: SnapshotTransport, req: SnapshotSendRequest) !SnapshotSubmitResult {
        return switch (self.sender) {
            .synchronous => |send_snapshot| blk: {
                try send_snapshot(self.ptr, req);
                break :blk .delivered;
            },
            .asynchronous => |sender| switch (try sender.submit_snapshot(self.ptr, req)) {
                .accepted => .accepted,
                .duplicate => .duplicate,
                .retry_later => |retry| .{ .retry_later = .{
                    .retry_after_ms = retry.retry_after_ms,
                } },
            },
        };
    }

    pub fn drainCompletions(self: SnapshotTransport, out: []SnapshotCompletion) usize {
        return switch (self.sender) {
            .synchronous => 0,
            .asynchronous => |sender| sender.drain_completions(self.ptr, out),
        };
    }

    pub fn cancelSubmission(self: SnapshotTransport, key: SnapshotAttemptKey) void {
        switch (self.sender) {
            .synchronous => {},
            .asynchronous => |sender| sender.cancel_submission(self.ptr, key),
        }
    }

    pub fn fetchSnapshot(self: SnapshotTransport, req: SnapshotFetchRequest, receiver: SnapshotReceiver) !void {
        if (self.vtable.fetch_snapshot) |fetch_snapshot| {
            return try fetch_snapshot(self.ptr, req, receiver);
        }
    }

    pub fn cancelSnapshot(self: SnapshotTransport, group_id: core.types.GroupId, snapshot_id: []const u8) !void {
        if (self.vtable.cancel_snapshot) |cancel_snapshot| {
            return try cancel_snapshot(self.ptr, group_id, snapshot_id);
        }
    }
};

test "snapshot transport iface compiles" {
    _ = SnapshotLocator;
    _ = SnapshotArtifactFormat;
    _ = SnapshotSendRequest;
    _ = SnapshotSubmitResult;
    _ = AsyncSnapshotSubmitResult;
    _ = SnapshotCompletion;
    _ = SnapshotAttemptKey;
    _ = SnapshotFetchRequest;
    _ = SnapshotReceiver;
    _ = SnapshotTransport;
}
