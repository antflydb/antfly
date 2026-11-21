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

//! Typed CopyBoth bridge for a retained slot owner. The authenticated host
//! supplies metadata/storage/policy capabilities and an independent close
//! context, and must drain any background work before releasing credentials.
//! This bridge does not register the native production replication factory.
const std = @import("std");
const backend = @import("backend.zig");
const retained = @import("../metadata/pg_spool_stream.zig");
const operation = @import("antfly_local_sources").api_operation;
const Context = operation.RequestContext;

pub fn metadataContext(request: backend.Request) !Context {
    try request.check();
    if (request.deadline.clock != .awake) return error.UnsupportedReplicationMode;
    const deadline = std.math.cast(u64, request.deadline.raw.nanoseconds);
    const context: Context = .{
        .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" },
        .deadline_ns = deadline,
        .deadline_io = if (deadline != null) @import("antfly_runtime_abi").io_abi.Borrow.init(&request.io) else null,
        .cancellation = operation.CancellationToken.fromAtomic(request.cancel_requested),
    };
    return context.platformDeadline();
}

fn temporaryTransportFailure(err: anyerror) bool {
    return switch (err) {
        error.Canceled, error.Cancelled, error.QueryCanceled, error.DeadlineExceeded, error.ReadUnavailable, error.ConcurrencyUnavailable, error.NetworkUnreachable, error.ConnectionRefused, error.ConnectionResetByPeer, error.Timeout => true,
        else => false,
    };
}

pub fn Bridge(comptime Host: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        host: *Host,
        owner: retained.Owner,
        pages: usize = 0,
        pub fn open(raw: *anyopaque, a: std.mem.Allocator, request: backend.Request, start: backend.replication_commands.Start) !backend.ReplicationStream {
            const host: *Host = @ptrCast(@alignCast(raw));
            const self = try a.create(Self);
            errdefer a.destroy(self);
            const binding = try host.binding(a, start, request);
            const req = try host.request(request);
            errdefer host.failedOpen();
            const owner = try retained.Owner.open(host.service, host.registry, host, request.scratch_allocator orelse a, host.id, start.slot, host.actor, @backingInt(start.position), binding, host.config, req);
            self.* = .{ .allocator = a, .host = host, .owner = owner };
            return .{ .context = self, .initial_lsn = @fromBackingInt(owner.initial_lsn), .resume_lsn = @fromBackingInt(owner.resume_lsn), .initial_end_lsn = @fromBackingInt(owner.initial_end_lsn), .poll = poll, .feedback = feedback, .close = close };
        }
        const Lease = struct {
            parent: *Self,
            allocator: std.mem.Allocator,
            bytes: []u8,
            fn release(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.parent.pages -= 1;
                const a = self.allocator;
                a.free(self.bytes);
                a.destroy(self);
            }
        };
        fn poll(raw: *anyopaque, a: std.mem.Allocator, request: backend.Request, after: backend.pgoutput.Lsn, maximum: u32) !backend.ReplicationPoll {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (self.pages != 0) return error.InvalidResult;
            if (comptime @hasDecl(Host, "schedulePublication"))
                self.host.schedulePublication(self.owner.session, request) catch |err|
                    return if (temporaryTransportFailure(err)) error.ReplicationReadDeferred else err;
            const req = self.host.request(request) catch |err|
                return if (temporaryTransportFailure(err)) error.ReplicationReadDeferred else err;
            // The connection budget can reclaim large transport buffers
            // immediately. A fetch arena would retain them through every
            // nested metadata and artifact read in this poll.
            const backing = request.scratch_allocator orelse a;
            // A failed read leaves the frontier at the last emitted byte.
            var page = self.owner.poll(self.host.service, self.host.registry, self.host, backing, @backingInt(after), maximum, req) catch |err|
                return if (temporaryTransportFailure(err)) error.ReplicationReadDeferred else err;
            defer page.deinit();
            var result: backend.ReplicationPoll = .{ .end_lsn = @fromBackingInt(page.end_lsn) };
            if (page.bytes) |bytes| {
                const lease = try backing.create(Lease);
                lease.* = .{ .parent = self, .allocator = backing, .bytes = bytes };
                page.bytes = null;
                self.pages += 1;
                result.chunk = .{ .start_lsn = @fromBackingInt(page.start_lsn), .bytes = bytes };
                result.owner = .{ .context = lease, .release = Lease.release };
            }
            return result;
        }
        fn feedback(raw: *anyopaque, a: std.mem.Allocator, request: backend.Request, value: backend.pgoutput.Feedback) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const req = self.host.request(request) catch |err|
                return if (temporaryTransportFailure(err)) error.ReplicationFeedbackDeferred else err;
            // The next client status update can re-observe any previously
            // committed acknowledgment under the same session and ceiling.
            self.owner.feedback(self.host.service, self.host, request.scratch_allocator orelse a, .{ .written = @backingInt(value.written), .flushed = @backingInt(value.flushed), .applied = @backingInt(value.applied) }, req) catch |err|
                return if (temporaryTransportFailure(err) or err == error.MetadataMutationOutcomeUnknown) error.ReplicationFeedbackDeferred else err;
        }
        fn close(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            std.debug.assert(self.pages == 0);
            self.host.closed(&self.owner);
            const a = self.allocator;
            a.destroy(self);
        }
    };
}
