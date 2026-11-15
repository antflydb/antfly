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

//! Bounded private root reads. A page belongs to exactly one committed frame;
//! completing a page does not acknowledge the frame or certify completion of a
//! distributed transaction. The feed owner must verify its global outcome.
const std = @import("std");
const history = @import("committed_history.zig");
const control = @import("history_consumer.zig");
const frame = history.frame;
pub const max_source_bytes = frame.max_bytes;
/// Private retained physical identity to lossless authored document key.
pub fn documentId(a: std.mem.Allocator, physical: []const u8) ![]u8 {
    return (try @import("internal_keys.zig").decodeStoredDocumentRowKeyAlloc(a, physical)) orelse error.ChangefeedCapabilityUnsupported;
}
pub const Position = struct {
    /// Fully consumed frames. A nonzero event resumes inside sequence + 1.
    sequence: u64 = 0,
    event: u32 = 0,
};
pub const Request = struct {
    reference: control.Reference,
    after: Position,
    now_seconds: u64,
    max_events: u32 = 256,
    /// Raw authenticated header/schema/event bytes, excluding fixed reader,
    /// descriptor and result-struct overhead. A large event requires an explicit
    /// larger budget; it is never allocated before this limit is checked.
    max_bytes: u32 = 1024 * 1024,
};
pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    header: ?frame.Header = null,
    events: []const frame.Event = &.{},
    first_ordinal: u32 = 0,
    frame_events: u32 = 0,
    next: Position,
    observed_latest: u64,
    caught_up: bool,
    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
/// The caller supplies a stable read transaction. All returned byte slices are
/// page-owned, so neither transaction teardown nor backend scratch reuse can
/// invalidate them. Descriptor and chunk buffers have independent ownership.
pub fn read(a: std.mem.Allocator, txn: anytype, request: Request) !Page {
    if (request.now_seconds == 0 or request.max_events == 0 or request.max_events > 1024 or request.max_bytes == 0 or request.max_bytes > frame.max_bytes) return error.InvalidHistoryAdmission;
    const consumer = try control.inspect(txn, request.reference);
    if (request.now_seconds >= consumer.expires_at_seconds or request.after.sequence < consumer.acknowledged) return error.HistoryResynchronizationRequired;
    const native = (try history.load(txn)) orelse return error.HistoryCorrupt;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const descriptor = (try history.read(a, txn, request.reference.namespace, consumer.id, consumer.epoch, request.after.sequence)) orelse {
        if (request.after.event != 0) return error.HistoryCheckpointConflict;
        return .{ .arena = arena, .next = request.after, .observed_latest = native.latest, .caught_up = true };
    };
    defer a.free(descriptor.raw);
    if (request.after.event >= descriptor.events) return error.HistoryCheckpointConflict;
    const header_bytes = descriptor.eventOffset(0);
    if (header_bytes > request.max_bytes) return error.HistoryTooLarge;
    const Txn = @typeInfo(@TypeOf(txn)).pointer.child;
    var chunks = try history.ChunkSource(Txn).init(a, txn, descriptor);
    defer chunks.deinit();
    var reader: frame.Reader = .{ .descriptor = descriptor, .source = chunks.source() };
    const header = try reader.seek(scratch, request.after.event);
    const capacity = @min(request.max_events, descriptor.events - request.after.event);
    const events = try scratch.alloc(frame.Event, capacity);
    var bytes: u32 = header_bytes;
    var count: u32 = 0;
    while (count < capacity) {
        const ordinal = request.after.event + count;
        const size = descriptor.eventOffset(ordinal + 1) - descriptor.eventOffset(ordinal);
        if (size > request.max_bytes - bytes) {
            if (count == 0) return error.HistoryTooLarge;
            break;
        }
        events[count] = (try reader.next(scratch)) orelse return error.HistoryCorrupt;
        bytes += size;
        count += 1;
    }
    const complete = request.after.event + count == descriptor.events;
    const next: Position = if (complete) .{ .sequence = descriptor.sequence } else .{ .sequence = request.after.sequence, .event = request.after.event + count };
    return .{ .arena = arena, .header = header, .events = events[0..count], .first_ordinal = request.after.event, .frame_events = descriptor.events, .next = next, .observed_latest = native.latest, .caught_up = complete and descriptor.sequence == native.latest };
}
