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
pub const max_projection_roots = history.max_completion_projection_roots;
pub const CompletionEffect = history.CompletionEffect;
pub const Request = struct {
    reference: control.Reference,
    after: Position,
    now_seconds: u64,
    max_events: u32 = 256,
    /// Raw authenticated header/schema/event bytes, excluding fixed reader,
    /// descriptor and result-struct overhead. A large event requires an explicit
    /// larger budget; it is never allocated before this limit is checked.
    max_bytes: u32 = 1024 * 1024,
    include_completion: bool = false,
    completion_roots: []const frame.Namespace = &.{},

    pub fn validateCompletionRoots(self: Request) !void {
        if (self.completion_roots.len > max_projection_roots or (self.completion_roots.len != 0 and !self.include_completion)) return error.InvalidHistoryAdmission;
        for (self.completion_roots, 0..) |root, i| {
            if (std.mem.allEqual(u8, &root, 0) or (i != 0 and std.mem.order(u8, &self.completion_roots[i - 1], &root) != .lt)) return error.InvalidHistoryAdmission;
        }
    }

    pub fn jsonStringify(self: Request, jw: anytype) !void {
        try jw.beginObject();
        inline for (.{ "reference", "after", "now_seconds", "max_events", "max_bytes" }) |field| {
            try jw.objectField(field);
            try jw.write(@field(self, field));
        }
        if (self.include_completion) {
            try jw.objectField("include_completion");
            try jw.write(true);
        }
        if (self.completion_roots.len != 0) {
            try jw.objectField("completion_roots");
            try jw.write(self.completion_roots);
        }
        try jw.endObject();
    }
};
pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    header: ?frame.Header = null,
    completion: ?history.FrameCompletion = null,
    completion_effects: []const ?history.CompletionEffect = &.{},
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
pub const BootstrapRequest = struct {
    reference: control.Reference,
    digest: control.Digest,
    now_seconds: u64,
    after_rows: u32 = 0,
    max_rows: u32 = 256,
    max_bytes: u32 = 1024 * 1024,

    pub fn validate(self: BootstrapRequest) !void {
        try self.reference.validate();
        if (self.now_seconds == 0 or std.mem.allEqual(u8, &self.digest, 0) or
            self.max_rows == 0 or self.max_rows > 1024 or self.after_rows > control.max_bootstrap_examined_keys or
            self.max_bytes == 0 or self.max_bytes > control.max_bootstrap_bytes) return error.InvalidHistoryAdmission;
    }
};

pub const BootstrapPage = struct {
    arena: std.heap.ArenaAllocator,
    schema: []const u8,
    rows: []const control.BootstrapImage.Row,
    next_row: u32,
    done: bool,

    pub fn deinit(self: *BootstrapPage) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// The position is the number of physical rows consumed from this exact pin.
/// Page buffers own the selected rows and schema across transaction teardown.
pub fn readBootstrap(a: std.mem.Allocator, txn: anytype, request: BootstrapRequest) !BootstrapPage {
    try request.validate();
    const raw = try control.readBootstrap(txn, request.reference, request.digest, request.now_seconds);
    var image = try control.BootstrapImage.decode(raw);
    if (request.after_rows > image.rows.count) return error.HistoryCheckpointConflict;
    if (image.schema.len > request.max_bytes) return error.HistoryTooLarge;
    for (0..request.after_rows) |_| _ = (try image.rows.next()) orelse return error.HistoryCorrupt;

    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const page_alloc = arena.allocator();
    var rows: std.ArrayList(control.BootstrapImage.Row) = .empty;
    var bytes = image.schema.len;
    while (rows.items.len < request.max_rows and image.rows.count != 0) {
        const candidate = image.rows;
        const row = (try image.rows.next()).?;
        const size = 8 + row.key.len + row.value.len;
        if (size > request.max_bytes - bytes) {
            if (rows.items.len == 0) return error.HistoryTooLarge;
            image.rows = candidate;
            break;
        }
        try rows.append(page_alloc, .{ .key = try page_alloc.dupe(u8, row.key), .value = try page_alloc.dupe(u8, row.value) });
        bytes += size;
    }
    if (image.rows.count == 0) _ = try image.rows.next();
    return .{
        .arena = arena,
        .schema = try page_alloc.dupe(u8, image.schema),
        .rows = rows.items,
        .next_row = request.after_rows + @as(u32, @intCast(rows.items.len)),
        .done = image.rows.count == 0,
    };
}

/// The caller supplies a stable read transaction. All returned byte slices are
/// page-owned, so neither transaction teardown nor backend scratch reuse can
/// invalidate them. Descriptor and chunk buffers have independent ownership.
pub fn read(a: std.mem.Allocator, txn: anytype, request: Request) !Page {
    return readWithCompletion(a, txn, request, null);
}

pub fn readWithCompletion(a: std.mem.Allocator, txn: anytype, request: Request, root_generation: ?u64) !Page {
    try request.validateCompletionRoots();
    if (request.include_completion and root_generation == null) return error.ChangefeedCapabilityUnsupported;
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
    const effects: []?history.CompletionEffect = if (request.include_completion and header.transaction_scope == .coordinated)
        try scratch.alloc(?history.CompletionEffect, request.completion_roots.len)
    else
        &.{};
    const completion = if (request.include_completion and header.transaction_scope == .coordinated)
        try history.readProjectedFrameCompletion(a, txn, root_generation.?, header, descriptor, if (effects.len == 0) null else .{ .roots = request.completion_roots, .effects = effects })
    else
        null;
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
    return .{ .arena = arena, .header = header, .completion = completion, .completion_effects = if (completion != null) effects else &.{}, .events = events[0..count], .first_ordinal = request.after.event, .frame_events = descriptor.events, .next = next, .observed_latest = native.latest, .caught_up = complete and descriptor.sequence == native.latest };
}
