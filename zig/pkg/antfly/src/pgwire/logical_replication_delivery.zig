// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded native journal/outbox delivery. The accepting feed control owns
//! offered LSNs; this connection owns only its successfully flushed boundary.
//! Neither a keepalive nor reading a native feed constitutes client feedback.
const std = @import("std");
const local = @import("antfly_local_sources");
const publisher = local.system_catalog_logical_replication_publisher;
const outbox = local.storage_db_logical_replication_outbox_contract;
const row_codec = @import("logical_replication_rows.zig");
const pg = @import("logical_replication_pgoutput.zig");
const operation = local.api_operation;
const A = std.mem.Allocator;

pub const Feedback = struct {
    received: u64,
    flushed: u64,
    applied: u64,
    client_time: i64,
    reply_requested: bool,

    pub fn decode(payload: []const u8) !Feedback {
        if (payload.len != 34 or payload[0] != 'r' or payload[33] > 1) return error.ProtocolViolation;
        const value: Feedback = .{
            .received = std.mem.readInt(u64, payload[1..9], .big),
            .flushed = std.mem.readInt(u64, payload[9..17], .big),
            .applied = std.mem.readInt(u64, payload[17..25], .big),
            .client_time = std.mem.readInt(i64, payload[25..33], .big),
            .reply_requested = payload[33] == 1,
        };
        if (value.applied > value.flushed or value.flushed > value.received) return error.InvalidReplicationAcknowledgement;
        return value;
    }
};

fn timestamp(unix_us: u64) !i64 {
    return std.math.cast(i64, @as(i128, unix_us) - 946684800000000) orelse error.InvalidLogicalPublisher;
}

/// One complete CopyData envelope per pgoutput message. A row is never split
/// into independent XLogData records. The writer is connection-owned and must
/// be serialized with keepalive output by the transport.
pub fn xlogData(writer: *std.Io.Writer, start: u64, head: u64, unix_us: u64, payload: []const u8) !void {
    try xlogDataHeader(writer, start, head, unix_us, payload.len);
    try writer.writeAll(payload);
}

/// Caller must preflight the immutable message and write exactly payload_bytes.
/// A write failure poisons the connection; no partial-message retry is valid.
pub fn xlogDataHeader(writer: *std.Io.Writer, start: u64, head: u64, unix_us: u64, payload_bytes: usize) !void {
    if (start == 0 or head < start or payload_bytes == 0) return error.InvalidLogicalPublisher;
    const length = std.math.cast(u32, std.math.add(usize, payload_bytes, 29) catch return error.ProgramLimitExceeded) orelse return error.ProgramLimitExceeded;
    const time = try timestamp(unix_us);
    try writer.writeByte('d');
    try writer.writeInt(u32, length, .big);
    try writer.writeByte('w');
    try writer.writeInt(u64, start, .big);
    try writer.writeInt(u64, head, .big);
    try writer.writeInt(i64, time, .big);
}

/// Validate and measure immutable control metadata before its frame header,
/// then write it directly. This path allocates no encoded message buffer.
fn emit(writer: *std.Io.Writer, start: u64, head: u64, unix_us: u64, comptime encode: anytype, args: anytype) !void {
    var size = std.Io.Writer.Discarding.init(&.{});
    try @call(.auto, encode, .{&size.writer} ++ args);
    try xlogDataHeader(writer, start, head, unix_us, size.fullCount());
    try @call(.auto, encode, .{writer} ++ args);
}

pub fn keepalive(writer: *std.Io.Writer, head: u64, unix_us: u64, reply_requested: bool) !void {
    if (head == 0) return error.InvalidLogicalPublisher;
    const time = try timestamp(unix_us);
    try writer.writeAll("d\x00\x00\x00\x16k");
    try writer.writeInt(u64, head, .big);
    try writer.writeInt(i64, time, .big);
    try writer.writeByte(@intFromBool(reply_requested));
}

/// A single connection coordinator serializes deliver/feedback calls. Socket
/// reader tasks may queue bounded feedback, but do not mutate this state or
/// acknowledge a transaction whose final flush is still pending.
pub const Sender = struct {
    realm: publisher.Realm,
    lease: publisher.Lease,
    delivered: u64,
    failed: bool = false,

    pub fn init(realm: publisher.Realm, lease: publisher.Lease, after: u64) !Sender {
        try realm.validate();
        try lease.validate();
        if (realm.database_id != lease.slot.database_id or after == 0) return error.InvalidLogicalPublisher;
        return .{ .realm = realm, .lease = lease, .delivered = after };
    }

    /// Decode transport bytes before proposing a native ACK. Client time is
    /// informational only: native metadata authors lease/acknowledgment time.
    pub fn feedback(self: Sender, payload: []const u8) !Feedback {
        if (self.failed) return error.LogicalReplicationDeliveryFailed;
        const value = try Feedback.decode(payload);
        if (value.received > self.delivered) return error.InvalidReplicationAcknowledgement;
        return value;
    }

    /// server is the existing native owner-I/O host (routedPublicationOutboxPage).
    /// Every Data page is independently permission-checked by that receiver.
    /// This method never creates a slot, changes its lease, retries a control,
    /// or ACKs rows. On failure the transport must close this stream.
    pub fn deliver(self: *Sender, a: A, server: anytype, writer: *std.Io.Writer, page: publisher.FeedPage, origin: local.system_catalog_logical_replication.Origin, unix_us: u64, context: operation.RequestContext) !void {
        if (self.failed) return error.LogicalReplicationDeliveryFailed;
        errdefer self.failed = true;
        try context.ensureActive();
        try page.realm.validate();
        try page.lease.validate();
        _ = try timestamp(unix_us);
        if (!page.realm.eql(self.realm) or !page.lease.eql(self.lease) or page.entries.len > publisher.max_feed_page or
            page.offered_lsn < self.delivered or page.offered_lsn > page.head_lsn or
            page.publications.len == 0 or page.publications.len > publisher.max_publications) return error.InvalidLogicalPublisher;
        var ids: [publisher.max_publications]u64 = undefined;
        for (page.publications, 0..) |publication, i| {
            if (publication.id == 0 or publication.generation == 0 or std.mem.allEqual(u8, &publication.digest, 0)) return error.InvalidLogicalPublisher;
            ids[i] = publication.id;
        }
        const selected = ids[0..page.publications.len];
        std.mem.sort(u64, selected, {}, std.sort.asc(u64));
        for (selected[1..], selected[0 .. selected.len - 1]) |id, before| if (id == before) return error.InvalidLogicalPublisher;
        // Validate the complete offered envelope before sending its first byte.
        var expected = self.delivered;
        for (page.entries) |entry| {
            try entry.validate();
            _ = try timestamp(entry.transaction.commit_time_unix_us);
            if (!entry.transaction.realm.eql(self.realm) or entry.begin_lsn != expected or entry.end_lsn > page.offered_lsn) return error.InvalidLogicalPublisher;
            expected = entry.end_lsn;
        }
        if (expected != page.offered_lsn) return error.InvalidLogicalPublisher;
        if (page.entries.len == 0) return;
        // The control allocator may be connection-limited; only the small
        // admission owner uses it. Actual Data responses, schema preparation
        // and row scratch use shared, refundable host working-set admission.
        // The host must retain its ResourceManager through this call.
        const memory = try server.acquireLogicalPublicationMemory(a);
        defer memory.deinit();
        for (page.entries) |entry| {
            try context.ensureActive();
            self.transaction(memory.allocator, server, writer, entry, page.head_lsn, selected, origin, unix_us, context) catch |err| return memory.mapError(err);
            // A failed flush cannot advance even this connection's volatile
            // delivery boundary. Native confirmed flush still needs real ACK.
            try writer.flush();
            self.delivered = entry.end_lsn;
        }
    }

    fn transaction(self: Sender, a: A, server: anytype, writer: *std.Io.Writer, entry: publisher.JournalEntry, head: u64, selected: []const u64, origin: local.system_catalog_logical_replication.Origin, unix_us: u64, context: operation.RequestContext) !void {
        if (origin == .none and (entry.transaction.origin != null or entry.transaction.source != null)) return;
        var begun = false;
        for (entry.transaction.captures, entry.routes) |reference, route| {
            var stream = try row_codec.CaptureStream.init(reference);
            var announced = false;
            while (true) {
                try context.ensureActive();
                var page_region = std.heap.ArenaAllocator.init(a);
                defer page_region.deinit();
                const temporary = page_region.allocator();
                // Reuse row scratch within this page, then refund its high
                // water mark before fetching another page/participant. No
                // retained cell may keep a previous response buffer alive.
                var messages = std.heap.ArenaAllocator.init(a);
                defer messages.deinit();
                const scratch = messages.allocator();
                const request: outbox.PageRequest = .{ .reference = reference, .after = stream.after, .max_bytes = outbox.max_page_wire_bytes };
                const bytes = try server.routedPublicationOutboxPage(temporary, .{
                    .access = .{ .realm = self.realm, .lease = self.lease, .reference = reference, .table_name = route.table_name },
                    .after = request.after,
                    .limit = request.limit,
                    .max_bytes = request.max_bytes,
                }, context);
                try context.ensureActive();
                // routedPublicationOutboxPage returns caller-owned bytes. Keep
                // their page arena alive through all row encoding, then release
                // once; do not copy each potentially large binary image again.
                const captured = try outbox.decodePageBorrowedLeaky(temporary, bytes, request.max_bytes);
                try stream.accept(temporary, request, captured);
                const actions = selectedActions(captured.roster, selected);
                var encoder: ?row_codec.Encoder = null;
                var projection: ?[]const publisher.Column = null;
                for (captured.changes) |change| {
                    try context.ensureActive();
                    const enabled = if (change.before == null) actions.insert else if (change.after == null) actions.delete else actions.update;
                    if (!enabled) continue;
                    if (projection == null) projection = try projectedColumns(temporary, captured.roster, selected);
                    if (change.before != null and captured.roster.relation.replica_identity != .full) {
                        for (captured.roster.relation.columns) |column| if (column.key and column.generated) {
                            const published = for (projection.?) |projected| {
                                if (projected.attnum == column.attnum) break true;
                            } else false;
                            if (!published) return error.UnpublishedReplicaIdentity;
                        };
                    }
                    _ = messages.reset(.retain_capacity);
                    if (encoder == null) encoder = try row_codec.Encoder.initProjectionLeaky(temporary, captured.roster.relation, captured.schemas, projection.?);
                    const prepared = try encoder.?.prepare(scratch, change);
                    if (!begun) {
                        try emit(writer, entry.begin_lsn, head, unix_us, pg.begin, .{ entry.commitLsn(), entry.transaction.commit_time_unix_us, entry.xid });
                        if (entry.transaction.origin) |value| {
                            try emit(writer, entry.begin_lsn, head, unix_us, pg.originMessage, .{value});
                        }
                        begun = true;
                    }
                    if (!announced) {
                        try emit(writer, entry.begin_lsn, head, unix_us, pg.relationProjection, .{ captured.roster.relation, projection.? });
                        announced = true;
                    }
                    try xlogDataHeader(writer, entry.begin_lsn, head, unix_us, prepared.encoded_bytes);
                    try prepared.write(scratch, writer);
                }
                if (captured.next_after == null) break;
            }
            // All participants, including filtered rows, must authenticate
            // their complete captured byte stream before a transaction COMMIT.
            try stream.finish();
        }
        if (begun) {
            try context.ensureActive();
            try emit(writer, entry.commitLsn(), head, unix_us, pg.commit, .{ entry.commitLsn(), entry.end_lsn, entry.transaction.commit_time_unix_us });
        }
    }
};

/// Apply only frozen publication options to the immutable physical columns.
/// PostgreSQL rejects conflicting generated-column selections for a table
/// containing generated columns; ordinary tables have no projection conflict.
pub fn projectedColumns(a: A, roster: publisher.PublishedRelation, selected: []const u64) ![]const publisher.Column {
    const mode = try pg.generatedMode(roster, selected);
    if (mode == .stored) return roster.relation.columns;
    const has_generated = for (roster.relation.columns) |column| {
        if (column.generated) break true;
    } else false;
    if (!has_generated) return roster.relation.columns;
    var count: usize = 0;
    for (roster.relation.columns) |column| if (!column.generated) {
        count += 1;
    };
    const columns = try a.alloc(publisher.Column, count);
    count = 0;
    for (roster.relation.columns) |column| if (!column.generated) {
        columns[count] = column;
        count += 1;
    };
    return columns;
}

fn selectedActions(roster: publisher.PublishedRelation, selected: []const u64) local.system_catalog_logical_replication.PublishActions {
    var result: local.system_catalog_logical_replication.PublishActions = .{ .insert = false, .update = false, .delete = false, .truncate = false };
    var index: usize = 0;
    for (roster.memberships) |membership| {
        while (index < selected.len and selected[index] < membership.publication.id) index += 1;
        if (index == selected.len) break;
        if (selected[index] != membership.publication.id) continue;
        const actions = membership.parameters.publish;
        result.insert = result.insert or actions.insert;
        result.update = result.update or actions.update;
        result.delete = result.delete or actions.delete;
        result.truncate = result.truncate or actions.truncate;
    }
    return result;
}

test "logical publisher transport framing and feedback enforce exact delivered boundaries" {
    const a = std.testing.allocator;
    var output = std.Io.Writer.Allocating.init(a);
    defer output.deinit();
    try xlogData(&output.writer, 10, 30, 946684800000123, "payload");
    try std.testing.expectEqual(@as(usize, 37), output.written().len);
    try std.testing.expectEqual(@as(u8, 'd'), output.written()[0]);
    try std.testing.expectEqual(@as(u32, 36), std.mem.readInt(u32, output.written()[1..5], .big));
    try std.testing.expectEqual(@as(u64, 10), std.mem.readInt(u64, output.written()[6..14], .big));
    try std.testing.expectEqual(@as(u64, 30), std.mem.readInt(u64, output.written()[14..22], .big));
    try std.testing.expectEqual(@as(i64, 123), std.mem.readInt(i64, output.written()[22..30], .big));
    try std.testing.expectEqualStrings("payload", output.written()[30..]);
    output.clearRetainingCapacity();
    try keepalive(&output.writer, 30, 946684800000123, true);
    try std.testing.expectEqual(@as(usize, 23), output.written().len);
    try std.testing.expectEqual(@as(u8, 'k'), output.written()[5]);
    try std.testing.expectEqual(@as(u8, 1), output.written()[22]);
    var bytes: [34]u8 = @splat(0);
    bytes[0] = 'r';
    std.mem.writeInt(u64, bytes[1..9], 20, .big);
    std.mem.writeInt(u64, bytes[9..17], 15, .big);
    std.mem.writeInt(u64, bytes[17..25], 12, .big);
    std.mem.writeInt(i64, bytes[25..33], -123, .big);
    bytes[33] = 1;
    const realm: publisher.Realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 1 };
    const lease: publisher.Lease = .{ .slot = .{ .database_id = 1, .name = "subscriber", .birth = 1 }, .generation = 1, .connection = @splat(1), .nonce = @splat(2) };
    var sender = try Sender.init(realm, lease, 10);
    try std.testing.expectError(error.InvalidReplicationAcknowledgement, sender.feedback(&bytes));
    sender.delivered = 20;
    const feedback = try sender.feedback(&bytes);
    try std.testing.expectEqual(@as(i64, -123), feedback.client_time);
    try std.testing.expect(feedback.reply_requested);
    for (0..bytes.len) |length| try std.testing.expectError(error.ProtocolViolation, Feedback.decode(bytes[0..length]));
    bytes[33] = 2;
    try std.testing.expectError(error.ProtocolViolation, Feedback.decode(&bytes));
    bytes[33] = 0;
    std.mem.writeInt(u64, bytes[17..25], 16, .big);
    try std.testing.expectError(error.InvalidReplicationAcknowledgement, Feedback.decode(&bytes));
    sender.failed = true;
    try std.testing.expectError(error.LogicalReplicationDeliveryFailed, sender.feedback(&bytes));
    output.clearRetainingCapacity();
    try std.testing.expectError(error.InvalidLogicalPublisher, xlogData(&output.writer, 31, 30, 946684800000123, "x"));
    try std.testing.expectEqual(@as(usize, 0), output.written().len);
    try pg.originMessage(&output.writer, .{ .name = "origin", .position = 4 });
    try std.testing.expectEqualStrings("O\x00\x00\x00\x00\x00\x00\x00\x04origin\x00", output.written());
}

test "logical publisher frozen generated projections match PostgreSQL none stored empty and conflict shapes" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const alloc = region.allocator();
    const columns = [_]publisher.Column{ .{ .attnum = 1, .name = "id", .type_oid = 23, .key = true }, .{ .attnum = 2, .name = "g", .type_oid = 23, .generated = true } };
    const description: publisher.RelationDescription = .{ .logical_relation_id = 10, .physical_table_id = 20, .database_id = 1, .namespace_id = 2, .relation_oid = 10, .namespace = "public", .name = "t", .schema_version = 0, .schema_digest = @splat(1), .replica_identity = .default_key, .columns = &columns };
    const roster: publisher.PublishedRelation = .{ .relation = description, .memberships = &.{
        .{ .publication = .{ .id = 30, .generation = 1, .digest = @splat(2) }, .parameters = .{} },
        .{ .publication = .{ .id = 40, .generation = 1, .digest = @splat(3) }, .parameters = .{ .publish_generated_columns = .stored } },
    } };
    try std.testing.expectEqualDeep(columns[0..1], try projectedColumns(alloc, roster, &.{30}));
    try std.testing.expectEqualDeep(&columns, try projectedColumns(alloc, roster, &.{40}));
    try std.testing.expectError(error.UnsupportedSqlShape, projectedColumns(alloc, roster, &.{ 30, 40 }));
    try std.testing.expectError(error.InvalidLogicalPublisher, projectedColumns(alloc, roster, &.{99}));
    try std.testing.expectError(error.InvalidLogicalPublisher, projectedColumns(alloc, roster, &.{ 40, 30 }));
    var ordinary = roster;
    ordinary.relation.columns = columns[0..1];
    try std.testing.expectEqualDeep(columns[0..1], try projectedColumns(alloc, ordinary, &.{ 30, 40 }));
    var generated_only = roster;
    generated_only.relation.columns = columns[1..2];
    try std.testing.expectEqual(@as(usize, 0), (try projectedColumns(alloc, generated_only, &.{30})).len);
    try std.testing.expectEqualStrings("42P10", local.sql_errors.describe(error.UnpublishedReplicaIdentity).code);
}
