// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable logical row images. SQL null/presence/typed payloads remain in
//! canonical AROW bytes; JSON serialization never collapses that provenance.
//! The native capture owner strictly validates AROW against its retained schema
//! before constructing these records. This checksum codec alone grants no row,
//! transaction, publication, snapshot or filesystem authority.
const std = @import("std");
const publisher = @import("../../system_catalog/logical_replication_publisher.zig");
const capture = @import("../../system_catalog/logical_replication_capture.zig");
const owner_mod = @import("../transaction_decision_owner.zig");
const namespace = @import("doc_identity_namespace.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const family = "\x00\x00__metadata__:logical_outbox:v1:";
/// Immutable same-family point locator, written with the real capture PREPARE.
/// Its absence is meaningful only with the TxnRecord's capture-required flag.
pub const Transaction = struct {
    reference: publisher.CaptureRef,
    intent_revision: u64,
    pub fn validate(self: @This()) !void {
        try self.reference.validate();
        if (self.intent_revision == 0) return error.InvalidLogicalOutbox;
    }
};
pub fn transactionKey(a: A, transaction_id: [16]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}transaction:{x}", .{ family, transaction_id });
}
pub const Image = struct { schema_digest: Digest, row: []const u8 };
pub const Change = struct {
    /// Native final-plan canonical order: frozen participant order, then the
    /// immutable per-participant final primary-key mutation order. It is not a
    /// reconstructed SQL statement or a claim about another server's WAL.
    ordinal: u64,
    key: []const u8,
    before: ?Image,
    after: ?Image,
    pub fn validate(self: @This(), max_bytes: usize) !void {
        if (self.ordinal == 0 or self.key.len == 0 or self.key.len > std.math.maxInt(u32) or std.mem.startsWith(u8, self.key, "\x00\x00") or (self.before == null and self.after == null)) return error.InvalidLogicalOutbox;
        var used: usize = 0;
        try charge(&used, 4 + 8 + 4 + 4 + 4 + 32, max_bytes);
        try charge(&used, self.key.len, max_bytes);
        for ([_]?Image{ self.before, self.after }) |maybe| if (maybe) |image| {
            if (image.row.len == 0 or image.row.len >= std.math.maxInt(u32) or zero(image.schema_digest)) return error.InvalidLogicalOutbox;
            try charge(&used, 32, max_bytes);
            try charge(&used, image.row.len, max_bytes);
        };
    }
    pub fn encodedLen(self: @This(), max_bytes: usize) !usize {
        try self.validate(max_bytes);
        var size: usize = 56 + self.key.len;
        if (self.before) |image| size += 32 + image.row.len;
        if (self.after) |image| size += 32 + image.row.len;
        return size;
    }
    pub fn encode(self: @This(), a: A, max_bytes: usize) ![]u8 {
        const bytes = try a.alloc(u8, try self.encodedLen(max_bytes));
        errdefer a.free(bytes);
        try self.writeEncoded(bytes, max_bytes);
        return bytes;
    }
    /// Synchronous copying boundary. No input row/key view is retained. Allows
    /// the final page allocation to own each LCR1 directly without another
    /// whole-row scratch allocation; durable LCR1 bytes/checksum are identical.
    pub fn writeEncoded(self: @This(), bytes: []u8, max_bytes: usize) !void {
        if (bytes.len != try self.encodedLen(max_bytes)) return error.InvalidLogicalOutbox;
        @memcpy(bytes[0..4], "LCR1");
        std.mem.writeInt(u64, bytes[4..12], self.ordinal, .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(self.key.len), .little);
        std.mem.writeInt(u32, bytes[16..20], if (self.before) |image| @intCast(image.row.len) else std.math.maxInt(u32), .little);
        std.mem.writeInt(u32, bytes[20..24], if (self.after) |image| @intCast(image.row.len) else std.math.maxInt(u32), .little);
        var at: usize = 24;
        @memcpy(bytes[at..][0..self.key.len], self.key);
        at += self.key.len;
        for ([_]?Image{ self.before, self.after }) |maybe| if (maybe) |image| {
            @memcpy(bytes[at..][0..32], &image.schema_digest);
            at += 32;
            @memcpy(bytes[at..][0..image.row.len], image.row);
            at += image.row.len;
        };
        if (at + 32 != bytes.len) return error.InvalidLogicalOutbox;
        std.crypto.hash.Blake3.hash(bytes[0..at], bytes[at..][0..32], .{});
    }
    /// Hash exactly the canonical LCR1 encoding without copying a whole row.
    /// Delivery authenticates the producer's original stream while encoding
    /// pgoutput; COMMIT is withheld until its final digest joins the receipt.
    pub fn hashEncoded(self: @This(), hash: *std.crypto.hash.Blake3, maximum: usize) !void {
        try self.validate(maximum);
        var header: [24]u8 = undefined;
        @memcpy(header[0..4], "LCR1");
        std.mem.writeInt(u64, header[4..12], self.ordinal, .little);
        std.mem.writeInt(u32, header[12..16], @intCast(self.key.len), .little);
        std.mem.writeInt(u32, header[16..20], if (self.before) |image| @intCast(image.row.len) else std.math.maxInt(u32), .little);
        std.mem.writeInt(u32, header[20..24], if (self.after) |image| @intCast(image.row.len) else std.math.maxInt(u32), .little);
        var checksum = std.crypto.hash.Blake3.init(.{});
        checksum.update(&header);
        hash.update(&header);
        checksum.update(self.key);
        hash.update(self.key);
        for ([_]?Image{ self.before, self.after }) |maybe| if (maybe) |image| {
            checksum.update(&image.schema_digest);
            hash.update(&image.schema_digest);
            checksum.update(image.row);
            hash.update(image.row);
        };
        var digest: [32]u8 = undefined;
        checksum.final(&digest);
        hash.update(&digest);
    }
    /// Borrowed view. The input lease must survive all image decoding/writes.
    pub fn decode(bytes: []const u8, max_bytes: usize) !@This() {
        if (bytes.len < 56 or bytes.len > max_bytes or !std.mem.eql(u8, bytes[0..4], "LCR1")) return error.InvalidLogicalOutbox;
        const body = bytes[0 .. bytes.len - 32];
        var actual: Digest = undefined;
        std.crypto.hash.Blake3.hash(body, &actual, .{});
        if (!std.mem.eql(u8, &actual, bytes[body.len..])) return error.InvalidLogicalOutbox;
        const key_len = std.mem.readInt(u32, bytes[12..16], .little);
        if (key_len > body.len - 24) return error.InvalidLogicalOutbox;
        var result: @This() = .{ .ordinal = std.mem.readInt(u64, bytes[4..12], .little), .key = bytes[24..][0..key_len], .before = null, .after = null };
        var at: usize = 24 + key_len;
        for ([_]u32{ std.mem.readInt(u32, bytes[16..20], .little), std.mem.readInt(u32, bytes[20..24], .little) }, 0..) |length, index| {
            if (length == std.math.maxInt(u32)) continue;
            if (at > body.len or body.len - at < 32 or length > body.len - at - 32) return error.InvalidLogicalOutbox;
            const image: Image = .{ .schema_digest = body[at..][0..32].*, .row = body[at + 32 ..][0..length] };
            at += 32 + length;
            if (index == 0) result.before = image else result.after = image;
        }
        if (at != body.len) return error.InvalidLogicalOutbox;
        try result.validate(max_bytes);
        return result;
    }
};
pub const Schema = struct {
    digest: Digest,
    /// Actual immutable storage.Schema serialization, including named type
    /// records/labels. It is not recompiled from current aliases on delivery.
    encoded: []const u8,
    pub fn validate(self: @This()) !void {
        if (self.encoded.len == 0 or zero(self.digest)) return error.InvalidLogicalOutbox;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/logical-outbox-schema/v1\x00");
        hash.update(self.encoded);
        var actual: Digest = undefined;
        hash.final(&actual);
        if (!std.mem.eql(u8, &actual, &self.digest)) return error.InvalidLogicalOutbox;
    }
};
pub const Header = struct {
    reference: publisher.CaptureRef,
    /// Exact durable prepared intent revision; never a source/replication clock.
    intent_revision: u64,
    demand_digest: Digest,
    rows_digest: Digest,
    roster_bytes: u32,
    schema_count: u32,
    schemas_digest: Digest,
    pub fn validate(self: @This()) !void {
        try self.reference.validate();
        if (self.intent_revision == 0 or self.roster_bytes == 0 or self.roster_bytes > publisher.max_roster_bytes + 32 or zero(self.demand_digest) or zero(self.rows_digest) or zero(self.schemas_digest) or self.schema_count == 0) return error.InvalidLogicalOutbox;
    }
};
/// max_bytes bounds the complete LCP1 wire frame, including all framing.
pub const PageRequest = struct {
    reference: publisher.CaptureRef,
    after: u64 = 0,
    limit: u16 = 32,
    max_bytes: usize,
    pub fn require(self: @This(), page: Page) !void {
        try self.reference.validate();
        try validatePageWireBudget(self.max_bytes);
        if (self.limit == 0 or self.limit > publisher.max_feed_page or self.after > self.reference.change_count or
            !page.header.reference.sameLogical(self.reference) or page.changes.len > self.limit) return error.InvalidLogicalOutbox;
        if (page.changes.len == 0) {
            if (self.after != self.reference.change_count or page.next_after != null) return error.InvalidLogicalOutbox;
        } else {
            if (self.after == std.math.maxInt(u64) or page.changes[0].ordinal != self.after + 1) return error.InvalidLogicalOutbox;
            const last = page.changes[page.changes.len - 1].ordinal;
            if (page.next_after) |next| {
                if (next != last or next >= self.reference.change_count) return error.InvalidLogicalOutbox;
            } else if (last != self.reference.change_count) return error.InvalidLogicalOutbox;
        }
    }
};
pub const Page = struct {
    header: Header,
    /// Exact publication/column/type/parameter roster frozen at PREPARE, never
    /// looked up from current aliases after publication ALTER/DROP.
    roster: publisher.PublishedRelation,
    changes: []const Change,
    schemas: []const Schema,
    next_after: ?u64 = null,
};
/// Maximum envelope overhead above existing capture admission. Kept explicit
/// so a valid admitted capture is not silently assigned a smaller feed quota.
pub const max_capture_payload_bytes = @import("table_catalog.zig").default_transaction_admission_bytes;
pub const max_metadata_record_bytes: usize = 32 * 1024;
pub const page_framing_bytes = 512 + publisher.max_feed_page * (4 + 2 * 36);
// Outbox capture admission charges roster/rows/schema but its separately
// bounded durable header also travels on a page. Retain that exact allowance.
pub const max_page_wire_bytes = @import("table_catalog.zig").default_transaction_admission_bytes + max_metadata_record_bytes + page_framing_bytes;
pub fn maxPageWireBytes(raw_budget: usize) !usize {
    if (raw_budget == 0 or raw_budget > @import("table_catalog.zig").default_transaction_admission_bytes) return error.InvalidLogicalOutbox;
    return std.math.add(usize, raw_budget, max_metadata_record_bytes + page_framing_bytes);
}
pub fn validatePageWireBudget(budget: usize) !void {
    if (budget == 0 or budget > max_page_wire_bytes) return error.InvalidLogicalOutbox;
}
const PageMetadata = struct { header: Header, roster: publisher.PublishedRelation, next_after: ?u64 };

fn validatePage(a: A, page: Page) !void {
    try page.header.validate();
    var budget: publisher.RosterBudget = .{};
    try budget.add(page.roster);
    const roster_digest = try capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", page.roster);
    if (!std.mem.eql(u8, &roster_digest, &page.header.reference.roster_digest) or page.changes.len > publisher.max_feed_page or page.schemas.len > 2 * page.changes.len or page.schemas.len > page.header.schema_count) return error.InvalidLogicalOutbox;
    for (page.schemas, 0..) |schema, index| {
        try schema.validate();
        for (page.schemas[0..index]) |previous| if (std.mem.eql(u8, &schema.digest, &previous.digest)) return error.InvalidLogicalOutbox;
    }
    var previous: ?u64 = null;
    for (page.changes) |change| {
        if (change.ordinal > page.header.reference.change_count) return error.InvalidLogicalOutbox;
        if (previous) |ordinal| if (ordinal == std.math.maxInt(u64) or change.ordinal != ordinal + 1) return error.InvalidLogicalOutbox;
        previous = change.ordinal;
        for ([_]?Image{ change.before, change.after }) |maybe| if (maybe) |image| {
            var found = false;
            for (page.schemas) |schema| if (std.mem.eql(u8, &image.schema_digest, &schema.digest)) {
                found = true;
                break;
            };
            if (!found) return error.InvalidLogicalOutbox;
        };
    }
    if (page.next_after) |next| {
        if (previous == null or next != previous.? or next >= page.header.reference.change_count) return error.InvalidLogicalOutbox;
    } else if (previous != null and previous.? != page.header.reference.change_count) return error.InvalidLogicalOutbox;
}

/// Raw row/schema bytes occur once on the wire. A checksum detects malformed
/// transport; actual feed/P authority remains the caller's independent check.
pub fn encodePage(page: Page, a: A, wire_budget: usize) ![]u8 {
    try validatePageWireBudget(wire_budget);
    const maximum = wire_budget;
    try validatePage(a, page);
    if (page.changes.len > publisher.max_feed_page or page.schemas.len > 2 * page.changes.len or page.schemas.len > page.header.schema_count) return error.InvalidLogicalOutbox;
    const metadata = try std.json.Stringify.valueAlloc(a, PageMetadata{ .header = page.header, .roster = page.roster, .next_after = page.next_after }, .{});
    defer a.free(metadata);
    var size: usize = 16 + 32;
    try charge(&size, metadata.len, maximum);
    for (page.changes) |change| {
        // The LCR1 size is exactly the charged durable row representation.
        try charge(&size, 4 + try change.encodedLen(wire_budget), maximum);
    }
    for (page.schemas) |schema| {
        try schema.validate();
        try charge(&size, 36 + schema.encoded.len, maximum);
    }
    const result = try a.alloc(u8, size);
    errdefer a.free(result);
    @memcpy(result[0..4], "LCP1");
    std.mem.writeInt(u32, result[4..8], @intCast(metadata.len), .little);
    std.mem.writeInt(u32, result[8..12], @intCast(page.changes.len), .little);
    std.mem.writeInt(u32, result[12..16], @intCast(page.schemas.len), .little);
    var at: usize = 16;
    @memcpy(result[at..][0..metadata.len], metadata);
    at += metadata.len;
    for (page.changes) |change| {
        const len = try change.encodedLen(wire_budget);
        std.mem.writeInt(u32, result[at..][0..4], @intCast(len), .little);
        at += 4;
        try change.writeEncoded(result[at..][0..len], wire_budget);
        at += len;
    }
    for (page.schemas) |schema| {
        @memcpy(result[at..][0..32], &schema.digest);
        at += 32;
        std.mem.writeInt(u32, result[at..][0..4], @intCast(schema.encoded.len), .little);
        at += 4;
        @memcpy(result[at..][0..schema.encoded.len], schema.encoded);
        at += schema.encoded.len;
    }
    if (at != result.len - 32) return error.InvalidLogicalOutbox;
    std.crypto.hash.Blake3.hash(result[0..at], result[at..][0..32], .{});
    return result;
}

/// All allocations belong to the caller's output arena, including row bytes.
/// No borrowed owner buffer survives the checked CABI call/buffer destruction.
pub fn decodePageLeaky(a: A, bytes: []const u8, wire_budget: usize) !Page {
    return decodePage(a, bytes, wire_budget, true);
}

/// Borrow binary row/schema slices from an immutable, caller-owned transport
/// frame. The frame MUST remain alive and unchanged through validation and
/// delivery; metadata and descriptor arrays still belong to the output arena.
/// Never use directly on a CABI buffer that is released by its returning call.
pub fn decodePageBorrowedLeaky(a: A, bytes: []const u8, wire_budget: usize) !Page {
    return decodePage(a, bytes, wire_budget, false);
}

fn decodePage(a: A, bytes: []const u8, wire_budget: usize, comptime own_payloads: bool) !Page {
    try validatePageWireBudget(wire_budget);
    const maximum = wire_budget;
    if (bytes.len < 48 or bytes.len > maximum or !std.mem.eql(u8, bytes[0..4], "LCP1")) return error.InvalidLogicalOutbox;
    const body = bytes[0 .. bytes.len - 32];
    var actual: Digest = undefined;
    std.crypto.hash.Blake3.hash(body, &actual, .{});
    if (!std.mem.eql(u8, &actual, bytes[body.len..])) return error.InvalidLogicalOutbox;
    const meta_len = std.mem.readInt(u32, body[4..8], .little);
    const change_count = std.mem.readInt(u32, body[8..12], .little);
    const schema_count = std.mem.readInt(u32, body[12..16], .little);
    if (meta_len > body.len - 16 or change_count > publisher.max_feed_page or schema_count > 2 * change_count) return error.InvalidLogicalOutbox;
    const meta = try std.json.parseFromSliceLeaky(PageMetadata, a, body[16..][0..meta_len], .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    try meta.header.validate();
    const changes = try a.alloc(Change, change_count);
    const schemas = try a.alloc(Schema, schema_count);
    var at: usize = 16 + meta_len;
    for (changes) |*change| {
        if (body.len - at < 4) return error.InvalidLogicalOutbox;
        const len = std.mem.readInt(u32, body[at..][0..4], .little);
        at += 4;
        if (len > body.len - at) return error.InvalidLogicalOutbox;
        const encoded = body[at..][0..len];
        change.* = try Change.decode(if (own_payloads) try a.dupe(u8, encoded) else encoded, wire_budget);
        at += len;
    }
    for (schemas) |*schema| {
        if (body.len - at < 36) return error.InvalidLogicalOutbox;
        const digest_value = body[at..][0..32].*;
        at += 32;
        const len = std.mem.readInt(u32, body[at..][0..4], .little);
        at += 4;
        if (len > body.len - at) return error.InvalidLogicalOutbox;
        const encoded = body[at..][0..len];
        schema.* = .{ .digest = digest_value, .encoded = if (own_payloads) try a.dupe(u8, encoded) else encoded };
        try schema.validate();
        at += len;
    }
    if (at != body.len) return error.InvalidLogicalOutbox;
    const result: Page = .{ .header = meta.header, .roster = meta.roster, .changes = changes, .schemas = schemas, .next_after = meta.next_after };
    try validatePage(a, result);
    return result;
}

pub fn ownerDigest(owner: owner_mod.External) !Digest {
    const bytes = try owner.encode();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-outbox-owner/v1\x00");
    hash.update(&bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}
pub fn prefix(a: A, owner: owner_mod.External, member: u16) ![]u8 {
    if (member >= publisher.max_capture_participants) return error.InvalidLogicalOutbox;
    return std.fmt.allocPrint(a, "{s}{s}:{x:0>4}:", .{ family, std.fmt.bytesToHex(try ownerDigest(owner), .lower), member });
}
pub fn headerKey(a: A, owner: owner_mod.External, member: u16) ![]u8 {
    return std.mem.concat(a, u8, &.{ try prefix(a, owner, member), "head" });
}
pub fn rosterKey(a: A, owner: owner_mod.External, member: u16) ![]u8 {
    return std.mem.concat(a, u8, &.{ try prefix(a, owner, member), "roster" });
}
pub fn rowKey(a: A, owner: owner_mod.External, member: u16, ordinal: u64) ![]u8 {
    if (ordinal == 0) return error.InvalidLogicalOutbox;
    return std.fmt.allocPrint(a, "{s}row:{x:0>16}", .{ try prefix(a, owner, member), ordinal });
}
pub fn schemaKey(a: A, owner: owner_mod.External, member: u16, digest: Digest) ![]u8 {
    return std.fmt.allocPrint(a, "{s}schema:{s}", .{ try prefix(a, owner, member), std.fmt.bytesToHex(digest, .lower) });
}
fn charge(used: *usize, bytes: usize, maximum: usize) !void {
    if (used.* > maximum or bytes > maximum - used.*) return error.TransactionTooLarge;
    used.* += bytes;
}
fn zero(bytes: Digest) bool {
    return std.mem.allEqual(u8, &bytes, 0);
}

test "logical outbox row codec preserves typed raw images and rejects damaged lengths provenance and aggregate budget" {
    const a = std.testing.allocator;
    const input: Change = .{ .ordinal = 1, .key = "row", .before = .{ .schema_digest = @splat(1), .row = "AROW-before" }, .after = .{ .schema_digest = @splat(2), .row = "AROW-after" } };
    const encoded = try input.encode(a, 4096);
    defer a.free(encoded);
    const direct = try a.alloc(u8, encoded.len);
    defer a.free(direct);
    try input.writeEncoded(direct, 4096);
    try std.testing.expectEqualSlices(u8, encoded, direct);
    try std.testing.expectError(error.InvalidLogicalOutbox, input.writeEncoded(direct[0 .. direct.len - 1], 4096));
    const decoded = try Change.decode(encoded, 4096);
    try std.testing.expectEqualStrings(input.before.?.row, decoded.before.?.row);
    try std.testing.expectEqualStrings(input.after.?.row, decoded.after.?.row);
    try std.testing.expectEqualSlices(u8, &input.before.?.schema_digest, &decoded.before.?.schema_digest);
    encoded[24] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, Change.decode(encoded, 4096));
    encoded[24] ^= 1;
    try std.testing.expectError(error.TransactionTooLarge, input.encode(a, 64));
    var missing = input;
    missing.before = null;
    missing.after = null;
    try std.testing.expectError(error.InvalidLogicalOutbox, missing.encode(a, 4096));
}

test "logical outbox page wire budget owns binary row bytes rejects tampering and never emits a zero progress continuation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const roster: publisher.PublishedRelation = .{
        .relation = .{ .logical_relation_id = 40, .physical_table_id = 50, .database_id = 2, .namespace_id = 3, .relation_oid = 40, .namespace = "public", .name = "published", .schema_version = 1, .schema_digest = @splat(5), .replica_identity = .full, .columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 23 }} },
        .memberships = &.{.{ .publication = .{ .id = 1, .generation = 1, .digest = @splat(1) }, .parameters = .{} }},
    };
    const roster_digest = try capture.canonicalDigest(out, "antfly/logical-capture-roster/v1\x00", roster);
    const schema_bytes = "retained-actual-schema-codec-fixture";
    var schema_hash = std.crypto.hash.Blake3.init(.{});
    schema_hash.update("antfly/logical-outbox-schema/v1\x00");
    schema_hash.update(schema_bytes);
    var schema_digest: Digest = undefined;
    schema_hash.final(&schema_digest);
    var binary_row: [6144]u8 = undefined;
    for (&binary_row, 0..) |*byte, i| byte.* = @truncate(i);
    const page: Page = .{
        .header = .{
            .reference = .{ .owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 }, .transaction_id = @splat(4), .participant = 0, .group_id = 9, .namespace = .{ .table_id = 50, .shard_id = 1, .range_id = 2 }, .physical_root_incarnation = 10, .position = .{ .raft = .{ .term = 1, .index = 2 } }, .roster_digest = roster_digest, .guard_digest = @splat(6), .pregrant_body_digest = @splat(7), .changes_digest = @splat(8), .capacity_digest = @splat(9), .before_digest = @splat(10), .change_count = 2, .encoded_bytes = 20000 },
            .intent_revision = 1,
            .demand_digest = @splat(9),
            .rows_digest = @splat(10),
            .roster_bytes = 2000,
            .schema_count = 1,
            .schemas_digest = @splat(11),
        },
        .roster = roster,
        .changes = &.{.{ .ordinal = 1, .key = "\xff\x00-key", .before = null, .after = .{ .schema_digest = schema_digest, .row = &binary_row } }},
        .schemas = &.{.{ .digest = schema_digest, .encoded = schema_bytes }},
        .next_after = 1,
    };
    const encoded = try encodePage(page, a, 32768);
    defer a.free(encoded);
    var owned_allocations = std.testing.FailingAllocator.init(out, .{});
    const decoded = try decodePageLeaky(owned_allocations.allocator(), encoded, encoded.len);
    var borrowed_allocations = std.testing.FailingAllocator.init(out, .{});
    const borrowed = try decodePageBorrowedLeaky(borrowed_allocations.allocator(), encoded, encoded.len);
    try std.testing.expectEqualDeep(decoded, borrowed);
    // Same validation/metadata allocations, with precisely the duplicate row
    // record and schema payload removed. No timing threshold hides a copy.
    try std.testing.expectEqual(try page.changes[0].encodedLen(encoded.len) + schema_bytes.len, owned_allocations.allocated_bytes - borrowed_allocations.allocated_bytes);
    const start = @intFromPtr(encoded.ptr);
    const end = start + encoded.len;
    for ([_][]const u8{ borrowed.changes[0].key, borrowed.changes[0].after.?.row, borrowed.schemas[0].encoded }) |view| {
        try std.testing.expect(@intFromPtr(view.ptr) >= start and @intFromPtr(view.ptr) + view.len <= end);
    }
    try std.testing.expectEqualSlices(u8, &binary_row, decoded.changes[0].after.?.row);
    try std.testing.expectEqualStrings(page.changes[0].key, decoded.changes[0].key);
    const request: PageRequest = .{ .reference = page.header.reference, .max_bytes = 32768 };
    try request.require(decoded);
    try request.require(borrowed);
    var skipped = request;
    skipped.after = 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, skipped.require(decoded));
    var truncated = page;
    truncated.next_after = null;
    try std.testing.expectError(error.InvalidLogicalOutbox, encodePage(truncated, out, 32768));
    try std.testing.expectError(error.TransactionTooLarge, encodePage(page, out, encoded.len - 1));
    try std.testing.expectError(error.TransactionTooLarge, encodePage(page, out, 1));
    try std.testing.expectError(error.InvalidLogicalOutbox, decodePageLeaky(out, encoded, encoded.len - 1));
    encoded[encoded.len - 33] ^= 1;
    try std.testing.expectError(error.InvalidLogicalOutbox, decodePageLeaky(out, encoded, 32768));
    try std.testing.expectError(error.InvalidLogicalOutbox, decodePageBorrowedLeaky(out, encoded, 32768));
    encoded[encoded.len - 33] ^= 1;
    // The decoded page owns its bytes before CABI releases or reuses output.
    @memset(encoded, 0);
    try std.testing.expectEqualSlices(u8, &binary_row, decoded.changes[0].after.?.row);
    var stalled = page;
    stalled.changes = &.{};
    stalled.schemas = &.{};
    try std.testing.expectError(error.InvalidLogicalOutbox, encodePage(stalled, out, 32768));
}

test "logical outbox streaming digest equals exact canonical binary row without row copy" {
    const a = std.testing.allocator;
    const changed: Change = .{ .ordinal = 7, .key = &.{ 0xff, 0, 0x80 }, .before = .{ .schema_digest = @splat(3), .row = "before" }, .after = .{ .schema_digest = @splat(4), .row = "after" } };
    for ([_]Change{ changed, .{ .ordinal = 8, .key = "delete", .before = changed.before, .after = null }, .{ .ordinal = 9, .key = "insert", .before = null, .after = changed.after } }) |value| {
        const raw = try value.encode(a, 4096);
        defer a.free(raw);
        var expected: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw, &expected, .{});
        var hash = std.crypto.hash.Blake3.init(.{});
        try value.hashEncoded(&hash, raw.len);
        var actual: [32]u8 = undefined;
        hash.final(&actual);
        try std.testing.expectEqualSlices(u8, &expected, &actual);
        try std.testing.expectError(error.TransactionTooLarge, value.hashEncoded(&hash, raw.len - 1));
    }
}
