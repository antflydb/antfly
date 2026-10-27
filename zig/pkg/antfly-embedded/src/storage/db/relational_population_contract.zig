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

//! Ordered typed-row production into an unpublished relation generation.
//! A native owner must persist the returned cursor with the prepared row batch.
//! This contract proves delivery, not SELECT or catalog publication authority.
const std = @import("std");
pub const Digest = [32]u8;
pub const max_page_rows = 128;
pub const max_page_bytes = 8 * 1024 * 1024;
pub const max_null_fields = 4096;

/// Metadata derives this immutable binding from the authorized, captured query.
/// A capture identifies one retained evaluation; it is not permission to
/// reexecute the query after losing that evaluation or its source snapshot.
pub const Binding = struct {
    capture_id: [16]u8,
    definition: Digest,
    source_definition: Digest,
    target_schema: Digest,
    principal_instance: [16]u8,
    catalog_revision: u64,
    write_version: u64,
    partition: u64,

    pub fn validate(self: Binding) !void {
        if (self.catalog_revision == 0 or self.write_version == 0 or self.partition == 0 or zero(&self.capture_id) or zero(&self.definition) or zero(&self.source_definition) or zero(&self.target_schema) or zero(&self.principal_instance)) return error.InvalidRelationPopulation;
    }
    pub fn digest(self: Binding) !Digest {
        try self.validate();
        return self.fingerprint();
    }
    /// Envelope hashing is total; admission still requires validate().
    pub fn fingerprint(self: Binding) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:relation-population:binding:v1\x00");
        hash.update(&self.capture_id);
        hash.update(&self.definition);
        hash.update(&self.source_definition);
        hash.update(&self.target_schema);
        hash.update(&self.principal_instance);
        number(&hash, self.catalog_revision);
        number(&hash, self.write_version);
        number(&hash, self.partition);
        return finish(&hash);
    }
};

/// A metadata plan retains the common request identity. Each destination
/// receives a distinct partition binding derived from this immutable capture.
pub const Capture = struct {
    capture_id: [16]u8,
    definition: Digest,
    source_definition: Digest,
    target_schema: Digest,
    principal_instance: [16]u8,
    catalog_revision: u64,
    write_version: u64,

    pub fn forPartition(self: Capture, partition: u64) Binding {
        return .{ .capture_id = self.capture_id, .definition = self.definition, .source_definition = self.source_definition, .target_schema = self.target_schema, .principal_instance = self.principal_instance, .catalog_revision = self.catalog_revision, .write_version = self.write_version, .partition = partition };
    }
    pub fn validate(self: Capture) !void {
        try self.forPartition(1).validate();
    }
};

/// Same typed JSON/SQL-null boundary as ordinary native mutations. The owner
/// parses and validates each document once; hashing does not parse JSON again.
/// JSON-null field names are strictly byte-sorted at the producer boundary.
pub const Row = struct { ordinal: u64, document: []const u8, json_null_fields: []const []const u8 = &.{} };
pub const Page = struct {
    binding: Digest,
    sequence: u64,
    first_row: u64,
    previous: Digest,
    rows: []const Row,

    pub fn digest(self: Page) !Digest {
        if (zero(&self.binding) or zero(&self.previous) or self.rows.len == 0 or self.rows.len > max_page_rows) return error.InvalidRelationPopulation;
        _ = std.math.add(u64, self.first_row, self.rows.len) catch return error.InvalidRelationPopulation;
        _ = std.math.add(u64, self.sequence, 1) catch return error.InvalidRelationPopulation;
        var bytes: usize = 0;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:relation-population:page:v1\x00");
        hash.update(&self.binding);
        hash.update(&self.previous);
        number(&hash, self.sequence);
        number(&hash, self.first_row);
        number(&hash, self.rows.len);
        for (self.rows, 0..) |row, index| {
            if (row.ordinal == std.math.maxInt(u64) or (index != 0 and self.rows[index - 1].ordinal >= row.ordinal)) return error.InvalidRelationPopulation;
            number(&hash, row.ordinal);
            if (row.document.len == 0 or row.json_null_fields.len > max_null_fields) return error.InvalidRelationPopulation;
            try charge(&bytes, row.document.len);
            framed(&hash, row.document);
            number(&hash, row.json_null_fields.len);
            for (row.json_null_fields, 0..) |field, i| {
                if (field.len == 0 or !std.unicode.utf8ValidateSlice(field)) return error.InvalidRelationPopulation;
                if (i != 0 and std.mem.order(u8, row.json_null_fields[i - 1], field) != .lt) return error.InvalidRelationPopulation;
                try charge(&bytes, field.len);
                framed(&hash, field);
            }
        }
        return finish(&hash);
    }
};

pub const Header = struct {
    sequence: u64,
    first_row: u64,
    previous: Digest,
    ordinals: []const u64,
};
pub const Delivery = union(enum) { page: Header, complete: Completion };

pub const Completion = struct { binding: Digest, pages: u64, rows: u64, chain: Digest, source_rows: u64 };
pub const Admission = union(enum) { replay, apply: Cursor };
pub const Cursor = struct {
    binding: Digest,
    pages: u64 = 0,
    rows: u64 = 0,
    chain: Digest,
    last_page: ?Digest = null,
    last_source_ordinal: ?u64 = null,
    sealed: bool = false,
    source_rows: ?u64 = null,

    pub fn init(binding: Binding) !Cursor {
        const identity = try binding.digest();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly:relation-population:start:v1\x00");
        hash.update(&identity);
        return .{ .binding = identity, .chain = finish(&hash) };
    }
    pub fn validate(self: Cursor) !void {
        if (zero(&self.binding) or zero(&self.chain) or self.rows < self.pages or
            (self.pages == 0) != (self.last_page == null) or
            (self.rows == 0) != (self.last_source_ordinal == null) or
            (self.pages == 0 and self.rows != 0)) return error.InvalidRelationPopulation;
        if (self.sealed != (self.source_rows != null)) return error.InvalidRelationPopulation;
        if (self.source_rows) |total| if (total < self.rows or (self.last_source_ordinal != null and self.last_source_ordinal.? >= total)) return error.InvalidRelationPopulation;
        if (self.last_source_ordinal) |ordinal| if (ordinal == std.math.maxInt(u64) or ordinal < self.rows - 1) return error.InvalidRelationPopulation;
        if (self.last_page) |receipt| if (!std.mem.eql(u8, &receipt, &self.chain)) return error.InvalidRelationPopulation;
    }
    pub fn admit(self: Cursor, page: Page) !Admission {
        try self.validate();
        if (self.sealed or !std.mem.eql(u8, &self.binding, &page.binding)) return error.RelationPopulationChanged;
        const receipt = try page.digest();
        // One page may be in flight. Lost replies may replay only that exact
        // committed page, never rewrite its rows or skip a missing page.
        if (page.sequence < self.pages) {
            if (page.sequence + 1 == self.pages and self.last_page != null and std.mem.eql(u8, &receipt, &self.last_page.?)) return .replay;
            return error.RelationPopulationChanged;
        }
        if (page.sequence != self.pages or page.first_row != self.rows or !std.mem.eql(u8, &page.previous, &self.chain)) return error.RelationPopulationChanged;
        if (self.last_source_ordinal) |ordinal| if (page.rows[0].ordinal <= ordinal) return error.RelationPopulationChanged;
        var next = self;
        next.last_source_ordinal = page.rows[page.rows.len - 1].ordinal;
        next.pages += 1;
        next.rows += @intCast(page.rows.len);
        next.chain = receipt;
        next.last_page = receipt;
        return .{ .apply = next };
    }
    pub fn completion(self: Cursor) Completion {
        return .{ .binding = self.binding, .pages = self.pages, .rows = self.rows, .chain = self.chain, .source_rows = self.source_rows orelse if (self.last_source_ordinal) |ordinal| ordinal + 1 else 0 };
    }
    pub fn completionForSource(self: Cursor, source_rows: u64) Completion {
        var receipt = self.completion();
        receipt.source_rows = source_rows;
        return receipt;
    }
    pub fn seal(self: Cursor, receipt: Completion) !Cursor {
        try self.validate();
        if (!std.meta.eql(self.completionForSource(receipt.source_rows), receipt) or receipt.source_rows < self.rows or
            (self.last_source_ordinal != null and self.last_source_ordinal.? >= receipt.source_rows) or
            (self.source_rows != null and self.source_rows.? != receipt.source_rows)) return error.RelationPopulationChanged;
        var next = self;
        next.sealed = true;
        next.source_rows = receipt.source_rows;
        return next;
    }
};

/// A source ordinal has one global identity across all destination owners.
/// Hashing the capture/ordinal prefix distributes rows across ordinary key
/// ranges. Keeping the ordinal suffix makes identities collision-free within
/// a capture, independent of owner-local page boundaries and counters.
pub fn rowKey(capture: [16]u8, ordinal: u64) [49]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:relation-population:row-key:v1\x00");
    hash.update(&capture);
    number(&hash, ordinal);
    const digest = finish(&hash);
    var result: [49]u8 = undefined;
    @memcpy(result[0..32], &std.fmt.bytesToHex(digest[0..16].*, .lower));
    result[32] = ':';
    const alphabet = "0123456789abcdef";
    for (0..16) |i| result[33 + i] = alphabet[@as(u4, @truncate(ordinal >> @as(u6, @intCast(4 * (15 - i)))))];
    return result;
}

fn zero(bytes: []const u8) bool {
    return std.mem.allEqual(u8, bytes, 0);
}
fn number(hash: *std.crypto.hash.Blake3, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn framed(hash: *std.crypto.hash.Blake3, bytes: []const u8) void {
    number(hash, bytes.len);
    hash.update(bytes);
}
fn finish(hash: *std.crypto.hash.Blake3) Digest {
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}
fn charge(bytes: *usize, count: usize) !void {
    bytes.* = std.math.add(usize, bytes.*, count) catch return error.RelationPopulationPageTooLarge;
    if (bytes.* > max_page_bytes) return error.RelationPopulationPageTooLarge;
}

fn fixtureBinding() Binding {
    return .{ .capture_id = @splat(1), .definition = @splat(2), .source_definition = @splat(3), .target_schema = @splat(4), .principal_instance = @splat(5), .catalog_revision = 6, .write_version = 7, .partition = 8 };
}
test "SQL relation population pages fence capture order replay and completion without allocations" {
    var state = try Cursor.init(fixtureBinding());
    const rows: [2]Row = .{ .{ .ordinal = 0, .document = "{\"id\":1}" }, .{ .ordinal = 1, .document = "{\"id\":2}" } };
    var page: Page = .{ .binding = state.binding, .sequence = 0, .first_row = 0, .previous = state.chain, .rows = &rows };
    const first = (try state.admit(page)).apply;
    try std.testing.expectEqual(@as(u64, 2), first.rows);
    try std.testing.expectEqual(Admission.replay, try first.admit(page));
    var corrupt = page;
    corrupt.rows = &.{.{ .ordinal = 0, .document = "{\"id\":99}" }};
    try std.testing.expectError(error.RelationPopulationChanged, first.admit(corrupt));
    page.sequence = 2;
    try std.testing.expectError(error.RelationPopulationChanged, first.admit(page));
    page.sequence = 1;
    page.first_row = 2;
    page.previous = first.chain;
    page.rows = &.{ .{ .ordinal = 7, .document = "{\"id\":1}" }, .{ .ordinal = 12, .document = "{\"id\":2}" } };
    state = (try first.admit(page)).apply;
    try std.testing.expectEqual(@as(u64, 4), state.rows);
    var receipt = state.completion();
    receipt.rows += 1;
    try std.testing.expectError(error.RelationPopulationChanged, state.seal(receipt));
    state = try state.seal(state.completion());
    try std.testing.expect((try state.seal(state.completion())).sealed);
    try std.testing.expectError(error.RelationPopulationChanged, state.admit(page));
    try std.testing.expectEqualSlices(u8, &rowKey(fixtureBinding().capture_id, 7), &rowKey(fixtureBinding().capture_id, 7));
    try std.testing.expect(!std.mem.eql(u8, &rowKey(fixtureBinding().capture_id, 7), &rowKey(fixtureBinding().capture_id, 8)));
}
test "SQL relation population empty completion and typed NULL provenance remain distinct" {
    const state = try Cursor.init(fixtureBinding());
    try std.testing.expect((try state.seal(state.completion())).sealed);
    var page: Page = .{ .binding = state.binding, .sequence = 0, .first_row = 0, .previous = state.chain, .rows = &.{.{ .ordinal = 0, .document = "{\"j\":null}" }} };
    const sql_null = try page.digest();
    page.rows = &.{.{ .ordinal = 0, .document = "{\"j\":null}", .json_null_fields = &.{"j"} }};
    try std.testing.expect(!std.mem.eql(u8, &sql_null, &try page.digest()));
    page.rows = &.{.{ .ordinal = 0, .document = "{}", .json_null_fields = &.{ "j", "j" } }};
    try std.testing.expectError(error.InvalidRelationPopulation, page.digest());
    page.rows = &.{};
    try std.testing.expectError(error.InvalidRelationPopulation, page.digest());
    var changed = fixtureBinding();
    changed.principal_instance[0] ^= 1;
    const other = try Cursor.init(changed);
    try std.testing.expect(!std.mem.eql(u8, &state.binding, &other.binding));
}

test "SQL relation population global ordinals fence sparse owner pages and common completion totals" {
    const binding = fixtureBinding();
    const state = try Cursor.init(binding);
    var page: Page = .{ .binding = state.binding, .sequence = 0, .first_row = 0, .previous = state.chain, .rows = &.{ .{ .ordinal = 4, .document = "{}" }, .{ .ordinal = 12, .document = "{}" } } };
    const accepted = (try state.admit(page)).apply;
    try std.testing.expectEqual(@as(u64, 2), accepted.rows);
    try std.testing.expectEqual(@as(?u64, 12), accepted.last_source_ordinal);
    try std.testing.expectError(error.RelationPopulationChanged, accepted.seal(accepted.completionForSource(12)));
    const sealed = try accepted.seal(accepted.completionForSource(20));
    try sealed.validate();
    try std.testing.expectError(error.RelationPopulationChanged, sealed.seal(sealed.completionForSource(21)));
    const empty = try state.seal(state.completionForSource(20));
    try std.testing.expect(empty.sealed and empty.rows == 0 and empty.source_rows.? == 20);
    page.rows = &.{ .{ .ordinal = 4, .document = "{}" }, .{ .ordinal = 4, .document = "{}" } };
    try std.testing.expectError(error.InvalidRelationPopulation, page.digest());
    page.sequence = accepted.pages;
    page.first_row = accepted.rows;
    page.previous = accepted.chain;
    page.rows = &.{.{ .ordinal = 11, .document = "{}" }};
    try std.testing.expectError(error.RelationPopulationChanged, accepted.admit(page));
    var other = binding;
    other.partition += 1;
    try std.testing.expectEqualSlices(u8, &rowKey(binding.capture_id, 4), &rowKey(other.capture_id, 4));
    other.capture_id[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &rowKey(binding.capture_id, 4), &rowKey(other.capture_id, 4)));
}
