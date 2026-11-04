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

//! Owned structural navigation in an immutable segment's active catalog marker.
//! Publication must atomically bind this descriptor to that segment ID. It is
//! not a source authority: the reader still leases and authenticates payloads.
const std = @import("std");
const segment = @import("../segment.zig");
const A = std.mem.Allocator;
const Admission = segment.SegmentReader.Admission;
const magic = "AFTSADM1";
const Footer = segment.SegmentReader.NativeCatalogFooter;
const header_bytes = magic.len + 6 * 8 + 4 + 21 + @sizeOf(Footer) + 2;
pub const maximum_bytes = 16 * 1024 * 1024;

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    metadata: Admission,
    footer: Footer,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Very large legacy directories retain the ordinary source-admission path.
pub fn encode(a: A, id: u64, metadata: Admission, footer: Footer) !?[]u8 {
    try metadata.validate();
    var length: usize = header_bytes + 4;
    for (metadata.fields) |field| {
        length = std.math.add(usize, length, 5 + field.name.len + @as(usize, if (field.stats != null) 12 else 0)) catch return null;
        length = std.math.add(usize, length, std.math.mul(usize, field.sections.len, 21) catch return null) catch return null;
        if (length > maximum_bytes) return null;
    }
    const bytes = try a.alloc(u8, length);
    var writer = Writer{ .bytes = bytes };
    writer.copy(magic);
    writer.integer(u64, id);
    writer.integer(u64, metadata.bytes);
    writer.integer(u64, metadata.stored_offset);
    writer.integer(u64, metadata.stored_length);
    writer.integer(u64, metadata.stored_metadata_length);
    writer.integer(u64, metadata.index_offset);
    writer.integer(u32, metadata.doc_count);
    writer.copy(&metadata.stored_header);
    writer.copy(&footer);
    writer.integer(u16, @intCast(metadata.fields.len));
    for (metadata.fields) |field| {
        writer.integer(u16, @intCast(field.name.len));
        writer.copy(field.name);
        writer.integer(u8, @intFromBool(field.stats != null));
        if (field.stats) |stats| {
            writer.integer(u32, stats.doc_count);
            writer.integer(u64, stats.total_field_len);
        }
        writer.integer(u16, @intCast(field.sections.len));
        for (field.sections) |section| {
            writer.integer(u8, @intCast(@backingInt(section.kind)));
            writer.integer(u64, section.offset);
            writer.integer(u64, section.length);
            writer.integer(u32, section.checksum);
        }
    }
    std.debug.assert(writer.position == length - 4);
    writer.integer(u32, std.hash.Crc32.hash(bytes[0 .. length - 4]));
    return bytes;
}

/// A one-byte active marker is the legacy representation. Malformed rich
/// descriptors fail closed; they must never silently remove an active segment.
pub fn decode(a: A, bytes: []const u8, id: u64, source_bytes: u64) !?Owned {
    if (std.mem.eql(u8, bytes, &.{1})) return null;
    if (bytes.len < header_bytes + 4 or bytes.len > maximum_bytes or
        !std.mem.eql(u8, bytes[0..magic.len], magic) or
        std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little) != std.hash.Crc32.hash(bytes[0 .. bytes.len - 4])) return error.InvalidSegmentAdmission;
    var input = Reader{ .bytes = bytes[magic.len .. bytes.len - 4] };
    if (try input.integer(u64) != id or try input.integer(u64) != source_bytes) return error.InvalidSegmentAdmission;
    var metadata: Admission = .{
        .bytes = source_bytes,
        .stored_offset = try input.integer(u64),
        .stored_length = try input.integer(u64),
        .stored_metadata_length = try input.integer(u64),
        .index_offset = try input.integer(u64),
        .doc_count = try input.integer(u32),
        .stored_header = (try input.take(21))[0..21].*,
        .fields = &.{},
    };
    const footer = (try input.take(@sizeOf(Footer)))[0..@sizeOf(Footer)].*;
    const count = try input.integer(u16);
    if (count > input.bytes.len / 5) return error.InvalidSegmentAdmission;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const fields = try scratch.alloc(Admission.Field, count);
    for (fields) |*field| {
        const name_length = try input.integer(u16);
        const name = try scratch.dupe(u8, try input.take(name_length));
        const stats: @FieldType(Admission.Field, "stats") = switch (try input.integer(u8)) {
            0 => null,
            1 => .{ .doc_count = try input.integer(u32), .total_field_len = try input.integer(u64) },
            else => return error.InvalidSegmentAdmission,
        };
        const section_count = try input.integer(u16);
        if (section_count > input.bytes.len / 21) return error.InvalidSegmentAdmission;
        const sections = try scratch.alloc(Admission.Section, section_count);
        for (sections) |*section| section.* = .{
            .kind = std.enums.fromInt(segment.SectionType, try input.integer(u8)) orelse return error.InvalidSegmentAdmission,
            .offset = try input.integer(u64),
            .length = try input.integer(u64),
            .checksum = try input.integer(u32),
        };
        field.* = .{ .name = name, .stats = stats, .sections = sections };
    }
    if (input.bytes.len != 0) return error.InvalidSegmentAdmission;
    metadata.fields = fields;
    metadata.validate() catch return error.InvalidSegmentAdmission;
    return .{ .arena = arena, .metadata = metadata, .footer = footer };
}

const Writer = struct {
    bytes: []u8,
    position: usize = 0,
    fn copy(self: *Writer, bytes: []const u8) void {
        @memcpy(self.bytes[self.position..][0..bytes.len], bytes);
        self.position += bytes.len;
    }
    fn integer(self: *Writer, comptime T: type, value: T) void {
        std.mem.writeInt(T, self.bytes[self.position..][0..@sizeOf(T)], value, .little);
        self.position += @sizeOf(T);
    }
};

const Reader = struct {
    bytes: []const u8,
    fn take(self: *Reader, length: usize) ![]const u8 {
        if (length > self.bytes.len) return error.InvalidSegmentAdmission;
        const value = self.bytes[0..length];
        self.bytes = self.bytes[length..];
        return value;
    }
    fn integer(self: *Reader, comptime T: type) !T {
        const value = try self.take(@sizeOf(T));
        return std.mem.readInt(T, value[0..@sizeOf(T)], .little);
    }
};

const Fixture = struct {
    bytes: []u8,
    descriptor: []u8,
    fn create(a: A) !Fixture {
        return createWithFields(a, 1);
    }
    fn createWithFields(a: A, count: usize) !Fixture {
        var writer = segment.SegmentWriter.init(a);
        defer writer.deinit();
        try writer.addStoredDoc("one", "{\"body\":\"alpha\"}");
        try writer.addStoredDoc("two", "{\"body\":\"alpha beta\"}");
        var inverted = @import("../section/inverted.zig").InvertedIndexBuilder.init(a, .{});
        defer inverted.deinit();
        try inverted.addDocument(0, &.{.{ .term = "alpha", .freq = 1, .positions = &.{0} }});
        try inverted.addDocument(1, &.{.{ .term = "beta", .freq = 1, .positions = &.{0} }});
        const section = try inverted.build();
        defer a.free(section);
        const names = [_][]const u8{
            "body",     "column01", "column02", "column03", "column04", "column05", "column06", "column07",
            "column08", "column09", "column10", "column11", "column12", "column13", "column14", "column15",
            "column16", "column17", "column18", "column19", "column20", "column21", "column22", "column23",
            "column24", "column25", "column26", "column27", "column28", "column29", "column30", "column31",
        };
        for (names[0..count]) |name| try writer.addSection(try writer.addField(name), .inverted_text, section);
        const bytes = try writer.build();
        errdefer a.free(bytes);
        var reader = try segment.SegmentReader.init(a, bytes);
        defer reader.deinit();
        const metadata = try reader.admission(a);
        defer metadata.deinit(a);
        return .{ .bytes = bytes, .descriptor = (try encode(a, 17, metadata, try segment.SegmentReader.nativeCatalogFooter(.{ .contiguous = bytes }))) orelse return error.MissingAdmission };
    }
    fn deinit(self: Fixture, a: A) void {
        a.free(self.bytes);
        a.free(self.descriptor);
    }
};

test "segment admission roundtrip owns metadata and performs no source reads before payload access" {
    const a = std.testing.allocator;
    const fixture = try Fixture.create(a);
    defer fixture.deinit(a);
    const Source = struct {
        bytes: []const u8,
        reads: usize = 0,
        denied: bool = true,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            if (self.denied) return error.TestReadDenied;
            if (offset > self.bytes.len or output.len > self.bytes.len - offset) return error.EndOfStream;
            @memcpy(output, self.bytes[@intCast(offset)..][0..output.len]);
        }
        fn close(_: *anyopaque) void {}
    };
    var source = Source{ .bytes = fixture.bytes };
    var owned = try decode(a, fixture.descriptor, 17, fixture.bytes.len);
    defer if (owned) |*value| value.deinit();
    var reader = try segment.SegmentReader.initAuthenticated(a, .{ .ranges = .{
        .ptr = &source,
        .length = fixture.bytes.len,
        .read_into = Source.read,
        .close = Source.close,
    } }, owned.?.metadata);
    defer reader.deinit();
    owned.?.deinit();
    owned = null;
    try std.testing.expectEqual(@as(usize, 0), source.reads);
    try std.testing.expectEqual(@as(u32, 2), reader.doc_count);
    try std.testing.expect((try reader.invertedFieldStats("body")) != null);
    try std.testing.expectEqual(@as(usize, 0), source.reads);
    try std.testing.expectError(error.TestReadDenied, reader.storedDoc(0));
    source.denied = false;
    try std.testing.expectEqualStrings("one", (try reader.storedDoc(0)).?.id);
}

test "segment admission rejects corrupt truncated mismatched and structurally invalid descriptors" {
    const a = std.testing.allocator;
    const fixture = try Fixture.create(a);
    defer fixture.deinit(a);
    try std.testing.expect((try decode(a, &.{1}, 17, fixture.bytes.len)) == null);
    for (0..fixture.descriptor.len) |end| try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, fixture.descriptor[0..end], 17, fixture.bytes.len));
    try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, fixture.descriptor, 18, fixture.bytes.len));
    try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, fixture.descriptor, 17, fixture.bytes.len + 1));
    const corrupt = try a.dupe(u8, fixture.descriptor);
    defer a.free(corrupt);
    corrupt[header_bytes] ^= 1;
    try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, corrupt, 17, fixture.bytes.len));
    @memcpy(corrupt, fixture.descriptor);
    // A plausible checksum cannot authenticate impossible physical bounds.
    std.mem.writeInt(u64, corrupt[24..32], fixture.bytes.len, .little);
    std.mem.writeInt(u32, corrupt[corrupt.len - 4 ..][0..4], std.hash.Crc32.hash(corrupt[0 .. corrupt.len - 4]), .little);
    try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, corrupt, 17, fixture.bytes.len));
    @memcpy(corrupt, fixture.descriptor);
    std.mem.writeInt(u16, corrupt[header_bytes - 2 ..][0..2], std.math.maxInt(u16), .little);
    std.mem.writeInt(u32, corrupt[corrupt.len - 4 ..][0..4], std.hash.Crc32.hash(corrupt[0 .. corrupt.len - 4]), .little);
    try std.testing.expectError(error.InvalidSegmentAdmission, decode(a, corrupt, 17, fixture.bytes.len));
}

test "segment admission allocation failures unwind decoded and reconstituted ownership" {
    const a = std.testing.allocator;
    const fixture = try Fixture.create(a);
    defer fixture.deinit(a);
    const Run = struct {
        fn run(alloc: A, input: Fixture) !void {
            var owned = (try decode(alloc, input.descriptor, 17, input.bytes.len)).?;
            defer owned.deinit();
            var reader = try segment.SegmentReader.initNativeCatalogAdmission(alloc, .{ .contiguous = input.bytes }, owned.metadata, owned.footer);
            defer reader.deinit();
            const captured = try reader.admission(alloc);
            defer captured.deinit(alloc);
            const encoded = (try encode(alloc, 17, captured, owned.footer)).?;
            defer alloc.free(encoded);
            try std.testing.expectEqualSlices(u8, input.descriptor, encoded);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{fixture});
}

test "segment admission avoids per field reads while fencing replacement and preserving payload checksums" {
    const a = std.testing.allocator;
    const fixture = try Fixture.createWithFields(a, 32);
    defer fixture.deinit(a);
    var owned = (try decode(a, fixture.descriptor, 17, fixture.bytes.len)).?;
    defer owned.deinit();
    const Source = struct {
        bytes: []const u8,
        reads: usize = 0,
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            if (offset > self.bytes.len or output.len > self.bytes.len - offset) return error.EndOfStream;
            @memcpy(output, self.bytes[@intCast(offset)..][0..output.len]);
        }
        fn close(_: *anyopaque) void {}
        fn source(self: *@This()) @import("../segment_source.zig").Source {
            return .{ .ranges = .{ .ptr = self, .length = self.bytes.len, .read_into = read, .close = close } };
        }
    };
    var source = Source{ .bytes = fixture.bytes };
    var ordinary = try segment.SegmentReader.initSource(a, source.source());
    defer ordinary.deinit();
    const ordinary_reads = source.reads;
    source.reads = 0;
    var admitted = try segment.SegmentReader.initNativeCatalogAdmission(a, source.source(), owned.metadata, owned.footer);
    defer admitted.deinit();
    const admitted_reads = source.reads;
    std.debug.print("SEGMENT_ADMISSION fields=32 ordinary_reads={d} admitted_reads={d}\n", .{ ordinary_reads, admitted_reads });
    try std.testing.expect(admitted_reads < ordinary_reads);
    try std.testing.expectEqualDeep(try ordinary.invertedFieldStats("column31"), try admitted.invertedFieldStats("column31"));
    var contiguous = try segment.SegmentReader.init(a, fixture.bytes);
    defer contiguous.deinit();
    const reads_before_stats = source.reads;
    for (admitted.fields, 0..) |field, ordinal| {
        const expected = try contiguous.invertedFieldStats(field.name);
        try std.testing.expectEqualDeep(expected, contiguous.invertedFieldStatsAt(ordinal));
        try std.testing.expectEqualDeep(expected, admitted.invertedFieldStatsAt(ordinal));
    }
    try std.testing.expectEqual(reads_before_stats, source.reads);
    try std.testing.expect(contiguous.invertedFieldStatsAt(contiguous.fields.len) == null);
    try std.testing.expect(admitted.invertedFieldStatsAt(std.math.maxInt(usize)) == null);
    var wrong_footer = owned.footer;
    wrong_footer[40] ^= 1;
    try std.testing.expectError(error.InvalidSegmentAdmission, segment.SegmentReader.initNativeCatalogAdmission(a, source.source(), owned.metadata, wrong_footer));
    const corrupt = try a.dupe(u8, fixture.bytes);
    defer a.free(corrupt);
    const section = owned.metadata.fields[owned.metadata.fields.len - 1].sections[0];
    corrupt[@intCast(section.offset)] ^= 1;
    // A changed payload with an unchanged footer must still fail AFSM page or
    // section validation, even when an enclosing provider accepts those bytes.
    var bad = segment.SegmentReader.initNativeCatalogAdmission(a, .{ .contiguous = corrupt }, owned.metadata, owned.footer) catch |err| {
        try std.testing.expectEqual(error.CrcMismatch, err);
        return;
    };
    defer bad.deinit();
    try std.testing.expectError(error.CrcMismatch, bad.invertedIndexScoped(a, "column31"));
}
