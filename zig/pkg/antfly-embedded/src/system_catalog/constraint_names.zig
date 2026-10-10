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

//! PostgreSQL-compatible generated PRIMARY/UNIQUE index names.
//! The admission owner supplies a captured namespace occupancy view. This
//! module does not scan a catalog, publish a name, or rename explicit names.
const std = @import("std");

pub const max_name_bytes = 63;
// Match the native relation-name registry's source-identifier bound. SQL
// generated names have the smaller PostgreSQL bound above. Reject oversized
// inputs before UTF-8 validation rather than rescanning arbitrary input for
// every constraint in a large DDL statement.
pub const max_source_name_bytes = 256;

/// Borrows the table name; owns a bounded prefix of the joined key names.
/// Initialize once per constraint, not once per collision probe.
pub const Candidate = struct {
    table: []const u8,
    primary: bool,
    addition: [max_name_bytes * 2 + 1]u8 = undefined,
    addition_len: usize = 0,

    pub fn init(table: []const u8, columns: []const []const u8, primary: bool) !Candidate {
        try validate(table);
        var self: Candidate = .{ .table = table, .primary = primary };
        if (primary) return self;
        if (columns.len == 0) return error.InvalidCatalogName;
        // Only a bounded prefix can contribute to a 63-byte name. Joining
        // every key would allocate proportional to the complete schema.
        for (columns) |column| {
            try validate(column);
            if (self.addition_len != 0) {
                self.addition[self.addition_len] = '_';
                self.addition_len += 1;
            }
            const part = clip(column, max_name_bytes);
            @memcpy(self.addition[self.addition_len..][0..part.len], part);
            self.addition_len += part.len;
            if (self.addition_len >= max_name_bytes + 1) break;
        }
        return self;
    }

    /// Zero selects the unsuffixed name; later attempts preserve the complete
    /// numeric label and shorten table/key prefixes at UTF-8 boundaries.
    pub fn format(self: *const Candidate, attempt: u32, output: *[max_name_bytes]u8) ![]const u8 {
        var label_buffer: [14]u8 = undefined;
        const stem = if (self.primary) "pkey" else "key";
        const label = if (attempt == 0) stem else try std.fmt.bufPrint(&label_buffer, "{s}{d}", .{ stem, attempt });
        const right = self.addition[0..self.addition_len];
        const overhead = label.len + 1 + @as(usize, if (self.primary) 0 else 1);
        const available = max_name_bytes - overhead;
        var left_len = self.table.len;
        var right_len = right.len;
        if (left_len > available or right_len > available - left_len) {
            // Equivalent to repeatedly shortening the longer prefix, with
            // ties shortening the right prefix, but constant-time in length.
            const left_half = (available + 1) / 2;
            if (left_len < left_half) {
                right_len = available - left_len;
            } else if (right_len < available / 2) {
                left_len = available - right_len;
            } else {
                left_len = left_half;
                right_len = available - left_half;
            }
        }
        const left_part = clip(self.table, left_len);
        const right_part = clip(right, right_len);
        var length: usize = 0;
        @memcpy(output[length..][0..left_part.len], left_part);
        length += left_part.len;
        if (!self.primary) {
            output[length] = '_';
            length += 1;
            @memcpy(output[length..][0..right_part.len], right_part);
            length += right_part.len;
        }
        output[length] = '_';
        length += 1;
        @memcpy(output[length..][0..label.len], label);
        return output[0 .. length + label.len];
    }
};

fn validate(name: []const u8) !void {
    if (name.len == 0 or name.len > max_source_name_bytes or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name))
        return error.InvalidCatalogName;
}

fn clip(bytes: []const u8, limit: usize) []const u8 {
    var end = @min(bytes.len, limit);
    while (end != 0 and end < bytes.len and bytes[end] & 0xc0 == 0x80) end -= 1;
    return bytes[0..end];
}

/// The occupancy callback must include relation names, constraint names
/// (including non-index CHECK/FK constraints), and this statement's reserved
/// names. It must check cancellation/deadlines during admission.
/// Allocate only the chosen name. Exhaustion is definite nonadmission, not a
/// reason to overwrite a conflicting relation or replay an uncertain write.
pub fn choose(alloc: std.mem.Allocator, candidate: *const Candidate, namespace: anytype, max_attempts: u32) ![]const u8 {
    var output: [max_name_bytes]u8 = undefined;
    for (0..max_attempts) |attempt| {
        const name = try candidate.format(@intCast(attempt), &output);
        if (!try namespace.occupied(name)) return alloc.dupe(u8, name);
    }
    return error.ProgramLimitExceeded;
}

fn repeated(comptime bytes: []const u8, comptime count: usize) [bytes.len * count]u8 {
    var result: [bytes.len * count]u8 = undefined;
    for (0..count) |i| @memcpy(result[i * bytes.len ..][0..bytes.len], bytes);
    return result;
}

test "generated unique constraint names preserve PostgreSQL prefixes suffixes and UTF8 clipping" {
    var output: [max_name_bytes]u8 = undefined;
    const unique = try Candidate.init("items", &.{ "tenant", "email" }, false);
    try std.testing.expectEqualStrings("items_tenant_email_key", try unique.format(0, &output));
    try std.testing.expectEqualStrings("items_tenant_email_key3", try unique.format(3, &output));
    const primary = try Candidate.init("items", &.{}, true);
    try std.testing.expectEqualStrings("items_pkey", try primary.format(0, &output));
    try std.testing.expectEqualStrings("items_pkey4294967295", try primary.format(std.math.maxInt(u32), &output));
    const left = repeated("a", 63);
    const right = repeated("b", 63);
    const long = try Candidate.init(&left, &.{&right}, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key", .{ repeated("a", 29), repeated("b", 29) }), try long.format(0, &output));
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key1", .{ repeated("a", 29), repeated("b", 28) }), try long.format(1, &output));
    const unicode_left = repeated("é", 31);
    const unicode_right = repeated("中", 21);
    const unicode = try Candidate.init(&unicode_left, &.{&unicode_right}, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_{s}_key", .{ repeated("é", 14), repeated("中", 9) }), try unicode.format(0, &output));
    try std.testing.expect(std.unicode.utf8ValidateSlice(try unicode.format(100, &output)));
    const long_primary = try Candidate.init(&left, &.{}, true);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("{s}_pkey", .{repeated("a", 58)}), try long_primary.format(0, &output));
    const first_column = repeated("a", 40);
    const second_column = repeated("b", 40);
    const composite = try Candidate.init("items", &.{ &first_column, &second_column }, false);
    try std.testing.expectEqualStrings(comptime std.fmt.comptimePrint("items_{s}_{s}_key", .{ repeated("a", 40), repeated("b", 12) }), try composite.format(0, &output));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("", &.{"id"}, false));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{}, false));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{"\xff"}, false));
    const oversized = repeated("x", max_source_name_bytes + 1);
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init(&oversized, &.{"id"}, true));
    try std.testing.expectError(error.InvalidCatalogName, Candidate.init("items", &.{&oversized}, false));
}

const Namespace = struct {
    calls: usize = 0,
    fail: bool = false,
    pub fn occupied(self: *Namespace, name: []const u8) !bool {
        self.calls += 1;
        if (self.fail) return error.DeadlineExceeded;
        return std.mem.eql(u8, name, "items_email_key") or std.mem.eql(u8, name, "items_email_key1");
    }
};

fn allocationCase(alloc: std.mem.Allocator) !void {
    const candidate = try Candidate.init("items", &.{"email"}, false);
    var namespace: Namespace = .{};
    const name = try choose(alloc, &candidate, &namespace, 3);
    defer alloc.free(name);
    try std.testing.expectEqualStrings("items_email_key2", name);
    try std.testing.expectEqual(@as(usize, 3), namespace.calls);
}

test "generated unique constraint name selection is bounded cancellable and allocation safe" {
    try allocationCase(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
    const candidate = try Candidate.init("items", &.{"email"}, false);
    var namespace: Namespace = .{};
    try std.testing.expectError(error.ProgramLimitExceeded, choose(std.testing.allocator, &candidate, &namespace, 2));
    try std.testing.expectEqual(@as(usize, 2), namespace.calls);
    namespace.fail = true;
    try std.testing.expectError(error.DeadlineExceeded, choose(std.testing.allocator, &candidate, &namespace, 3));
}
