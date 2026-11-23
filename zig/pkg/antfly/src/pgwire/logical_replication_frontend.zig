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

//! Authenticated connection-owned replication port. Transport owns framing and
//! serializes all callbacks; native metadata owns slots, leases and ACKs.
const std = @import("std");
const backend = @import("backend.zig");
const wire = @import("antfly_local_sources").system_catalog_logical_replication_publisher;
pub const commands = @import("logical_replication_commands.zig");
pub const Reply = union(enum) {
    identity: struct { identity: wire.PublisherIdentity, lsn: u64, database: []const u8 },
    created: wire.CreatedSlot,
    dropped,
    started,
};
pub const Port = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        /// Local release only; called before command parsing/authorization so
        /// failed commands cannot prolong a previous snapshot's public name.
        command_boundary: ?*const fn (*anyopaque) void = null,
        command: *const fn (*anyopaque, std.mem.Allocator, backend.Request, commands.Command) anyerror!Reply,
        poll: *const fn (*anyopaque, std.mem.Allocator, backend.Request, *std.Io.Writer) anyerror!bool,
        feedback: *const fn (*anyopaque, std.mem.Allocator, backend.Request, []const u8, *std.Io.Writer) anyerror!void,
        stop: *const fn (*anyopaque, std.mem.Allocator, backend.Request) anyerror!void,
        heartbeat: *const fn (*anyopaque, std.mem.Allocator, backend.Request) anyerror!void,
        /// Joins all native work before freeing the owner. False means exact
        /// native lease/outcome recovery, rather than teardown, owns completion.
        close: *const fn (*anyopaque) bool,
    };
};
fn message(writer: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    const length = std.math.cast(u32, payload.len + 4) orelse return error.ProgramLimitExceeded;
    try writer.writeByte(tag);
    try writer.writeInt(u32, length, .big);
    try writer.writeAll(payload);
}
const Column = struct { name: []const u8, oid: u32 = 25, size: i16 = -1 };
fn row(_: std.mem.Allocator, writer: *std.Io.Writer, columns: []const Column, values: []const ?[]const u8) !void {
    if (columns.len != values.len) return error.InvalidResult;
    // Four bounded fields include the fixed-length opaque snapshot token.
    // Frame metadata without any allocation after slot admission.
    var description_buffer: [512]u8 = undefined;
    var data_buffer: [1024]u8 = undefined;
    var bytes: std.Io.Writer = .fixed(&description_buffer);
    var data: std.Io.Writer = .fixed(&data_buffer);
    try bytes.writeInt(u16, @intCast(columns.len), .big);
    for (columns) |column| {
        try bytes.writeAll(column.name);
        try bytes.writeByte(0);
        try bytes.writeInt(u32, 0, .big);
        try bytes.writeInt(u16, 0, .big);
        try bytes.writeInt(u32, column.oid, .big);
        try bytes.writeInt(i16, column.size, .big);
        try bytes.writeInt(i32, -1, .big);
        try bytes.writeInt(u16, 0, .big);
    }
    try data.writeInt(u16, @intCast(values.len), .big);
    for (values) |value| {
        if (value) |text| {
            try data.writeInt(i32, std.math.cast(i32, text.len) orelse return error.ProgramLimitExceeded, .big);
            try data.writeAll(text);
        } else try data.writeInt(i32, -1, .big);
    }
    try message(writer, 'T', bytes.buffered());
    try message(writer, 'D', data.buffered());
}
pub fn reply(a: std.mem.Allocator, writer: *std.Io.Writer, value: Reply) !void {
    var lsn_buffer: [17]u8 = undefined;
    switch (value) {
        .identity => |result| {
            var system_buffer: [20]u8 = undefined;
            var timeline_buffer: [10]u8 = undefined;
            const system = try std.fmt.bufPrint(&system_buffer, "{d}", .{result.identity.system_identifier});
            const timeline = try std.fmt.bufPrint(&timeline_buffer, "{d}", .{result.identity.timeline});
            const lsn = try commands.formatLsn(&lsn_buffer, result.lsn);
            try row(a, writer, &.{ .{ .name = "systemid" }, .{ .name = "timeline", .oid = 20, .size = 8 }, .{ .name = "xlogpos" }, .{ .name = "dbname" } }, &.{ system, timeline, lsn, result.database });
            try message(writer, 'C', "IDENTIFY_SYSTEM\x00");
        },
        .created => |result| {
            const lsn = try commands.formatLsn(&lsn_buffer, result.consistent_point);
            try row(a, writer, &.{ .{ .name = "slot_name" }, .{ .name = "consistent_point" }, .{ .name = "snapshot_name" }, .{ .name = "output_plugin" } }, &.{ result.slot.reference.name, lsn, result.snapshot_name, "pgoutput" });
            try message(writer, 'C', "CREATE_REPLICATION_SLOT\x00");
        },
        .dropped => try message(writer, 'C', "DROP_REPLICATION_SLOT\x00"),
        .started => try message(writer, 'W', &.{ 0, 0, 0 }),
    }
}

test "pgwire logical walsender control framing needs no heap after native slot admission" {
    var failing: std.testing.FailingAllocator = .init(std.testing.allocator, .{ .fail_index = 0 });
    var buffer: [2048]u8 = undefined;
    var output: std.Io.Writer = .fixed(&buffer);
    try reply(failing.allocator(), &output, .{ .identity = .{ .identity = .{ .realm = .{ .group_id = 1, .database_id = 1, .incarnation = @splat('1') }, .system_identifier = std.math.maxInt(u64), .timeline = 1, .database_oid = 1 }, .lsn = 1, .database = "default" } });
    try reply(failing.allocator(), &output, .{ .created = .{ .slot = .{ .reference = .{ .database_id = 1, .birth = 1, .name = "exported" }, .restart_lsn = 1, .confirmed_flush_lsn = 1, .applied_lsn = 0, .offered_lsn = 1 }, .consistent_point = 1, .snapshot_name = "antfly-11223344556677889900112233445566" } });
    try std.testing.expect(std.mem.indexOf(u8, output.buffered(), "antfly-11223344556677889900112233445566") != null);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}
