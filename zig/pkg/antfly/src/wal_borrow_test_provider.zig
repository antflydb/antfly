// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Minimal compiled-owner fixture for the existing WAL read-buffer ABI.
const std = @import("std");
const abi = @import("kernel_owner_abi");
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
var mode: u32 = 0;
var reads: u32 = 0;
var retired: u32 = 0;
export fn wal_borrow_fixture_mode(next: u32) callconv(.c) void {
    mode = next;
}
export fn wal_borrow_fixture_reads() callconv(.c) u32 {
    return reads;
}
export fn wal_borrow_fixture_retired() callconv(.c) u32 {
    return retired;
}
export fn antfly_storage_wal_read(_: ?*anyopaque, request: *const abi.WalPositionRequest, output: *abi.WalReadResult) callconv(.c) abi.Status {
    reads += 1;
    output.* = .{};
    if (request.version != abi.abi_version or request.lsn != 7) return .invalid_argument;
    if (mode == 4) return .out_of_memory;
    if (mode == 1) return .ok;
    const bytes = std.heap.c_allocator.dupe(u8, "wire") catch return .out_of_memory;
    output.* = .{ .found = if (mode == 2) 2 else if (mode == 3) 0 else 1, .buffer = .{ .ptr = bytes.ptr, .len = bytes.len } };
    return .ok;
}
export fn antfly_storage_owner_buffer_destroy(buffer: *abi.OwnedBytes) callconv(.c) void {
    retired += 1;
    if (buffer.ptr) |ptr| std.heap.c_allocator.free(ptr[0..@intCast(buffer.len)]);
    buffer.* = .{};
}

export fn antfly_storage_wal_read_with_admission(raw: ?*anyopaque, request: *const abi.WalReadAdmissionRequest, output: *abi.WalReadResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const read = @import("antfly_local_sources").common_read_admission;
    const ptr = request.port orelse return .invalid_argument;
    if (@intFromPtr(ptr) % @alignOf(read.Port) != 0) return .invalid_argument;
    const control: *const read.Port = @ptrCast(@alignCast(ptr));
    // Deliberately coarse owner status: the client must retain the original
    // lifetime error rather than replacing it with this unrelated identity.
    control.checkpoint() catch return .out_of_memory;
    const status = antfly_storage_wal_read(raw, &.{ .lsn = request.lsn }, output);
    if (status != .ok) return status;
    control.checkpoint() catch return .out_of_memory;
    return .ok;
}

var position_committed: bool = false;
var prepared_marker: u64 = 0;
var prepared_retired: u32 = 0;
export fn wal_prepared_fixture_retired() callconv(.c) u32 {
    return prepared_retired;
}
export fn antfly_storage_wal_prepare_append(_: ?*anyopaque, request: *const abi.WalPrepareAppendRequest, output: *abi.WalPrepareAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version or request.prefix_len != 4 or !std.mem.eql(u8, request.tail.slice(), "tail")) return .invalid_argument;
    const control = fixtureControl(request.port) catch return .invalid_argument;
    if (mode != 2 and mode != 3) control.checkpoint() catch {};
    if (mode == 1 or mode == 2) return .out_of_memory;
    output.handle = &prepared_marker;
    return .ok;
}
export fn antfly_storage_wal_prepared_publish(_: ?*anyopaque, request: *const abi.WalPreparedPublishRequest, output: *abi.WalAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version or request._reserved != 0 or !std.mem.eql(u8, request.prefix.slice(), "head")) return .invalid_argument;
    output.* = .{ .assigned_lsn = 7, .next_lsn = 8 };
    return .ok;
}
export fn antfly_storage_wal_prepared_destroy(raw: ?*anyopaque) callconv(.c) void {
    if (raw == @as(?*anyopaque, &prepared_marker)) prepared_retired += 1;
}
export fn wal_position_fixture_reset(next: u32) callconv(.c) void {
    mode = next;
    position_committed = false;
}
export fn wal_position_fixture_committed() callconv(.c) u8 {
    return @intFromBool(position_committed);
}

fn fixturePosition() abi.WalRecoveryPosition {
    return .{ .origin = 11, .owner = 22, .revision = 33, .next_lsn = 7 };
}
fn fixtureControl(raw: ?*const anyopaque) !@import("antfly_local_sources").common_read_admission.Port {
    const read_control = @import("antfly_local_sources").common_read_admission;
    const ptr = raw orelse return error.InvalidArgument;
    const port: *const read_control.Port = @ptrCast(@alignCast(ptr));
    try port.validate();
    return port.*;
}
export fn antfly_storage_wal_recovery_position(_: ?*anyopaque, request: *const abi.WalRecoveryPositionRequest, output: *abi.WalRecoveryPositionResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0) return .invalid_argument;
    const control = fixtureControl(request.port) catch return .invalid_argument;
    control.checkpoint() catch return .out_of_memory;
    output.position = fixturePosition();
    control.checkpoint() catch return .out_of_memory;
    switch (mode) {
        1 => output.* = .{ .busy = 1 },
        3 => output.position.revision = 0,
        4 => output._reserved[0] = 1,
        5 => output.busy = 1,
        7 => return .out_of_memory,
        else => {},
    }
    return .ok;
}
export fn antfly_storage_wal_append_if_recovery_position(_: ?*anyopaque, request: *const abi.WalRecoveryAppendRequest, output: *abi.WalRecoveryAppendResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or !std.meta.eql(request.expected, fixturePosition()) or !std.mem.eql(u8, request.data.slice(), "conditional")) return .invalid_argument;
    const control = fixtureControl(request.port) catch return .invalid_argument;
    control.checkpoint() catch return .out_of_memory;
    // A deliberately populated failure reply still must not become success.
    output.* = .{ .outcome = 2, .assigned_lsn = 7, .next_lsn = 8 };
    control.checkpoint() catch return .out_of_memory;
    switch (mode) {
        1 => output.* = .{ .outcome = 1 },
        2 => output.* = .{},
        3 => output.outcome = 9,
        4 => output.assigned_lsn = 8,
        5 => output.next_lsn = 0,
        6 => output._reserved[0] = 1,
        7 => return .out_of_memory,
        else => position_committed = true,
    }
    return .ok;
}

// The rooted reply ABI deliberately allocates in the consumer's origin. No
// callback descriptor or pointer to a descriptor survives these calls.
export fn antfly_storage_wal_open_with_allocator(request: *const abi.WalOpenAllocatorRequest, output: *abi.WalOpenResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    output.handle = @ptrFromInt(16);
    return .ok;
}
export fn antfly_storage_wal_close(_: ?*anyopaque) callconv(.c) void {}

export fn antfly_storage_wal_read_with_allocator(_: ?*anyopaque, request: *const abi.WalReadAllocatorRequest, output: *abi.WalReadResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request._reserved != 0 or request.lsn != 7) return .invalid_argument;
    const memory = request.allocator orelse return .invalid_argument;
    if (!memory.valid()) return .invalid_abi;
    const control = if (request.port) |raw| fixtureControl(raw) catch return .invalid_argument else null;
    if (control) |port| port.checkpoint() catch return .out_of_memory;
    if (mode == 1) return .ok;
    if (mode == 4) return .out_of_memory;
    if (mode == 6) {
        output.buffer.len = 4;
        output.found = 1;
        return .ok;
    }
    const bytes = memory.asStd().dupe(u8, "wire") catch return .out_of_memory;
    output.* = .{ .found = if (mode == 2) 2 else if (mode == 3) 0 else 1, .buffer = .{ .ptr = bytes.ptr, .len = bytes.len } };
    if (control) |port| port.checkpoint() catch return .out_of_memory;
    return if (mode == 5) .out_of_memory else .ok;
}

export fn antfly_storage_wal_iterate_with_allocator(_: ?*anyopaque, request: *const abi.WalScanAllocatorRequest, output: *abi.WalScanResult) callconv(.c) abi.Status {
    output.* = .{};
    if (request.version != abi.abi_version) return .invalid_abi;
    const memory = request.allocator orelse return .invalid_argument;
    if (!memory.valid()) return .invalid_abi;
    if (mode == 4) return .out_of_memory;
    const wire = @import("storage/kernel_wal_wire.zig");
    const bytes = wire.encodeEntriesAlloc(memory.asStd(), &[_]wire.Entry{.{ .lsn = 7, .data = "wire" }}) catch return .out_of_memory;
    output.* = .{ .entries = .{ .ptr = bytes.ptr, .len = bytes.len }, .next_lsn = 8, .done = if (mode == 2) 2 else 1 };
    if (mode == 3) bytes[0] = 0;
    return if (mode == 5) .out_of_memory else .ok;
}

// Legacy scan is unused by these fixtures but remains linkable alongside the
// rooted path so tests can exercise one runtime-selected implementation.
export fn antfly_storage_wal_iterate(_: ?*anyopaque, _: *const abi.WalScanRequest, output: *abi.WalScanResult) callconv(.c) abi.Status {
    output.* = .{};
    return .invalid_argument;
}

const OwnedFixture = struct {
    memory: @import("runtime_memory_abi").Allocator,
};
var owned_mode: u32 = 0;
var owned_retired: u32 = 0;
export fn wal_owned_fixture_mode(value: u32) callconv(.c) void {
    owned_mode = value;
}
export fn wal_owned_fixture_retired() callconv(.c) u32 {
    return owned_retired;
}
export fn antfly_storage_wal_owned_read_begin(_: ?*anyopaque, request: *const abi.WalOwnedReadBeginRequest, output: *abi.WalOwnedReadBeginResult) callconv(.c) abi.Status {
    output.* = .{};
    if (owned_mode == 1) return .ok;
    const memory = request.allocator.?;
    const handle = memory.asStd().create(OwnedFixture) catch return .out_of_memory;
    handle.* = .{ .memory = memory.* };
    output.handle = handle;
    const control = fixtureControl(request.port) catch return .invalid_argument;
    // Deliberately ignore original rejection and publish an owner anyway.
    control.checkpoint() catch {};
    return if (owned_mode == 2) .out_of_memory else .ok;
}
export fn antfly_storage_wal_owned_read_step(_: ?*anyopaque, request: *const abi.WalOwnedReadStepRequest, output: *abi.WalOwnedReadStepResult) callconv(.c) abi.Status {
    const control = fixtureControl(request.port) catch return .invalid_argument;
    control.checkpoint() catch {};
    output.* = .{ .ready = if (owned_mode == 4) 2 else 1 };
    if (owned_mode == 5) output._reserved[0] = 1;
    return .ok;
}
export fn antfly_storage_wal_owned_read_chunk(_: ?*anyopaque, request: *const abi.WalOwnedReadChunkRequest, output: *abi.WalOwnedReadChunkResult) callconv(.c) abi.Status {
    const control = fixtureControl(request.port) catch return .invalid_argument;
    control.checkpoint() catch {};
    const offset: usize = @intCast(request.offset);
    if (offset > 4) return .invalid_argument;
    const len = @min(@as(usize, @intCast(request.output.len)), 4 - offset);
    if (len != 0) @memcpy(request.output.ptr.?[0..len], "wire"[offset..][0..len]);
    output.* = .{ .len = if (owned_mode == 6) request.output.len + 1 else if (owned_mode == 8) 0 else len, .done = if (owned_mode == 7) 2 else if (owned_mode == 8) 0 else @intFromBool(offset + len == 4) };
    if (owned_mode == 9) output._reserved[0] = 1;
    return .ok;
}
export fn antfly_storage_wal_owned_read_destroy(raw: ?*anyopaque) callconv(.c) void {
    const handle: *OwnedFixture = @ptrCast(@alignCast(raw orelse return));
    var memory = handle.memory;
    memory.asStd().destroy(handle);
    owned_retired += 1;
}
