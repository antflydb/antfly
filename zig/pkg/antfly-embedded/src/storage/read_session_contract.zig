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

//! Pure owner-issued retained-read capabilities. Only complete page/batch
//! operations cross this boundary; private session fields never do.
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const callback = @import("antfly_runtime_abi").callback_abi;
pub const IoBorrow = native.IoBorrow;
pub const IoReceiver = @import("antfly_runtime_abi").io_abi.Receiver;
pub const NativeOutcome = @import("native_outcome.zig").Outcome;
const Outcome = NativeOutcome;
const Values = @import("read_session_values.zig");
const ReaderValues = @import("read_reader_values.zig");
const View = @import("relational_read_view.zig").View;
const Types = @import("db/types.zig");
const Proof = @import("range_protection.zig").Proof;
pub const abi_version: u32 = 1;
pub const Kind = enum(u32) { statement = 1, relational = 2, document = 3, reader = 4, document_reader = 5, _ };
pub const Operations = struct {
    lookup_document: ?*const fn (Handle, []const u8) Outcome([]const u8) = null,
    lookup_documents: ?*const fn (Handle, []const []const u8, usize) Outcome(@import("read_document_values.zig").Batch) = null,
    lookup_projected_documents: ?*const fn (Handle, []const []const u8, []const []const u8, usize) Outcome(@import("read_document_values.zig").ProjectedBatch) = null,
    release_page: ?*const fn (Handle) Outcome(void) = null,
    next_page: ?*const fn (Handle, std.mem.Allocator, ?native.IoBorrow, Values.Budget) Outcome(Values.Page) = null,
    lookup_batch: ?*const fn (Handle, std.mem.Allocator, []const []const u8, *const ReaderValues.TypedLayout, usize) Outcome(ReaderValues.Hydrated) = null,
    lookup_one: ?*const fn (Handle, std.mem.Allocator, []const u8, bool, ?*?*const ReaderValues.TypedLayout) Outcome(?Values.Row) = null,
    close: ?*const fn (Handle) Outcome(void) = null,
    checkpoint: ?*const fn (Handle) Outcome(void) = null,
    range_proofs: ?*const fn (Handle, std.mem.Allocator) Outcome([]Proof) = null,
    normalize: ?*const fn (Handle, std.mem.Allocator, []const Types.BatchWrite) Outcome([]Types.BatchWrite) = null,
    next_typed: ?*const fn (Handle, std.mem.Allocator, ?native.IoBorrow, Values.Budget) Outcome(Values.Page) = null,
    next_view: ?*const fn (Handle, std.mem.Allocator, u32) Outcome(View.Page) = null,
};
pub const Boundary = callback.Boundary(Operations);
pub const Handle = extern struct {
    version: u32 = abi_version,
    kind: Kind,
    context: ?*anyopaque = null,
    operations: ?*const Operations = null,
    dispatch: ?Boundary.Dispatch = null,
    owner_allocator: ?*const std.mem.Allocator = null,
    pub fn issue(comptime kind: Kind, value: anytype, operations: *const Operations, dispatch: Boundary.Dispatch, owner_allocator: *const std.mem.Allocator) Handle {
        return .{ .kind = kind, .context = @ptrCast(value), .operations = operations, .dispatch = dispatch, .owner_allocator = owner_allocator };
    }
    pub fn validate(self: Handle, comptime expected: Kind) !void {
        if (self.version != abi_version or self.kind != expected) return error.UnsupportedVersion;
        if (self.context == null or self.operations == null or self.dispatch == null or self.owner_allocator == null) return error.InvalidArgument;
    }
    pub fn resolve(self: Handle, comptime kind: Kind, comptime T: type) !*T {
        try self.validate(kind);
        const pointer = self.context.?;
        if (@intFromPtr(pointer) % @alignOf(T) != 0) return error.InvalidArgument;
        return @ptrCast(@alignCast(pointer));
    }
};
pub const Statement = struct { handle: Handle, schema_version: u32 };
/// Explicit provider release for a complete immutable page. Payloads may
/// outlive the session; no session or temporary IO adapter is retained.
pub const Release = struct {
    context: *anyopaque,
    release: *const fn (*anyopaque) callconv(.c) void,
    pub fn deinit(self: Release) void {
        self.release(self.context);
    }
};
fn Result(comptime name: []const u8) type {
    const Method = @typeInfo(@FieldType(Operations, name)).optional.child;
    const Function = @typeInfo(Method).pointer.child;
    const Wire = @typeInfo(Function).@"fn".return_type.?;
    return anyerror!@FieldType(Wire, "value");
}
pub fn call(comptime name: []const u8, comptime kind: Kind, handle: Handle, args: anytype) Result(name) {
    try handle.validate(kind);
    const method = @field(handle.operations.?.*, name) orelse return error.InvalidArgument;
    const Function = @typeInfo(@TypeOf(method)).pointer.child;
    const Args = std.meta.ArgsTuple(Function);
    var wire_args: Args = args;
    inline for (@typeInfo(Args).@"struct".field_names, @typeInfo(Args).@"struct".field_types) |field, T| {
        if (comptime T == std.mem.Allocator) {
            const value = @field(wire_args, field);
            // Preserve custom/arena/quota allocator identity. Only stateless
            // process libc allocation is canonicalized to the issuing owner.
            if (value.vtable == std.heap.c_allocator.vtable) @field(wire_args, field) = handle.owner_allocator.?.*;
        }
    }
    const outcome = try Boundary.callRemote(name, handle.dispatch.?, method, wire_args);
    if (outcome.failure.ptr) |text| return @import("storage_engine_errors").fromName(text[0..outcome.failure.len]);
    return outcome.value;
}
test "read session handles reject stale kind version and alignment before pointer use" {
    const T = struct { value: u64 };
    var handle: Handle = .{ .kind = .relational, .context = @ptrFromInt(1) };
    handle.version = 0;
    try std.testing.expectError(error.UnsupportedVersion, handle.resolve(.relational, T));
    handle.version = abi_version;
    try std.testing.expectError(error.UnsupportedVersion, handle.resolve(.document, T));
    handle.kind = @fromBackingInt(@intCast(std.math.maxInt(u32)));
    try std.testing.expectError(error.UnsupportedVersion, handle.resolve(.relational, T));
    handle.kind = .relational;
    const empty: Operations = .{};
    handle.operations = &empty;
    handle.dispatch = @ptrFromInt(0x1000);
    handle.owner_allocator = &std.heap.c_allocator;
    try std.testing.expectError(error.InvalidArgument, handle.resolve(.relational, T));
    handle.context = null;
    try std.testing.expectError(error.InvalidArgument, handle.resolve(.relational, T));
}

/// Test-only receipt produced by an independently compiled production client.
pub const ProbeResult = extern struct {
    value: i64 = 0,
    schema_version: u32 = 0,
    owner_page: u32 = 0,
    canceled: u32 = 0,
    released: u32 = 0,
    guarded: u32 = 0,
    failure: [128]u8 = @splat(0),
};
