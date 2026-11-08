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

//! A query lease retains an owner apply fence; complete search/read operations
//! cross this boundary. Both relational and document cursors are independently
//! retained owner-issued capabilities.
const std = @import("std");
const callback = @import("antfly_runtime_abi").callback_abi;
const sessions = @import("read_session_contract.zig");
const Outcome = sessions.NativeOutcome;
const types = @import("db/types.zig");
pub fn Protocol(comptime SearchResult: type) type {
    return struct {
        pub const Operations = struct {
            close: *const fn (Handle) Outcome(void),
            reader: *const fn (Handle, std.mem.Allocator, []const []const u8, u32) Outcome(sessions.Handle),
            documents: *const fn (Handle, std.mem.Allocator, u32) Outcome(sessions.Handle),
            search: *const fn (Handle, std.mem.Allocator, types.SearchRequest) Outcome(SearchResult),
        };
        pub const Boundary = callback.Boundary(Operations);
        pub const Handle = extern struct {
            version: u32 = 1,
            context: ?*anyopaque = null,
            operations: ?*const Operations = null,
            dispatch: ?Boundary.Dispatch = null,
            owner_allocator: ?*const std.mem.Allocator = null,
            pub fn validate(self: Handle) !void {
                if (self.version != 1) return error.UnsupportedVersion;
                if (self.context == null or self.operations == null or self.dispatch == null or self.owner_allocator == null) return error.InvalidArgument;
            }
            pub fn resolve(self: Handle, comptime T: type) !*T {
                try self.validate();
                if (@intFromPtr(self.context.?) % @alignOf(T) != 0) return error.InvalidArgument;
                return @ptrCast(@alignCast(self.context.?));
            }
        };
        fn Result(comptime name: []const u8) type {
            const Function = @typeInfo(@FieldType(Operations, name)).pointer.child;
            const Wire = @typeInfo(Function).@"fn".return_type.?;
            return anyerror!@FieldType(Wire, "value");
        }
        pub fn call(comptime name: []const u8, handle: Handle, args: anytype) Result(name) {
            try handle.validate();
            const method = @field(handle.operations.?.*, name);
            const Args = std.meta.ArgsTuple(@typeInfo(@TypeOf(method)).pointer.child);
            var wire_args: Args = args;
            inline for (@typeInfo(Args).@"struct".field_names, @typeInfo(Args).@"struct".field_types) |field, T| {
                if (comptime T == std.mem.Allocator) {
                    if (@field(wire_args, field).vtable == std.heap.c_allocator.vtable) @field(wire_args, field) = handle.owner_allocator.?.*;
                }
            }
            const outcome = try Boundary.callRemote(name, handle.dispatch.?, method, wire_args);
            if (outcome.failure.ptr) |text| return @import("storage_engine_errors").fromName(text[0..outcome.failure.len]);
            return outcome.value;
        }
    };
}
