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

//! Private sequence binding and demand contracts. Allocation belongs to an
//! execution owner, never a native scalar callback or an immutable plan.
const std = @import("std");
const scalar = @import("scalar.zig");
const domain = @import("../system_catalog/domain.zig");
pub const Identity = @import("sequence_session.zig").Identity;
pub const Function = enum { nextval, currval, lastval, setval };
pub const Reference = struct {
    identity: Identity,
    namespace_id: u64,
    target: domain.Target,

    pub fn validate(self: Reference) !void {
        try self.identity.validate();
        if (self.namespace_id == 0) return error.InvalidSequenceSession;
        try self.target.validate();
    }
};
pub const Resolver = struct {
    ptr: *anyopaque,
    resolve: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!Reference,
};
pub const Provider = struct {
    ptr: *anyopaque,
    invoke: *const fn (*anyopaque, std.mem.Allocator, Function, ?Reference, []const scalar.Datum) anyerror!i64,
    /// A result boundary publishes current values before any dependent rows
    /// become observable. Unconsumed cached ranges are never persisted.
    flush: *const fn (*anyopaque) anyerror!void,
};
pub const Demand = struct {
    instruction: u32,
    function: Function,
    reference: ?Reference,
    args: []const scalar.Datum,
};

pub fn function(name: []const u8) ?Function {
    return std.meta.stringToEnum(Function, name);
}
