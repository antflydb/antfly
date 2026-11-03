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

//! Ordered advisory demand contracts. Native ownership and provider I/O belong
//! to the execution owner; immutable scalar programs retain neither.
const std = @import("std");
const scalar = @import("scalar.zig");
pub const Function = enum {
    pg_advisory_lock,
    pg_advisory_lock_shared,
    pg_try_advisory_lock,
    pg_try_advisory_lock_shared,
    pg_advisory_unlock,
    pg_advisory_unlock_shared,
    pg_advisory_unlock_all,
    pg_advisory_xact_lock,
    pg_advisory_xact_lock_shared,
    pg_try_advisory_xact_lock,
    pg_try_advisory_xact_lock_shared,

    pub fn tries(self: Function) bool {
        return std.mem.startsWith(u8, @tagName(self), "pg_try_");
    }
    pub fn shared(self: Function) bool {
        return std.mem.endsWith(u8, @tagName(self), "_shared");
    }
    pub fn transactional(self: Function) bool {
        return std.mem.indexOf(u8, @tagName(self), "_xact_") != null;
    }
    pub fn unlocks(self: Function) bool {
        return self == .pg_advisory_unlock or self == .pg_advisory_unlock_shared;
    }
    pub fn booleanResult(self: Function) bool {
        return self.tries() or self.unlocks();
    }
    pub fn validArity(self: Function, count: usize) bool {
        return if (self == .pg_advisory_unlock_all) count == 0 else count == 1 or count == 2;
    }
};
pub fn function(name: []const u8) ?Function {
    return std.meta.stringToEnum(Function, name);
}
pub const Provider = struct {
    ptr: *anyopaque,
    invoke: *const fn (*anyopaque, std.mem.Allocator, Function, []const scalar.Datum) anyerror!scalar.Datum,
};
pub const Demand = struct {
    instruction: u32,
    destination: ?*?scalar.Datum = null,
    function: Function,
    args: []const scalar.Datum,
};
