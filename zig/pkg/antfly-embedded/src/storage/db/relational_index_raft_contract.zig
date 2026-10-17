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

//! Data-only private Raft controls. Decoding is not metadata authorization.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const inputs = @import("relational_index_inputs.zig");

pub const Effect = enum(u8) { admitted, fenced, reserved };
/// A control is one isolated store transaction. Reflect the request's default
/// shape so future effects cannot accidentally be silently ignored here.
pub fn validateBatchRequest(request: anytype) !void {
    const command = request.relational_index_owner_control orelse return;
    try command.validate();
    const defaults: @TypeOf(request) = .{};
    inline for (comptime std.meta.fieldNames(@TypeOf(request))) |name| {
        if (comptime std.mem.eql(u8, name, "relational_index_owner_control") or std.mem.eql(u8, name, "timestamp_ns")) continue;
        const value = @field(request, name);
        const info = @typeInfo(@TypeOf(value));
        if (comptime info == .pointer and info.pointer.size == .slice) {
            // Parsed empty arrays own a different address than &.{}. Pointer
            // identity is neither a mutation nor a semantic nondefault value.
            if (comptime @field(defaults, name).len != 0) @compileError("review nonempty default in isolated index control: " ++ name);
            if (value.len != 0) return error.InvalidBatchRequest;
        } else if (!std.meta.eql(value, @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

pub const Command = struct {
    effect: Effect,
    binding: jobs.Binding,
    inputs_b64: ?[]const u8 = null,

    pub fn validate(self: Command) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .raft or self.binding.phase != .fencing or
            ((self.effect == .admitted) != (self.inputs_b64 != null))) return error.InvalidPrivateIndexOwnerJob;
        if (self.inputs_b64) |bytes| if (bytes.len == 0 or bytes.len > std.base64.standard.Encoder.calcSize(inputs.max_bytes)) return error.InvalidPrivateIndexInputs;
    }
};
