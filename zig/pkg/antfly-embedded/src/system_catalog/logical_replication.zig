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

//! Pure logical-replication options shared by SQL intent and native records.
//! No option creates upstream effects or grants Source/replica authority.
const std = @import("std");
pub const max_members = 4096;
pub const max_options = 32;
pub const Option = struct { name: []const u8, value: Value };
pub const Value = union(enum) { boolean: bool, text: []const u8, none };
pub const Slot = union(enum) { name: []const u8, none };
pub const Streaming = enum { off, on, parallel };
pub const SynchronousCommit = enum { off, local, remote_write, on, remote_apply };
pub const Origin = enum { any, none };

pub const PublishActions = struct { insert: bool = true, update: bool = true, delete: bool = true, truncate: bool = true };
pub const GeneratedColumns = enum { none, stored };
pub const MembershipMode = enum { set, add, drop };
pub const PublicationOptions = struct {
    publish: PublishActions = .{},
    publish_via_partition_root: bool = false,
    publish_generated_columns: GeneratedColumns = .none,
};
pub const SubscriptionOptions = struct {
    connect: bool = true,
    create_slot: bool = true,
    enabled: bool = true,
    copy_data: bool = true,
    binary: bool = false,
    two_phase: bool = false,
    disable_on_error: bool = false,
    password_required: bool = true,
    run_as_owner: bool = false,
    failover: bool = false,
    synchronous_commit: SynchronousCommit = .off,
    streaming: Streaming = .parallel,
    origin: Origin = .any,
    slot_name: Slot,

    pub fn validate(self: @This()) !void {
        if (self.slot_name == .none and (self.enabled or self.create_slot)) return error.SqlInvalidParameterValue;
        if (self.slot_name == .name) {
            const name = self.slot_name.name;
            if (name.len == 0 or name.len > 63) return error.SqlInvalidParameterValue;
            for (name) |byte| if (!(std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '_')) return error.SqlInvalidParameterValue;
        }
    }
    pub fn validateCreation(self: @This()) !void {
        try self.validate();
        if (!self.connect and (self.create_slot or self.enabled or self.copy_data)) return error.SqlInvalidParameterValue;
    }
};

fn unique(options: []const Option) !void {
    if (options.len > max_options) return error.SqlLimitExceeded;
    for (options, 0..) |option, i| {
        for (options[0..i]) |prior| if (std.mem.eql(u8, prior.name, option.name)) return error.SqlInvalidParameterValue;
    }
}
fn boolean(value: Value) !bool {
    return switch (value) {
        .boolean => |b| b,
        .text => |s| @import("../sql/builtin_cast.zig").booleanText(s) catch error.SqlInvalidParameterValue,
        .none => error.SqlInvalidParameterValue,
    };
}
fn text(value: Value) ![]const u8 {
    return switch (value) {
        .text => |value_| value_,
        .none => "none",
        .boolean => error.SqlInvalidParameterValue,
    };
}
fn enumeration(comptime T: type, value: Value) !T {
    return std.meta.stringToEnum(T, try text(value)) orelse error.SqlInvalidParameterValue;
}

pub fn publicationOptions(options: []const Option, previous: PublicationOptions) !PublicationOptions {
    try unique(options);
    var result = previous;
    for (options) |option| {
        if (std.mem.eql(u8, option.name, "publish")) {
            result.publish = .{ .insert = false, .update = false, .delete = false, .truncate = false };
            var names = std.mem.splitScalar(u8, try text(option.value), ',');
            var seen: u8 = 0;
            while (names.next()) |name_| {
                const name = std.mem.trim(u8, name_, " \t\r\n");
                const index: u3 = if (std.mem.eql(u8, name, "insert")) 0 else if (std.mem.eql(u8, name, "update")) 1 else if (std.mem.eql(u8, name, "delete")) 2 else if (std.mem.eql(u8, name, "truncate")) 3 else return error.SqlInvalidParameterValue;
                const mask = @as(u8, 1) << index;
                if (seen & mask != 0) return error.SqlInvalidParameterValue;
                seen |= mask;
                switch (index) {
                    0 => result.publish.insert = true,
                    1 => result.publish.update = true,
                    2 => result.publish.delete = true,
                    3 => result.publish.truncate = true,
                    else => unreachable,
                }
            }
        } else if (std.mem.eql(u8, option.name, "publish_via_partition_root")) {
            result.publish_via_partition_root = try boolean(option.value);
        } else if (std.mem.eql(u8, option.name, "publish_generated_columns")) {
            result.publish_generated_columns = try enumeration(@FieldType(PublicationOptions, "publish_generated_columns"), option.value);
        } else return error.SqlInvalidParameterValue;
    }
    return result;
}

/// CREATE defaults are applied only after all explicitly authored options are
/// seen. connect=false forces absent creation options false; explicit true is
/// rejected instead of silently erased. No I/O occurs in this function.
pub fn subscriptionOptions(name: []const u8, options: []const Option) !SubscriptionOptions {
    try unique(options);
    var result: SubscriptionOptions = .{ .slot_name = .{ .name = name } };
    var create_slot_explicit = false;
    var enabled_explicit = false;
    var copy_data_explicit = false;
    for (options) |option| {
        const key = option.name;
        if (std.mem.eql(u8, key, "slot_name")) {
            result.slot_name = if (option.value == .none) .none else .{ .name = try text(option.value) };
        } else if (std.mem.eql(u8, key, "streaming")) {
            result.streaming = if (option.value == .boolean) (if (option.value.boolean) .on else .off) else try enumeration(Streaming, option.value);
        } else if (std.mem.eql(u8, key, "synchronous_commit")) {
            result.synchronous_commit = if (option.value == .boolean) (if (option.value.boolean) .on else .off) else try enumeration(SynchronousCommit, option.value);
        } else if (std.mem.eql(u8, key, "origin")) {
            result.origin = try enumeration(Origin, option.value);
        } else {
            const b = try boolean(option.value);
            if (std.mem.eql(u8, key, "connect")) result.connect = b else if (std.mem.eql(u8, key, "create_slot")) {
                result.create_slot = b;
                create_slot_explicit = true;
            } else if (std.mem.eql(u8, key, "enabled")) {
                result.enabled = b;
                enabled_explicit = true;
            } else if (std.mem.eql(u8, key, "copy_data")) {
                result.copy_data = b;
                copy_data_explicit = true;
            } else if (std.mem.eql(u8, key, "binary")) result.binary = b else if (std.mem.eql(u8, key, "two_phase")) result.two_phase = b else if (std.mem.eql(u8, key, "disable_on_error")) result.disable_on_error = b else if (std.mem.eql(u8, key, "password_required")) result.password_required = b else if (std.mem.eql(u8, key, "run_as_owner")) result.run_as_owner = b else if (std.mem.eql(u8, key, "failover")) result.failover = b else return error.SqlInvalidParameterValue;
        }
    }
    if (!result.connect) {
        if (!create_slot_explicit) result.create_slot = false;
        if (!enabled_explicit) result.enabled = false;
        if (!copy_data_explicit) result.copy_data = false;
    }
    try result.validateCreation();
    return result;
}

pub const RefreshOptions = struct { refresh: bool = true, copy_data: bool = true };
pub fn refreshOptions(values: []const Option, allow_refresh: bool) !RefreshOptions {
    try unique(values);
    var result: RefreshOptions = .{};
    for (values) |value| {
        if (std.mem.eql(u8, value.name, "copy_data")) result.copy_data = try boolean(value.value) else if (allow_refresh and std.mem.eql(u8, value.name, "refresh")) result.refresh = try boolean(value.value) else return error.SqlInvalidParameterValue;
    }
    return result;
}

/// ALTER is a merge, never reapplication of CREATE defaults. Creation-only
/// controls cannot acquire provider effects through an ordinary SET clause.
pub fn alterSubscriptionOptions(previous: SubscriptionOptions, values: []const Option) !SubscriptionOptions {
    try unique(values);
    var result = previous;
    for (values) |value| {
        const key = value.name;
        if (std.mem.eql(u8, key, "slot_name")) {
            if (previous.enabled) return error.SqlInvalidParameterValue;
            result.slot_name = if (value.value == .none) .none else .{ .name = try text(value.value) };
        } else if (std.mem.eql(u8, key, "streaming")) {
            result.streaming = if (value.value == .boolean) (if (value.value.boolean) .on else .off) else try enumeration(Streaming, value.value);
        } else if (std.mem.eql(u8, key, "synchronous_commit")) {
            result.synchronous_commit = if (value.value == .boolean) (if (value.value.boolean) .on else .off) else try enumeration(SynchronousCommit, value.value);
        } else if (std.mem.eql(u8, key, "origin")) {
            result.origin = try enumeration(Origin, value.value);
        } else {
            const b = try boolean(value.value);
            if (std.mem.eql(u8, key, "binary")) result.binary = b else if (std.mem.eql(u8, key, "two_phase")) result.two_phase = b else if (std.mem.eql(u8, key, "disable_on_error")) result.disable_on_error = b else if (std.mem.eql(u8, key, "password_required")) result.password_required = b else if (std.mem.eql(u8, key, "run_as_owner")) result.run_as_owner = b else if (std.mem.eql(u8, key, "failover")) result.failover = b else return error.SqlInvalidParameterValue;
        }
    }
    try result.validate();
    return result;
}

test "replication SQL disconnected subscription defaults forbid upstream creation and explicit conflicts" {
    const off: Option = .{ .name = "connect", .value = .{ .boolean = false } };
    const options = try subscriptionOptions("usage_sub", &.{off});
    try std.testing.expect(!options.connect and !options.create_slot and !options.enabled and !options.copy_data);
    try std.testing.expectError(error.SqlInvalidParameterValue, subscriptionOptions("usage_sub", &.{ off, .{ .name = "enabled", .value = .{ .boolean = true } } }));
    try std.testing.expectError(error.SqlInvalidParameterValue, subscriptionOptions("usage_sub", &.{.{ .name = "slot_name", .value = .none }}));
    try std.testing.expectError(error.SqlInvalidParameterValue, subscriptionOptions("usage_sub", &.{ off, off }));
}
