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

const std = @import("std");
const builtin = @import("builtin");
const domain = @import("antfly_local_sources").system_catalog_domain;
const server = @import("server.zig");
const storage = @import("../system_catalog/storage.zig");
const LinuxPolicy = domain.selinux_linux.LinuxPolicy;
const Key = std.crypto.sign.Ed25519.KeyPair;

pub const Control = union(enum) {
    none,
    rotate: u64,
    withdraw: u64,

    pub fn fromEnvironment(env: *const std.process.Environ.Map) !Control {
        const rotate = env.get("ANTFLY_PGWIRE_SELINUX_ROTATE_EPOCH");
        const withdraw = env.get("ANTFLY_PGWIRE_SELINUX_WITHDRAW_EPOCH");
        if (rotate != null and withdraw != null) return error.InvalidPgwireConfig;
        if (rotate) |epoch| return .{ .rotate = try parseEpoch(epoch) };
        if (withdraw) |epoch| return .{ .withdraw = try parseEpoch(epoch) };
        return .none;
    }

    fn parseEpoch(text: []const u8) !u64 {
        const epoch = std.fmt.parseInt(u64, text, 10) catch return error.InvalidPgwireConfig;
        if (epoch == 0 or epoch == std.math.maxInt(u64)) return error.InvalidPgwireConfig;
        return epoch;
    }

    pub fn expectedUpdate(self: Control, state: storage.SelinuxAuthorityState, desired: ?domain.security_labels.KernelRelabel.Authority) !?u64 {
        var persisted = state.active;
        if (persisted) |*authority| authority.epoch = 0;
        return switch (self) {
            .none => if (state.epoch == 0 and desired != null) 0 else null,
            .rotate, .withdraw => |expected| if (state.epoch == expected)
                expected
            else if (state.epoch == expected + 1 and std.meta.eql(persisted, desired))
                null
            else
                error.SelinuxAdmissionChanged,
        };
    }
};

/// Owns the process-local signer and socket name until pgwire has shut down.
pub const Owner = struct {
    policy: LinuxPolicy,
    key: Key,
    name: [std.Io.net.UnixAddress.max_len]u8,
    name_len: usize,

    pub fn fromEnvironment(io: std.Io, alloc: std.mem.Allocator, env: *const std.process.Environ.Map) !?Owner {
        const name = env.get("ANTFLY_PGWIRE_SELINUX_SOCKET");
        const seed = env.get("ANTFLY_PGWIRE_SELINUX_SEED_FILE");
        if (name == null and seed == null) return null;
        if (comptime builtin.os.tag != .linux or !builtin.link_libc) return error.SelinuxUnavailable;
        const socket = name orelse return error.InvalidPgwireConfig;
        const path = seed orelse return error.InvalidPgwireConfig;
        if (socket.len == 0 or socket.len >= std.Io.net.UnixAddress.max_len or
            std.mem.indexOfScalar(u8, socket, 0) != null or !std.fs.path.isAbsolute(path))
            return error.InvalidPgwireConfig;

        var seed_bytes: [Key.seed_length]u8 = undefined;
        defer std.crypto.secureZero(u8, &seed_bytes);
        try readSeed(io, path, &seed_bytes);
        var policy = try LinuxPolicy.init(io, alloc);
        errdefer policy.deinit();
        var result: Owner = .{
            .policy = policy,
            .key = try Key.generateDeterministic(seed_bytes),
            .name = undefined,
            .name_len = socket.len + 1,
        };
        result.name[0] = 0;
        @memcpy(result.name[1..result.name_len], socket);
        return result;
    }

    pub fn deinit(self: *Owner) void {
        self.policy.deinit();
        std.crypto.secureZero(u8, std.mem.asBytes(&self.key));
    }

    pub fn authority(self: *Owner) !domain.security_labels.KernelRelabel.Authority {
        return .{ .verifier = self.key.public_key.toBytes(), .policy = try self.policy.currentPolicy() };
    }

    pub fn endpoint(self: *Owner, state: storage.SelinuxAuthorityState) !server.SelinuxEndpoint {
        var expected = try self.authority();
        expected.epoch = state.epoch;
        if (state.epoch == 0 or !std.meta.eql(state.active, @as(?@TypeOf(expected), expected)))
            return error.SelinuxAdmissionChanged;
        return .{ .address = try std.Io.net.UnixAddress.init(self.name[0..self.name_len]), .policy = &self.policy, .key = &self.key };
    }
};

fn readSeed(io: std.Io, path: []const u8, seed: *[Key.seed_length]u8) !void {
    const file = try std.Io.Dir.openFileAbsolute(io, path, .{ .allow_directory = false, .follow_symlinks = false });
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size != Key.seed_length or stat.permissions.toMode() & 0o077 != 0)
        return error.InvalidPgwireConfig;

    var bytes: [Key.seed_length + 1]u8 = undefined;
    defer std.crypto.secureZero(u8, &bytes);
    if (try file.readPositionalAll(io, &bytes, 0) != Key.seed_length or std.mem.allEqual(u8, bytes[0..Key.seed_length], 0))
        return error.InvalidPgwireConfig;
    seed.* = bytes[0..Key.seed_length].*;
}

test "SELinux startup signer requires an exactly sized private seed file" {
    if (comptime !@hasDecl(std.Io.File.Permissions, "fromMode")) return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(base);
    const Fixture = struct {
        fn write(dir: std.Io.Dir, path: []const u8, permissions: std.Io.File.Permissions, bytes: []const u8) !void {
            const file = try dir.createFile(std.testing.io, path, .{ .permissions = permissions });
            defer file.close(std.testing.io);
            try file.writeStreamingAll(std.testing.io, bytes);
            try file.setPermissions(std.testing.io, permissions);
        }

        fn read(dir: []const u8, name: []const u8, seed: *[Key.seed_length]u8) !void {
            const relative = try std.fs.path.join(std.testing.allocator, &.{ dir, name });
            defer std.testing.allocator.free(relative);
            const path = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, relative, std.testing.allocator);
            defer std.testing.allocator.free(path);
            try readSeed(std.testing.io, path, seed);
        }
    };
    const valid: [Key.seed_length]u8 = @splat(7);
    try Fixture.write(tmp.dir, "private", .fromMode(0o600), &valid);
    try Fixture.write(tmp.dir, "readable", .fromMode(0o644), &valid);
    try Fixture.write(tmp.dir, "truncated", .fromMode(0o600), valid[1..]);
    try Fixture.write(tmp.dir, "zero", .fromMode(0o600), &@as([Key.seed_length]u8, @splat(0)));
    var actual: [Key.seed_length]u8 = undefined;
    try Fixture.read(base, "private", &actual);
    try std.testing.expectEqual(valid, actual);
    for ([_][]const u8{ "readable", "truncated", "zero" }) |name| {
        try std.testing.expectError(error.InvalidPgwireConfig, Fixture.read(base, name, &actual));
    }
}
