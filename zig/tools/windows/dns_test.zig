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

//! Internet-dependent DNS qualification; kept out of ordinary unit tests.
const std = @import("std");
const platform = @import("antfly_platform");

fn lookup(io: std.Io, name: []const u8, family: ?std.Io.net.IpAddress.Family) !void {
    var buffer: [16]std.Io.net.HostName.LookupResult = undefined;
    var queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&buffer);
    var canonical: [std.Io.net.HostName.max_len]u8 = undefined;
    try (try std.Io.net.HostName.init(name)).lookup(io, &queue, .{ .port = 443, .family = family, .canonical_name_buffer = &canonical });
    var addresses: usize = 0;
    var names: usize = 0;
    while (queue.getOneUncancelable(io)) |result| switch (result) {
        .address => |address| {
            try std.testing.expectEqual(@as(u16, 443), address.getPort());
            if (family) |requested| switch (requested) {
                .ip4 => try std.testing.expect(address == .ip4),
                .ip6 => try std.testing.expect(address == .ip6),
            };
            addresses += 1;
        },
        .canonical_name => |host| {
            try std.Io.net.HostName.validate(host.bytes);
            names += 1;
        },
    } else |err| try std.testing.expectEqual(error.Closed, err);
    try std.testing.expect(addresses > 0);
    try std.testing.expectEqual(@as(usize, 1), names);
}

test "Windows DNS resolves external names on caller and worker threads" {
    var executor = platform.Io.Threaded.init(std.testing.allocator, .{});
    defer executor.deinit();
    const io = executor.io();
    try lookup(io, "example.com", .ip4);
    var task = try io.concurrent(lookup, .{ io, "github.com", @as(?std.Io.net.IpAddress.Family, null) });
    try task.await(io);
}

test "Windows DNS returns an unknown-host error and closes the result queue" {
    var executor = platform.Io.Threaded.init(std.testing.allocator, .{});
    defer executor.deinit();
    const io = executor.io();
    var buffer: [16]std.Io.net.HostName.LookupResult = undefined;
    var queue: std.Io.Queue(std.Io.net.HostName.LookupResult) = .init(&buffer);
    try std.testing.expectError(error.UnknownHostName, (try std.Io.net.HostName.init("antfly-resolver-test.invalid")).lookup(io, &queue, .{ .port = 80 }));
    try std.testing.expectError(error.Closed, queue.getOneUncancelable(io));
}
