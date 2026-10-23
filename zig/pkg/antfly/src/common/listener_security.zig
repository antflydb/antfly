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

pub fn isLoopback(host: []const u8) bool {
    const address = std.Io.net.IpAddress.parse(host, 0) catch return false;
    return switch (address) {
        .ip4 => |ip| ip.bytes[0] == 127,
        .ip6 => |ip| if (std.Io.net.Ip4Address.fromIp6(ip)) |ip4|
            ip4.bytes[0] == 127
        else
            std.mem.eql(u8, &ip.bytes, &std.Io.net.Ip6Address.loopback(0).bytes),
    };
}

pub fn warnIfUnauthenticated(label: []const u8, host: []const u8, port: u16, protected: bool) void {
    if (protected or isLoopback(host)) return;
    std.log.warn("{s} API listening on {s}:{d} without authentication: reachable clients have administrator access; enable auth with a unique bootstrap password or restrict access. In Docker, publish host ports on 127.0.0.1", .{ label, host, port });
}

test "listener security recognizes literal loopback addresses conservatively" {
    for ([_][]const u8{ "127.0.0.1", "127.0.0.2", "::1", "::ffff:127.0.0.1" }) |host| {
        try std.testing.expect(isLoopback(host));
    }
    for ([_][]const u8{ "0.0.0.0", "::", "192.168.1.10", "::ffff:192.168.1.10", "localhost", "db.example" }) |host| {
        try std.testing.expect(!isLoopback(host));
    }
}
