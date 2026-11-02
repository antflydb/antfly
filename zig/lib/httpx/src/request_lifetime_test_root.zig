// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: MIT
const std = @import("std");

test "HTTP request lifetime discovers transport contracts" {
    std.testing.refAllDecls(@import("server/server.zig"));
    std.testing.refAllDecls(@import("net/socket.zig"));
}
