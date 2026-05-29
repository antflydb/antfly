// Copyright 2026 Antfly, Inc.
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

pub const Formatter = enum {
    json,
    text,
};

pub const Config = struct {
    formatter: Formatter = .text,
    level: std.log.Level = .info,
};

var global_config: Config = .{};

pub fn init(config: Config) void {
    global_config = config;
}

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: anytype,
    comptime format: []const u8,
    args: anytype,
) void {
    _ = scope;

    if (@intFromEnum(level) > @intFromEnum(global_config.level)) return;

    switch (global_config.formatter) {
        .json => {
            std.debug.print(
                "{{\"level\":\"{s}\",\"message\":\"",
                .{@tagName(level)},
            );
            std.debug.print(format, args);
            std.debug.print("\"}}\n", .{});
        },
        .text => {
            std.debug.print("[{s}] ", .{@tagName(level)});
            std.debug.print(format, args);
            std.debug.print("\n", .{});
        },
    }
}
