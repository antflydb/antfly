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

pub fn isHelpRequest(args: []const []const u8) bool {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "help")) return true;
        if (runFlagTakesValue(arg) and i + 1 < args.len) i += 1;
    }
    return false;
}

fn runFlagTakesValue(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--host") or
        std.mem.eql(u8, arg, "--port") or
        std.mem.eql(u8, arg, "--models-dir") or
        std.mem.eql(u8, arg, "--ml-dir") or
        std.mem.eql(u8, arg, "--config") or
        std.mem.eql(u8, arg, "--max-loaded-models") or
        std.mem.eql(u8, arg, "--max-concurrent-requests") or
        std.mem.eql(u8, arg, "--preload-model");
}

pub fn parseMaxLoadedModelsOverride(args: []const []const u8) !?usize {
    var override: ?usize = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (!std.mem.eql(u8, args[i], "--max-loaded-models")) continue;
        if (i + 1 >= args.len) return error.InvalidArguments;
        // Zero deliberately means unlimited, matching NodeConfig and the JSON
        // run configuration contract. The final flag wins consistently with
        // the rest of the command-line option parser.
        override = std.fmt.parseInt(usize, args[i + 1], 10) catch return error.InvalidArguments;
        i += 1;
    }
    return override;
}

test "run cli parses max loaded models residency override" {
    try std.testing.expectEqual(
        @as(?usize, 1),
        try parseMaxLoadedModelsOverride(&.{ "--host", "127.0.0.1", "--max-loaded-models", "1" }),
    );
    try std.testing.expectEqual(
        @as(?usize, 4),
        try parseMaxLoadedModelsOverride(&.{ "--max-loaded-models", "2", "--max-loaded-models", "4" }),
    );
    try std.testing.expectError(
        error.InvalidArguments,
        parseMaxLoadedModelsOverride(&.{"--max-loaded-models"}),
    );
    try std.testing.expectError(
        error.InvalidArguments,
        parseMaxLoadedModelsOverride(&.{ "--max-loaded-models", "not-a-number" }),
    );
    try std.testing.expectEqual(
        @as(?usize, 0),
        try parseMaxLoadedModelsOverride(&.{ "--max-loaded-models", "0" }),
    );
}

test "inference run recognizes help before server startup" {
    try std.testing.expect(isHelpRequest(&.{"--help"}));
    try std.testing.expect(isHelpRequest(&.{ "--port", "8091", "-h" }));
    try std.testing.expect(!isHelpRequest(&.{ "--host", "127.0.0.1" }));
    try std.testing.expect(!isHelpRequest(&.{ "--config", "help" }));
    try std.testing.expect(!isHelpRequest(&.{ "--host", "-h" }));
}
