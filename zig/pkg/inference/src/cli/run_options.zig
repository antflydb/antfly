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
