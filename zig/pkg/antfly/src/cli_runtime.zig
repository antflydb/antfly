// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Non-server CLI surfaces compiled together as one coarse codegen unit.

const std = @import("std");
const client = @import("client_runtime.zig");

pub fn runFromIterator(
    init: std.process.Init,
    command: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    return client.runFromIterator(init, command, args);
}
