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
const plan = @import("plan.zig");
const runner = @import("runner.zig");

pub fn main(init: std.process.Init) !void {
    execute(init) catch |err| {
        std.debug.print("antfly-migrate: {s}\n", .{@errorName(err)});
        if (err == error.PublishedDurabilityUnknown) std.debug.print("The destination was published, but directory durability could not be confirmed. Inspect it before retrying.\n", .{});
        std.process.exit(1);
    };
}

fn execute(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const command = args.next() orelse "--help";
    if (std.mem.eql(u8, command, "--help")) {
        std.debug.print("Usage: antfly-migrate migrate --plan plan.json [--dry-run]\n       antfly-migrate verify --plan seed-plan.json\nPaths in the plan are relative to the plan file. Existing destinations are never replaced.\n", .{});
        return;
    }
    const verify_only = std.mem.eql(u8, command, "verify");
    if (!verify_only and !std.mem.eql(u8, command, "migrate")) return error.UnknownCommand;
    var path: ?[]const u8 = null;
    var dry_run = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--plan")) {
            if (path != null) return error.DuplicateArgument;
            path = args.next() orelse return error.MissingPlan;
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            if (dry_run) return error.DuplicateArgument;
            dry_run = true;
        } else return error.UnknownArgument;
    }
    const plan_path = path orelse return error.MissingPlan;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, plan_path, init.gpa, .limited(4 * 1024 * 1024));
    defer init.gpa.free(bytes);
    const parsed = try std.json.parseFromSlice(plan.Plan, init.gpa, bytes, .{});
    defer parsed.deinit();
    if (verify_only and dry_run) return error.InvalidArguments;
    try runner.run(init, parsed.value, std.fs.path.dirname(plan_path) orelse ".", dry_run, verify_only);
}

test {
    _ = @import("plan.zig");
    _ = @import("sqlite.zig");
    _ = @import("aflite.zig");
    _ = @import("runner.zig");
}
