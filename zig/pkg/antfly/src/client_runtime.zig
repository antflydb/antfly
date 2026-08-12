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

//! Remote client commands compiled independently from the final CLI dispatcher.

const std = @import("std");
const cli = @import("cmd/cli/mod.zig");
const httpx = @import("httpx");

pub fn runFromIterator(
    init: std.process.Init,
    command: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    if (helpRequested(args)) {
        cli.printCommandUsage(command);
        return;
    }

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var http = httpx.Client.initWithConfig(init.gpa, io, .{});
    defer http.deinit();

    const config = cli.parseGlobalFlags();
    var client = try cli.initClient(init.gpa, &http, config);
    defer client.deinit();

    if (std.mem.eql(u8, command, "table")) return cli.table.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "index")) return cli.index.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "artifact")) return cli.artifact.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "query")) return cli.query.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "lookup")) return cli.query.lookup(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "load")) return cli.data.load(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "insert")) return cli.data.insert(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "delete")) return cli.data.delete(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "agents")) return cli.agents.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "backup")) return cli.backup.runBackup(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "restore")) return cli.backup.runRestore(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "auth")) return cli.auth.run(init.gpa, io, &client, args);
    if (std.mem.eql(u8, command, "internal")) return cli.internal.run(init.gpa, io, &client, args);
    return error.InvalidArguments;
}

fn helpRequested(args: *std.process.Args.Iterator) bool {
    var probe = args.*;
    var first = true;
    while (probe.next()) |arg| {
        if (cli.isHelpArg(arg)) return true;
        if (first and std.mem.eql(u8, arg, "help")) return true;
        first = false;
    }
    return false;
}

test "client runtime recognizes help without consuming arguments" {
    var argv = [_][*:0]const u8{ "--table", "docs", "--help" };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expect(helpRequested(&args));
    try std.testing.expectEqualStrings("--table", args.next().?);
}
