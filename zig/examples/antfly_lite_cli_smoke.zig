// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

const max_output_bytes = 256 * 1024;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next();
    const antfly_path = args.next() orelse return error.MissingAntflyPath;
    if (args.next() != null) return error.UnexpectedArgument;

    const allocator = init.gpa;
    const io = init.io;
    const pid: u32 = @intCast(std.posix.system.getpid());
    const root = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/lite-cli-smoke-{d}", .{pid});
    defer allocator.free(root);
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};

    const db_path = try join(allocator, root, "app.aflite");
    defer allocator.free(db_path);
    const restored_path = try join(allocator, root, "restored.aflite");
    defer allocator.free(restored_path);
    const backup_path = try join(allocator, root, "app.afb");
    defer allocator.free(backup_path);
    const index_path = try join(allocator, root, "index.json");
    defer allocator.free(index_path);
    const batch_path = try join(allocator, root, "batch.json");
    defer allocator.free(batch_path);
    const lookup_path = try join(allocator, root, "lookup.json");
    defer allocator.free(lookup_path);
    const scan_path = try join(allocator, root, "scan.json");
    defer allocator.free(scan_path);
    const query_path = try join(allocator, root, "query.json");
    defer allocator.free(query_path);

    try writeFile(io, index_path,
        \\{"name":"ft_body","kind":"full_text","config_json":"{}"}
    );
    try writeFile(io, batch_path,
        \\{"inserts":{"doc:cli-smoke":{"title":"CLI smoke","body":"native lite command smoke"}},"sync_level":"full_index"}
    );
    try writeFile(io, lookup_path,
        \\{"fields":["title","body"]}
    );
    try writeFile(io, scan_path,
        \\{"from":"doc:","to":"doc;","include_documents":true,"limit":10}
    );
    try writeFile(io, query_path,
        \\{"full_text_search":{"match":{"field":"body","text":"command smoke"}},"limit":1}
    );

    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "init", db_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "status", db_path }, "\"engine\":\"native_single_file\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "batch", db_path, "--file", batch_path }, "\"inserted\":1");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "run-until-idle", db_path }, "\"has_async_indexes\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", db_path, "--key", "doc:cli-smoke", "--file", lookup_path }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "scan", db_path, "--file", scan_path }, "doc:cli-smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", db_path, "--file", query_path }, "\"status\":200");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "check", db_path }, "\"valid\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "backup", db_path, "--out", backup_path }, "\"format\":\"afb\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "restore", backup_path, "--out", restored_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", restored_path, "--key", "doc:cli-smoke" }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "vacuum", restored_path }, "\"after_size\":");
}

fn join(allocator: std.mem.Allocator, root: []const u8, name: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root, name });
}

fn writeFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = contents });
}

fn expectCommandContains(
    allocator: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
    expected: []const u8,
) !void {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(max_output_bytes),
        .stderr_limit = .limited(max_output_bytes),
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| if (code != 0) {
            printCommandFailure(argv, result.stdout, result.stderr);
            return error.CommandFailed;
        },
        else => {
            printCommandFailure(argv, result.stdout, result.stderr);
            return error.CommandFailed;
        },
    }

    if (std.mem.indexOf(u8, result.stdout, expected) == null) {
        std.debug.print("expected command output to contain: {s}\n", .{expected});
        printCommandFailure(argv, result.stdout, result.stderr);
        return error.UnexpectedOutput;
    }
}

fn printCommandFailure(argv: []const []const u8, stdout: []const u8, stderr: []const u8) void {
    std.debug.print("command failed:", .{});
    for (argv) |arg| std.debug.print(" {s}", .{arg});
    std.debug.print("\nstdout:\n{s}\nstderr:\n{s}\n", .{ stdout, stderr });
}
