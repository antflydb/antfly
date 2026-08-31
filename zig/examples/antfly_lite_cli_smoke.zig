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
    const restored_snapshot_path = try join(allocator, root, "restored-snapshot.aflite");
    defer allocator.free(restored_snapshot_path);
    const imported_path = try join(allocator, root, "imported.aflite");
    defer allocator.free(imported_path);
    const backup_path = try join(allocator, root, "app.afb");
    defer allocator.free(backup_path);
    const schema_path = try join(allocator, root, "schema.json");
    defer allocator.free(schema_path);
    const enrichment_path = try join(allocator, root, "enrichment.json");
    defer allocator.free(enrichment_path);
    const temp_enrichment_path = try join(allocator, root, "temp-enrichment.json");
    defer allocator.free(temp_enrichment_path);
    const text_index_path = try join(allocator, root, "text-index.json");
    defer allocator.free(text_index_path);
    const temp_index_path = try join(allocator, root, "temp-index.json");
    defer allocator.free(temp_index_path);
    const dense_index_path = try join(allocator, root, "dense-index.json");
    defer allocator.free(dense_index_path);
    const sparse_index_path = try join(allocator, root, "sparse-index.json");
    defer allocator.free(sparse_index_path);
    const graph_index_path = try join(allocator, root, "graph-index.json");
    defer allocator.free(graph_index_path);
    const batch_path = try join(allocator, root, "batch.json");
    defer allocator.free(batch_path);
    const lookup_path = try join(allocator, root, "lookup.json");
    defer allocator.free(lookup_path);
    const scan_path = try join(allocator, root, "scan.json");
    defer allocator.free(scan_path);
    const text_query_path = try join(allocator, root, "text-query.json");
    defer allocator.free(text_query_path);
    const dense_query_path = try join(allocator, root, "dense-query.json");
    defer allocator.free(dense_query_path);
    const sparse_query_path = try join(allocator, root, "sparse-query.json");
    defer allocator.free(sparse_query_path);
    const graph_query_path = try join(allocator, root, "graph-query.json");
    defer allocator.free(graph_query_path);

    try writeFile(io, schema_path,
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","required":["title"],"additionalProperties":true}}}}
    );
    try writeFile(io, enrichment_path,
        \\{"name":"cli_chunks","kind":"chunk","field":"body","chunk_size":64,"chunk_overlap":8}
    );
    try writeFile(io, temp_enrichment_path,
        \\{"name":"cli_temp_chunks","kind":"chunk","field":"body","chunk_size":32,"chunk_overlap":4}
    );
    try writeFile(io, text_index_path,
        \\{"name":"ft_body","kind":"full_text","config_json":"{}"}
    );
    try writeFile(io, temp_index_path,
        \\{"name":"ft_temp","kind":"full_text","config_json":"{}"}
    );
    try writeFile(io, dense_index_path,
        \\{"name":"dv_cli","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}"}
    );
    try writeFile(io, sparse_index_path,
        \\{"name":"sv_cli","kind":"sparse_vector","config_json":"{\"field\":\"sparse_embedding\",\"external\":true}"}
    );
    try writeFile(io, graph_index_path,
        \\{"name":"gr_cli","kind":"graph","config_json":"{}"}
    );
    try writeFile(io, batch_path,
        \\{"inserts":{"doc:cli-smoke":{"title":"CLI smoke","body":"native lite command smoke"},"doc:cli-vec-a":{"title":"vector alpha","body":"dense sparse graph source","_embeddings":{"dv_cli":[1,0,0],"sv_cli":{"indices":[7,42],"values":[1.5,0.5]}},"_edges":{"gr_cli":{"links":[{"target":"doc:cli-vec-c","weight":1.0}]}}},"doc:cli-vec-b":{"title":"vector beta","_embeddings":{"dv_cli":[0,1,0],"sv_cli":{"indices":[99],"values":[2.0]}}},"doc:cli-vec-c":{"title":"graph target"}},"sync_level":"full_index"}
    );
    try writeFile(io, lookup_path,
        \\{"fields":["title","body"]}
    );
    try writeFile(io, scan_path,
        \\{"from":"doc:","to":"doc;","include_documents":true,"limit":10}
    );
    try writeFile(io, text_query_path,
        \\{"full_text_search":{"match":{"field":"body","text":"command smoke"}},"limit":1}
    );
    try writeFile(io, dense_query_path,
        \\{"embeddings":{"dv_cli":[1,0,0]},"indexes":["dv_cli"],"limit":1}
    );
    try writeFile(io, sparse_query_path,
        \\{"embeddings":{"sv_cli":{"indices":[7,42],"values":[1.5,0.5]}},"indexes":["sv_cli"],"limit":1}
    );
    try writeFile(io, graph_query_path,
        \\{"graph_queries":{"neighbors":{"index":"gr_cli","traverse":{"start":{"keys":["doc:cli-vec-a"]},"edge_types":["links"],"max_depth":1}}},"limit":10}
    );

    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "init", db_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "status", db_path }, "\"engine\":\"native_single_file\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "schema", "set", db_path, "--file", schema_path }, "\"updated\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "schema", "get", db_path }, "\"required\":[\"title\"]");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "enrichment", "create", db_path, "--file", enrichment_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "enrichment", "create", db_path, "--file", temp_enrichment_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "enrichment", "list", db_path }, "cli_chunks");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "enrichment", "drop", db_path, "--kind", "chunk", "--name", "cli_temp_chunks" }, "\"removed\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", text_index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", temp_index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "list", db_path }, "ft_temp");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "drop", db_path, "--index", "ft_temp" }, "\"removed\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", dense_index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", sparse_index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "create", db_path, "--file", graph_index_path }, "\"created\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "index", "list", db_path }, "gr_cli");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "batch", db_path, "--file", batch_path }, "\"inserted\":4");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "run-until-idle", db_path }, "\"has_async_indexes\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", db_path, "--key", "doc:cli-smoke", "--file", lookup_path }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "scan", db_path, "--file", scan_path }, "doc:cli-smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", db_path, "--file", text_query_path }, "doc:cli-smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", db_path, "--file", dense_query_path }, "doc:cli-vec-a");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", db_path, "--file", sparse_query_path }, "doc:cli-vec-a");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", db_path, "--file", graph_query_path }, "doc:cli-vec-c");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "check", db_path }, "\"valid\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "compact", db_path }, "\"compacted\":true");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "backup", db_path, "--out", backup_path }, "\"format\":\"afb\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "restore", backup_path, "--out", restored_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", restored_path, "--key", "doc:cli-smoke" }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "schema", "get", restored_path }, "\"required\":[\"title\"]");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "enrichment", "list", restored_path }, "cli_chunks");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "init", imported_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "import", imported_path, "--from", backup_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", imported_path, "--key", "doc:cli-smoke" }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", imported_path, "--file", text_query_path }, "doc:cli-smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", imported_path, "--file", dense_query_path }, "doc:cli-vec-a");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "restore", db_path, "--out", restored_snapshot_path }, "\"format\":\"aflite\"");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "lookup", restored_snapshot_path, "--key", "doc:cli-smoke" }, "native lite command smoke");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", restored_snapshot_path, "--file", dense_query_path }, "doc:cli-vec-a");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", restored_snapshot_path, "--file", sparse_query_path }, "doc:cli-vec-a");
    try expectCommandContains(allocator, io, &.{ antfly_path, "lite", "query", restored_snapshot_path, "--file", graph_query_path }, "doc:cli-vec-c");
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
