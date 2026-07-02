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

const std = @import("std");
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        return listArtifactsWithFirstArg(allocator, io, client, null, args);
    };

    if (std.mem.eql(u8, subcommand, "list")) return listArtifactsWithFirstArg(allocator, io, client, null, args);
    if (std.mem.eql(u8, subcommand, "get")) return getArtifact(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "put")) return putArtifactEnrichment(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "delete")) return deleteArtifactEnrichment(client, args);
    if (std.mem.eql(u8, subcommand, "reprocess")) return reprocessArtifact(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "job")) return artifactJob(allocator, io, client, args);

    if (std.mem.startsWith(u8, subcommand, "--")) {
        return listArtifactsWithFirstArg(allocator, io, client, subcommand, args);
    }

    cli.fatal("unknown artifact subcommand: {s}", .{subcommand});
}

fn listArtifactsWithFirstArg(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, first_arg: ?[]const u8, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var raw = false;
    var current_arg = first_arg;

    while (true) {
        const arg = if (current_arg) |value| value else args.next() orelse break;
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--raw")) {
            raw = true;
        }
        current_arg = null;
    }

    const table = table_name orelse cli.fatal("--table is required", .{});
    if (key) |doc_key| {
        var resp = try client.listDocumentArtifactManifests(table, doc_key, .{ .detail = if (raw) "raw" else "summary" });
        defer resp.deinit();
        try cli.printResponse(allocator, io, &resp);
        return;
    }

    var resp = try client.listArtifactEnrichments(table);
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn getArtifact(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var artifact_name: ?[]const u8 = null;
    var raw = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--artifact") or std.mem.eql(u8, arg, "-a")) {
            artifact_name = args.next();
        } else if (std.mem.eql(u8, arg, "--raw")) {
            raw = true;
        }
    }

    const table = table_name orelse cli.fatal("--table is required", .{});
    const doc_key = key orelse cli.fatal("--key is required", .{});
    const artifact = artifact_name orelse cli.fatal("--artifact is required", .{});
    var resp = try client.getDocumentArtifactManifest(table, doc_key, artifact, .{ .detail = if (raw) "raw" else "summary" });
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn putArtifactEnrichment(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var artifact_name: ?[]const u8 = null;
    var file_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--artifact") or std.mem.eql(u8, arg, "-a")) {
            artifact_name = args.next();
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        }
    }

    const table = table_name orelse cli.fatal("--table is required", .{});
    const artifact = artifact_name orelse cli.fatal("--artifact is required", .{});
    const path = file_path orelse cli.fatal("--file is required", .{});
    const file_data = cli.readFileAlloc(io, allocator, path, 10 * 1024 * 1024) catch |err| {
        cli.fatal("reading artifact enrichment file {s}: {}", .{ path, err });
    };
    defer allocator.free(file_data);
    var parsed_value = std.json.parseFromSlice(std.json.Value, allocator, file_data, .{}) catch |err| {
        cli.fatal("parsing artifact enrichment file {s}: {}", .{ path, err });
    };
    defer parsed_value.deinit();
    if (parsed_value.value != .object) cli.fatal("artifact enrichment file {s} must contain a JSON object", .{path});
    const normalized_json = normalizeEnrichmentJson(allocator, artifact, parsed_value.value.object) catch |err| {
        cli.fatal("normalizing artifact enrichment file {s}: {}", .{ path, err });
    };
    defer allocator.free(normalized_json);

    var parsed = std.json.parseFromSlice(antfly_client.types.EnrichmentConfig, allocator, normalized_json, .{}) catch |err| {
        cli.fatal("parsing artifact enrichment file {s}: {}", .{ path, err });
    };
    defer parsed.deinit();

    var resp = try client.putArtifactEnrichment(table, artifact, parsed.value);
    defer resp.deinit();
    cli.expectHttpSuccess(&resp);
    std.debug.print("Put artifact enrichment command successful.\n", .{});
}

fn normalizeEnrichmentJson(allocator: std.mem.Allocator, artifact: []const u8, object: std.json.ObjectMap) ![]u8 {
    if (object.contains("name")) return try std.json.Stringify.valueAlloc(allocator, std.json.Value{ .object = object }, .{});

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"name\":");
    const artifact_json = try std.json.Stringify.valueAlloc(allocator, artifact, .{});
    defer allocator.free(artifact_json);
    try writer.writeAll(artifact_json);

    var it = object.iterator();
    while (it.next()) |entry| {
        try writer.writeAll(",");
        const key_json = try std.json.Stringify.valueAlloc(allocator, entry.key_ptr.*, .{});
        defer allocator.free(key_json);
        try writer.writeAll(key_json);
        try writer.writeAll(":");
        const value_json = try std.json.Stringify.valueAlloc(allocator, entry.value_ptr.*, .{});
        defer allocator.free(value_json);
        try writer.writeAll(value_json);
    }
    try writer.writeAll("}");
    return try out.toOwnedSlice();
}

fn deleteArtifactEnrichment(client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var artifact_name: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--artifact") or std.mem.eql(u8, arg, "-a")) {
            artifact_name = args.next();
        }
    }

    const table = table_name orelse cli.fatal("--table is required", .{});
    const artifact = artifact_name orelse cli.fatal("--artifact is required", .{});
    var resp = try client.deleteArtifactEnrichment(table, artifact);
    defer resp.deinit();
    cli.expectHttpSuccess(&resp);
    std.debug.print("Delete artifact enrichment command successful.\n", .{});
}

fn reprocessArtifact(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var artifact_name: ?[]const u8 = null;
    var from_key: ?[]const u8 = null;
    var to_key: ?[]const u8 = null;
    var limit: ?i64 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--artifact") or std.mem.eql(u8, arg, "-a")) {
            artifact_name = args.next();
        } else if (std.mem.eql(u8, arg, "--from-key")) {
            from_key = args.next();
        } else if (std.mem.eql(u8, arg, "--to-key")) {
            to_key = args.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            limit = parseIntFlag(args.next(), "--limit");
        }
    }

    const table = table_name orelse cli.fatal("--table is required", .{});
    const artifact = artifact_name orelse cli.fatal("--artifact is required", .{});
    if (key) |doc_key| {
        var resp = try client.reprocessDocumentArtifact(table, doc_key, artifact);
        defer resp.deinit();
        try cli.printResponse(allocator, io, &resp);
        return;
    }

    var resp = try client.reprocessDocumentArtifactRange(table, artifact, .{
        .from_key = from_key,
        .to_key = to_key,
        .limit = limit,
    });
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn artifactJob(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse cli.fatal("artifact job subcommand is required", .{});
    if (std.mem.eql(u8, subcommand, "start")) return startJob(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "get")) return getJob(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "advance")) return advanceJob(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "cancel")) return cancelJob(allocator, io, client, args);
    cli.fatal("unknown artifact job subcommand: {s}", .{subcommand});
}

fn startJob(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = try parseJobArgs(args, true);
    var resp = try client.startDocumentArtifactReprocessJob(opts.table, opts.artifact, .{
        .from_key = opts.from_key,
        .to_key = opts.to_key,
        .limit = opts.limit,
        .advance = opts.advance,
    });
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn getJob(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = try parseJobArgs(args, false);
    var resp = try client.getDocumentArtifactReprocessJob(opts.table, opts.artifact, opts.job_id.?);
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn advanceJob(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = try parseJobArgs(args, false);
    var resp = try client.advanceDocumentArtifactReprocessJob(opts.table, opts.artifact, opts.job_id.?);
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

fn cancelJob(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = try parseJobArgs(args, false);
    var resp = try client.cancelDocumentArtifactReprocessJob(opts.table, opts.artifact, opts.job_id.?);
    defer resp.deinit();
    try cli.printResponse(allocator, io, &resp);
}

const JobArgs = struct {
    table: []const u8,
    artifact: []const u8,
    job_id: ?[]const u8 = null,
    from_key: ?[]const u8 = null,
    to_key: ?[]const u8 = null,
    limit: ?i64 = null,
    advance: ?bool = null,
};

fn parseJobArgs(args: *std.process.Args.Iterator, start: bool) !JobArgs {
    var table_name: ?[]const u8 = null;
    var artifact_name: ?[]const u8 = null;
    var job_id: ?[]const u8 = null;
    var from_key: ?[]const u8 = null;
    var to_key: ?[]const u8 = null;
    var limit: ?i64 = null;
    var advance: ?bool = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--artifact") or std.mem.eql(u8, arg, "-a")) {
            artifact_name = args.next();
        } else if (std.mem.eql(u8, arg, "--job")) {
            job_id = args.next();
        } else if (std.mem.eql(u8, arg, "--from-key")) {
            from_key = args.next();
        } else if (std.mem.eql(u8, arg, "--to-key")) {
            to_key = args.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            limit = parseIntFlag(args.next(), "--limit");
        } else if (std.mem.eql(u8, arg, "--advance")) {
            advance = true;
        } else if (std.mem.eql(u8, arg, "--no-advance")) {
            advance = false;
        }
    }

    if (!start and job_id == null) cli.fatal("--job is required", .{});
    return .{
        .table = table_name orelse cli.fatal("--table is required", .{}),
        .artifact = artifact_name orelse cli.fatal("--artifact is required", .{}),
        .job_id = job_id,
        .from_key = from_key,
        .to_key = to_key,
        .limit = limit,
        .advance = advance,
    };
}

fn parseIntFlag(raw: ?[]const u8, flag_name: []const u8) i64 {
    const value = raw orelse cli.fatal("{s} requires a value", .{flag_name});
    return std.fmt.parseInt(i64, value, 10) catch cli.fatal("invalid integer for {s}: {s}", .{ flag_name, value });
}
