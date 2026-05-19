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
        cli.fatal("agents requires a subcommand: retrieval, query-builder", .{});
    };

    if (std.mem.eql(u8, subcommand, "retrieval")) return retrieval(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "query-builder")) return queryBuilder(allocator, io, client, args);

    cli.fatal("unknown agents subcommand: {s}", .{subcommand});
}

fn retrieval(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;
    var semantic_search: ?[]const u8 = null;
    var full_text_search: ?[]const u8 = null;
    var prompt: ?[]const u8 = null;
    var system_prompt: ?[]const u8 = null;
    var streaming = true;
    var classify = false;
    var reasoning = false;
    var generate = false;
    var followup = false;
    var confidence = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--generator")) {
            generator_json = args.next();
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            semantic_search = args.next();
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            full_text_search = args.next();
        } else if (std.mem.eql(u8, arg, "--prompt")) {
            prompt = args.next();
        } else if (std.mem.eql(u8, arg, "--system-prompt")) {
            system_prompt = args.next();
        } else if (std.mem.eql(u8, arg, "--streaming")) {
            streaming = true;
        } else if (std.mem.eql(u8, arg, "--no-streaming")) {
            streaming = false;
        } else if (std.mem.eql(u8, arg, "--classify")) {
            classify = true;
        } else if (std.mem.eql(u8, arg, "--reasoning")) {
            reasoning = true;
        } else if (std.mem.eql(u8, arg, "--generate")) {
            generate = true;
        } else if (std.mem.eql(u8, arg, "--followup")) {
            followup = true;
        } else if (std.mem.eql(u8, arg, "--confidence")) {
            confidence = true;
        }
    }

    const gen_json = generator_json orelse cli.fatal("--generator is required", .{});
    const table = table_name orelse cli.fatal("--table is required", .{});
    if (semantic_search == null and full_text_search == null) {
        cli.fatal("one of --semantic-search or --full-text-search is required", .{});
    }
    const query_text = prompt orelse semantic_search orelse full_text_search orelse "";

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    try writer.print("\"generator\":{s}", .{gen_json});
    try writer.writeAll(",\"query\":");
    try std.json.Stringify.value(query_text, .{}, writer);
    try writer.print(",\"stream\":{}", .{streaming});
    try writer.writeAll(",\"queries\":[{");
    try writer.writeAll("\"table\":");
    try std.json.Stringify.value(table, .{}, writer);
    if (semantic_search) |s| {
        try writer.writeAll(",\"semantic_search\":");
        try std.json.Stringify.value(s, .{}, writer);
    }
    if (full_text_search) |f| {
        try writer.writeAll(",\"full_text_search\":{\"query\":");
        try std.json.Stringify.value(f, .{}, writer);
        try writer.writeAll("}");
    }
    try writer.writeAll(",\"limit\":5");
    try writer.writeAll("}]");

    try writer.writeAll(",\"steps\":{");
    try writer.print("\"classification\":{{\"enabled\":{},\"with_reasoning\":{}}}", .{ classify or reasoning, reasoning });
    try writer.writeAll(",\"generation\":{");
    try writer.print("\"enabled\":{}", .{generate});
    if (system_prompt) |sp| {
        try writer.writeAll(",\"system_prompt\":");
        try std.json.Stringify.value(sp, .{}, writer);
    }
    try writer.writeAll("}");
    try writer.print(",\"followup\":{{\"enabled\":{}}}", .{followup});
    try writer.print(",\"confidence\":{{\"enabled\":{}}}", .{confidence});
    try writer.writeAll("}");

    try writer.writeAll("}");

    const json_body = out.written();

    var parsed = std.json.parseFromSlice(antfly_client.types.RetrievalAgentRequest, allocator, json_body, .{ .ignore_unknown_fields = true }) catch |err| {
        cli.fatal("failed to build retrieval agent request: {}", .{err});
    };
    defer parsed.deinit();

    var resp = try client.retrievalAgent(parsed.value);
    defer resp.deinit();
    if (resp.body) |body| {
        cli.writeStdout(io, body);
        cli.writeStdout(io, "\n");
    }
}

fn queryBuilder(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var intent: ?[]const u8 = null;
    var table_name: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--intent")) {
            intent = args.next();
        } else if (std.mem.eql(u8, arg, "--table")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--generator")) {
            generator_json = args.next();
        }
    }

    const i = intent orelse cli.fatal("--intent is required", .{});
    const gen_json = generator_json orelse cli.fatal("--generator is required", .{});

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    try writer.print("\"intent\":\"{s}\"", .{i});
    try writer.print(",\"generator\":{s}", .{gen_json});
    if (table_name) |t| try writer.print(",\"table\":\"{s}\"", .{t});
    try writer.writeAll("}");

    const json_body = out.written();

    var parsed = std.json.parseFromSlice(antfly_client.types.QueryBuilderRequest, allocator, json_body, .{ .ignore_unknown_fields = true }) catch |err| {
        cli.fatal("failed to build query builder request: {}", .{err});
    };
    defer parsed.deinit();

    var resp = try client.queryBuilder(parsed.value);
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}
