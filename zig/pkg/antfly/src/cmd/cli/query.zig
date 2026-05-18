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
    var table_name: ?[]const u8 = null;
    var full_text_search: ?[]const u8 = null;
    var full_text_search_json: ?[]const u8 = null;
    var semantic_search: ?[]const u8 = null;
    var fields_str: ?[]const u8 = null;
    var limit: ?i64 = null;
    var offset: ?i64 = null;
    var indexes_str: ?[]const u8 = null;
    var filter_query: ?[]const u8 = null;
    var exclusion_query: ?[]const u8 = null;
    var aggregations_json: ?[]const u8 = null;
    var reranker_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            full_text_search = args.next();
        } else if (std.mem.eql(u8, arg, "--full-text-search-json")) {
            full_text_search_json = args.next();
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            semantic_search = args.next();
        } else if (std.mem.eql(u8, arg, "--fields")) {
            fields_str = args.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            if (args.next()) |s| limit = std.fmt.parseInt(i64, s, 10) catch null;
        } else if (std.mem.eql(u8, arg, "--offset")) {
            if (args.next()) |s| offset = std.fmt.parseInt(i64, s, 10) catch null;
        } else if (std.mem.eql(u8, arg, "--indexes")) {
            indexes_str = args.next();
        } else if (std.mem.eql(u8, arg, "--filter-query")) {
            filter_query = args.next();
        } else if (std.mem.eql(u8, arg, "--exclusion-query")) {
            exclusion_query = args.next();
        } else if (std.mem.eql(u8, arg, "--aggregations")) {
            aggregations_json = args.next();
        } else if (std.mem.eql(u8, arg, "--reranker")) {
            reranker_json = args.next();
        }
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{");
    var first = true;

    if (full_text_search) |q| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"full_text_search\":{{\"query\":\"{s}\"}}", .{q});
        first = false;
    }
    if (full_text_search_json) |q| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"full_text_search\":{s}", .{q});
        first = false;
    }
    if (semantic_search) |q| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"semantic_search\":\"{s}\"", .{q});
        first = false;
    }
    if (limit) |l| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"limit\":{d}", .{l});
        first = false;
    }
    if (offset) |o| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"offset\":{d}", .{o});
        first = false;
    }
    if (fields_str) |f| {
        if (!first) try writer.writeAll(",");
        try writer.writeAll("\"fields\":[");
        var it = std.mem.splitScalar(u8, f, ',');
        var field_first = true;
        while (it.next()) |field| {
            if (!field_first) try writer.writeAll(",");
            try writer.print("\"{s}\"", .{std.mem.trim(u8, field, " ")});
            field_first = false;
        }
        try writer.writeAll("]");
        first = false;
    }
    if (indexes_str) |idx| {
        if (!first) try writer.writeAll(",");
        try writer.writeAll("\"indexes\":[");
        var it = std.mem.splitScalar(u8, idx, ',');
        var idx_first = true;
        while (it.next()) |index_name| {
            if (!idx_first) try writer.writeAll(",");
            try writer.print("\"{s}\"", .{std.mem.trim(u8, index_name, " ")});
            idx_first = false;
        }
        try writer.writeAll("]");
        first = false;
    }
    if (filter_query) |fq| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"filter_query\":{s}", .{fq});
        first = false;
    }
    if (exclusion_query) |eq| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"exclusion_query\":{s}", .{eq});
        first = false;
    }
    if (aggregations_json) |agg| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"aggregations\":{s}", .{agg});
        first = false;
    }
    if (reranker_json) |rr| {
        if (!first) try writer.writeAll(",");
        try writer.print("\"reranker\":{s}", .{rr});
        first = false;
    }

    try writer.writeAll("}");

    const json_body = out.written();
    var parsed = std.json.parseFromSlice(antfly_client.types.QueryRequest, allocator, json_body, .{ .ignore_unknown_fields = true }) catch |err| {
        cli.fatal("failed to build query request: {}", .{err});
    };
    defer parsed.deinit();

    if (table_name) |tbl| {
        var resp = try client.queryTable(tbl, parsed.value);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    } else {
        var resp = try client.query(parsed.value);
        defer resp.deinit();
        if (resp.data) |data| {
            try cli.writeJson(allocator, io, data.value);
        }
    }
}

pub fn lookup(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var key: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--key") or std.mem.eql(u8, arg, "-k")) {
            key = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});

    var resp = try client.lookupKey(tbl, k, .{});
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}
