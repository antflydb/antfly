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
    var indexes_str: ?[]const u8 = null;
    var fields_str: ?[]const u8 = null;
    var limit: i64 = 5;
    var prompt: ?[]const u8 = null;
    var system_prompt: ?[]const u8 = null;
    var streaming = true;
    var classify = false;
    var reasoning = false;
    var generate = false;
    var followup = false;
    var confidence = false;
    var max_context_tokens: ?i64 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (std.mem.eql(u8, arg, "--generator")) {
            generator_json = args.next();
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            semantic_search = args.next();
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            full_text_search = args.next();
        } else if (std.mem.eql(u8, arg, "--indexes") or std.mem.eql(u8, arg, "-i")) {
            indexes_str = args.next();
        } else if (std.mem.eql(u8, arg, "--fields")) {
            fields_str = args.next();
        } else if (std.mem.eql(u8, arg, "--limit")) {
            if (args.next()) |s| limit = std.fmt.parseInt(i64, s, 10) catch limit;
        } else if (std.mem.eql(u8, arg, "--prompt")) {
            prompt = args.next();
        } else if (std.mem.eql(u8, arg, "--system-prompt")) {
            system_prompt = args.next();
        } else if (std.mem.eql(u8, arg, "--max-context-tokens")) {
            if (args.next()) |s| max_context_tokens = std.fmt.parseInt(i64, s, 10) catch |err| {
                cli.fatal("invalid --max-context-tokens: {}", .{err});
            };
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
        } else {
            cli.fatal("unknown agents retrieval flag: {s}", .{arg});
        }
    }

    const gen_json = generator_json orelse cli.fatal("--generator is required", .{});
    const table = table_name orelse cli.fatal("--table is required", .{});
    if (semantic_search == null and full_text_search == null) {
        cli.fatal("one of --semantic-search or --full-text-search is required", .{});
    }
    const query_text = prompt orelse semantic_search orelse full_text_search orelse "";

    var generator_value = parseJsonArg(antfly_client.types.GeneratorConfig, allocator, "--generator", gen_json);
    defer generator_value.deinit();

    var full_text_value: ?std.json.Parsed(std.json.Value) = null;
    defer if (full_text_value) |*parsed| parsed.deinit();
    if (full_text_search) |q| full_text_value = buildFullTextSearchValue(allocator, q);

    var fields: ?[]const []const u8 = null;
    defer if (fields) |slice| allocator.free(slice);
    if (fields_str) |raw| fields = try cli.splitCommaListAlloc(allocator, raw);

    var indexes: ?[]const []const u8 = null;
    defer if (indexes) |slice| allocator.free(slice);
    if (indexes_str) |raw| indexes = try cli.splitCommaListAlloc(allocator, raw);

    const retrieval_query = antfly_client.types.RetrievalQueryRequest{
        .table = table,
        .full_text_search = if (full_text_value) |*parsed| parsed.value else null,
        .semantic_search = semantic_search,
        .indexes = indexes,
        .fields = fields,
        .limit = limit,
    };
    const queries = [_]antfly_client.types.RetrievalQueryRequest{retrieval_query};

    const steps = antfly_client.types.RetrievalAgentSteps{
        .classification = .{
            .enabled = classify or reasoning,
            .with_reasoning = reasoning,
        },
        .generation = .{
            .enabled = generate,
            .system_prompt = system_prompt,
        },
        .followup = .{
            .enabled = followup,
        },
        .confidence = .{
            .enabled = confidence,
        },
    };

    const body = antfly_client.types.RetrievalAgentRequest{
        .query = query_text,
        .queries = queries[0..],
        .max_context_tokens = max_context_tokens,
        .stream = streaming,
        .generator = generator_value.value,
        .steps = steps,
    };

    var resp = try client.retrievalAgent(body);
    defer resp.deinit();
    if (resp.body) |response_body| {
        cli.writeStdout(io, response_body);
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

    var generator_value = parseJsonArg(antfly_client.types.GeneratorConfig, allocator, "--generator", gen_json);
    defer generator_value.deinit();

    const body = antfly_client.types.QueryBuilderRequest{
        .intent = i,
        .table = table_name,
        .generator = generator_value.value,
    };

    var resp = try client.queryBuilder(body);
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}

fn buildFullTextSearchValue(allocator: std.mem.Allocator, query: []const u8) std.json.Parsed(std.json.Value) {
    const escaped = std.json.Stringify.valueAlloc(allocator, query, .{}) catch |err| {
        cli.fatal("failed to encode --full-text-search: {}", .{err});
    };
    defer allocator.free(escaped);

    const json_body = std.fmt.allocPrint(allocator, "{{\"query\":{s}}}", .{escaped}) catch |err| {
        cli.fatal("failed to build --full-text-search value: {}", .{err});
    };
    defer allocator.free(json_body);

    return parseJsonArg(std.json.Value, allocator, "--full-text-search", json_body);
}

fn parseJsonArg(comptime T: type, allocator: std.mem.Allocator, flag: []const u8, raw: []const u8) std.json.Parsed(T) {
    return std.json.parseFromSlice(T, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| {
        cli.fatal("invalid JSON for {s}: {}", .{ flag, err });
    };
}
