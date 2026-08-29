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
const index_readiness = @import("index_readiness.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        cli.fatal("agents requires a subcommand: retrieval, query-builder", .{});
    };

    if (std.mem.eql(u8, subcommand, "retrieval")) return retrieval(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "query-builder")) return queryBuilder(allocator, io, client, args);

    cli.fatal("unknown agents subcommand: {s}", .{subcommand});
}

fn takeUniqueValue(args: *std.process.Args.Iterator, slot: *?[]const u8, flag: []const u8) void {
    if (slot.* != null) cli.fatal("{s} may only be provided once", .{flag});
    slot.* = args.next() orelse cli.fatal("{s} requires a value", .{flag});
}

fn takeUniqueSwitch(seen: *bool, flag: []const u8) void {
    if (seen.*) cli.fatal("{s} may only be provided once", .{flag});
    seen.* = true;
}

fn retrieval(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;
    var semantic_search: ?[]const u8 = null;
    var full_text_search: ?[]const u8 = null;
    var indexes_str: ?[]const u8 = null;
    var fields_str: ?[]const u8 = null;
    var reranker_json: ?[]const u8 = null;
    var pruner_json: ?[]const u8 = null;
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
    var limit_set = false;
    var streaming_set = false;
    var classify_set = false;
    var reasoning_set = false;
    var generate_set = false;
    var followup_set = false;
    var confidence_set = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            takeUniqueValue(args, &table_name, arg);
        } else if (std.mem.eql(u8, arg, "--generator")) {
            takeUniqueValue(args, &generator_json, arg);
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            takeUniqueValue(args, &semantic_search, arg);
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            takeUniqueValue(args, &full_text_search, arg);
        } else if (std.mem.eql(u8, arg, "--indexes") or std.mem.eql(u8, arg, "-i")) {
            takeUniqueValue(args, &indexes_str, arg);
        } else if (std.mem.eql(u8, arg, "--fields")) {
            takeUniqueValue(args, &fields_str, arg);
        } else if (std.mem.eql(u8, arg, "--reranker")) {
            takeUniqueValue(args, &reranker_json, arg);
        } else if (std.mem.eql(u8, arg, "--pruner")) {
            takeUniqueValue(args, &pruner_json, arg);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            takeUniqueSwitch(&limit_set, arg);
            const raw = args.next() orelse cli.fatal("--limit requires a value", .{});
            limit = std.fmt.parseInt(i64, raw, 10) catch cli.fatal("invalid --limit value: {s}", .{raw});
            if (limit <= 0) cli.fatal("--limit must be greater than zero", .{});
        } else if (std.mem.eql(u8, arg, "--prompt")) {
            takeUniqueValue(args, &prompt, arg);
        } else if (std.mem.eql(u8, arg, "--system-prompt")) {
            takeUniqueValue(args, &system_prompt, arg);
        } else if (std.mem.eql(u8, arg, "--max-context-tokens")) {
            if (max_context_tokens != null) cli.fatal("--max-context-tokens may only be provided once", .{});
            const raw = args.next() orelse cli.fatal("--max-context-tokens requires a value", .{});
            max_context_tokens = std.fmt.parseInt(i64, raw, 10) catch cli.fatal("invalid --max-context-tokens value: {s}", .{raw});
            if (max_context_tokens.? <= 0) cli.fatal("--max-context-tokens must be greater than zero", .{});
        } else if (std.mem.eql(u8, arg, "--streaming")) {
            takeUniqueSwitch(&streaming_set, arg);
            streaming = true;
        } else if (std.mem.eql(u8, arg, "--no-streaming")) {
            takeUniqueSwitch(&streaming_set, arg);
            streaming = false;
        } else if (std.mem.eql(u8, arg, "--classify")) {
            takeUniqueSwitch(&classify_set, arg);
            classify = true;
        } else if (std.mem.eql(u8, arg, "--reasoning")) {
            takeUniqueSwitch(&reasoning_set, arg);
            reasoning = true;
        } else if (std.mem.eql(u8, arg, "--generate")) {
            takeUniqueSwitch(&generate_set, arg);
            generate = true;
        } else if (std.mem.eql(u8, arg, "--followup")) {
            takeUniqueSwitch(&followup_set, arg);
            followup = true;
        } else if (std.mem.eql(u8, arg, "--confidence")) {
            takeUniqueSwitch(&confidence_set, arg);
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

    var reranker_value: ?std.json.Parsed(antfly_client.types.RerankerConfig) = null;
    defer if (reranker_value) |*parsed| parsed.deinit();
    if (reranker_json) |raw| reranker_value = parseJsonArg(antfly_client.types.RerankerConfig, allocator, "--reranker", raw);

    var pruner_value: ?std.json.Parsed(antfly_client.types.Pruner) = null;
    defer if (pruner_value) |*parsed| parsed.deinit();
    if (pruner_json) |raw| pruner_value = parseJsonArg(antfly_client.types.Pruner, allocator, "--pruner", raw);

    const retrieval_query = antfly_client.types.RetrievalQueryRequest{
        .table = table,
        .full_text_search = if (full_text_value) |*parsed| parsed.value else null,
        .semantic_search = semantic_search,
        .indexes = indexes,
        .fields = fields,
        .limit = limit,
        .reranker = if (reranker_value) |*parsed| parsed.value else null,
        .pruner = if (pruner_value) |*parsed| parsed.value else null,
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

    if (semantic_search != null) index_readiness.warnIfSelectedSemanticIndexesAreNotReadyForRetrieval(client, table, indexes);
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
            takeUniqueValue(args, &intent, arg);
        } else if (std.mem.eql(u8, arg, "--table")) {
            takeUniqueValue(args, &table_name, arg);
        } else if (std.mem.eql(u8, arg, "--generator")) {
            takeUniqueValue(args, &generator_json, arg);
        } else {
            cli.fatal("unknown agents query-builder flag: {s}", .{arg});
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
