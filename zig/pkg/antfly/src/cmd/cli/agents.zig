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
        cli.fatal("agents requires a subcommand: retrieval, research, query-builder", .{});
    };

    if (std.mem.eql(u8, subcommand, "retrieval")) return retrieval(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "research")) return research(allocator, io, client, args);
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
    var web_connection: ?[]const u8 = null;
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
    var iterations_arg: ?[]const u8 = null;
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
        } else if (std.mem.eql(u8, arg, "--web-search-connection")) {
            takeUniqueValue(args, &web_connection, arg);
        } else if (std.mem.eql(u8, arg, "--generator")) {
            takeUniqueValue(args, &generator_json, arg);
        } else if (std.mem.eql(u8, arg, "--max-internal-iterations")) {
            takeUniqueValue(args, &iterations_arg, arg);
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
        } else if (std.mem.eql(u8, arg, "--prompt") or std.mem.eql(u8, arg, "--intent")) {
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
    if (table_name == null and web_connection == null) cli.fatal("--table or --web-search-connection is required", .{});
    if (table_name == null and (semantic_search != null or full_text_search != null or indexes_str != null or fields_str != null or reranker_json != null or pruner_json != null)) cli.fatal("database search options require --table", .{});
    const intent_only = semantic_search == null and full_text_search == null;
    if (intent_only and prompt == null) cli.fatal("provide --intent, --semantic-search, or --full-text-search", .{});
    const iterations = try parseIterations(iterations_arg orelse if (intent_only or web_connection != null) "8" else "0");
    if ((intent_only or web_connection != null) and iterations == 0) cli.fatal("--intent without a query requires positive --max-internal-iterations", .{});
    const query_text = prompt orelse semantic_search orelse full_text_search orelse "";

    var generator_value = parseJsonArg(antfly_client.types.GeneratorConfig, allocator, "--generator", gen_json);
    defer generator_value.deinit();

    var full_text_value: ?std.json.Parsed(antfly_client.types.RawQuery) = null;
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

    const retrieval_query = antfly_client.types.QueryRequest{
        .table = table_name,
        .full_text_search = if (full_text_value) |*parsed| parsed.value else null,
        .semantic_search = semantic_search,
        .indexes = indexes,
        .fields = fields,
        .limit = limit,
        .reranker = if (reranker_value) |*parsed| parsed.value else null,
        .pruner = if (pruner_value) |*parsed| parsed.value else null,
    };
    const queries = [_]antfly_client.types.QueryRequest{retrieval_query};

    const steps = antfly_client.types.RetrievalAgentSteps{
        .classification = .{
            .enabled = classify or reasoning,
            .with_reasoning = reasoning,
        },
        .generation = .{
            .enabled = generate or intent_only or web_connection != null,
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
        .queries = if (table_name != null) queries[0..] else &.{},
        .tools = if (web_connection) |name| .{ .web_search_connection = name } else null,
        .max_context_tokens = max_context_tokens,
        .max_internal_iterations = iterations,
        .stream = streaming,
        .generator = generator_value.value,
        .steps = steps,
    };

    if (semantic_search != null) index_readiness.warnIfSelectedSemanticIndexesAreNotReadyForRetrieval(client, table_name.?, indexes);
    return sendRetrieval(allocator, io, client, body);
}

fn sendRetrieval(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, body: antfly_client.types.RetrievalAgentRequest) !void {
    if (body.stream orelse true) {
        var output = StreamingSseWriter{ .io = io };
        var resp = try client.retrievalAgentToWriter(body, &output);
        defer resp.deinit();
        output.detector.finish();
        cli.writeStdout(io, "\n");
        const is_sse = if (resp.content_type) |value|
            std.mem.startsWith(u8, value, "text/event-stream")
        else
            false;
        if (resp.status_code >= 300 or !is_sse or output.detector.failed()) {
            return error.RetrievalAgentResponseError;
        }
    } else {
        var resp = try client.retrievalAgent(body);
        defer resp.deinit();
        const response_failed = resp.status_code >= 300 or
            isSseFailureResponse(resp.content_type, resp.body orelse "") or
            !try retrievalCompleted(allocator, resp.body);
        if (resp.body) |response_body| {
            cli.writeStdout(io, response_body);
            cli.writeStdout(io, "\n");
        }
        if (response_failed) return error.RetrievalAgentResponseError;
    }
}

fn isSseFailureResponse(content_type: ?[]const u8, body: []const u8) bool {
    const value = content_type orelse return false;
    if (!std.mem.startsWith(u8, value, "text/event-stream")) return false;

    var detector = SseCompletionDetector{};
    detector.push(body);
    detector.finish();
    return detector.failed();
}

const SseCompletionDetector = struct {
    const Event = enum { message, done, failure };
    line: [128]u8 = undefined,
    line_len: usize = 0,
    line_overflowed: bool = false,
    skip_lf: bool = false,
    event: Event = .message,
    has_data: bool = false,
    frame_pending: bool = false,
    saw_done: bool = false,
    saw_error: bool = false,

    fn push(self: *@This(), bytes: []const u8) void {
        for (bytes) |byte| {
            if (self.skip_lf) {
                self.skip_lf = false;
                if (byte == '\n') continue;
            }
            if (byte == '\r' or byte == '\n') {
                self.finishLine();
                self.skip_lf = byte == '\r';
            } else if (self.line_len < self.line.len) {
                self.line[self.line_len] = byte;
                self.line_len += 1;
            } else {
                self.line_overflowed = true;
            }
        }
    }

    fn finish(self: *@This()) void {
        // EOF does not dispatch an unterminated event. Parsing the last line
        // only marks its frame pending; success requires a blank delimiter.
        if (self.line_len > 0 or self.line_overflowed) self.finishLine();
    }

    fn failed(self: *const @This()) bool {
        return self.saw_error or !self.saw_done or self.frame_pending;
    }

    fn finishLine(self: *@This()) void {
        defer {
            self.line_len = 0;
            self.line_overflowed = false;
        }
        const line = self.line[0..self.line_len];
        if (line.len == 0 and !self.line_overflowed) {
            if (self.has_data) {
                self.saw_done = self.event == .done;
                self.saw_error = self.saw_error or self.event == .failure;
            }
            self.event = .message;
            self.has_data = false;
            self.frame_pending = false;
            return;
        }
        // Data values need not fit the line buffer: only their field name
        // matters for completion, and event-like strings in data are ignored.
        if (std.mem.eql(u8, line, "data") or std.mem.startsWith(u8, line, "data:")) {
            self.has_data = true;
            self.frame_pending = true;
        } else if (std.mem.eql(u8, line, "event") or std.mem.startsWith(u8, line, "event:")) {
            self.frame_pending = true;
            var name = if (line.len > "event:".len) line["event:".len..] else "";
            if (std.mem.startsWith(u8, name, " ")) name = name[1..];
            self.event = if (!self.line_overflowed and std.mem.eql(u8, name, "done"))
                .done
            else if (!self.line_overflowed and std.mem.eql(u8, name, "error"))
                .failure
            else
                .message;
        }
    }
};

const StreamingSseWriter = struct {
    io: std.Io,
    detector: SseCompletionDetector = .{},

    pub fn writeAll(self: *@This(), bytes: []const u8) !void {
        cli.writeStdout(self.io, bytes);
        self.detector.push(bytes);
    }
};

fn parseBudgetValue(args: *std.process.Args.Iterator, slot: *?i64, flag: []const u8) void {
    if (slot.* != null) cli.fatal("{s} may only be provided once", .{flag});
    const raw = args.next() orelse cli.fatal("{s} requires a value", .{flag});
    slot.* = std.fmt.parseInt(i64, raw, 10) catch cli.fatal("invalid {s} value: {s}", .{ flag, raw });
}

/// `antfly agents research`: run the research agent synchronously (SSE or
/// JSON), or as a durable job advanced phase by phase with `--job`.
fn research(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var question: ?[]const u8 = null;
    var table_name: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;
    var full_text_search: ?[]const u8 = null;
    var semantic_search: ?[]const u8 = null;
    var indexes_str: ?[]const u8 = null;
    var fields_str: ?[]const u8 = null;
    var web_connection: ?[]const u8 = null;
    var fetch_hosts: ?[]const u8 = null;
    var outline_str: ?[]const u8 = null;
    var instructions: ?[]const u8 = null;
    var knowledge: ?[]const u8 = null;
    var resume_job: ?[]const u8 = null;
    var max_rounds: ?i64 = null;
    var max_sub_questions: ?i64 = null;
    var max_parallel: ?i64 = null;
    var researcher_iterations: ?i64 = null;
    var max_llm_calls: ?i64 = null;
    var deadline_ms: ?i64 = null;
    var limit: ?i64 = null;
    var verify = false;
    var job = false;
    var streaming = true;
    var streaming_set = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--query") or std.mem.eql(u8, arg, "--question") or std.mem.eql(u8, arg, "--prompt")) {
            takeUniqueValue(args, &question, arg);
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            takeUniqueValue(args, &table_name, arg);
        } else if (std.mem.eql(u8, arg, "--generator")) {
            takeUniqueValue(args, &generator_json, arg);
        } else if (std.mem.eql(u8, arg, "--full-text-search")) {
            takeUniqueValue(args, &full_text_search, arg);
        } else if (std.mem.eql(u8, arg, "--semantic-search")) {
            takeUniqueValue(args, &semantic_search, arg);
        } else if (std.mem.eql(u8, arg, "--indexes") or std.mem.eql(u8, arg, "-i")) {
            takeUniqueValue(args, &indexes_str, arg);
        } else if (std.mem.eql(u8, arg, "--fields")) {
            takeUniqueValue(args, &fields_str, arg);
        } else if (std.mem.eql(u8, arg, "--web-search-connection")) {
            takeUniqueValue(args, &web_connection, arg);
        } else if (std.mem.eql(u8, arg, "--fetch-allowed-hosts")) {
            takeUniqueValue(args, &fetch_hosts, arg);
        } else if (std.mem.eql(u8, arg, "--outline")) {
            takeUniqueValue(args, &outline_str, arg);
        } else if (std.mem.eql(u8, arg, "--instructions")) {
            takeUniqueValue(args, &instructions, arg);
        } else if (std.mem.eql(u8, arg, "--agent-knowledge")) {
            takeUniqueValue(args, &knowledge, arg);
        } else if (std.mem.eql(u8, arg, "--resume-job")) {
            takeUniqueValue(args, &resume_job, arg);
        } else if (std.mem.eql(u8, arg, "--max-rounds")) {
            parseBudgetValue(args, &max_rounds, arg);
        } else if (std.mem.eql(u8, arg, "--max-sub-questions")) {
            parseBudgetValue(args, &max_sub_questions, arg);
        } else if (std.mem.eql(u8, arg, "--max-parallel")) {
            parseBudgetValue(args, &max_parallel, arg);
        } else if (std.mem.eql(u8, arg, "--researcher-iterations")) {
            parseBudgetValue(args, &researcher_iterations, arg);
        } else if (std.mem.eql(u8, arg, "--max-llm-calls")) {
            parseBudgetValue(args, &max_llm_calls, arg);
        } else if (std.mem.eql(u8, arg, "--deadline-ms")) {
            parseBudgetValue(args, &deadline_ms, arg);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            parseBudgetValue(args, &limit, arg);
        } else if (std.mem.eql(u8, arg, "--verify")) {
            takeUniqueSwitch(&verify, arg);
        } else if (std.mem.eql(u8, arg, "--job")) {
            takeUniqueSwitch(&job, arg);
        } else if (std.mem.eql(u8, arg, "--streaming") or std.mem.eql(u8, arg, "--no-streaming")) {
            takeUniqueSwitch(&streaming_set, arg);
            streaming = std.mem.eql(u8, arg, "--streaming");
        } else {
            cli.fatal("unknown agents research flag: {s}", .{arg});
        }
    }

    if (resume_job) |job_id| return advanceResearchJobUntilDone(allocator, io, client, job_id);
    const q = question orelse cli.fatal("--query is required", .{});
    const gen_json = generator_json orelse cli.fatal("--generator is required", .{});
    if (table_name == null and web_connection == null) cli.fatal("--table or --web-search-connection is required", .{});
    if (job and streaming_set) cli.fatal("streaming flags do not apply to --job", .{});

    var generator_value = parseJsonArg(antfly_client.types.GeneratorConfig, allocator, "--generator", gen_json);
    defer generator_value.deinit();
    var full_text_value: ?std.json.Parsed(antfly_client.types.RawQuery) = null;
    defer if (full_text_value) |*parsed| parsed.deinit();
    if (full_text_search) |text| full_text_value = buildFullTextSearchValue(allocator, text);
    const fields = if (fields_str) |raw| try cli.splitCommaListAlloc(allocator, raw) else null;
    defer if (fields) |slice| allocator.free(slice);
    const indexes = if (indexes_str) |raw| try cli.splitCommaListAlloc(allocator, raw) else null;
    defer if (indexes) |slice| allocator.free(slice);
    const outline = if (outline_str) |raw| try cli.splitCommaListAlloc(allocator, raw) else null;
    defer if (outline) |slice| allocator.free(slice);
    const hosts = if (fetch_hosts) |raw| try cli.splitCommaListAlloc(allocator, raw) else null;
    defer if (hosts) |slice| allocator.free(slice);

    const queries = [_]antfly_client.types.QueryRequest{.{
        .table = table_name,
        .full_text_search = if (full_text_value) |*parsed| parsed.value else null,
        .semantic_search = semantic_search,
        .indexes = indexes,
        .fields = fields,
        .limit = limit orelse 8,
    }};
    const body = antfly_client.types.ResearchAgentRequest{
        .query = q,
        .queries = if (table_name != null) queries[0..] else &.{},
        .agent_knowledge = knowledge,
        .generator = generator_value.value,
        .tools = if (web_connection != null or hosts != null) .{
            .web_search_connection = web_connection,
            .fetch_config = if (hosts) |list| .{ .allowed_hosts = list } else null,
        } else null,
        .budget = .{
            .max_rounds = max_rounds,
            .max_sub_questions = max_sub_questions,
            .max_parallel = max_parallel,
            .researcher_iterations = researcher_iterations,
            .max_llm_calls = max_llm_calls,
            .deadline_ms = deadline_ms,
        },
        .steps = .{
            .write = .{ .outline = outline, .instructions = instructions },
            .verify = if (verify) .{ .enabled = true } else null,
        },
        .stream = if (job) false else streaming,
    };

    if (job) {
        var started = try client.startResearchJob(.{ .request = body, .advance = 1 });
        defer started.deinit();
        const record = started.data orelse return error.ResearchAgentResponseError;
        const job_id = try allocator.dupe(u8, record.value.job_id);
        defer allocator.free(job_id);
        std.debug.print("research job {s}: {s} (phase {s})\n", .{ job_id, @tagName(record.value.state), @tagName(record.value.phase) });
        return advanceResearchJobUntilDone(allocator, io, client, job_id);
    }
    if (streaming) {
        var output = StreamingSseWriter{ .io = io };
        var resp = try client.researchAgentToWriter(body, &output);
        defer resp.deinit();
        output.detector.finish();
        cli.writeStdout(io, "\n");
        const is_sse = if (resp.content_type) |value| std.mem.startsWith(u8, value, "text/event-stream") else false;
        if (resp.status_code >= 300 or !is_sse or output.detector.failed()) return error.ResearchAgentResponseError;
        return;
    }
    var resp = try client.researchAgent(body);
    defer resp.deinit();
    if (resp.body) |response_body| {
        cli.writeStdout(io, response_body);
        cli.writeStdout(io, "\n");
    }
    if (resp.status_code >= 300 or !try retrievalCompleted(allocator, resp.body)) return error.ResearchAgentResponseError;
}

/// Advance one phase per request until the job is terminal, reporting each
/// checkpoint on stderr and the final job (with its report) on stdout.
fn advanceResearchJobUntilDone(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, job_id: []const u8) !void {
    while (true) {
        var advanced = try client.advanceResearchJob(job_id, 1);
        defer advanced.deinit();
        const record = advanced.data orelse return error.ResearchAgentResponseError;
        const state = record.value.state;
        if (advanced.status_code == 409) {
            std.debug.print("research job {s}: another advance is running; waiting\n", .{job_id});
            io.sleep(.fromSeconds(2), .awake) catch {};
            continue;
        }
        std.debug.print("research job {s}: {s} (phase {s}, advances {d})\n", .{ job_id, @tagName(state), @tagName(record.value.phase), record.value.advances orelse 0 });
        switch (state) {
            .queued, .running => continue,
            .succeeded, .failed, .cancelled => {
                try cli.writeJson(allocator, io, record.value);
                if (state != .succeeded) return error.ResearchAgentResponseError;
                return;
            },
        }
    }
}

fn queryBuilder(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var intent: ?[]const u8 = null;
    var table_name: ?[]const u8 = null;
    var generator_json: ?[]const u8 = null;
    var iterations_arg: ?[]const u8 = null;
    var fields_arg: ?[]const u8 = null;
    var mode: ?[]const u8 = null;
    var execute = false;
    var streaming = true;
    var streaming_set = false;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--intent")) {
            takeUniqueValue(args, &intent, arg);
        } else if (std.mem.eql(u8, arg, "--table")) {
            takeUniqueValue(args, &table_name, arg);
        } else if (std.mem.eql(u8, arg, "--generator")) {
            takeUniqueValue(args, &generator_json, arg);
        } else if (std.mem.eql(u8, arg, "--max-internal-iterations")) {
            takeUniqueValue(args, &iterations_arg, arg);
        } else if (std.mem.eql(u8, arg, "--fields")) {
            takeUniqueValue(args, &fields_arg, arg);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            takeUniqueValue(args, &mode, arg);
        } else if (std.mem.eql(u8, arg, "--execute")) {
            takeUniqueSwitch(&execute, arg);
        } else if (std.mem.eql(u8, arg, "--streaming") or std.mem.eql(u8, arg, "--no-streaming")) {
            takeUniqueSwitch(&streaming_set, arg);
            streaming = std.mem.eql(u8, arg, "--streaming");
        } else {
            cli.fatal("unknown agents query-builder flag: {s}", .{arg});
        }
    }

    const i = intent orelse cli.fatal("--intent is required", .{});
    const gen_json = generator_json orelse cli.fatal("--generator is required", .{});

    var generator_value = parseJsonArg(antfly_client.types.GeneratorConfig, allocator, "--generator", gen_json);
    defer generator_value.deinit();

    const iterations = try parseIterations(iterations_arg orelse if (execute) "8" else "0");
    if (execute and table_name == null) cli.fatal("--execute requires --table", .{});
    if (execute and iterations == 0) cli.fatal("--execute requires positive --max-internal-iterations", .{});
    if (streaming_set and !execute) cli.fatal("streaming flags require --execute", .{});
    const fields = if (fields_arg) |value| try cli.splitCommaListAlloc(allocator, value) else null;
    defer if (fields) |value| allocator.free(value);

    if (execute) {
        const knowledge = if (mode) |value| try std.fmt.allocPrint(allocator, "Query planning mode preference: {s}", .{value}) else null;
        defer if (knowledge) |value| allocator.free(value);
        return sendRetrieval(allocator, io, client, .{
            .query = i,
            .queries = &.{.{ .table = table_name, .fields = fields, .limit = 5 }},
            .generator = generator_value.value,
            .agent_knowledge = knowledge,
            .stream = streaming,
            .max_internal_iterations = iterations,
            .steps = .{ .generation = .{ .enabled = true } },
        });
    }

    const body = antfly_client.types.QueryBuilderRequest{
        .intent = i,
        .table = table_name,
        .generator = generator_value.value,
        .schema_fields = fields,
        .mode = mode,
        .max_internal_iterations = iterations,
    };

    var resp = try client.queryBuilder(body);
    defer resp.deinit();
    if (resp.status_code >= 300 or resp.data == null) {
        return error.QueryBuilderResponseError;
    }
    const result = resp.data.?.value;
    try cli.writeJson(allocator, io, result);
    if (result.status == .incomplete or result.status == .failed) return error.QueryBuilderResponseError;
}

fn retrievalCompleted(allocator: std.mem.Allocator, body: ?[]const u8) !bool {
    var parsed = std.json.parseFromSlice(struct { status: antfly_client.types.AgentStatus }, allocator, body orelse return false, .{ .ignore_unknown_fields = true }) catch return false;
    defer parsed.deinit();
    return parsed.value.status == .completed;
}

fn parseIterations(raw: ?[]const u8) !i64 {
    const value = std.fmt.parseInt(i64, raw orelse "0", 10) catch return error.InvalidAgentIterations;
    if (value < 0 or value > 20) return error.InvalidAgentIterations;
    return value;
}

test "agent CLI iterations are bounded" {
    try std.testing.expectEqual(@as(i64, 0), try parseIterations(null));
    try std.testing.expectEqual(@as(i64, 4), try parseIterations("4"));
    try std.testing.expectError(error.InvalidAgentIterations, parseIterations("-1"));
    try std.testing.expectError(error.InvalidAgentIterations, parseIterations("21"));
}

test "agent CLI execution request round trips through the server schema" {
    const alloc = std.testing.allocator;
    const request = antfly_client.types.RetrievalAgentRequest{
        .query = "Find anatomy",
        .queries = &.{.{ .table = "docs", .full_text_search = .{ .bytes = "{\"match\":\"anatomy\",\"field\":\"title\"}" }, .limit = 10 }},
        .generator = .{ .provider = "antfly", .model = "ggml-org/gemma-4-E4B-it-GGUF", .max_tokens = 512, .temperature = 0 },
        .stream = false,
        .max_internal_iterations = 4,
        .steps = .{ .generation = .{ .enabled = true } },
    };
    const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(body);
    var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").RetrievalAgentRequest, alloc, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?i64, 4), parsed.value.max_internal_iterations);
}

fn buildFullTextSearchValue(allocator: std.mem.Allocator, query: []const u8) std.json.Parsed(antfly_client.types.RawQuery) {
    const escaped = std.json.Stringify.valueAlloc(allocator, query, .{}) catch |err| {
        cli.fatal("failed to encode --full-text-search: {}", .{err});
    };
    defer allocator.free(escaped);

    const json_body = std.fmt.allocPrint(allocator, "{{\"query\":{s}}}", .{escaped}) catch |err| {
        cli.fatal("failed to build --full-text-search value: {}", .{err});
    };
    defer allocator.free(json_body);

    return parseJsonArg(antfly_client.types.RawQuery, allocator, "--full-text-search", json_body);
}

fn parseJsonArg(comptime T: type, allocator: std.mem.Allocator, flag: []const u8, raw: []const u8) std.json.Parsed(T) {
    return std.json.parseFromSlice(T, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch |err| {
        cli.fatal("invalid JSON for {s}: {}", .{ flag, err });
    };
}

test "retrieval SSE errors fail the CLI response" {
    try std.testing.expect(isSseFailureResponse(
        "text/event-stream; charset=utf-8",
        "event: generation\r\ndata: \"partial\"\r\n\r\nevent: error\r\ndata: {\"error\":\"IncompatibleModel\"}\r\n\r\n",
    ));
    try std.testing.expect(!isSseFailureResponse(
        "text/event-stream",
        "event: generation\ndata: \"the words event: error are data\"\n\nevent: done\ndata: {}\n\n",
    ));
    try std.testing.expect(!isSseFailureResponse(
        "application/json",
        "{\"event\":\"error\"}",
    ));

    var split_detector = SseCompletionDetector{};
    split_detector.push("event: gen\n\neve");
    split_detector.push("nt: err");
    split_detector.push("or\r\ndata: {}\r\n\r\n");
    split_detector.finish();
    try std.testing.expect(split_detector.saw_error);
}

test "retrieval SSE requires a complete terminal done event" {
    for ([_][]const u8{
        "",
        "event: generation\ndata: partial\n\n",
        "event: done",
        "event: done\ndata: {}\n",
        "event: done\n\n",
        "data: event: done\n\n",
        "event: done\nevent: generation\ndata: {}\n\n",
        "event: error\ndata: failed\n\nevent: done\ndata: {}\n\n",
        "event: done\ndata: {}\n\nevent: error\ndata: failed",
        "event: done\ndata: {}\n\nevent: generation\ndata: late\n\n",
    }) |body| {
        try std.testing.expect(isSseFailureResponse("text/event-stream", body));
    }
    for ([_][]const u8{
        "event: done\ndata: {}\n\n",
        "event: done\r\ndata: {}\r\n\r\n",
        "event: done\rdata: {}\r\r",
        "data: {}\nevent: done\n\n: heartbeat\n\n",
        "event: done\ndata: " ++ ("x" ** 256) ++ "\n\n",
    }) |body| {
        try std.testing.expect(!isSseFailureResponse("text/event-stream", body));
        var detector = SseCompletionDetector{};
        for (body) |byte| detector.push(&.{byte});
        detector.finish();
        try std.testing.expect(!detector.failed());
    }
}
