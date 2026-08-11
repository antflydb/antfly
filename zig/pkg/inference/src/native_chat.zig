// Copyright 2026 Antfly, Inc.
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

// `antfly inference chat`: ollama-style interactive chat with a local
// generative model. Resolves friendly model names, pulls missing models from
// HuggingFace, then runs a multi-turn REPL over the native generation
// pipeline with prompt-prefix KV reuse so follow-up turns only prefill the
// new part of the conversation.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const api = @import("inference_api");
const backends = @import("backends/backends.zig");
const session_factory = @import("architectures/session_factory.zig");
const generation = @import("pipelines/generation.zig");
const model_manager_mod = @import("server/model_manager.zig");
const gpt_mod = @import("models/gpt.zig");
const runtime = @import("runtime/root.zig");
const registry_mod = @import("registry/registry.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const native_generate = @import("native_generate.zig");
const tokenizer_mod = @import("inference_tokenizer");
const ops = @import("ops/ops.zig");

const print = std.debug.print;

const default_max_tokens: i32 = 512;
const default_max_context: usize = 8192;
const hard_max_context: usize = 32768;
/// Chat conversations should not lose their KV prefix mid-session; the cache
/// default of five minutes assumes server request churn, not a REPL.
const context_slack_tokens: usize = 64;

const Options = struct {
    model: []const u8,
    backend: native_backend_choice.Choice = .auto,
    max_tokens: i32 = default_max_tokens,
    max_context: usize = 0,
    temperature: f32 = 0.7,
    top_p: f32 = 0.95,
    top_k: i32 = 64,
    repetition_penalty: f32 = 1.0,
    system: ?[]const u8 = null,
    models_dir: ?[]const u8 = null,
    hf_token: ?[]const u8 = null,
    server_url: ?[]const u8 = null,
    /// Reserved for multi-turn KV prefix reuse. Streaming chat rejects this
    /// until both local and server paths can preserve correct behavior.
    prompt_cache: bool = false,
    print_timing: bool = false,
};

/// Sampling parameters the REPL can mutate via `/set`.
const ChatParams = struct {
    max_tokens: i32,
    temperature: f32,
    top_p: f32,
    top_k: i32,
    repetition_penalty: f32,

    fn fromOptions(opts: Options) ChatParams {
        return .{
            .max_tokens = opts.max_tokens,
            .temperature = opts.temperature,
            .top_p = opts.top_p,
            .top_k = opts.top_k,
            .repetition_penalty = opts.repetition_penalty,
        };
    }
};

const ReplCommand = union(enum) {
    bye,
    clear,
    show,
    help,
    set: struct { param: []const u8, value: []const u8 },
    unknown: []const u8,
};

const ReplLine = union(enum) {
    message: []const u8,
    command: ReplCommand,
    multiline_toggle,
    empty,
};

fn classifyReplLine(line: []const u8) ReplLine {
    const trimmed = std.mem.trim(u8, line, " \t\r");
    if (trimmed.len == 0) return .empty;
    if (std.mem.eql(u8, trimmed, "\"\"\"")) return .multiline_toggle;
    if (trimmed[0] != '/') return .{ .message = trimmed };
    var it = std.mem.tokenizeAny(u8, trimmed, " \t");
    const cmd = it.next().?;
    if (std.mem.eql(u8, cmd, "/bye") or std.mem.eql(u8, cmd, "/exit") or std.mem.eql(u8, cmd, "/quit")) {
        return .{ .command = .bye };
    }
    if (std.mem.eql(u8, cmd, "/clear")) return .{ .command = .clear };
    if (std.mem.eql(u8, cmd, "/show")) return .{ .command = .show };
    if (std.mem.eql(u8, cmd, "/help")) return .{ .command = .help };
    if (std.mem.eql(u8, cmd, "/set")) {
        const param = it.next() orelse return .{ .command = .{ .unknown = trimmed } };
        const value = it.next() orelse return .{ .command = .{ .unknown = trimmed } };
        return .{ .command = .{ .set = .{ .param = param, .value = value } } };
    }
    return .{ .command = .{ .unknown = trimmed } };
}

fn applySet(params: *ChatParams, param: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, param, "temperature")) {
        const parsed = try std.fmt.parseFloat(f32, value);
        if (!std.math.isFinite(parsed) or parsed < 0 or parsed > 2) return error.InvalidParamValue;
        params.temperature = parsed;
    } else if (std.mem.eql(u8, param, "top-p") or std.mem.eql(u8, param, "top_p")) {
        const parsed = try std.fmt.parseFloat(f32, value);
        if (!std.math.isFinite(parsed) or parsed < 0 or parsed > 1) return error.InvalidParamValue;
        params.top_p = parsed;
    } else if (std.mem.eql(u8, param, "top-k") or std.mem.eql(u8, param, "top_k")) {
        const parsed = try std.fmt.parseInt(i32, value, 10);
        if (parsed < 0) return error.InvalidParamValue;
        params.top_k = parsed;
    } else if (std.mem.eql(u8, param, "max-tokens") or std.mem.eql(u8, param, "max_tokens")) {
        const parsed = try std.fmt.parseInt(i32, value, 10);
        if (parsed < 1) return error.InvalidParamValue;
        params.max_tokens = parsed;
    } else if (std.mem.eql(u8, param, "repetition-penalty") or std.mem.eql(u8, param, "repetition_penalty")) {
        const parsed = try std.fmt.parseFloat(f32, value);
        if (!std.math.isFinite(parsed) or parsed <= 0) return error.InvalidParamValue;
        params.repetition_penalty = parsed;
    } else {
        return error.UnknownParam;
    }
}

/// Conversation history. Message text is arena-owned; the optional system
/// prompt points at argv and survives `/clear`.
const ChatHistory = struct {
    arena: std.heap.ArenaAllocator,
    messages: std.ArrayListUnmanaged(generation.Message) = .empty,
    system_text: ?[]const u8,
    system_count: usize = 0,
    /// First non-dropped message index (>= system_count). Advanced when the
    /// rendered conversation exceeds the context budget.
    start: usize = 0,

    fn init(base_allocator: std.mem.Allocator, system_text: ?[]const u8) !ChatHistory {
        var self = ChatHistory{
            .arena = std.heap.ArenaAllocator.init(base_allocator),
            .system_text = system_text,
        };
        try self.reset();
        return self;
    }

    fn deinit(self: *ChatHistory) void {
        self.arena.deinit();
    }

    fn reset(self: *ChatHistory) !void {
        self.messages = .empty;
        self.system_count = 0;
        if (self.system_text) |sys| {
            try self.messages.append(self.arena.allocator(), .{ .role = "system", .content = sys });
            self.system_count = 1;
        }
        self.start = self.system_count;
    }

    fn clear(self: *ChatHistory) !void {
        _ = self.arena.reset(.retain_capacity);
        try self.reset();
    }

    fn appendUser(self: *ChatHistory, text: []const u8) !void {
        const owned = try self.arena.allocator().dupe(u8, text);
        try self.messages.append(self.arena.allocator(), .{ .role = "user", .content = owned });
    }

    fn appendAssistant(self: *ChatHistory, text: []const u8) !void {
        const owned = try self.arena.allocator().dupe(u8, text);
        try self.messages.append(self.arena.allocator(), .{ .role = "assistant", .content = owned });
    }

    /// Drop the most recent message (used to roll back a user turn whose
    /// generation failed or whose prompt cannot fit the context window).
    fn popLast(self: *ChatHistory) void {
        if (self.messages.items.len > self.system_count) {
            _ = self.messages.pop();
            if (self.start > self.messages.items.len) self.start = self.messages.items.len;
        }
    }

    fn visibleCount(self: *const ChatHistory) usize {
        return self.system_count + (self.messages.items.len - self.start);
    }

    /// Copy the visible window (system messages plus everything from `start`)
    /// into `out`.
    fn collectVisible(self: *const ChatHistory, allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(generation.Message)) !void {
        out.clearRetainingCapacity();
        try out.ensureTotalCapacity(allocator, self.visibleCount());
        for (self.messages.items[0..self.system_count]) |msg| out.appendAssumeCapacity(msg);
        for (self.messages.items[self.start..]) |msg| out.appendAssumeCapacity(msg);
    }

    /// How many messages the next context trim should drop from the front of
    /// the visible window, preserving user/assistant alternation. Null when
    /// only the newest message (plus system) remains.
    fn nextTrimDrop(self: *const ChatHistory) ?usize {
        const remaining = self.messages.items.len - self.start;
        if (remaining <= 1) return null;
        const first = self.messages.items[self.start];
        if (remaining >= 3 and std.mem.eql(u8, first.role, "user")) {
            const second = self.messages.items[self.start + 1];
            if (std.mem.eql(u8, second.role, "assistant")) return 2;
        }
        return 1;
    }

    /// Drop old visible messages and rebuild the arena around the retained
    /// window. Advancing an index alone would keep every discarded turn alive
    /// for the lifetime of a long-running REPL.
    fn trimOldest(self: *ChatHistory, count: usize) !void {
        if (count == 0 or count > self.messages.items.len - self.start) return error.InvalidTrimCount;
        const retained_start = self.start + count;
        var replacement = std.heap.ArenaAllocator.init(self.arena.child_allocator);
        errdefer replacement.deinit();
        var retained = std.ArrayListUnmanaged(generation.Message).empty;
        const replacement_allocator = replacement.allocator();
        if (self.system_text) |sys| {
            // `system_text` is argv-owned rather than arena-owned so it remains
            // valid across both this rebuild and `/clear` arena resets.
            try retained.append(replacement_allocator, .{ .role = "system", .content = sys });
        }
        for (self.messages.items[retained_start..]) |msg| {
            const content = try replacement_allocator.dupe(u8, msg.content);
            try retained.append(replacement_allocator, .{ .role = msg.role, .content = content });
        }

        self.arena.deinit();
        self.arena = replacement;
        self.messages = retained;
        self.system_count = @intFromBool(self.system_text != null);
        self.start = self.system_count;
    }
};

/// Set from the SIGINT handler; polled by the token callback so Ctrl-C stops
/// the in-flight generation instead of killing the REPL. A second Ctrl-C
/// while the flag is still set exits the process.
var chat_interrupt = std.atomic.Value(bool).init(false);

fn handleSigint(_: std.posix.SIG) callconv(.c) void {
    if (chat_interrupt.swap(true, .acq_rel)) {
        // Only async-signal-safe work here (see the note in main.zig about
        // signal-context stop paths): exit() is a plain syscall.
        std.process.exit(130);
    }
}

fn installSigintHandler() void {
    if (builtin.os.tag == .windows) return;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSigint },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.INT, &act, null);
}

fn durationMillis(from: std.Io.Timestamp, to: std.Io.Timestamp) u64 {
    const nanos = std.Io.Timestamp.durationTo(from, to).nanoseconds;
    if (nanos <= 0) return 0;
    return @intCast(@divTrunc(nanos, std.time.ns_per_ms));
}

fn tokensPerSecond(tokens: usize, millis: u64) f64 {
    if (tokens == 0 or millis == 0) return 0.0;
    return @as(f64, @floatFromInt(tokens)) * 1000.0 / @as(f64, @floatFromInt(millis));
}

/// Returns ~/.antfly/inference/models if $HOME is set, otherwise ./models.
fn defaultModelsDir(allocator: std.mem.Allocator) []const u8 {
    if (platform.env.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value| return value;
    const home = platform.env.getenv("HOME") orelse return "./models";
    return std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models" }) catch "./models";
}

fn looksLikePath(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "/") or
        std.mem.startsWith(u8, name, "./") or
        std.mem.startsWith(u8, name, "../") or
        std.mem.startsWith(u8, name, "~/");
}

fn printKnownAliases() void {
    print("known model aliases:\n", .{});
    for (registry_mod.friendly_aliases) |entry| {
        print("  {s:<16} -> {s}\n", .{ entry.alias, entry.ref });
    }
    print("or pass a HuggingFace reference (owner/name[:variant]) or a local model directory.\n", .{});
}

/// Resolve the positional model argument to a local model directory, pulling
/// the model from HuggingFace when it is not installed yet. Returned path is
/// allocator-owned.
fn resolveModelDir(allocator: std.mem.Allocator, io: std.Io, opts: Options) ![]const u8 {
    const name = opts.model;
    if (looksLikePath(name)) return try allocator.dupe(u8, name);

    const alias_ref = registry_mod.resolveFriendlyRef(name);
    if (alias_ref == null) {
        // Not a known alias: an existing directory wins before we try to
        // interpret the argument as a remote model reference.
        if (std.Io.Dir.cwd().access(io, name, .{})) |_| {
            return try allocator.dupe(u8, name);
        } else |_| {}
    }
    const ref_str = alias_ref orelse name;
    const ref = registry_mod.ModelRef.parse(ref_str) catch {
        print("unknown model: {s}\n\n", .{name});
        printKnownAliases();
        return error.InvalidArguments;
    };

    const models_dir = opts.models_dir orelse defaultModelsDir(allocator);
    const dest = try registry_mod.modelInstallDirAlloc(allocator, models_dir, ref);
    errdefer allocator.free(dest);

    const dir_exists = if (std.Io.Dir.cwd().access(io, dest, .{})) |_| true else |_| false;
    const installed = switch (registry_mod.download.managedDownloadState(allocator, io, dest)) {
        .complete => if (std.mem.eql(u8, ref.variant, "auto"))
            true
        else if (registry_mod.download.managedDownloadMatchesSource(
            allocator,
            io,
            dest,
            ref.owner,
            ref.name,
            ref.variant,
        ))
            true
        else
            return error.ManagedModelIdentityMismatch,
        // Directories not created by `pull` (hand-placed models) have no
        // receipt; trust them when they exist.
        .unmanaged => dir_exists,
        .incomplete => false,
    };
    if (!installed) {
        const token = opts.hf_token orelse platform.env.getenv("HF_TOKEN");
        print("pulling {s}...\n", .{ref_str});
        var reg = registry_mod.ModelRegistry.init(allocator, models_dir);
        defer reg.deinit();
        try reg.pull(io, ref_str, .{ .token = token }, null, null, .auto);
        print("pull complete.\n", .{});
    }
    return dest;
}

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = parseArgs(args) catch |err| switch (err) {
        error.HelpRequested => return,
        else => return err,
    };
    try native_backend_choice.validate(opts.backend);
    switch (opts.backend) {
        .auto, .native, .metal, .cuda => {},
        else => {
            print("chat supports --backend auto|native|metal|cuda\n", .{});
            return error.InvalidArguments;
        },
    }

    if (opts.prompt_cache) {
        print("--prompt-cache is not available in the streaming chat CLI yet\n", .{});
        return error.InvalidArguments;
    }
    if (opts.server_url) |server_url| {
        return runServerChat(allocator, io, opts, server_url);
    }
    return runLocalChat(allocator, io, opts);
}

const LocalChatSession = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    model: *model_manager_mod.LoadedModel,
    model_dir: []const u8,
    gpt_config: gpt_mod.Config,
    tokenizer: tokenizer_mod.Tokenizer,
    cb: ops.ComputeBackend,
    kv_dtype: runtime.kv.pool.KvDType,
    kv_manager: *runtime.kv.manager.KvManager,
    kv_storage: *runtime.kv.storage_runtime.KvStorageRuntime,
    pool_id: runtime.kv.block.KvPoolId,
    effective_max_context: usize,
    print_timing: bool,
    ansi: bool,

    fn promptBudget(self: *const LocalChatSession, params: ChatParams) usize {
        const reserved = @as(usize, @intCast(@max(params.max_tokens, 1))) + context_slack_tokens;
        if (self.effective_max_context <= reserved + 1) return 1;
        return self.effective_max_context - reserved;
    }

    /// Render the visible history and advance `history.start` until the
    /// prompt fits the context budget. Returns the messages to send, or null
    /// when even the newest user message alone does not fit (that message is
    /// rolled back so the user can retry with shorter input).
    fn planTurnMessages(
        self: *LocalChatSession,
        turn_allocator: std.mem.Allocator,
        history: *ChatHistory,
        params: ChatParams,
        visible: *std.ArrayListUnmanaged(generation.Message),
    ) !?[]const generation.Message {
        const budget = self.promptBudget(params);
        var dropped: usize = 0;
        while (true) {
            try history.collectVisible(turn_allocator, visible);
            const rendered = if (self.model.chat_tmpl) |tmpl|
                try tmpl.apply(turn_allocator, visible.items, true)
            else
                try generation.formatMessages(turn_allocator, visible.items);
            var encoded = try generation.encodePromptForGeneration(
                self.tokenizer,
                turn_allocator,
                rendered,
                budget + 1,
                self.model.manifest.add_bos_token,
                self.model.manifest.bos_token,
            );
            defer encoded.deinit();
            var token_count: usize = 0;
            while (token_count < encoded.attention_mask.len and encoded.attention_mask[token_count] != 0) : (token_count += 1) {}
            if (token_count <= budget) {
                if (dropped > 0) {
                    printDim(self.ansi, "(context full: dropped the {d} oldest messages)\n", .{dropped});
                }
                return visible.items;
            }
            if (history.nextTrimDrop()) |drop| {
                try history.trimOldest(drop);
                dropped += drop;
                continue;
            }
            printDim(self.ansi, "(message too long for the {d}-token context window; try shorter input or --max-context)\n", .{self.effective_max_context});
            history.popLast();
            return null;
        }
    }

    fn runTurn(self: *LocalChatSession, history: *ChatHistory, params: ChatParams) !void {
        var turn_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer turn_arena.deinit();
        const turn_allocator = turn_arena.allocator();

        var visible = std.ArrayListUnmanaged(generation.Message).empty;
        const messages = (try self.planTurnMessages(turn_allocator, history, params, &visible)) orelse return;

        chat_interrupt.store(false, .release);
        var decode_state = generation.NativeDecodeState.initPaged(
            self.allocator,
            self.kv_manager,
            self.pool_id,
            self.model.shared_moe_cache,
        );
        decode_state.kv_storage = self.kv_storage;
        defer decode_state.deinit();

        var printer = StreamPrinter{ .io = self.io };
        var pipeline = generation.NativeGenerationPipeline{
            .allocator = self.allocator,
            .io = self.io,
            .cb = self.cb,
            .session = self.model.session,
            .gpt_config = self.gpt_config,
            .kv_dtype = self.kv_dtype,
            .shared_moe_cache = self.model.shared_moe_cache,
            .tokenizer = self.tokenizer,
            .add_bos_token = self.model.manifest.add_bos_token,
            .bos_token = self.model.manifest.bos_token,
            .chat_template = self.model.chat_tmpl,
            .print_timing = self.print_timing,
            .model_dir = self.model_dir,
            .gguf_projector_path = self.model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .continue_ctx = @ptrCast(&printer),
            .continue_fn = StreamPrinter.shouldContinue,
        };
        const config = generation.GenerationConfig{
            .max_tokens = params.max_tokens,
            .temperature = params.temperature,
            .top_p = params.top_p,
            .top_k = params.top_k,
            .repetition_penalty = params.repetition_penalty,
        };

        const turn_started_at = std.Io.Timestamp.now(self.io, .awake);
        var result = try pipeline.generateStreaming(messages, config, @ptrCast(&printer), StreamPrinter.onToken);
        defer result.deinit();
        const turn_finished_at = std.Io.Timestamp.now(self.io, .awake);
        if (!printer.wrote_text and result.text.len > 0) {
            print("{s}", .{result.text});
        }
        if (!printer.wrote_text and result.text.len == 0 and result.tokens_used > 0) {
            printDim(self.ansi, "(the model produced {d} private thought-channel tokens and no public reply)", .{result.tokens_used});
        }
        print("\n", .{});

        const interrupted = chat_interrupt.load(.acquire);
        const ttft_ms: ?u64 = if (printer.first_token_at) |at| durationMillis(turn_started_at, at) else null;
        printTurnFooter(self.ansi, &result, ttft_ms, durationMillis(turn_started_at, turn_finished_at), interrupted);
        chat_interrupt.store(false, .release);

        try history.appendAssistant(result.text);
    }

    fn showInfo(self: *LocalChatSession, params: ChatParams) void {
        print("model: {s}\n", .{self.model_dir});
        print("backend: {s}\n", .{@tagName(self.model.session.backend())});
        print("family: {s}  layers: {d}  model context: {d}  chat context: {d}\n", .{
            @tagName(self.gpt_config.family),
            self.gpt_config.num_hidden_layers,
            self.gpt_config.max_position_embeddings,
            self.effective_max_context,
        });
        print("params: temperature={d:.2} top-p={d:.2} top-k={d} max-tokens={d} repetition-penalty={d:.2}\n", .{
            params.temperature,
            params.top_p,
            params.top_k,
            params.max_tokens,
            params.repetition_penalty,
        });
        print("prompt cache: unavailable in streaming chat\n", .{});
    }
};

const StreamPrinter = struct {
    io: std.Io,
    first_token_at: ?std.Io.Timestamp = null,
    wrote_text: bool = false,

    fn shouldContinue(_: *anyopaque) bool {
        return !chat_interrupt.load(.acquire);
    }

    fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
        const self: *@This() = @ptrCast(@alignCast(raw_ctx));
        if (chat_interrupt.load(.acquire)) return false;
        if (token_text.len > 0) {
            if (self.first_token_at == null) {
                self.first_token_at = std.Io.Timestamp.now(self.io, .awake);
            }
            print("{s}", .{token_text});
            self.wrote_text = true;
        }
        return true;
    }
};

fn printDim(ansi: bool, comptime fmt: []const u8, args: anytype) void {
    if (ansi) {
        print("\x1b[2m" ++ fmt ++ "\x1b[0m", args);
    } else {
        print(fmt, args);
    }
}

fn printTurnFooter(
    ansi: bool,
    result: *const generation.GenerationResult,
    ttft_ms: ?u64,
    total_ms: u64,
    interrupted: bool,
) void {
    const decode_ms = if (result.timing_ms) |timing| timing.decode else total_ms;
    const reason = if (interrupted) "interrupted" else result.finish_reason;
    if (ttft_ms) |ttft| {
        printDim(ansi, "({d} tok \u{b7} {d:.1} tok/s \u{b7} ttft {d}ms \u{b7} {d} prompt / {d} cached \u{b7} {s})\n", .{
            result.tokens_used,
            tokensPerSecond(result.tokens_used, decode_ms),
            ttft,
            result.prompt_tokens,
            result.cached_prompt_tokens,
            reason,
        });
    } else {
        printDim(ansi, "({d} tok \u{b7} {d:.1} tok/s \u{b7} {d} prompt / {d} cached \u{b7} {s})\n", .{
            result.tokens_used,
            tokensPerSecond(result.tokens_used, decode_ms),
            result.prompt_tokens,
            result.cached_prompt_tokens,
            reason,
        });
    }
}

fn printReplHelp() void {
    print(
        \\slash commands:
        \\  /bye, /exit, /quit      leave the chat
        \\  /clear                  reset conversation history
        \\  /set <param> <value>    temperature, top-p, top-k, max-tokens, repetition-penalty
        \\  /show                   model, parameter, and prompt-cache info
        \\  /help                   this list
        \\  """                     begin/end multi-line input
        \\
    , .{});
}

fn runLocalChat(allocator: std.mem.Allocator, io: std.Io, opts: Options) !void {
    const model_dir = try resolveModelDir(allocator, io, opts);
    defer allocator.free(model_dir);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    native_backend_choice.configureSessionPreference(&session_manager, opts.backend);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const load_started_at = std.Io.Timestamp.now(io, .awake);
    print("loading {s}...\n", .{model_dir});
    const model = try model_manager.loadFromDir(model_dir);
    const load_finished_at = std.Io.Timestamp.now(io, .awake);

    const gpt_config = session_factory.getGptConfig(model.session) orelse {
        print("{s} is not a generative model\n", .{model_dir});
        return error.InvalidModelForGeneration;
    };
    const tokenizer = model.getTokenizer();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        else => {
            print("chat requires a native, metal, or cuda generation backend (model loaded as {s})\n", .{@tagName(model.session.backend())});
            return error.UnsupportedChatBackend;
        },
    };

    const model_context: usize = if (gpt_config.max_position_embeddings > 0)
        @intCast(gpt_config.max_position_embeddings)
    else
        hard_max_context;
    var effective_max_context: usize = if (opts.max_context > 0)
        opts.max_context
    else
        @min(model_context, default_max_context);
    effective_max_context = @min(effective_max_context, @min(model_context, hard_max_context));

    const requested_kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const kv_dtype = native_generate.effectiveGenerationKvDType(
        requested_kv_dtype,
        backend_kind,
        gpt_config,
        effective_max_context,
        @intCast(@max(opts.max_tokens, 1)),
    );

    const budget_class: runtime.tier.memory.BackendClass = if (backend_kind != .native) .gpu else .cpu;
    var budget_limits = runtime.tier.memory.defaultLimitsForBackend(budget_class);
    budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
    var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
    run_budget.reserveEstimate(try runtime.tier.memory.estimateGptGeneration(
        backend_kind,
        kv_dtype,
        gpt_config,
        effective_max_context,
        @intCast(@max(opts.max_tokens, 1)),
        256,
    )) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            print("model does not fit the inference memory budget at --max-context {d}; try a smaller --max-context\n", .{effective_max_context});
        }
        return err;
    };

    var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
        if (err == error.MemoryBudgetExceeded) {
            print("model does not fit the inference memory budget; try a smaller --max-context\n", .{});
        }
        return err;
    };
    defer cb.deinit();

    // Mixed-attention models (iSWA-style, e.g. Gemma) need the full KV
    // history for their global layers; see the matching rule in
    // native_generate.zig for the trade-off and the env escape hatch.
    const sliding_trim_forced = platform.env.getenvBool("ANTFLY_INFERENCE_KV_SLIDING_TRIM");
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0 and gpt_config.hasGlobalAttentionLayers() and !sliding_trim_forced)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;
    const pool_config: runtime.kv.pool.KvPoolConfig = .{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    };

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();
    const pool_id = try kv_manager.addPool(pool_config);
    var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, pool_config);
    defer kv_storage.deinit();
    try cb.provisionKvDeviceWriteHook(&kv_storage);

    const ansi = supportsAnsi(io);
    print("loaded ({s}, {d}ms). Type /help for commands, /bye to exit.\n", .{
        @tagName(model.session.backend()),
        durationMillis(load_started_at, load_finished_at),
    });

    var session = LocalChatSession{
        .allocator = allocator,
        .io = io,
        .model = model,
        .model_dir = model_dir,
        .gpt_config = gpt_config,
        .tokenizer = tokenizer,
        .cb = cb,
        .kv_dtype = kv_dtype,
        .kv_manager = &kv_manager,
        .kv_storage = &kv_storage,
        .pool_id = pool_id,
        .effective_max_context = effective_max_context,
        .print_timing = opts.print_timing,
        .ansi = ansi,
    };

    var history = try ChatHistory.init(allocator, opts.system);
    defer history.deinit();
    var params = ChatParams.fromOptions(opts);

    installSigintHandler();
    try runRepl(allocator, io, &history, &params, &session);
}

fn supportsAnsi(io: std.Io) bool {
    return std.Io.File.stderr().supportsAnsiEscapeCodes(io) catch false;
}

/// The REPL loop. `runner` provides `runTurn(history, params)` and
/// `showInfo(params)`; a stub runner keeps this testable without a model.
fn runRepl(
    allocator: std.mem.Allocator,
    io: std.Io,
    history: *ChatHistory,
    params: *ChatParams,
    runner: anytype,
) !void {
    var stdin_buf: [64 * 1024]u8 = undefined;
    // readerStreaming: piped stdin stats as size 0, so the positional
    // File.reader would report EOF immediately (scripted use, smoke tests).
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buf);
    const stdin = &stdin_reader.interface;

    var multiline: ?std.ArrayListUnmanaged(u8) = null;
    defer if (multiline) |*buf| buf.deinit(allocator);

    while (true) {
        if (multiline == null) print(">>> ", .{}) else print("... ", .{});
        const line = (stdin.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => {
                print("input line too long\n", .{});
                return err;
            },
            else => return err,
        }) orelse break;

        if (multiline) |*buf| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (std.mem.eql(u8, trimmed, "\"\"\"")) {
                const message = std.mem.trim(u8, buf.items, " \t\r\n");
                if (message.len > 0) {
                    try submitMessage(history, params.*, runner, message);
                }
                buf.deinit(allocator);
                multiline = null;
            } else {
                try buf.appendSlice(allocator, line);
                try buf.append(allocator, '\n');
            }
            continue;
        }

        switch (classifyReplLine(line)) {
            .empty => continue,
            .multiline_toggle => {
                multiline = .empty;
            },
            .message => |text| try submitMessage(history, params.*, runner, text),
            .command => |cmd| switch (cmd) {
                .bye => break,
                .clear => {
                    try history.clear();
                    print("conversation cleared\n", .{});
                },
                .show => runner.showInfo(params.*),
                .help => printReplHelp(),
                .set => |set| {
                    applySet(params, set.param, set.value) catch {
                        print("usage: /set <temperature|top-p|top-k|max-tokens|repetition-penalty> <value>\n", .{});
                        continue;
                    };
                    print("{s} = {s}\n", .{ set.param, set.value });
                },
                .unknown => |raw| {
                    print("unknown command: {s} (try /help)\n", .{raw});
                },
            },
        }
    }
}

fn submitMessage(history: *ChatHistory, params: ChatParams, runner: anytype, text: []const u8) !void {
    try history.appendUser(text);
    runner.runTurn(history, params) catch |err| {
        history.popLast();
        if (err == error.ChatInterrupted) {
            chat_interrupt.store(false, .release);
            print("\n", .{});
            return;
        }
        print("generation failed: {s}\n", .{@errorName(err)});
    };
}

const ServerChatSession = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    http: *httpx.Client,
    url: []const u8,
    model_name: []const u8,
    backend: native_backend_choice.Choice,
    effective_max_context: usize,
    ansi: bool,

    fn requestWithInterrupt(
        self: *ServerChatSession,
        body: []const u8,
        writer: *native_generate.ServerGenerateSseWriter,
    ) !httpx.Response {
        const headers = [_][2][]const u8{
            .{ "Accept", "text/event-stream" },
        };
        return self.http.requestToWriter(.POST, self.url, .{
            .json = body,
            .headers = &headers,
            .timeout_ms = 300_000,
            .cancellation = &chat_interrupt,
        }, writer, null, null) catch |err| {
            if (err == error.Cancelled and chat_interrupt.load(.acquire)) return error.ChatInterrupted;
            return err;
        };
    }

    fn promptBudget(self: *const ServerChatSession, params: ChatParams) usize {
        const reserved = @as(usize, @intCast(@max(params.max_tokens, 1))) + context_slack_tokens;
        if (self.effective_max_context <= reserved + 1) return 1;
        return self.effective_max_context - reserved;
    }

    /// A tokenizer is owned by the remote server, so use a deliberately
    /// conservative upper bound: a byte can become at most one byte-fallback
    /// token, with fixed room for chat-template role markers per message.
    fn estimatedPromptTokenUpperBound(messages: []const generation.Message) usize {
        var total: usize = 32;
        for (messages) |msg| total +|= msg.content.len +| 16;
        return total;
    }

    fn planTurnMessages(
        self: *ServerChatSession,
        turn_allocator: std.mem.Allocator,
        history: *ChatHistory,
        params: ChatParams,
        visible: *std.ArrayListUnmanaged(generation.Message),
    ) !?[]const generation.Message {
        const budget = self.promptBudget(params);
        var dropped: usize = 0;
        while (true) {
            try history.collectVisible(turn_allocator, visible);
            if (estimatedPromptTokenUpperBound(visible.items) <= budget) {
                if (dropped > 0) {
                    printDim(self.ansi, "(context full: dropped the {d} oldest messages)\n", .{dropped});
                }
                return visible.items;
            }
            if (history.nextTrimDrop()) |drop| {
                try history.trimOldest(drop);
                dropped += drop;
                continue;
            }
            printDim(self.ansi, "(message too long for the {d}-token server context budget; try shorter input or --max-context)\n", .{self.effective_max_context});
            history.popLast();
            return null;
        }
    }

    fn runTurn(self: *ServerChatSession, history: *ChatHistory, params: ChatParams) !void {
        var turn_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer turn_arena.deinit();
        const turn_allocator = turn_arena.allocator();

        var visible = std.ArrayListUnmanaged(generation.Message).empty;
        const planned = (try self.planTurnMessages(turn_allocator, history, params, &visible)) orelse return;
        var messages = try turn_allocator.alloc(api.ChatMessage, planned.len);
        for (planned, 0..) |msg, i| {
            const role: api.Role = if (std.mem.eql(u8, msg.role, "assistant"))
                .assistant
            else if (std.mem.eql(u8, msg.role, "system"))
                .system
            else
                .user;
            messages[i] = .{
                .role = role,
                .content = .{ .string = msg.content },
            };
        }

        const request = makeServerGenerateRequest(self.model_name, messages, params, self.backend);
        const body = try httpx.json.Json.stringify(turn_allocator, request);

        var capture = std.ArrayListUnmanaged(u8).empty;
        var stream_writer = native_generate.ServerGenerateSseWriter{
            .allocator = self.allocator,
            .capture = &capture,
        };
        defer capture.deinit(self.allocator);
        defer stream_writer.deinit();

        chat_interrupt.store(false, .release);
        const started_at = std.Io.Timestamp.now(self.io, .awake);
        var resp = self.requestWithInterrupt(body, &stream_writer) catch |err| {
            if (err != error.ChatInterrupted) return err;
            const finished_at = std.Io.Timestamp.now(self.io, .awake);
            print("\n", .{});
            printDim(self.ansi, "({d}ms \u{b7} interrupted)\n", .{durationMillis(started_at, finished_at)});
            chat_interrupt.store(false, .release);
            if (capture.items.len == 0) return error.ChatInterrupted;
            try history.appendAssistant(capture.items);
            return;
        };
        defer resp.deinit();
        const finished_at = std.Io.Timestamp.now(self.io, .awake);
        chat_interrupt.store(false, .release);

        if (!resp.ok()) {
            if (stream_writer.buffer.items.len > 0) {
                print("server_error status={d} body={s}\n", .{ resp.status.code, stream_writer.buffer.items });
            } else {
                print("server_error status={d}\n", .{resp.status.code});
            }
            return error.GenerateRequestFailed;
        }
        try stream_writer.finish();
        if (stream_writer.stream_error) return error.GenerateRequestFailed;

        print("\n", .{});
        const finish_reason = if (stream_writer.finish_reason) |reason| @tagName(reason) else "unknown";
        printDim(self.ansi, "({d}ms \u{b7} {s})\n", .{ durationMillis(started_at, finished_at), finish_reason });

        try history.appendAssistant(capture.items);
    }

    fn showInfo(self: *ServerChatSession, params: ChatParams) void {
        print("server: {s}\n", .{self.url});
        print("model: {s}\n", .{self.model_name});
        print("backend: {s}  context: {d}\n", .{
            @tagName(self.backend),
            self.effective_max_context,
        });
        print("params: temperature={d:.2} top-p={d:.2} top-k={d} max-tokens={d} repetition-penalty={d:.2}\n", .{
            params.temperature,
            params.top_p,
            params.top_k,
            params.max_tokens,
            params.repetition_penalty,
        });
    }
};

fn serverBackendOverride(choice: native_backend_choice.Choice) ?api.ModelBackend {
    return switch (choice) {
        .auto => null,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .onnx, .xla, .webgpu => unreachable,
    };
}

fn makeServerGenerateRequest(
    model_name: []const u8,
    messages: []const api.ChatMessage,
    params: ChatParams,
    backend: native_backend_choice.Choice,
) api.GenerateRequest {
    return .{
        .model = model_name,
        .messages = messages,
        .max_tokens = params.max_tokens,
        .temperature = params.temperature,
        .top_p = params.top_p,
        .top_k = params.top_k,
        .repetition_penalty = params.repetition_penalty,
        .backend = serverBackendOverride(backend),
        .stream = true,
    };
}

fn runServerChat(allocator: std.mem.Allocator, io: std.Io, opts: Options, server_url: []const u8) !void {
    if (opts.models_dir != null or opts.hf_token != null) {
        print("--models-dir and --token apply only to local chat and cannot be used with --server\n", .{});
        return error.InvalidArguments;
    }
    const resolved_ref = registry_mod.resolveFriendlyRef(opts.model) orelse opts.model;
    const model_name = resolved_ref;

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http = httpx.Client.init(allocator, io_impl.io());
    defer http.deinit();

    const url = try native_generate.generateEndpointUrl(allocator, server_url);
    defer allocator.free(url);

    var session = ServerChatSession{
        .allocator = allocator,
        .io = io,
        .http = &http,
        .url = url,
        .model_name = model_name,
        .backend = opts.backend,
        .effective_max_context = @min(if (opts.max_context > 0) opts.max_context else default_max_context, hard_max_context),
        .ansi = supportsAnsi(io),
    };

    var history = try ChatHistory.init(allocator, opts.system);
    defer history.deinit();
    var params = ChatParams.fromOptions(opts);

    print("chatting with {s} via {s}. Type /help for commands, /bye to exit.\n", .{ model_name, server_url });
    installSigintHandler();
    try runRepl(allocator, io, &history, &params, &session);
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 1) {
        printUsage();
        return error.InvalidArguments;
    }
    if (std.mem.eql(u8, args[0], "--help") or std.mem.eql(u8, args[0], "-h")) {
        printUsage();
        return error.HelpRequested;
    }

    var opts = Options{ .model = args[0] };
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = native_backend_choice.parse(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-tokens")) {
            i += 1;
            if (i >= args.len) return error.MissingMaxTokens;
            opts.max_tokens = try std.fmt.parseInt(i32, args[i], 10);
            if (opts.max_tokens < 1) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--max-context")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.max_context = try std.fmt.parseInt(usize, args[i], 10);
            if (opts.max_context < 256 or opts.max_context > hard_max_context) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            i += 1;
            if (i >= args.len) return error.MissingTemperature;
            opts.temperature = try std.fmt.parseFloat(f32, args[i]);
            if (!std.math.isFinite(opts.temperature) or opts.temperature < 0 or opts.temperature > 2) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--top-p")) {
            i += 1;
            if (i >= args.len) return error.MissingTopP;
            opts.top_p = try std.fmt.parseFloat(f32, args[i]);
            if (!std.math.isFinite(opts.top_p) or opts.top_p < 0 or opts.top_p > 1) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--top-k")) {
            i += 1;
            if (i >= args.len) return error.MissingTopK;
            opts.top_k = try std.fmt.parseInt(i32, args[i], 10);
            if (opts.top_k < 0) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--repetition-penalty")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.repetition_penalty = try std.fmt.parseFloat(f32, args[i]);
            if (!std.math.isFinite(opts.repetition_penalty) or opts.repetition_penalty <= 0) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--system")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.system = args[i];
        } else if (std.mem.eql(u8, arg, "--models-dir")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.models_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--token")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.hf_token = args[i];
        } else if (std.mem.eql(u8, arg, "--server")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            opts.server_url = args[i];
        } else if (std.mem.eql(u8, arg, "--prompt-cache")) {
            opts.prompt_cache = true;
        } else if (std.mem.eql(u8, arg, "--no-prompt-cache")) {
            opts.prompt_cache = false;
        } else if (std.mem.eql(u8, arg, "--print-timing")) {
            opts.print_timing = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return error.HelpRequested;
        } else {
            print("unknown option: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }
    return opts;
}

fn printUsage() void {
    print(
        \\usage: antfly inference chat <model> [options]
        \\
        \\<model> is a friendly alias (gemma4-e2b, gemma4-e4b), a HuggingFace
        \\reference (owner/name[:variant]), or a local model directory. Known
        \\models are pulled automatically on first use.
        \\
        \\options:
        \\  --backend <choice>        auto|native|metal|cuda (default: auto)
        \\  --max-tokens <n>          per-turn response limit (default: 512)
        \\  --max-context <n>         context window budget in tokens
        \\                            (default: min(model context, 8192))
        \\  --temperature <float>     sampling temperature (default: 0.7; 0 = greedy)
        \\  --top-p <float>           nucleus sampling (default: 0.95)
        \\  --top-k <n>               top-k sampling (default: 64)
        \\  --repetition-penalty <f>  repetition penalty (default: 1.0)
        \\  --system <text>           system prompt (survives /clear)
        \\  --models-dir <dir>        models directory (default: ~/.antfly/inference/models)
        \\  --token <token>           HuggingFace token for pulls (or HF_TOKEN)
        \\  --server <url>            chat against a running inference server
        \\  --prompt-cache            reserved; rejected until streaming prefix reuse is safe
        \\  --print-timing            verbose per-turn pipeline timing
        \\
        \\slash commands: /bye /clear /set /show /help, and """ for multi-line input.
        \\Ctrl-C stops the current response; Ctrl-D or /bye exits.
        \\
    , .{});
}

test "classifyReplLine parses commands, messages, and multiline toggles" {
    try std.testing.expect(classifyReplLine("") == .empty);
    try std.testing.expect(classifyReplLine("   ") == .empty);
    try std.testing.expect(classifyReplLine("\"\"\"") == .multiline_toggle);
    try std.testing.expect(classifyReplLine("  \"\"\"  ") == .multiline_toggle);

    const msg = classifyReplLine("  hello world  ");
    try std.testing.expectEqualStrings("hello world", msg.message);

    try std.testing.expect(classifyReplLine("/bye").command == .bye);
    try std.testing.expect(classifyReplLine("/exit").command == .bye);
    try std.testing.expect(classifyReplLine("/quit").command == .bye);
    try std.testing.expect(classifyReplLine("/clear").command == .clear);
    try std.testing.expect(classifyReplLine("/show").command == .show);
    try std.testing.expect(classifyReplLine("/help").command == .help);

    const set = classifyReplLine("/set temperature 0.7").command.set;
    try std.testing.expectEqualStrings("temperature", set.param);
    try std.testing.expectEqualStrings("0.7", set.value);

    try std.testing.expect(classifyReplLine("/set temperature").command == .unknown);
    try std.testing.expect(classifyReplLine("/frobnicate").command == .unknown);
}

test "applySet validates parameters and values" {
    var params = ChatParams{
        .max_tokens = 512,
        .temperature = 0.7,
        .top_p = 0.95,
        .top_k = 64,
        .repetition_penalty = 1.0,
    };
    try applySet(&params, "temperature", "0.2");
    try std.testing.expectApproxEqAbs(@as(f32, 0.2), params.temperature, 0.0001);
    try applySet(&params, "top-p", "0.9");
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), params.top_p, 0.0001);
    try applySet(&params, "top_k", "40");
    try std.testing.expectEqual(@as(i32, 40), params.top_k);
    try applySet(&params, "max-tokens", "128");
    try std.testing.expectEqual(@as(i32, 128), params.max_tokens);
    try applySet(&params, "repetition-penalty", "1.1");
    try std.testing.expectApproxEqAbs(@as(f32, 1.1), params.repetition_penalty, 0.0001);

    try std.testing.expectError(error.UnknownParam, applySet(&params, "nope", "1"));
    try std.testing.expectError(error.InvalidParamValue, applySet(&params, "max-tokens", "0"));
    try std.testing.expectError(error.InvalidParamValue, applySet(&params, "top-p", "1.5"));
    try std.testing.expectError(error.InvalidParamValue, applySet(&params, "top-p", "nan"));
    try std.testing.expectError(error.InvalidParamValue, applySet(&params, "temperature", "inf"));
    try std.testing.expectError(error.InvalidParamValue, applySet(&params, "repetition-penalty", "nan"));
    try std.testing.expectError(error.InvalidCharacter, applySet(&params, "temperature", "abc"));
}

test "ChatHistory keeps the system message across clear and trims in pairs" {
    var history = try ChatHistory.init(std.testing.allocator, "be brief");
    defer history.deinit();

    try std.testing.expectEqual(@as(usize, 1), history.messages.items.len);
    try history.appendUser("first question");
    try history.appendAssistant("first answer");
    try history.appendUser("second question");
    try std.testing.expectEqual(@as(usize, 4), history.messages.items.len);
    try std.testing.expectEqual(@as(usize, 4), history.visibleCount());

    // Oldest droppable unit is the completed user/assistant pair.
    try std.testing.expectEqual(@as(usize, 2), history.nextTrimDrop().?);
    try history.trimOldest(history.nextTrimDrop().?);
    try std.testing.expectEqual(@as(usize, 2), history.visibleCount());
    try std.testing.expectEqual(@as(usize, 2), history.messages.items.len);
    // Only the newest user message remains: nothing left to trim.
    try std.testing.expect(history.nextTrimDrop() == null);

    var visible = std.ArrayListUnmanaged(generation.Message).empty;
    defer visible.deinit(std.testing.allocator);
    try history.collectVisible(std.testing.allocator, &visible);
    try std.testing.expectEqual(@as(usize, 2), visible.items.len);
    try std.testing.expectEqualStrings("system", visible.items[0].role);
    try std.testing.expectEqualStrings("second question", visible.items[1].content);

    history.popLast();
    try std.testing.expectEqual(@as(usize, 1), history.visibleCount());

    try history.clear();
    try std.testing.expectEqual(@as(usize, 1), history.messages.items.len);
    try std.testing.expectEqualStrings("be brief", history.messages.items[0].content);
    try std.testing.expectEqual(@as(usize, 1), history.start);
}

test "ChatHistory without system prompt trims single leading messages" {
    var history = try ChatHistory.init(std.testing.allocator, null);
    defer history.deinit();
    try std.testing.expectEqual(@as(usize, 0), history.messages.items.len);

    try history.appendUser("only question");
    try std.testing.expect(history.nextTrimDrop() == null);

    try history.appendAssistant("answer");
    try history.appendUser("follow-up");
    try std.testing.expectEqual(@as(usize, 2), history.nextTrimDrop().?);
}

test "parseArgs parses model, flags, and rejects unknown options" {
    const opts = try parseArgs(&.{
        "gemma4-e2b",
        "--max-tokens",
        "128",
        "--temperature",
        "0",
        "--system",
        "be brief",
        "--prompt-cache",
        "--server",
        "http://127.0.0.1:8090",
    });
    try std.testing.expectEqualStrings("gemma4-e2b", opts.model);
    try std.testing.expectEqual(@as(i32, 128), opts.max_tokens);
    try std.testing.expectApproxEqAbs(@as(f32, 0), opts.temperature, 0.0001);
    try std.testing.expectEqualStrings("be brief", opts.system.?);
    try std.testing.expect(opts.prompt_cache);
    try std.testing.expectEqualStrings("http://127.0.0.1:8090", opts.server_url.?);

    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{}));
    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{ "gemma4-e2b", "--wat" }));
    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{ "gemma4-e2b", "--temperature", "nan" }));
    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{ "gemma4-e2b", "--top-p", "inf" }));
    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{ "gemma4-e2b", "--top-k", "-1" }));
    try std.testing.expectError(error.InvalidArguments, parseArgs(&.{ "gemma4-e2b", "--repetition-penalty", "nan" }));
    try std.testing.expectError(error.HelpRequested, parseArgs(&.{"--help"}));
}

test "prompt cache is rejected before local or server chat starts" {
    try std.testing.expectError(
        error.InvalidArguments,
        main(std.testing.allocator, std.testing.io, &.{ "owner/model", "--prompt-cache" }),
    );
    try std.testing.expectError(
        error.InvalidArguments,
        main(std.testing.allocator, std.testing.io, &.{ "owner/model", "--server", "http://127.0.0.1:1", "--prompt-cache" }),
    );
}

test "server prompt estimate is conservative and bounded" {
    const messages = [_]generation.Message{
        .{ .role = "system", .content = "brief" },
        .{ .role = "user", .content = "hello" },
    };
    try std.testing.expectEqual(@as(usize, 74), ServerChatSession.estimatedPromptTokenUpperBound(&messages));
}

test "server request carries backend and sampling options" {
    const messages = [_]api.ChatMessage{.{
        .role = .user,
        .content = .{ .string = "hello" },
    }};
    const params = ChatParams{
        .max_tokens = 37,
        .temperature = 0.4,
        .top_p = 0.8,
        .top_k = 12,
        .repetition_penalty = 1.1,
    };
    const request = makeServerGenerateRequest("owner/model:gguf:Q4_K_M", &messages, params, .metal);
    try std.testing.expectEqualStrings("owner/model:gguf:Q4_K_M", request.model);
    try std.testing.expectEqual(api.ModelBackend.metal, request.backend.?);
    try std.testing.expect(request.prompt_cache == null);
    try std.testing.expect(request.prompt_cache_key == null);
    try std.testing.expectEqual(@as(?i64, 37), request.max_tokens);
    try std.testing.expectEqual(@as(?i64, 12), request.top_k);
}

test "server request cancellation interrupts a blocked response" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    chat_interrupt.store(false, .release);
    defer chat_interrupt.store(false, .release);

    // Leave the accepted request intentionally unserved. The interrupt task
    // must cancel the in-flight socket wait rather than waiting for 300s.
    var server = try httpx.TestServer.start(allocator, io, &.{});
    defer server.deinit();
    const url = try server.url("/v1/generate");
    defer allocator.free(url);
    var client = httpx.Client.init(allocator, io);
    defer client.deinit();
    var session: ServerChatSession = undefined;
    session.http = &client;
    session.url = url;
    var writer = native_generate.ServerGenerateSseWriter{ .allocator = allocator };
    defer writer.deinit();

    const Interrupt = struct {
        fn run(test_io: std.Io) std.Io.Cancelable!void {
            try (std.Io.Timeout{ .duration = .{
                .raw = std.Io.Duration.fromMilliseconds(40),
                .clock = .awake,
            } }).sleep(test_io);
            chat_interrupt.store(true, .release);
        }
    };
    var group = std.Io.Group.init;
    try group.concurrent(io, Interrupt.run, .{io});
    defer group.await(io) catch {};

    try std.testing.expectError(
        error.ChatInterrupted,
        session.requestWithInterrupt("{}", &writer),
    );
}

test "looksLikePath detects explicit path prefixes" {
    try std.testing.expect(looksLikePath("/abs/model"));
    try std.testing.expect(looksLikePath("./model"));
    try std.testing.expect(looksLikePath("../model"));
    try std.testing.expect(!looksLikePath("gemma4-e2b"));
    try std.testing.expect(!looksLikePath("owner/name"));
}

test "runRepl drives a stub runner through commands and messages" {
    const StubRunner = struct {
        turns: usize = 0,
        shows: usize = 0,
        last_message: [256]u8 = undefined,
        last_message_len: usize = 0,

        fn runTurn(self: *@This(), history: *ChatHistory, params: ChatParams) !void {
            _ = params;
            self.turns += 1;
            const last = history.messages.items[history.messages.items.len - 1];
            const len = @min(last.content.len, self.last_message.len);
            @memcpy(self.last_message[0..len], last.content[0..len]);
            self.last_message_len = len;
            try history.appendAssistant("ok");
        }

        fn showInfo(self: *@This(), params: ChatParams) void {
            _ = params;
            self.shows += 1;
        }
    };

    // Exercise the pure line-classification plus history plumbing the way the
    // REPL does, without a TTY: feed classified lines through submitMessage.
    var history = try ChatHistory.init(std.testing.allocator, null);
    defer history.deinit();
    var params = ChatParams{
        .max_tokens = 16,
        .temperature = 0,
        .top_p = 0,
        .top_k = 0,
        .repetition_penalty = 1.0,
    };
    var runner = StubRunner{};

    try submitMessage(&history, params, &runner, "hello");
    try std.testing.expectEqual(@as(usize, 1), runner.turns);
    try std.testing.expectEqualStrings("hello", runner.last_message[0..runner.last_message_len]);
    try std.testing.expectEqual(@as(usize, 2), history.messages.items.len);

    switch (classifyReplLine("/set temperature 0.3")) {
        .command => |cmd| switch (cmd) {
            .set => |set| try applySet(&params, set.param, set.value),
            else => unreachable,
        },
        else => unreachable,
    }
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), params.temperature, 0.0001);

    try history.clear();
    try std.testing.expectEqual(@as(usize, 0), history.messages.items.len);
}
