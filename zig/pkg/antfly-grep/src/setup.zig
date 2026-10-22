// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Explicit local configuration and agent integration commands.
const std = @import("std");
const Options = @import("options.zig").Options;
const providers = @import("providers.zig");
pub const Config = struct { provider: []const u8 = "antfly", endpoint: []const u8 = "http://127.0.0.1:8080/ai/v1/decisions", model: ?[]const u8 = null, token: ?[]const u8 = null, models_dir: ?[]const u8 = null, library: ?[]const u8 = null, decision_concurrency: ?usize = null, batch_size: ?usize = null, decision_batch_wait_ms: ?u64 = null, decision_transport: ?@FieldType(Options, "decision_transport") = null, decision_context: ?@FieldType(Options, "decision_context") = null };
pub fn configPath(a: std.mem.Allocator, env: *const std.process.Environ.Map) !?[]const u8 {
    if (env.get("ANTFLY_GREP_CREDENTIALS_PATH")) |path| return try a.dupe(u8, path);
    if (env.get("XDG_CONFIG_HOME")) |path| return try std.fs.path.join(a, &.{ path, "antfly-grep", "credentials.json" });
    if (@import("builtin").os.tag == .windows) if (env.get("APPDATA")) |path| return try std.fs.path.join(a, &.{ path, "antfly-grep", "credentials.json" });
    if (env.get("HOME") orelse env.get("USERPROFILE")) |path| return try std.fs.path.join(a, &.{ path, ".config", "antfly-grep", "credentials.json" });
    return null;
}
pub fn load(a: std.mem.Allocator, io: std.Io, env: *const std.process.Environ.Map, o: *Options) !void {
    const path = try configPath(a, env) orelse return;
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(65536)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    const parsed = try std.json.parseFromSlice(Config, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    const c = parsed.value;
    o.endpoint = c.endpoint;
    o.model = c.model;
    o.token = c.token;
    o.models_dir = c.models_dir;
    o.library_path = c.library;
    if (c.decision_concurrency) |value| o.decision_concurrency = value;
    if (c.batch_size) |value| o.batch_size = value;
    if (c.decision_batch_wait_ms) |value| o.decision_batch_wait_ms = value;
    if (c.decision_context) |value| o.decision_context = value;
    const provider = providers.get(c.provider) orelse {
        // Stale decision configuration must not break ordinary regex searches.
        if (!providers.removed(c.provider)) return error.UnsupportedProvider;
        o.provider_name = c.provider;
        return;
    };
    o.provider_name = c.provider;
    o.decision_transport = c.decision_transport orelse (if (provider.protocol == .decisions) .single else .antfly_batch);
    o.backend = switch (provider.protocol) {
        .local => .local,
        .decisions, .antfly => .http,
    };
}
pub fn auth(parent_a: std.mem.Allocator, io: std.Io, env: *const std.process.Environ.Map, args: []const []const u8) !void {
    for (args) |arg| if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
        try std.Io.File.stdout().writeStreamingAll(io,
            \\Usage: antfly-grep auth [OPTIONS]
            \\
            \\Configure a nonsecret decision profile. Keys come from the environment.
            \\  --provider NAME    antfly, openai, local, custom
            \\  --endpoint URL     Explicit decision endpoint
            \\  --base-url URL     Provider base URL
            \\  --model NAME       Decision model
            \\  --stdin            Removed: use OPENAI_API_KEY / ANTFLY_API_KEY / ANTFLY_GREP_TOKEN
            \\  --models-dir PATH  Local model directory
            \\  --library PATH     Local inference library
            \\  --decision-concurrency N   Save provider lane limit (0 follows threads)
            \\  --batch-size N             Save inputs per request (1..128)
            \\  --decision-batch-wait-ms N Save cross-file gather window (0..1000)
            \\  --decision-context FORMAT Save legacy or shared-v1 transport
            \\  -h, --help         Show this help
            \\
        );
        return;
    };
    var arena = std.heap.ArenaAllocator.init(parent_a);
    defer arena.deinit();
    const a = arena.allocator();
    var c = Config{};
    var stdin = false;
    var explicit_provider = false;
    var explicit_endpoint = false;
    var base_url = false;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const flag = args[i];
        if (std.mem.eql(u8, flag, "--stdin")) {
            stdin = true;
            continue;
        }
        if (i + 1 == args.len) return error.MissingFlagValue;
        i += 1;
        const value = args[i];
        if (std.mem.eql(u8, flag, "--provider")) {
            c.provider = value;
            explicit_provider = true;
        } else if (std.mem.eql(u8, flag, "--endpoint") or std.mem.eql(u8, flag, "--base-url")) {
            c.endpoint = value;
            explicit_endpoint = true;
            base_url = std.mem.eql(u8, flag, "--base-url");
        } else if (std.mem.eql(u8, flag, "--model")) c.model = value else if (std.mem.eql(u8, flag, "--models-dir")) c.models_dir = value else if (std.mem.eql(u8, flag, "--library")) c.library = value else if (std.mem.eql(u8, flag, "--decision-concurrency")) {
            const n = std.fmt.parseInt(usize, value, 10) catch return error.InvalidNumber;
            if (n > 256) return error.InvalidNumber;
            c.decision_concurrency = n;
        } else if (std.mem.eql(u8, flag, "--batch-size")) {
            const n = std.fmt.parseInt(usize, value, 10) catch return error.InvalidNumber;
            if (n < 1 or n > 128) return error.InvalidNumber;
            c.batch_size = n;
        } else if (std.mem.eql(u8, flag, "--decision-batch-wait-ms")) {
            const n = std.fmt.parseInt(u64, value, 10) catch return error.InvalidNumber;
            if (n > 1000) return error.InvalidNumber;
            c.decision_batch_wait_ms = n;
        } else if (std.mem.eql(u8, flag, "--decision-transport")) {
            c.decision_transport = if (std.mem.eql(u8, value, "single")) .single else if (std.mem.eql(u8, value, "antfly-batch")) .antfly_batch else return error.InvalidDecisionTransport;
        } else if (std.mem.eql(u8, flag, "--decision-context")) {
            c.decision_context = std.meta.stringToEnum(@FieldType(Options, "decision_context"), if (std.mem.eql(u8, value, "shared-v1")) "shared_v1" else value) orelse return error.InvalidDecisionContext;
        } else return error.UnknownFlag;
    }
    const interactive = !stdin and (try std.Io.File.stdin().isTty(io));
    if (interactive and !explicit_provider) {
        const selected = try @import("terminal.zig").prompt(a, io, "Provider [antfly/openai/local/custom] (antfly): ", false);
        if (selected.len > 0) c.provider = selected;
    }
    const provider = providers.get(c.provider) orelse return if (providers.removed(c.provider)) error.JevRequiresAntflyGateway else error.UnsupportedProvider;
    if (!explicit_endpoint) c.endpoint = provider.endpoint;
    if (c.model == null) c.model = provider.model;
    if (std.mem.eql(u8, c.provider, "custom") and !explicit_endpoint) {
        if (!interactive) return error.ProviderEndpointRequired;
        c.endpoint = try @import("terminal.zig").prompt(a, io, "Decisions base URL: ", false);
        base_url = true;
    }
    if (provider.protocol != .local and base_url and !std.mem.endsWith(u8, c.endpoint, "/decisions")) c.endpoint = try std.fmt.allocPrint(a, "{s}/decisions", .{std.mem.trimEnd(u8, c.endpoint, "/")});
    if (provider.protocol != .local) try providers.validateCustom(c.endpoint);
    if (c.model == null and interactive) {
        c.model = try @import("terminal.zig").prompt(a, io, "Decision model: ", false);
        if (c.model.?.len == 0) return error.DecisionModelRequired;
    }
    if (c.model == null) return error.DecisionModelRequired;
    const transport: @FieldType(Options, "decision_transport") = c.decision_transport orelse (if (provider.protocol == .decisions) .single else .antfly_batch);
    if (provider.protocol != .local and transport == .single and c.decision_context == .shared_v1) return error.SharedContextRequiresAntflyBatch;
    // Profiles contain no new persisted secrets. Existing credentials are read
    // for migration, but environment keys are resolved only at request time.
    if (stdin) return error.UseEnvironmentCredential;
    const path = try configPath(a, env) orelse return error.MissingConfigDirectory;
    if (std.fs.path.dirname(path)) |dir| try std.Io.Dir.cwd().createDirPath(io, dir);
    const data = try std.json.Stringify.valueAlloc(a, c, .{ .emit_null_optional_fields = false });
    var nonce: [8]u8 = undefined;
    io.random(&nonce);
    const temp = try std.fmt.allocPrint(a, "{s}.{s}.tmp", .{ path, std.fmt.bytesToHex(nonce, .lower) });
    defer std.Io.Dir.cwd().deleteFile(io, temp) catch {};
    var file = try std.Io.Dir.cwd().createFile(io, temp, .{ .exclusive = true, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) });
    file.writeStreamingAll(io, data) catch |err| {
        file.close(io);
        return err;
    };
    file.close(io);
    try std.Io.Dir.cwd().rename(temp, .cwd(), path, io);
    try std.Io.File.stdout().writeStreamingAll(io, "Saved antfly-grep provider configuration. Run antfly-grep doctor to verify access.\n");
}
const skill =
    \\---
    \\name: antfly-grep
    \\description: Search source by exact text or behavior using the native Antfly CLI.
    \\---
    \\
    \\Use `antfly-grep -n PATTERN PATH` for known names or strings.
    \\Use `antfly-grep ask 'behavior question' PATH` for unfamiliar behavior.
    \\Read returned source and declaration/call locations before editing.
    \\File preview scores are reading leads; region and individual line scores
    \\have different scopes. Omitted excerpts and exit 2 mean incomplete evidence.
    \\Use `--broad`, a lower threshold, or exact search to investigate missing evidence.
    \\Run `antfly-grep files PATH` before remote searches to inspect eligible paths.
    \\Configure explicitly with `auth --provider antfly --base-url URL --model MODEL`
    \\or use `--backend local --models-dir PATH --library PATH --model MODEL`.
    \\Never print or save credentials in repository files. Run `doctor` to verify setup.
    \\
;
pub fn installSkill(a: std.mem.Allocator, io: std.Io, env: *const std.process.Environ.Map, args: []const []const u8) !void {
    var agents: std.ArrayList([]const u8) = .empty;
    var global = false;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--global")) global = true else if (std.mem.eql(u8, args[i], "--yes")) {} else if (std.mem.eql(u8, args[i], "--agent")) {
            i += 1;
            if (i == args.len) return error.MissingFlagValue;
            try agents.append(a, args[i]);
        } else return error.UnknownFlag;
    }
    if (agents.items.len == 0) try agents.append(a, "codex");
    const root = if (global) env.get("HOME") orelse env.get("USERPROFILE") orelse return error.MissingHome else ".";
    for (agents.items) |agent| {
        const directory = if (std.mem.eql(u8, agent, "codex")) ".agents/skills/antfly-grep" else if (std.mem.eql(u8, agent, "claude-code") or std.mem.eql(u8, agent, "claude")) ".claude/skills/antfly-grep" else if (std.mem.eql(u8, agent, "opencode")) ".opencode/skills/antfly-grep" else return error.UnsupportedAgent;
        const dir = try std.fs.path.join(a, &.{ root, directory });
        try std.Io.Dir.cwd().createDirPath(io, dir);
        const path = try std.fs.path.join(a, &.{ dir, "SKILL.md" });
        var file = try std.Io.Dir.cwd().createFile(io, path, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, skill);
        const notice = try std.fmt.allocPrint(a, "Installed {s}\n", .{path});
        try std.Io.File.stdout().writeStreamingAll(io, notice);
    }
}
