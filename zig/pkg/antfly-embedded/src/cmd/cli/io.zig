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

pub const std = @import("std");
pub const agent = @import("agent.zig");
pub const human = @import("human.zig");

// One policy per invocation in the CLI runtime unit. It is deliberately not
// installed in SDKs or server request handlers.
pub var policy: agent.Policy = .{};
pub var context: agent.Context = .{};
var stderr_mutex: std.Io.Mutex = .init;
var diagnostic_io: ?std.Io = null;
var diagnostic_reported: bool = false;
pub var progress_enabled: bool = false;
pub var execution_bounded: bool = false;

fn writeStderrLine(io: std.Io, bytes: []const u8) void {
    stderr_mutex.lockUncancelable(io);
    defer stderr_mutex.unlock(io);
    std.Io.File.stderr().writeStreamingAll(io, bytes) catch {};
    std.Io.File.stderr().writeStreamingAll(io, "\n") catch {};
}

pub fn configure(io: std.Io, selected: agent.Policy) void {
    diagnostic_io = io;
    diagnostic_reported = false;
    policy = selected;
    context = .{};
    progress_enabled = false;
    execution_bounded = false;
}

pub fn isAgent() bool {
    return policy.enabled;
}

pub fn useJson() bool {
    return policy.enveloped() or policy.format == .json or policy.format == .compact;
}

pub fn isHuman() bool {
    return !policy.enveloped() and policy.format != .json and policy.format != .compact;
}

pub const ErrorCategory = enum { argument, command, http, delivery };

pub fn emitError(code: []const u8, category: ErrorCategory, status: ?u16, diagnostic: ?std.json.Value) void {
    if (diagnostic_reported) return;
    const io = diagnostic_io orelse return;
    const bytes = std.json.Stringify.valueAlloc(std.heap.page_allocator, .{
        .agent_contract = @as(u32, 1),
        .@"error" = .{
            .code = code,
            .category = category,
            .http_status = status,
            .execution = if (category == .argument) "rejected" else agent.failureStatus(code, diagnostic),
            .retryable = if (diagnostic != null and diagnostic.? == .object) diagnostic.?.object.get("retryable") else null,
            .hint = if (diagnostic != null and diagnostic.? == .string) diagnostic.?.string else "Inspect diagnostics and reconcile mutation receipts before retrying; antfly --help describes global options.",
            .diagnostic = diagnostic,
        },
    }, .{}) catch return;
    defer std.heap.page_allocator.free(bytes);
    writeStderrLine(io, bytes);
    diagnostic_reported = true;
}

pub fn reportFailure(err: anyerror) bool {
    if (!policy.enabled) return false;
    emitError(@errorName(err), .command, null, null);
    return true;
}

pub fn httpDiagnostic(status: u16, body: ?[]const u8) void {
    if (!policy.enabled) return;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var safe: std.json.ObjectMap = .empty;
    if (body) |raw| {
        const parsed = std.json.parseFromSlice(std.json.Value, a, raw, .{ .parse_numbers = false }) catch {
            emitError("HttpFailure", .http, status, null);
            return;
        };
        var value = parsed.value;
        if (value == .object) if (value.object.get("error")) |inner| {
            if (inner == .object) value = inner;
        };
        if (value == .object) {
            // Do not echo server prose, SQL text, parameters or credential-bearing
            // transport URLs. Preserve authoritative recovery fields verbatim.
            for ([_][]const u8{
                "code",
                "column_name",
                "command_tag",
                "rows_affected",
                "connection_id",
                "restore_job_id",
                "idempotency_key",
                "status",
                "sqlstate",
                "position",
                "parameter_index",
                "source_position",
                "transaction_id",
                "transaction_status",
                "session_id",
                "mutation_outcome",
                "ddl_receipt",
                "receipt",
                "receipts",
                "retryable",
                "retryability",
                "reconciliation",
            }) |key| if (value.object.get(key)) |field| {
                safe.put(a, key, field) catch {};
            };
        }
    }
    emitError("HttpFailure", .http, status, .{ .object = safe });
}

pub fn progress(io: std.Io, event: []const u8, elapsed_ms: ?i64) void {
    if (!policy.enabled or !progress_enabled) return;
    const bytes = std.json.Stringify.valueAlloc(std.heap.page_allocator, .{
        .agent_contract = @as(u32, 1),
        .event = event,
        .elapsed_ms = elapsed_ms,
    }, .{}) catch return;
    defer std.heap.page_allocator.free(bytes);
    writeStderrLine(io, bytes);
}

pub fn writeStdout(io: std.Io, bytes: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, bytes) catch {
        if (policy.enabled) emitError("StdoutWriteFailed", .delivery, null, null);
        std.process.exit(1);
    };
}

/// Delivery failure is independent of server execution. This is scoped to
/// the response being written, so a previous batch receipt cannot be reused.
fn responseDeliveryFailed(input: anytype) noreturn {
    if (policy.enabled) deliveryDiagnostic(input) catch emitError("StdoutWriteFailed", .delivery, null, null);
    std.process.exit(1);
}

fn deliveryDiagnostic(input: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try std.json.Stringify.valueAlloc(a, input, .{});
    const parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{ .parse_numbers = false });
    const controls = try agent.recoveryControls(a, context.scope, parsed.value);
    const bytes = try std.json.Stringify.valueAlloc(a, .{
        .agent_contract = @as(u32, 1),
        .@"error" = .{
            .code = "StdoutWriteFailed",
            .category = "delivery",
            .http_status = @as(?u16, null),
            .execution = @tagName(context.execution),
            .retryable = false,
            .hint = "The response was received but stdout delivery failed. Reconcile the supplied controls; never replay a mutation to recover its output.",
            .diagnostic = .{
                .control = std.json.Value{ .object = controls },
                // Opaque mutation outcomes may have operation-specific recovery
                // fields unknown to the shared renderer. Preserve them all.
                .outcome = if (context.scope == .operation) parsed.value else std.json.Value.null,
            },
        },
    }, .{});
    writeStderrLine(diagnostic_io orelse return, bytes);
    diagnostic_reported = true;
}

fn writeResponse(io: std.Io, bytes: []const u8, input: anytype) void {
    std.Io.File.stdout().writeStreamingAll(io, bytes) catch responseDeliveryFailed(input);
}

pub fn readFileAlloc(io: std.Io, allocator: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
}

/// Runtime failures never imply that a submitted operation was rejected.
pub fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    if (policy.enabled) {
        emitFormattedError("CommandFailed", .command, fmt, args);
    } else std.debug.print("error: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

/// Call only while validating input, before submitting the operation.
pub fn argumentError(comptime fmt: []const u8, args: anytype) noreturn {
    if (policy.enabled) {
        emitFormattedError("InvalidArguments", .argument, fmt, args);
    } else std.debug.print("error: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

/// Agent output carries the formatted message, never the raw format string.
fn emitFormattedError(code: []const u8, category: ErrorCategory, comptime fmt: []const u8, args: anytype) void {
    const a = std.heap.page_allocator;
    const message = formatMessage(a, fmt, args) orelse return emitError(code, category, null, .{ .string = fmt });
    defer a.free(message);
    emitError(code, category, null, .{ .string = message });
}

fn formatMessage(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ?[]u8 {
    return std.fmt.allocPrint(allocator, fmt, args) catch null;
}

test "agent error messages are formatted with their arguments" {
    const a = std.testing.allocator;
    const message = formatMessage(a, "timed out after {d}ms waiting for index {s}", .{ @as(u64, 30000), "knowledge" }).?;
    defer a.free(message);
    try std.testing.expectEqualStrings("timed out after 30000ms waiting for index knowledge", message);
}

pub fn argumentFailure(err: anyerror) anyerror {
    if (policy.enabled) emitError(@errorName(err), .argument, null, null);
    return err;
}

/// Global flags and legacy command-local flags share one presentation policy.
pub fn selectFormat(format: agent.Format) !void {
    policy.format = format;
    validatePolicy() catch |err| return argumentFailure(err);
}

pub fn validatePolicy() !void {
    if (policy.fingerprint != null and (policy.format == .json or policy.format == .compact)) return error.InvalidFingerprintOutputFormat;
}

pub const platform = @import("antfly_platform");
pub const antfly_client = @import("antfly-client");
pub const httpx = @import("httpx");

pub const OutputFormat = enum { json, table_fmt };

pub const GlobalConfig = struct {
    url: []const u8 = "http://127.0.0.1:8080",
    token: ?[]const u8 = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    output: OutputFormat = .json,
};

pub fn parseGlobalFlags() GlobalConfig {
    var config = GlobalConfig{};
    if (platform.env.getenv("ANTFLY_URL")) |raw| {
        config.url = raw;
    }
    if (platform.env.getenv("ANTFLY_TOKEN")) |raw| {
        config.token = raw;
    }
    config.username = platform.env.getenv("ANTFLY_USERNAME");
    config.password = platform.env.getenv("ANTFLY_PASSWORD");
    return config;
}

pub fn initClient(allocator: std.mem.Allocator, http: *httpx.Client, config: GlobalConfig) !antfly_client.AntflyClient {
    var client = try antfly_client.AntflyClient.init(allocator, http, config.url);
    errdefer client.deinit();
    if ((config.username == null) != (config.password == null)) return error.BasicAuthRequiresUsernameAndPassword;
    if (config.token != null and config.username != null) return error.ConflictingAuthentication;
    if (config.username) |username| try client.setBasicAuth(username, config.password.?);
    if (config.token) |token| {
        try client.setBearer(token);
    }
    if (policy.enabled) client.error_reporter = httpDiagnostic;
    return client;
}

pub fn writeJson(allocator: std.mem.Allocator, io: std.Io, value: anytype) !void {
    const configured_width = if (platform.env.getenv("COLUMNS")) |raw| std.fmt.parseUnsigned(usize, raw, 10) catch 100 else 100;
    const json = if (isHuman()) try human.render(allocator, policy, context, value, .{ .width = configured_width }) else try agent.encode(allocator, policy, context, value);
    defer allocator.free(json);
    writeResponse(io, json, value);
    if (!isHuman()) writeResponse(io, "\n", value);
}

pub const backup = @import("backup_wait.zig");

pub fn expectHttpSuccess(resp: anytype) void {
    if (resp.status_code < 200 or resp.status_code >= 300) {
        if (policy.enabled) {
            httpDiagnostic(resp.status_code, resp.err_body);
            std.process.exit(1);
        }
        if (resp.err_body) |body| fatal("request failed with HTTP {d}: {s}", .{ resp.status_code, body });
        fatal("request failed with HTTP {d}", .{resp.status_code});
    }
}

/// Mutation responses are never sampled: recovery needs the entire outcome.
pub fn mutationResponse(resp: anytype) !void {
    expectHttpSuccess(resp);
    const io = diagnostic_io orelse return;
    const previous = context;
    context = .{ .scope = .operation, .execution = .complete };
    defer context = previous;
    if (resp.data) |data| return writeJson(std.heap.page_allocator, io, data.value);
    return writeJson(std.heap.page_allocator, io, .{ .http_status = resp.status_code });
}

pub fn acknowledge(operation: []const u8) !void {
    const io = diagnostic_io orelse return;
    const previous = context;
    context = .{ .scope = .operation, .execution = .complete };
    defer context = previous;
    try writeJson(std.heap.page_allocator, io, .{ .operation = operation, .status = "acknowledged" });
}

/// Status prose remains available to humans; agent stderr stays JSON Lines.
pub fn notice(comptime fmt: []const u8, args: anytype) void {
    if (!policy.enabled) {
        std.debug.print(fmt, args);
        return;
    }
    const io = diagnostic_io orelse return;
    const a = std.heap.page_allocator;
    const message = std.fmt.allocPrint(a, fmt, args) catch return;
    defer a.free(message);
    const bytes = std.json.Stringify.valueAlloc(a, .{ .agent_contract = @as(u32, 1), .event = "notice", .message = std.mem.trim(u8, message, "\r\n") }, .{}) catch return;
    defer a.free(bytes);
    writeStderrLine(io, bytes);
}

/// Decode a transport response before applying presentation policy.
pub fn writeJsonResponseBytes(allocator: std.mem.Allocator, io: std.Io, body: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    try writeJson(allocator, io, parsed.value);
}

/// Artifact exports are input to another program, not presentation responses.
/// Preserve their JSON document under every mode/format and display budget.
/// Parsing is only for validation and recovery on a failed stdout write.
pub fn writeJsonArtifact(allocator: std.mem.Allocator, io: std.Io, body: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const previous = context;
    context = .{ .scope = .operation, .execution = .complete };
    defer context = previous;
    writeResponse(io, body, parsed.value);
    if (!std.mem.endsWith(u8, body, "\n")) writeResponse(io, "\n", parsed.value);
}

pub fn progressStats(io: std.Io, stats: anytype) void {
    if (!policy.enabled or !progress_enabled) return;
    const bytes = std.json.Stringify.valueAlloc(std.heap.page_allocator, .{ .agent_contract = @as(u32, 1), .event = "query_statistics", .measured = stats }, .{}) catch return;
    defer std.heap.page_allocator.free(bytes);
    writeStderrLine(io, bytes);
}
