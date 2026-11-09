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

const std = @import("std");
const api = @import("antfly_metadata_openapi");
const g = @import("antfly_generating");
const time = @import("antfly_platform").time;
pub const context = g.context_management;

pub fn configFromPublic(a: std.mem.Allocator, config: api.AgentContextManagement) !context.Config {
    if ((config.keep_recent_turns orelse 2) < 0 or (config.keep_recent_turns orelse 2) > 4096 or
        (config.max_summary_batches orelse 8) < 1 or (config.max_summary_batches orelse 8) > 32) return error.InvalidContextRequest;
    return .{
        .mode = if (config.mode) |mode| std.meta.stringToEnum(context.preparation.Mode, mode) orelse return error.InvalidContextRequest else .compact,
        .context_window_tokens = try positive(config.context_window_tokens),
        .target_input_tokens = if (config.target_input_tokens) |v| try positive(v) else null,
        .reserved_output_tokens = try positive(config.reserved_output_tokens orelse 4000),
        .keep_recent_turns = if ((config.keep_recent_turns orelse 2) >= 0) @intCast(config.keep_recent_turns orelse 2) else return error.InvalidContextRequest,
        .summarizer_context_window_tokens = try positive(config.summarizer_context_window_tokens orelse 8192),
        .summary_output_tokens = try positive(config.summary_output_tokens orelse 1024),
        .max_summary_batches = try positive(config.max_summary_batches orelse 8),
        .summarizer = if (config.summarizer) |cfg| try g.configFromOpenApi(a, cfg) else null,
    };
}
fn positive(value: anytype) !usize {
    if (value <= 0) return error.InvalidContextRequest;
    return std.math.cast(usize, value) orelse return error.InvalidContextRequest;
}

pub const Prepared = struct { messages: []const g.ChatMessage, next: context.State, event: api.AgentContextEvent };

pub fn prepare(a: std.mem.Allocator, runner: anytype, config: api.AgentContextManagement, chain: []const g.ChainLink, messages: []const g.ChatMessage, previous: context.State, control: context.preparation.Control) !Prepared {
    const started = time.monotonicNs();
    const result = try context.prepare(a, runner, try configFromPublic(a, config), chain, messages, previous, control);
    const encoded = try std.json.Stringify.valueAlloc(a, .{
        .diagnostics = result.event.diagnostics,
        .counting_method = result.event.counting_method,
        .summary_provider = result.event.summary_provider,
        .summary_model = result.event.summary_model,
        .latency_ms = (time.monotonicNs() -| started) / std.time.ns_per_ms,
    }, .{ .emit_null_optional_fields = false });
    const event = try std.json.parseFromSlice(api.AgentContextEvent, a, encoded, .{});
    return .{ .messages = result.messages, .next = result.next, .event = event.value };
}

/// Completed retrieval/research evidence remains data, with a separate live
/// question. No authoritative instructions move into the summarized span.
pub fn completedEvidence(a: std.mem.Allocator, system: g.ChatMessage, evidence: g.ChatMessage, question: []const u8) ![]g.ChatMessage {
    return a.dupe(g.ChatMessage, &.{
        system,
        .{ .role = .user, .content = .{ .text = question } },
        .{ .role = .assistant, .tool_calls = &.{.{ .id = "antfly_context_evidence", .name = "assembled_evidence", .arguments = "{}" }} },
        .{ .role = .tool, .tool_call_id = "antfly_context_evidence", .content = evidence.content },
        .{ .role = .assistant, .content = .{ .text = "Evidence collection is complete; continue with the requested task." } },
    });
}

pub fn requireHitIds(a: std.mem.Allocator, config: api.AgentContextManagement, hits: []const api.QueryHit, source: []const g.ChatMessage, messages: []const g.ChatMessage) !void {
    if (config.mode != null and !std.mem.eql(u8, config.mode.?, "compact")) return;
    for (hits) |hit| {
        if (try hasIdentifier(a, source, hit._id) and !try hasIdentifier(a, messages, hit._id)) return error.ContextSummaryMissingCitation;
    }
}

pub const Failure = struct { status: u16, message: []const u8 };
pub fn failure(err: anyerror) ?Failure {
    return switch (err) {
        error.InvalidContextRequest => .{ .status = 400, .message = "context_management requires positive, bounded input/output budgets within the destination and summarizer context windows" },
        error.ContextOutputReservationTooSmall => .{ .status = 400, .message = "increase context_management.reserved_output_tokens to cover every destination generator output limit" },
        error.UnsupportedContextMedia, error.UnsupportedContextHistoryMetadata => .{ .status = 400, .message = "agent context management requires text-only public history; opaque provider metadata cannot be imported through this agent API" },
        error.AgentContextBudgetExceeded, error.InsufficientContextReduction => .{ .status = 413, .message = "protected context and validated memory exceed target_input_tokens; increase the destination budget or reduce evidence/history" },
        error.CompactionUnitTooLarge => .{ .status = 413, .message = "one complete exchange plus memory, instructions and schema exceeds the summarizer input budget; increase summarizer_context_window_tokens or reduce tool output" },
        error.ContextCompactionBatchLimitExceeded => .{ .status = 422, .message = "max_summary_batches exhausted before context fit; increase the bounded limit or reduce history" },
        error.InvalidContextSummary, error.ContextSummaryMissingCitation, error.ContextSummaryTruncated => .{ .status = 502, .message = "summarizer returned invalid, incomplete or unattributed memory; authoritative history was preserved; select a suitable summarizer or increase summary_output_tokens" },
        else => null,
    };
}

pub fn publicHistory(a: std.mem.Allocator, messages: ?[]const @import("antfly_generating_openapi").ChatMessage) ![]const g.ChatMessage {
    const input = messages orelse return &.{};
    const out = try a.alloc(g.ChatMessage, input.len);
    for (input, out) |message, *target| {
        if (message.extra_content != null) return error.UnsupportedContextHistoryMetadata;
        const calls = try a.alloc(g.ToolCall, if (message.tool_calls) |items| items.len else 0);
        if (message.tool_calls) |items| for (items, calls) |call, *copy| {
            if (call.extra_content != null) return error.UnsupportedContextHistoryMetadata;
            copy.* = .{ .id = call.id, .name = call.function.name, .arguments = call.function.arguments };
        };
        target.* = .{ .role = std.meta.stringToEnum(g.Role, @tagName(message.role)).?, .content = if (message.content) |content| if (content == .string) .{ .text = content.string } else if (content == .null) null else return error.UnsupportedContextMedia else null, .tool_calls = if (calls.len > 0) calls else null, .tool_call_id = message.tool_call_id };
    }
    return out;
}

/// Compare identifier tokens, including JSON-escaped identifiers inside a
/// rendered evidence record. E1 must not be satisfied by E10.
pub fn hasIdentifier(a: std.mem.Allocator, messages: []const g.ChatMessage, id: []const u8) !bool {
    if (id.len == 0) return false;
    var needle = id;
    for (0..3) |_| {
        for (messages) |message| if (message.content) |content| {
            if (content != .text) continue;
            var start: usize = 0;
            while (std.mem.indexOfPos(u8, content.text, start, needle)) |index| {
                const end = index + needle.len;
                const left = index == 0 or !identifierByte(content.text[index - 1]);
                const right = end == content.text.len or !identifierByte(content.text[end]);
                if (left and right) return true;
                start = end;
            }
        };
        const encoded = try std.json.Stringify.valueAlloc(a, needle, .{});
        const next = encoded[1 .. encoded.len - 1];
        if (std.mem.eql(u8, needle, next)) break;
        needle = next;
    }
    return false;
}
fn identifierByte(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or byte == '_';
}
