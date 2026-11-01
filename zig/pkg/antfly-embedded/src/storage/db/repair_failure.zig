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

//! Shared classification of enrichment and repair failures by error name.
//! Transient failures (admission refusals, rate limits, temporary provider
//! errors) clear on their own and are retried; every other failure needs an
//! operator change. Repair summaries use the same table for their hints, so
//! the retry policy and the advice shown to operators cannot disagree.
const std = @import("std");
const platform_time = @import("antfly_platform").time;
const execution_context = @import("antfly_inference_execution_context");

/// Query-time request context admission retries wait against. Shared here
/// so every admission-retry caller (reranking, generation) checks and waits
/// the same way instead of inventing its own sleep.
pub const RequestContext = execution_context.RequestContext;

const transient_names = [_][]const u8{
    "ResourceTemporarilyUnavailable",
    "InferenceCapacityUnavailable",
    "QueueFull",
    "EnrichmentRetryInProgress",
    "InferenceCapabilitiesStale",
    "RemoteDocumentFetchFailed",
    "ConnectionResetByPeer",
    "ConnectionTimedOut",
    "Timeout",
    // The managed embedding adapters' normalized name for a local provider's
    // admission refusal (see managed_embedder.normalizeLocalEmbeddingError).
    // Distinct from EmbedTransientFailure, which stays a real provider
    // failure (5xx, timeout, connection error) and must keep spending the
    // document's attempt budget.
    "EmbedAdmissionRefused",
};

const transient_suffixes = [_][]const u8{
    "RateLimited",
    "TransientFailure",
    "TemporarilyUnavailable",
    "Busy",
};

pub fn isTransientName(name: []const u8) bool {
    for (transient_names) |candidate| {
        if (std.mem.eql(u8, name, candidate)) return true;
    }
    for (transient_suffixes) |suffix| {
        if (std.mem.endsWith(u8, name, suffix)) return true;
    }
    return false;
}

pub fn isTransient(err: anyerror) bool {
    return isTransientName(@errorName(err));
}

/// The transient failures that refused to admit work, so the request never
/// ran. They don't spend a document's attempt budget. Other transient failures
/// (rate limits, timeouts) still do, so a lasting outage eventually parks as
/// repair debt instead of retrying forever.
pub fn isAdmissionRefusalName(name: []const u8) bool {
    if (std.mem.eql(u8, name, "ResourceTemporarilyUnavailable") or
        std.mem.eql(u8, name, "InferenceCapacityUnavailable") or
        std.mem.eql(u8, name, "QueueFull") or
        // The managed embedding adapters' distinct identity for a local
        // provider's admission refusal, preserved separately from
        // EmbedTransientFailure precisely so this exemption does not also
        // cover real provider transient failures.
        std.mem.eql(u8, name, "EmbedAdmissionRefused")) return true;
    return std.mem.endsWith(u8, name, "Busy") or std.mem.endsWith(u8, name, "TemporarilyUnavailable");
}

pub fn isAdmissionRefusal(err: anyerror) bool {
    return isAdmissionRefusalName(@errorName(err));
}

/// Retries for one document within one operator repair pass, including the
/// first attempt.
pub const repair_transient_max_attempts: u32 = 4;
/// Total time one repair pass may spend backing off transient failures.
pub const repair_transient_pass_budget_ns: u64 = 60 * std.time.ns_per_s;

/// Total time a synchronous, query-time call (reranking, generation, the
/// embedding dimension probe) may spend backing off an admission refusal
/// before surfacing it to the caller. An admission refusal means the work
/// never ran, so retrying it costs nothing but time: the model may still be
/// loading, or another request may be briefly holding the scratch budget.
/// Shared so every admission-retry caller uses the same ceiling.
pub const default_admission_retry_budget_ns: u64 = 45 * std.time.ns_per_s;

/// Backoff before retry `attempt` (1-based): 500ms, 1s, 2s, capped at 4s.
pub fn repairTransientBackoffNs(attempt: u32) u64 {
    const shift: u6 = @intCast(@min(attempt -| 1, 3));
    return @min((@as(u64, 500) * std.time.ns_per_ms) << shift, 4 * std.time.ns_per_s);
}

/// Interval between cancellation/deadline polls while waiting out an
/// admission-retry backoff. Backoffs grow to `repairTransientBackoffNs`'s
/// 4s cap, so a plain unconditional sleep would hold a cancelled or expired
/// query-time request (and its worker thread) for seconds after it should
/// have stopped; polling at this granularity bounds that to one interval.
pub const admission_retry_poll_ns: u64 = 25 * std.time.ns_per_ms;

/// Minimum time that must remain before a request deadline to make
/// starting another admission-retry dispatch worth it. Below this, treat
/// the request the same as already expired instead of starting it.
pub const admission_retry_min_remaining_ns: u64 = 5 * std.time.ns_per_ms;

/// Checked immediately before every admission-retry provider dispatch --
/// the first attempt and every retry. Surfaces the request's own
/// cancellation/timeout error, never a provider error, once the request is
/// cancelled, has expired, or has too little time left before its deadline
/// to usefully try again.
pub fn checkAdmissionDispatch(context: RequestContext) !void {
    try context.check();
    const deadline = context.deadline_ns orelse return;
    if (deadline -| platform_time.monotonicNs() <= admission_retry_min_remaining_ns) return error.Timeout;
}

/// Waits out one admission-retry backoff of `delay_ns`. The wait is
/// cancellable and bounded by whichever is sooner, `delay_ns` or the
/// request's remaining deadline: it polls in `admission_retry_poll_ns`
/// steps, clipped to the remaining deadline, so a cancelled or expiring
/// request is noticed within one poll interval instead of only after the
/// full backoff elapses. Returns the request's own cancellation/timeout
/// error, never a provider error, if either fires during the wait.
pub fn admissionRetryWaitNs(context: RequestContext, delay_ns: u64) !void {
    var remaining_ns = delay_ns;
    while (remaining_ns > 0) {
        try checkAdmissionDispatch(context);
        var step_ns = @min(remaining_ns, admission_retry_poll_ns);
        if (context.deadline_ns) |deadline| {
            step_ns = @min(step_ns, @max(deadline -| platform_time.monotonicNs(), 1));
        }
        // The Io's own cancellation is a distinct signal from the request's
        // CancellationToken (checked above via checkAdmissionDispatch); a
        // bare wake here just shortens this step, and the loop re-checks
        // the request's context before sleeping again.
        context.io.sleep(std.Io.Duration.fromNanoseconds(@intCast(step_ns)), .awake) catch {};
        remaining_ns -= step_ns;
    }
    try checkAdmissionDispatch(context);
}

/// Decides whether an admission-refusal failure should be retried again.
/// Returns the backoff to wait before the next attempt, consuming it from
/// `remaining_budget_ns` (mutated in place); returns `null` once the
/// admission-retry budget itself is spent, telling the caller to surface
/// the provider's refusal unchanged. The request's deadline is always
/// checked first and wins: a request that would expire before the next
/// backoff completes returns the request's own timeout/cancellation error
/// instead, since retrying past the deadline would both dispatch work that
/// can never be used and mislead the caller about why the call failed. This
/// makes the effective retry budget the earlier of the configured
/// admission-retry budget and the request's remaining deadline.
pub fn nextAdmissionRetryDelayNs(context: RequestContext, attempt: u32, remaining_budget_ns: *u64) !?u64 {
    try checkAdmissionDispatch(context);
    const delay_ns = repairTransientBackoffNs(attempt);
    if (context.deadline_ns) |deadline| {
        const deadline_remaining_ns = deadline -| platform_time.monotonicNs();
        if (delay_ns > deadline_remaining_ns) {
            try context.check();
            return error.Timeout;
        }
    }
    if (delay_ns > remaining_budget_ns.*) return null;
    remaining_budget_ns.* -= delay_ns;
    return delay_ns;
}

pub const transient_hint = "temporary failure; run repair again";

/// The next step for an operator, keyed by the recorded error name.
pub fn hint(name: []const u8) []const u8 {
    if (isTransientName(name)) return transient_hint;
    if (std.mem.eql(u8, name, "MemoryBudgetExceeded"))
        return "the request can never fit the configured inference memory budget; raise --inference-scratch-budget-mb (or the other inference memory budget flags) or use a smaller model, then run repair again";
    if (std.mem.eql(u8, name, "ModelNotFound") or std.mem.eql(u8, name, "UnknownModel"))
        return "pull the model named in the index's enrichment config with `antfly inference pull <owner/model>`, then run repair again";
    if (std.mem.eql(u8, name, "source_document_missing"))
        return "the source document no longer exists; rewrite or delete it";
    if (std.mem.eql(u8, name, "embedding_enrichment_unavailable") or std.mem.eql(u8, name, "artifact_reprocessor_unavailable"))
        return "configure the index's enrichment provider on this server, then run repair again";
    if (std.mem.eql(u8, name, "DocumentExtractionModelRequired") or
        (std.mem.startsWith(u8, name, "Invalid") and std.mem.indexOf(u8, name, "Config") != null))
        return "fix the index enrichment config";
    if (std.mem.endsWith(u8, name, "Unauthorized") or std.mem.endsWith(u8, name, "Forbidden") or
        std.mem.endsWith(u8, name, "AccessDenied"))
        return "check the provider credentials and permissions";
    if (std.mem.startsWith(u8, name, "Unsupported"))
        return "use a provider, model, or input this index supports";
    if (std.mem.endsWith(u8, name, "TooLarge") or std.mem.endsWith(u8, name, "LimitExceeded") or
        std.mem.endsWith(u8, name, "BudgetExhausted"))
        return "the document exceeds a size or memory limit; raise the limit or split the document";
    return "fix the cause named by the error, then run repair again";
}

pub const FailureGroup = struct {
    error_code: []const u8,
    count: u64,
    /// Highest repair attempt count among the group's documents.
    max_attempts: u64 = 0,
    transient: bool,
    hint: []const u8,
};

/// A repair attempt records the current failure in last_error. Before any
/// repair attempt, fall back to the original generation failure.
pub fn issueErrorCode(issue: anytype) []const u8 {
    if (issue.last_error.len != 0) return issue.last_error;
    if (issue.generation_error.len != 0) return issue.generation_error;
    return @tagName(issue.reason);
}

pub fn sourceDocument(issue: anytype) []const u8 {
    if (@hasField(@TypeOf(issue), "parent_doc_key")) {
        if (issue.parent_doc_key.len != 0) return issue.parent_doc_key;
    }
    return if (@hasField(@TypeOf(issue), "doc_key")) issue.doc_key else "";
}

/// Groups issues by error code, counting each source document once per group.
/// Shared consumers and chunks may contribute multiple issues; all of them
/// still contribute to max_attempts. Strings borrow from `issues` and static
/// hint text; free only the returned slice.
pub fn groupIssuesAlloc(alloc: std.mem.Allocator, issues: anytype) ![]FailureGroup {
    var groups = std.ArrayListUnmanaged(FailureGroup).empty;
    errdefer groups.deinit(alloc);
    for (issues, 0..) |issue, i| {
        const code = issueErrorCode(issue);
        const document = sourceDocument(issue);
        const duplicate = if (document.len == 0) false else for (issues[0..i]) |earlier| {
            if (std.mem.eql(u8, code, issueErrorCode(earlier)) and
                std.mem.eql(u8, document, sourceDocument(earlier))) break true;
        } else false;
        const attempts: u64 = @max(issue.attempts, issue.generation_attempts);
        for (groups.items) |*group| {
            if (std.mem.eql(u8, group.error_code, code)) {
                if (!duplicate) group.count += 1;
                group.max_attempts = @max(group.max_attempts, attempts);
                break;
            }
        } else try groups.append(alloc, .{
            .error_code = code,
            .count = 1,
            .max_attempts = attempts,
            .transient = isTransientName(code),
            .hint = hint(code),
        });
    }
    return try groups.toOwnedSlice(alloc);
}

test "repair failure classification separates transient from operator errors" {
    try std.testing.expect(isTransient(error.ResourceTemporarilyUnavailable));
    try std.testing.expect(isTransient(error.EmbedRateLimited));
    try std.testing.expect(isTransient(error.ExtractionTransientFailure));
    try std.testing.expect(isTransient(error.EnrichmentRetryInProgress));
    try std.testing.expect(!isTransient(error.ModelNotFound));
    try std.testing.expect(!isTransient(error.InvalidExtractionConfig));
    try std.testing.expect(!isTransient(error.DocumentBudgetExhausted));
    try std.testing.expectEqual(@as(u64, 500 * std.time.ns_per_ms), repairTransientBackoffNs(1));
    try std.testing.expectEqual(@as(u64, 4 * std.time.ns_per_s), repairTransientBackoffNs(9));
}

test "repair failure hints group by error code" {
    const Issue = struct {
        generation_error: []const u8 = "",
        last_error: []const u8 = "",
        attempts: u64 = 0,
        generation_attempts: u64 = 0,
        reason: enum { enrichment_failed } = .enrichment_failed,
    };
    const issues = [_]Issue{
        .{ .generation_error = "ModelNotFound", .generation_attempts = 1 },
        .{ .generation_error = "ModelNotFound", .attempts = 2 },
        .{ .generation_error = "EmbedRateLimited", .last_error = "ResourceTemporarilyUnavailable", .attempts = 3 },
        .{ .generation_error = "InvalidExtractionConfig" },
    };
    const groups = try groupIssuesAlloc(std.testing.allocator, &issues);
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 3), groups.len);
    try std.testing.expectEqualStrings("ModelNotFound", groups[0].error_code);
    try std.testing.expectEqual(@as(u64, 2), groups[0].count);
    try std.testing.expectEqual(@as(u64, 2), groups[0].max_attempts);
    try std.testing.expect(std.mem.indexOf(u8, groups[0].hint, "antfly inference pull") != null);
    try std.testing.expectEqualStrings("ResourceTemporarilyUnavailable", groups[1].error_code);
    try std.testing.expect(groups[1].transient);
    try std.testing.expectEqualStrings(transient_hint, groups[1].hint);
    try std.testing.expectEqualStrings("fix the index enrichment config", groups[2].hint);
}

test "repair failure summary reports the latest permanent repair error" {
    const issues = [_]struct {
        generation_error: []const u8,
        last_error: []const u8,
        attempts: u64 = 1,
        generation_attempts: u64 = 1,
        reason: enum { enrichment_failed } = .enrichment_failed,
    }{
        .{ .generation_error = "ModelNotFound", .last_error = "InvalidEmbeddingResponse" },
        .{ .generation_error = "EmbedRateLimited", .last_error = "source_document_missing" },
        .{ .generation_error = "ModelNotFound", .last_error = "" },
    };
    const groups = try groupIssuesAlloc(std.testing.allocator, &issues);
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 3), groups.len);
    try std.testing.expectEqualStrings("InvalidEmbeddingResponse", groups[0].error_code);
    try std.testing.expectEqualStrings(hint("InvalidEmbeddingResponse"), groups[0].hint);
    try std.testing.expect(!groups[1].transient);
    try std.testing.expectEqualStrings("source_document_missing", groups[1].error_code);
    try std.testing.expectEqualStrings("ModelNotFound", groups[2].error_code);
}

test "repair failure counts source documents once per error across consumers and chunks" {
    const Issue = struct {
        doc_key: []const u8,
        parent_doc_key: []const u8 = "",
        index_name: []const u8 = "a",
        generation_error: []const u8 = "ModelNotFound",
        last_error: []const u8 = "",
        attempts: u64 = 0,
        generation_attempts: u64 = 1,
        reason: enum { enrichment_failed } = .enrichment_failed,
    };
    const issues = [_]Issue{
        .{ .doc_key = "doc:a" },
        .{ .doc_key = "doc:a", .index_name = "b", .attempts = 3 },
        .{ .doc_key = "chunk:1", .parent_doc_key = "doc:a", .attempts = 4 },
        .{ .doc_key = "chunk:2", .parent_doc_key = "doc:a" },
        .{ .doc_key = "doc:b" },
        .{ .doc_key = "doc:a", .last_error = "InvalidEmbeddingResponse" },
    };
    const groups = try groupIssuesAlloc(std.testing.allocator, &issues);
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 2), groups[0].count);
    try std.testing.expectEqual(@as(u64, 4), groups[0].max_attempts);
    try std.testing.expectEqual(@as(u64, 1), groups[1].count);
}

test "a budget that can never fit is permanent and names the flag" {
    try std.testing.expect(!isTransientName("MemoryBudgetExceeded"));
    try std.testing.expect(std.mem.indexOf(u8, hint("MemoryBudgetExceeded"), "--inference-scratch-budget-mb") != null);
    try std.testing.expect(isTransientName("ResourceTemporarilyUnavailable"));
}

test "admission refusals are the transient failures that never ran" {
    for ([_][]const u8{ "ResourceTemporarilyUnavailable", "InferenceCapacityUnavailable", "QueueFull", "AppleNativeBusy", "EmbedAdmissionRefused" }) |name| {
        try std.testing.expect(isAdmissionRefusalName(name));
        try std.testing.expect(isTransientName(name));
    }
    try std.testing.expect(!isAdmissionRefusalName("EmbedRateLimited"));
    try std.testing.expect(!isAdmissionRefusalName("Timeout"));
    try std.testing.expect(!isAdmissionRefusalName("MemoryBudgetExceeded"));
    // The managed embedding adapter's normalized name for a local admission
    // refusal is exempt; its sibling for a real provider transient failure
    // (5xx, timeout, connection error) is not.
    try std.testing.expect(isAdmissionRefusal(error.EmbedAdmissionRefused));
    try std.testing.expect(!isAdmissionRefusalName("EmbedTransientFailure"));
}
