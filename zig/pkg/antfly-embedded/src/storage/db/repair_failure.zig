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

const transient_names = [_][]const u8{
    "ResourceTemporarilyUnavailable",
    "EnrichmentRetryInProgress",
    "InferenceCapabilitiesStale",
    "RemoteDocumentFetchFailed",
    "ConnectionResetByPeer",
    "ConnectionTimedOut",
    "Timeout",
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

/// Retries for one document within one operator repair pass, including the
/// first attempt.
pub const repair_transient_max_attempts: u32 = 4;
/// Total time one repair pass may spend backing off transient failures.
pub const repair_transient_pass_budget_ns: u64 = 60 * std.time.ns_per_s;

/// Backoff before retry `attempt` (1-based): 500ms, 1s, 2s, capped at 4s.
pub fn repairTransientBackoffNs(attempt: u32) u64 {
    const shift: u6 = @intCast(@min(attempt -| 1, 3));
    return @min((@as(u64, 500) * std.time.ns_per_ms) << shift, 4 * std.time.ns_per_s);
}

pub const transient_hint = "temporary failure; run repair again";

/// The next step for an operator, keyed by the recorded error name.
pub fn hint(name: []const u8) []const u8 {
    if (isTransientName(name)) return transient_hint;
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

/// Error name to report for one repair issue: the source generation error
/// when there is one, otherwise the last repair error.
pub fn issueErrorCode(issue: anytype) []const u8 {
    if (issue.generation_error.len != 0) {
        // A later repair attempt that failed transiently is the more current
        // signal for what to do next.
        if (issue.last_error.len != 0 and isTransientName(issue.last_error)) return issue.last_error;
        return issue.generation_error;
    }
    if (issue.last_error.len != 0) return issue.last_error;
    return @tagName(issue.reason);
}

/// Groups issues by error code, in first-seen order. Strings borrow from
/// `issues` and from static hint text; free only the returned slice.
pub fn groupIssuesAlloc(alloc: std.mem.Allocator, issues: anytype) ![]FailureGroup {
    var groups = std.ArrayListUnmanaged(FailureGroup).empty;
    errdefer groups.deinit(alloc);
    for (issues) |issue| {
        const code = issueErrorCode(issue);
        const attempts: u64 = @max(issue.attempts, issue.generation_attempts);
        for (groups.items) |*group| {
            if (std.mem.eql(u8, group.error_code, code)) {
                group.count += 1;
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
