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

//! Stable failure policy shared by durable document owners and provider adapters.
const std = @import("std");
const A = std.mem.Allocator;
pub const Failure = struct {
    retryable: bool,
    code: []const u8,
    /// Present only for recognized protocol errors, never caller-provided text.
    cause: ?anyerror = null,
};
const terminal = .{
    error.DocumentStorageNotConfigured,       error.DocumentModelRevisionMismatch,
    error.DocumentBudgetExhausted,            error.UnsupportedMediaTokenBudget,
    error.InvalidDocumentCheckpoint,          error.InvalidDocumentSource,
    error.UnsupportedDocumentFormat,          error.DocumentSourceLimitExceeded,
    error.DocumentOutputTooLarge,             error.DocumentPageLimitExceeded,
    error.DocumentPixelLimitExceeded,         error.DocumentMediaTypeMismatch,
    error.DocumentTokenLimitExceeded,         error.InvalidDocumentSelection,
    error.DuplicateDocumentSource,            error.DocumentUnresolved,
    error.DocumentCapabilityMismatch,         error.UnsupportedDocumentRequestField,
    error.ModelNotFound,                      error.RemoteDocumentModelDenied,
    error.CloudflareCredentialMissing,        error.InvalidCloudflareConfig,
    error.UnsupportedCloudflareModel,         error.UnsupportedCloudflareSchema,
    error.CloudflareAuthenticationFailed,     error.InvalidExtractorResponse,
    error.UnsupportedExtractionSchemaVersion, error.InvalidInferenceRequest,
    error.InferenceEndpointAccessDenied,      error.DocumentArtifactNotFound,
    error.DocumentArtifactExpired,            error.UnsupportedAudioReadCapability,
    error.AudioTooLong,                       error.UnsupportedAudioInput,
    error.InvalidTranscriptionLanguage,       error.SpeakerModelUnavailable,
};
const transient = .{
    error.CloudflareRateLimited,        error.InferenceUpstreamRateLimited,
    error.InferenceCapacityUnavailable, error.InferenceTransportUnavailable,
    error.DocumentRunLeaseLost,         error.DocumentRunStoreNeedsRecovery,
    error.DocumentRunBusy,              error.DocumentStageInFlight,
    error.Timeout,                      error.ResourceTemporarilyUnavailable,
    error.CloudflareInferenceFailed,    error.InvalidCloudflareResponse,
    error.OutOfMemory,
};
pub fn fromError(err: anyerror) Failure {
    inline for (terminal) |candidate| if (err == candidate) return .{ .retryable = false, .code = @errorName(err), .cause = err };
    return .{ .retryable = true, .code = @errorName(err), .cause = err };
}
/// Decode only a bounded, recognized error envelope. Protocol errors retain
/// their native meaning even when a peer maps them to a generic HTTP status.
pub fn recognized(a: A, body: []const u8) ?Failure {
    if (body.len > 4096) return null;
    var parsed = std.json.parseFromSlice(struct { @"error": []const u8 = "" }, a, body, .{ .ignore_unknown_fields = true, .duplicate_field_behavior = .@"error" }) catch return null;
    defer parsed.deinit();
    inline for (terminal ++ transient) |err| if (std.mem.eql(u8, parsed.value.@"error", @errorName(err))) return fromError(err);
    return null;
}
pub fn fromResponse(a: A, status: u16, body: []const u8) Failure {
    if (recognized(a, body)) |failure| return failure;
    return switch (status) {
        408, 425, 429 => .{ .retryable = true, .code = "InferenceUpstreamTemporarilyUnavailable" },
        400...407, 409...424, 426...428, 430...499 => .{ .retryable = false, .code = "DocumentRequestRejected" },
        else => .{ .retryable = true, .code = "InferenceTransportUnavailable" },
    };
}

test "document failure classification preserves native error identity across HTTP statuses" {
    const a = std.testing.allocator;
    for ([_]u16{ 400, 408, 429, 500, 502, 504 }) |status| {
        const rate = fromResponse(a, status, "{\"error\":\"CloudflareRateLimited\"}");
        try std.testing.expect(rate.retryable);
        try std.testing.expectEqual(fromError(error.CloudflareRateLimited), rate);
        const budget = fromResponse(a, status, "{\"error\":\"DocumentBudgetExhausted\"}");
        try std.testing.expect(!budget.retryable);
        try std.testing.expectEqual(fromError(error.DocumentBudgetExhausted), budget);
        const tokens = fromResponse(a, status, "{\"error\":\"DocumentTokenLimitExceeded\"}");
        try std.testing.expect(!tokens.retryable);
        try std.testing.expectEqual(fromError(error.DocumentTokenLimitExceeded), tokens);
    }
    for ([_]u16{ 408, 425, 429, 500, 502, 503, 504 }) |status| try std.testing.expect(fromResponse(a, status, "invalid upstream error body").retryable);
    for ([_]u16{ 400, 401, 403, 404, 409, 413, 415, 422 }) |status| try std.testing.expect(!fromResponse(a, status, "{}").retryable);
    try std.testing.expect(recognized(a, "{\"error\":\"unrecognized user-controlled error\"}") == null);
    try std.testing.expect(recognized(a, "{\"error\":\"Timeout\",\"error\":\"DocumentBudgetExhausted\"}") == null);
    try std.testing.expect(recognized(a, &@as([4097]u8, @splat(' '))) == null);
}
