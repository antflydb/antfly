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

/// The HTTP router accepts repeated and trailing slashes. Classify the same
/// namespace here so a normalized route cannot evade deployment policy/auth.
pub fn isInferenceApiPath(path: []const u8) bool {
    var segments = std.mem.tokenizeScalar(u8, path, '/');
    const namespace = segments.next() orelse return false;
    const version = segments.next() orelse return false;
    return (std.mem.eql(u8, namespace, "ai") or std.mem.eql(u8, namespace, "ml")) and
        std.mem.eql(u8, version, "v1");
}

test "httpx inference gateway policy recognizes router-normalized namespaces" {
    for ([_][]const u8{ "/ai/v1/generate", "/ai//v1/audio/transcriptions/", "//ml/v1/predict", "/ai/v1", "/ml/v1/" }) |path|
        try std.testing.expect(isInferenceApiPath(path));
    for ([_][]const u8{ "/ai/v10/models", "/ml/v1evil/predict", "/db/v1/query", "/internal/v1/groups/7", "/healthz", "/" }) |path|
        try std.testing.expect(!isInferenceApiPath(path));
}

pub fn isDocumentJobPath(path: []const u8) bool {
    var parts = std.mem.tokenizeScalar(u8, path, '/');
    if (!std.mem.eql(u8, parts.next() orelse "", "ai") or !std.mem.eql(u8, parts.next() orelse "", "v1")) return false;
    const task = parts.next() orelse return false;
    return (std.mem.eql(u8, task, "read") or std.mem.eql(u8, task, "extract")) and std.mem.eql(u8, parts.next() orelse "", "jobs");
}
test "httpx inference gateway document HA route classification is exact and normalized" {
    for ([_][]const u8{ "/ai/v1/read/jobs", "//ai/v1/extract//jobs/id/cancel/" }) |path| try std.testing.expect(isDocumentJobPath(path));
    for ([_][]const u8{ "/ai/v1/read/execute", "/ai/v1/read/jobs-other", "/ai/v10/read/jobs" }) |path| try std.testing.expect(!isDocumentJobPath(path));
}

pub fn isDocumentJobMutation(method: []const u8, path: []const u8) bool {
    if (!std.mem.eql(u8, method, "POST") or !isDocumentJobPath(path)) return false;
    var parts = std.mem.tokenizeScalar(u8, path, '/');
    for (0..4) |_| _ = parts.next();
    _ = parts.next() orelse return true;
    const action = parts.next() orelse return false;
    return parts.next() == null and (std.mem.eql(u8, action, "cancel") or std.mem.eql(u8, action, "resume"));
}
test "httpx inference gateway HA admits only declared document job mutations" {
    for ([_][]const u8{ "/ai/v1/read/jobs", "/ai/v1/extract/jobs/id/resume", "/ai/v1/read/jobs/id/cancel/" }) |path| {
        try std.testing.expect(isDocumentJobMutation("POST", path));
        try std.testing.expect(!isDocumentJobMutation("PATCH", path));
    }
    for ([_][]const u8{ "/ai/v1/read/jobs/id/events", "/ai/v1/read/jobs/id/unknown", "/ai/v1/read/execute" }) |path| try std.testing.expect(!isDocumentJobMutation("POST", path));
}
