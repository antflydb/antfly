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
