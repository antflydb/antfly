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

//! Explicit backend selection shared by Laya parity and serving qualification.
const std = @import("std");
const platform = @import("antfly_platform");
const backends = @import("../backends/backends.zig");
const factory = @import("../architectures/session_factory.zig");

pub fn parseBackend(value: ?[]const u8, legacy_metal: bool) !backends.BackendType {
    const backend: backends.BackendType = if (value) |name| blk: {
        if (std.mem.eql(u8, name, "native")) break :blk .native;
        if (std.mem.eql(u8, name, "metal")) break :blk .metal;
        if (std.mem.eql(u8, name, "cuda")) break :blk .cuda;
        return error.InvalidLayaTestBackend;
    } else if (legacy_metal) .metal else .native;
    if (legacy_metal and backend != .metal) return error.ConflictingLayaTestBackends;
    return backend;
}

pub fn selectedBackend() !backends.BackendType {
    return parseBackend(platform.env.getenv("ANTFLY_LAYA_BACKEND"), platform.env.getenv("ANTFLY_LAYA_METAL") != null);
}

pub fn fixture(name: [:0]const u8) ![]const u8 {
    return platform.env.getenv(name) orelse {
        if (platform.env.getenvBoolDefault("ANTFLY_LAYA_REQUIRE_TESTS", false)) return error.MissingLayaFixture;
        return error.SkipZigTest;
    };
}

pub fn createSession(a: std.mem.Allocator, path: []const u8) !backends.Session {
    const backend = try selectedBackend();
    const session = switch (backend) {
        .native => try factory.createNativeSession(a, path),
        .metal => try factory.createMetalSession(a, path),
        .cuda => try factory.createCudaSession(a, path),
        else => unreachable,
    };
    errdefer session.close();
    try std.testing.expectEqual(backend, session.backend());
    return session;
}

pub fn expectReadbacks(session: backends.Session, before: ?factory.CudaRuntimeStats, bytes: usize) !void {
    if (comptime @import("build_options").enable_cuda) {
        if (before) |previous| {
            const delta = factory.cudaStatsDelta(factory.getCudaRuntimeStats(session).?, previous);
            try std.testing.expectEqual(bytes, delta.to_float32_bytes);
            try std.testing.expectEqual(bytes, delta.d2h_bytes);
            try std.testing.expectEqual(@as(usize, 0), delta.rope_host_fallbacks);
            try std.testing.expectEqual(@as(usize, 0), delta.gqa_dense_host_fallbacks);
            try std.testing.expectEqual(@as(usize, 0), delta.resident_weight_bytes);
        }
    }
}

test "laya test backend selection rejects conflicts" {
    try std.testing.expectEqual(backends.BackendType.native, try parseBackend(null, false));
    try std.testing.expectEqual(backends.BackendType.metal, try parseBackend(null, true));
    try std.testing.expectEqual(backends.BackendType.cuda, try parseBackend("cuda", false));
    try std.testing.expectError(error.ConflictingLayaTestBackends, parseBackend("cuda", true));
    try std.testing.expectError(error.InvalidLayaTestBackend, parseBackend("automatic", false));
}
