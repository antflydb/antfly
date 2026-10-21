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

//! Decoder selection is independent of the model and preparation devices.
const std = @import("std");
pub const Policy = enum { automatic, portable, apple };
pub const Backend = enum { portable, apple_hardware, apple_software };
/// May be shared with preparation workers. Read a complete snapshot after join.
pub const Telemetry = struct {
    portable: std.atomic.Value(usize) = .init(0),
    apple_hardware: std.atomic.Value(usize) = .init(0),
    apple_software: std.atomic.Value(usize) = .init(0),
    fallbacks: std.atomic.Value(usize) = .init(0),
    validated_frames: std.atomic.Value(usize) = .init(0),
    pub fn record(self: *Telemetry, backend: Backend) void {
        const count = switch (backend) {
            .portable => &self.portable,
            .apple_hardware => &self.apple_hardware,
            .apple_software => &self.apple_software,
        };
        _ = count.fetchAdd(1, .monotonic);
    }
};
pub const Options = struct {
    policy: Policy = .automatic,
    telemetry: ?*Telemetry = null,
    pub fn record(self: Options, backend: Backend) void {
        if (self.telemetry) |stats| stats.record(backend);
    }
    pub fn fallback(self: Options) void {
        if (self.telemetry) |stats| _ = stats.fallbacks.fetchAdd(1, .monotonic);
    }
};
/// Only capability failures before publication permit automatic replay. Invalid
/// input, cancellation, budgets and consumer failures must retain their identity.
pub fn unavailable(err: anyerror) bool {
    return switch (err) {
        error.UnsupportedVideoBackend,
        error.UnsupportedVideoProfile,
        error.VideoDecoderUnavailable,
        error.HardwareVideoDecoderRequired,
        error.UnsupportedSurfaceFormat,
        error.UnsupportedDynamicGeometry,
        => true,
        else => false,
    };
}
test "decoder fallback excludes malformed input cancellation and admission" {
    try std.testing.expect(unavailable(error.VideoDecoderUnavailable));
    try std.testing.expect(unavailable(error.UnsupportedSurfaceFormat));
    for ([_]anyerror{ error.VideoDecodeFailed, error.InvalidVideoData, error.Cancelled, error.OutOfMemory, error.SharedAdmissionExceeded, error.ConsumerFailure }) |err|
        try std.testing.expect(!unavailable(err));
    var stats = Telemetry{};
    const options = Options{ .telemetry = &stats };
    options.record(.apple_hardware);
    options.record(.portable);
    options.fallback();
    try std.testing.expectEqual(@as(usize, 1), stats.apple_hardware.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), stats.portable.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), stats.fallbacks.load(.monotonic));
}
