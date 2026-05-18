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

const std = @import("std");

pub const zig_backend = @import("mp3/mp3.zig");
pub const pure_zig = zig_backend;
pub const bitstream = @import("mp3/bitstream.zig");
pub const huffman = @import("mp3/huffman.zig");
pub const requantize = @import("mp3/requantize.zig");
pub const imdct = @import("mp3/imdct.zig");
pub const synthesis = @import("mp3/synthesis.zig");
pub const conformance = @import("mp3/conformance.zig");

pub const Backend = enum {
    zig,
};

pub const Decoded = struct {
    samples: []f32,
    sample_rate: u32,
};

pub const DecodedInterleaved = struct {
    samples: []f32,
    sample_rate: u32,
    channels: u8,
};

pub fn enabled() bool {
    return zig_backend.enabled();
}

pub fn backendEnabled(backend: Backend) bool {
    return switch (backend) {
        .zig => zig_backend.enabled(),
    };
}

pub fn preferredBackend() ?Backend {
    if (backendEnabled(.zig)) return .zig;
    return null;
}

pub fn decodeMono(allocator: std.mem.Allocator, mp3_bytes: []const u8) !Decoded {
    return decodeMonoWithBackend(allocator, mp3_bytes, .zig);
}

pub fn decodeInterleaved(allocator: std.mem.Allocator, mp3_bytes: []const u8) !DecodedInterleaved {
    return decodeInterleavedWithBackend(allocator, mp3_bytes, .zig);
}

pub fn decodeMonoWithBackend(
    allocator: std.mem.Allocator,
    mp3_bytes: []const u8,
    backend: Backend,
) !Decoded {
    return switch (backend) {
        .zig => zig_backend.decodeMono(allocator, mp3_bytes),
    };
}

pub fn decodeInterleavedWithBackend(
    allocator: std.mem.Allocator,
    mp3_bytes: []const u8,
    backend: Backend,
) !DecodedInterleaved {
    return switch (backend) {
        .zig => zig_backend.decodeInterleaved(allocator, mp3_bytes),
    };
}

test "preferred backend is coherent with enabled backends" {
    const backend = preferredBackend();
    if (backend) |selected| {
        try std.testing.expect(backendEnabled(selected));
    } else {
        try std.testing.expect(!backendEnabled(.zig));
    }
}

test "preferred backend uses pure zig when available" {
    if (!backendEnabled(.zig)) return error.SkipZigTest;
    try std.testing.expectEqual(Backend.zig, preferredBackend().?);
}
