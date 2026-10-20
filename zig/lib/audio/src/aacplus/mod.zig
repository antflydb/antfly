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

//! Bounded, request-owned AAC enhancement decoding. Output blocks contain raw
//! PCM16-scale floats, matching lib/audio's AAC core convention before downmix.
const std = @import("std");
const core = @import("core.zig");
const Sink = @import("../pcm_sink.zig").Sink;
const work = @import("../work_control.zig");
pub const max_packet_bytes = 64 * 1024;
const padding = 8192;
pub const Decoder = struct {
    allocator: std.mem.Allocator,
    memory: []u64,
    input: []u8,
    state: core.tPVMP4AudioDecoderExternal,
    output: [4096]i16 = undefined,
    floats: [4096]f32 = undefined,
    valid: bool = true,
    automatic_channels: bool,
    pub fn create(a: std.mem.Allocator, config: []const u8, channels: u8) !*Decoder {
        if (channels > 2 or config.len == 0 or config.len > max_packet_bytes) return error.UnsupportedAudioFormat;
        try work.check();
        const memory = try a.alloc(u64, (core.PVMP4AudioDecoderGetMemRequirements() + 7) / 8);
        errdefer a.free(memory);
        const input = try a.alloc(u8, max_packet_bytes + padding);
        errdefer a.free(input);
        const self = try a.create(Decoder);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .memory = memory, .input = input, .state = std.mem.zeroes(core.tPVMP4AudioDecoderExternal), .automatic_channels = channels == 0 };
        self.state.aacPlusEnabled = true;
        self.state.desiredChannels = if (channels == 0) 2 else channels;
        self.state.outputFormat = 0;
        if (core.PVMP4AudioDecoderInitLibrary(&self.state, memory.ptr) != 0) return error.UnsupportedAudioFormat;
        self.load(config);
        if (core.PVMP4AudioDecoderConfig(&self.state, memory.ptr) != 0 or self.state.samplingRate <= 0 or self.state.samplingRate > 192000) return error.UnsupportedAudioFormat;
        return self;
    }
    pub fn deinit(self: *Decoder) void {
        const a = self.allocator;
        a.free(self.input);
        a.free(self.memory);
        a.destroy(self);
    }
    fn load(self: *Decoder, bytes: []const u8) void {
        @memcpy(self.input[0..bytes.len], bytes);
        // The scalar bit reader can look ahead beyond the final coded byte.
        // A bounded zero tail belongs to this decoder, never to its caller.
        @memset(self.input[bytes.len..][0..padding], 0);
        self.state.pInputBuffer = self.input.ptr;
        self.state.inputBufferMaxLength = @intCast(self.input.len);
        self.state.inputBufferCurrentLength = @intCast(bytes.len);
        self.state.inputBufferUsedLength = 0;
        self.state.remainderBits = 0;
        self.state.repositionFlag = 0;
    }
    pub fn decode(self: *Decoder, unit: []const u8, sink: Sink) !void {
        if (!self.valid or unit.len == 0 or unit.len > max_packet_bytes) return error.UnsupportedAudioFormat;
        try work.check();
        errdefer self.valid = false;
        self.load(unit);
        self.state.pOutputBuffer = &self.output;
        self.state.pOutputBuffer_plus = self.output[2048..].ptr;
        if (core.PVMP4AudioDecodeFrame(&self.state, self.memory.ptr) != 0) return error.UnsupportedAudioFormat;
        if (self.state.inputBufferUsedLength < 0 or self.state.inputBufferUsedLength > unit.len or self.state.frameLength != 1024 or self.state.samplingRate <= 0 or self.state.samplingRate > 192000 or (self.state.aacPlusUpsamplingFactor != 1 and self.state.aacPlusUpsamplingFactor != 2) or self.state.encodedChannels < 1 or self.state.encodedChannels > 2) return error.UnsupportedAudioFormat;
        const native_channels: u8 = @intCast(self.state.desiredChannels);
        const ps = self.state.extendedAudioObjectType == core.MP4AUDIO_PS;
        if (ps and native_channels != 2) return error.UnsupportedAudioFormat;
        const channels: u8 = if (self.automatic_channels and !ps) @intCast(self.state.encodedChannels) else native_channels;
        const frames: usize = @intCast(self.state.frameLength * self.state.aacPlusUpsamplingFactor);
        if (native_channels == 1 and self.state.aacPlusUpsamplingFactor == 2) @memcpy(self.output[1024..2048], self.output[2048..3072]);
        const count = frames * channels;
        for (self.floats[0..count], 0..) |*out, i| out.* = @floatFromInt(self.output[if (channels == 1 and native_channels == 2) i * 2 else i]);
        try work.check();
        try sink.push(self.floats[0..count], @intCast(self.state.samplingRate), channels);
    }
};
pub fn visit(a: std.mem.Allocator, config: []const u8, units: []const []const u8, channels: u8, sink: Sink) !void {
    if (units.len == 0) return error.UnsupportedAudioFormat;
    const decoder = try Decoder.create(a, config, channels);
    defer decoder.deinit();
    for (units) |unit| try decoder.decode(unit, sink);
}
