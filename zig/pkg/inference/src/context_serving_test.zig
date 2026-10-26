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

//! Persistent, test-binary-only qualification driver. It owns one Node and
//! loaded-model cache for the complete stdin JSONL session. Release binaries
//! cannot import its manager candidate or serving observation hooks.
const std = @import("std");
const builtin = @import("builtin");
const server = @import("server/server.zig");
const serving_driver = @import("context_serving_driver.zig");
const serving_protocol = @import("context_serving_protocol.zig");
const serving_entrypoint = @import("context_serving_entrypoint.zig");
comptime {
    _ = serving_entrypoint;
}

const max_command_bytes = 64 * 1024;
const Variant = enum { exact, packed_exact, attention_matching };

const Command = struct {
    command: enum { next, measure, fault, stop },
    session_id: ?[]const u8 = null,
    request_index: ?u32 = null,
    route: ?enum { native, cli, http, stream, serial_mixed_batch } = null,
    pair_id: ?[]const u8 = null,
    warmup: ?bool = null,
    variant: ?Variant = null,
    prompt_file: ?[]const u8 = null,
    prompt_sha256: ?[]const u8 = null,
    prompt_tokens: ?u32 = null,
    reserved_output_tokens: ?u32 = null,
    settings_sha256: ?[]const u8 = null,
    variant_settings_sha256: ?[]const u8 = null,
    fault: ?[]const u8 = null,

    fn validate(self: @This()) !void {
        if (self.command == .stop) return;
        if (self.session_id == null or self.session_id.?.len != 64 or self.request_index == null or self.route == null)
            return error.InvalidServingDriverCommand;
        if (self.command == .measure) {
            if (self.pair_id == null or self.warmup == null or self.variant == null or
                self.prompt_file == null or self.prompt_sha256 == null or self.prompt_tokens == null or
                self.reserved_output_tokens == null or self.settings_sha256 == null or self.variant_settings_sha256 == null)
                return error.InvalidServingDriverCommand;
            if (self.route.? != .native or self.prompt_tokens.? == 0 or self.reserved_output_tokens.? < 2)
                return error.InvalidServingDriverCommand;
        }
        if (self.command == .fault and (self.route.? != .native or self.fault == null))
            return error.InvalidServingDriverCommand;
    }
};

fn digest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidServingDriverIdentity;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidServingDriverIdentity;
    return result;
}

fn canonicalVariantSettings(variant: Variant, optimize_decode: bool, optimize_prefill: bool) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context-serving.variant.v1\x00");
    hash.update(@tagName(variant));
    hash.update(&.{ @intFromBool(optimize_decode), @intFromBool(optimize_prefill) });
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn canonicalGenerationSettings(reserved_output_tokens: u32) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context-serving.generation.v1\x00");
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, reserved_output_tokens, .little);
    hash.update(&bytes);
    // Qualification generation is greedy, ignores EOS, and disables thinking,
    // tools, grammar, speculation, and prompt caching.
    hash.update(&.{ 1, 0, 0, 0, 0, 0 });
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn expectDigest(text: []const u8, actual: [32]u8) !void {
    const expected = try digest32(text);
    if (!std.mem.eql(u8, &expected, &actual)) return error.ServingDriverSettingsMismatch;
}

test "context serving driver command and settings contracts fail closed" {
    try std.testing.expectError(error.InvalidServingDriverCommand, (Command{ .command = .next }).validate());
    const command = Command{
        .command = .measure,
        .session_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        .request_index = 1,
        .route = .native,
        .pair_id = "pair-1",
        .warmup = false,
        .variant = .attention_matching,
        .prompt_file = "prompt.txt",
        .prompt_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        .prompt_tokens = 1024,
        .reserved_output_tokens = 8,
        .settings_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        .variant_settings_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    };
    try command.validate();
    try std.testing.expect(!std.mem.eql(u8, &canonicalVariantSettings(.exact, false, false), &canonicalVariantSettings(.packed_exact, false, false)));
    try std.testing.expect(!std.mem.eql(u8, &canonicalGenerationSettings(8), &canonicalGenerationSettings(9)));
}

test "context serving qualification hooks are absent from release builds" {
    try std.testing.expect(builtin.is_test);
    try std.testing.expect(server.ContextServingQualificationSlot != void);
    try std.testing.expect(server.ContextServingSinkSlot != void);
}

test "serving qualification persistent driver body declarations" {
    const Loopback = @import("context_serving_loopback.zig").Loopback;
    std.mem.doNotOptimizeAway(&serving_driver.Driver.init);
    std.mem.doNotOptimizeAway(&serving_driver.Driver.execute);
    std.mem.doNotOptimizeAway(&serving_driver.Driver.deinit);
    std.mem.doNotOptimizeAway(&Loopback.init);
    std.mem.doNotOptimizeAway(&Loopback.start);
    std.mem.doNotOptimizeAway(&Loopback.request);
    std.mem.doNotOptimizeAway(&Loopback.idle);
    std.mem.doNotOptimizeAway(&Loopback.finish);
    std.mem.doNotOptimizeAway(&Loopback.deinit);
    std.mem.doNotOptimizeAway(&server.Node.generateMessagesDirectQualification);
    std.mem.doNotOptimizeAway(&serving_protocol.Session.run);
    try std.testing.expectEqualStrings("native", @tagName(serving_driver.Route.native));
    try std.testing.expectEqualStrings("http", @tagName(serving_driver.Route.http));
    try std.testing.expectEqualStrings("stream", @tagName(serving_driver.Route.stream));
    try std.testing.expectEqualStrings("serial_mixed_batch", @tagName(serving_driver.Route.serial_mixed_batch));
}
