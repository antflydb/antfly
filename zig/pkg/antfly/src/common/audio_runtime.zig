// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const httpx = @import("httpx");
const config_mod = @import("config.zig");
const transcribing = @import("antfly_transcribing");
const synthesizing = @import("antfly_synthesizing");

pub const ActiveRuntime = struct {
    client: ?httpx.Client = null,
    transcribing_runtime: ?transcribing.Runtime = null,
    synthesizing_runtime: ?synthesizing.Runtime = null,
    previous_transcribing_runtime: ?*const transcribing.Runtime = null,
    previous_synthesizing_runtime: ?*const synthesizing.Runtime = null,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        cfg: ?*const config_mod.Config,
    ) !ActiveRuntime {
        var out = ActiveRuntime{};
        const loaded = cfg orelse return out;
        const has_transcribing = loaded.speech_to_text.defaultProviderName() != null;
        const has_synthesizing = loaded.text_to_speech.defaultProviderName() != null;
        if (!has_transcribing and !has_synthesizing) return out;

        out.client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        errdefer if (out.client) |*client| client.deinit();

        if (has_transcribing) {
            out.transcribing_runtime = transcribing.Runtime.init(alloc);
            errdefer if (out.transcribing_runtime) |*runtime| runtime.deinit();
            try out.transcribing_runtime.?.loadFromRegistry(&out.client.?, &loaded.speech_to_text);
            out.previous_transcribing_runtime = transcribing.getActiveRuntime();
            transcribing.setActiveRuntime(&out.transcribing_runtime.?);
        }

        if (has_synthesizing) {
            out.synthesizing_runtime = synthesizing.Runtime.init(alloc);
            errdefer if (out.synthesizing_runtime) |*runtime| runtime.deinit();
            try out.synthesizing_runtime.?.loadFromRegistry(&out.client.?, &loaded.text_to_speech);
            out.previous_synthesizing_runtime = synthesizing.getActiveRuntime();
            synthesizing.setActiveRuntime(&out.synthesizing_runtime.?);
        }

        return out;
    }

    pub fn deinit(self: *ActiveRuntime) void {
        if (self.synthesizing_runtime) |*runtime| {
            synthesizing.setActiveRuntime(self.previous_synthesizing_runtime);
            runtime.deinit();
        }
        if (self.transcribing_runtime) |*runtime| {
            transcribing.setActiveRuntime(self.previous_transcribing_runtime);
            runtime.deinit();
        }
        if (self.client) |*client| client.deinit();
        self.* = undefined;
    }
};

test "audio runtime activates configured transcribing and synthesizing providers" {
    const alloc = std.testing.allocator;
    var io = std.Io.Threaded.init(alloc, .{});
    defer io.deinit();

    var cfg = config_mod.Config{
        .registry = @import("provider_registry.zig").Registry.init(alloc),
        .speech_to_text = transcribing.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
    };
    defer cfg.deinit();

    var stt_cfg = transcribing.Config{
        .provider = .antfly,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:9090"),
        .model = try alloc.dupe(u8, "whisper-small"),
    };
    defer transcribing.deinitConfig(alloc, &stt_cfg);
    try cfg.speech_to_text.registerConfig("local-stt", stt_cfg);

    var tts_cfg = synthesizing.Config{
        .provider = .openai,
        .api_key = try alloc.dupe(u8, "sk-test"),
        .model = try alloc.dupe(u8, "gpt-4o-mini-tts"),
        .voice = try alloc.dupe(u8, "alloy"),
    };
    defer synthesizing.deinitConfig(alloc, &tts_cfg);
    try cfg.text_to_speech.registerConfig("local-tts", tts_cfg);

    const prev_stt = transcribing.getActiveRuntime();
    const prev_tts = synthesizing.getActiveRuntime();
    var active = try ActiveRuntime.init(alloc, io.io(), &cfg);
    defer active.deinit();

    try std.testing.expect(transcribing.getActiveRuntime() != null);
    try std.testing.expect(synthesizing.getActiveRuntime() != null);
    try std.testing.expect(transcribing.getActiveRuntime() != prev_stt or prev_stt == null);
    try std.testing.expect(synthesizing.getActiveRuntime() != prev_tts or prev_tts == null);
}
