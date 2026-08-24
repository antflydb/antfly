// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production media-provider execution on deterministic HTTP and `VoprIo`.
//! The harness scripts only the peer: request construction, response parsing,
//! retry, timeout, cancellation, registry replacement, and teardown all run
//! through the production httpx/transcribing/synthesizing implementations.

const std = @import("std");
const httpx = @import("httpx");
const vopr = @import("vopr");
const audio_runtime = @import("../common/audio_runtime.zig");
const config_mod = @import("../common/config.zig");
const provider_registry = @import("../common/provider_registry.zig");
const transcribing = @import("antfly_transcribing");
const readers = @import("antfly_readers");
const synthesizing = @import("antfly_synthesizing");

const valid_transcript =
    "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0,\"text\":\"vopr transcript\",\"language\":\"en\"}],\"model\":\"vopr-stt\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":2,\"total_tokens\":2}}";

const stt_success_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/transcribe", .respond = .{ .body = valid_transcript } },
};
const tts_success_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/audio/speech", .respond = .{ .body = "VOPR-AUDIO", .content_type = "audio/mpeg" } },
};
const malformed_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/transcribe", .respond = .{ .body = "{not-json" } },
};
const partial_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/audio/speech", .respond = .{ .body = "VOPR-AUDIO", .content_type = "audio/mpeg", .truncate_body_at = 3 } },
};
const timeout_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/transcribe", .respond = .{ .body = valid_transcript, .delay_ns = 50 * std.time.ns_per_ms } },
};
const retry_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/audio/speech", .max_uses = 1, .respond = .{ .status = 503, .body = "retry" } },
    .{ .method = .POST, .path = "/audio/speech", .respond = .{ .body = "VOPR-AUDIO", .content_type = "audio/mpeg" } },
};
const delayed_routes = [_]httpx.TestRoute{
    .{ .method = .POST, .path = "/transcribe", .respond = .{ .body = valid_transcript, .delay_ns = 50 * std.time.ns_per_ms } },
};

pub const Scenario = struct {
    pub const name = "media-runtime";
    pub const version: u32 = 2;

    const restored_id = vopr.id.stable(name, "globals-restored");
    const result_id = vopr.id.stable(name, "provider-result-classified");
    const timeout_id = vopr.id.stable(name, "timeout-propagated");
    const retry_id = vopr.id.stable(name, "retry-recovers");
    const cancellation_id = vopr.id.stable(name, "shutdown-cancels-and-drains");
    const replacement_id = vopr.id.stable(name, "replacement-preserves-active-call");
    const cleanup_id = vopr.id.stable(name, "resources-cleaned");
    const complete_id = vopr.id.stable(name, "mode-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = restored_id, .name = name ++ ".globals-restored", .kind = .always },
        .{ .id = result_id, .name = name ++ ".provider-result-classified", .kind = .always },
        .{ .id = timeout_id, .name = name ++ ".timeout-propagated", .kind = .always },
        .{ .id = retry_id, .name = name ++ ".retry-recovers", .kind = .always },
        .{ .id = cancellation_id, .name = name ++ ".shutdown-cancels-and-drains", .kind = .always },
        .{ .id = replacement_id, .name = name ++ ".replacement-preserves-active-call", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        empty,
        configured,
        nested,
        partial_startup_rollback,
        stt_success,
        tts_success,
        stt_malformed,
        tts_partial,
        stt_timeout,
        tts_retry,
        stt_cancel_shutdown,
        stt_runtime_replacement,
    };

    const mode_ids = idsForModes();
    const mode_names = namesForModes();
    const shutdown_transition_id = vopr.id.stable(name, "begin-shutdown");
    const replace_transition_id = vopr.id.stable(name, "replace-runtime");
    const finalize_transition_id = vopr.id.stable(name, "finalize");

    fn idsForModes() [std.meta.tags(Mode).len]vopr.id.StableId {
        var ids: [std.meta.tags(Mode).len]vopr.id.StableId = undefined;
        inline for (std.meta.tags(Mode), 0..) |mode, index| ids[index] = vopr.id.stable(name, @tagName(mode));
        return ids;
    }

    fn namesForModes() [std.meta.tags(Mode).len][]const u8 {
        var names: [std.meta.tags(Mode).len][]const u8 = undefined;
        inline for (std.meta.tags(Mode), 0..) |mode, index| names[index] = name ++ "." ++ @tagName(mode);
        return names;
    }

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        original_stt: ?*const transcribing.Runtime,
        original_tts: ?*const synthesizing.Runtime,
        mode: ?Mode = null,
        server: ?httpx.TestServer = null,
        active: ?audio_runtime.ActiveRuntime = null,
        active_live: bool = false,
        request_done: bool = false,
        server_done: bool = false,
        shutdown_started: bool = false,
        shutdown_done: bool = false,
        replacement_done: bool = false,
        request_error: ?anyerror = null,
        server_error: ?anyerror = null,
        transcript: ?[]u8 = null,
        audio: ?[]u8 = null,
        complete: bool = false,
        result_classified: bool = true,
        timeout_propagated: bool = true,
        retry_recovered: bool = true,
        cancellation_propagated: bool = true,
        replacement_preserved: bool = true,
        resources_cleaned: bool = true,

        fn isHttpMode(mode: Mode) bool {
            return switch (mode) {
                .empty, .configured, .nested, .partial_startup_rollback => false,
                else => true,
            };
        }

        fn isTtsMode(mode: Mode) bool {
            return switch (mode) {
                .tts_success, .tts_partial, .tts_retry => true,
                else => false,
            };
        }

        fn routesFor(mode: Mode) []const httpx.TestRoute {
            return switch (mode) {
                .stt_success => &stt_success_routes,
                .tts_success => &tts_success_routes,
                .stt_malformed => &malformed_routes,
                .tts_partial => &partial_routes,
                .stt_timeout => &timeout_routes,
                .tts_retry => &retry_routes,
                .stt_cancel_shutdown, .stt_runtime_replacement => &delayed_routes,
                else => unreachable,
            };
        }

        fn makeConfig(self: *State, base_url: []const u8) !config_mod.Config {
            var cfg = config_mod.Config{
                .registry = provider_registry.Registry.init(self.allocator),
                .transcribers = transcribing.Registry.init(self.allocator),
                .readers = readers.Registry.init(self.allocator),
                .text_to_speech = synthesizing.Registry.init(self.allocator),
            };
            errdefer cfg.deinit();

            var stt = transcribing.Config{
                .provider = .antfly,
                .api_url = try self.allocator.dupe(u8, base_url),
                .model = try self.allocator.dupe(u8, "vopr-stt"),
                .api_key = try self.allocator.dupe(u8, "vopr-key"),
            };
            defer transcribing.deinitConfig(self.allocator, &stt);
            try cfg.transcribers.registerConfig("vopr-stt", stt);

            var tts = synthesizing.Config{
                .provider = .openai,
                .base_url = try self.allocator.dupe(u8, base_url),
                .api_key = try self.allocator.dupe(u8, "vopr-key"),
                .model = try self.allocator.dupe(u8, "vopr-tts"),
                .voice = try self.allocator.dupe(u8, "alloy"),
            };
            defer synthesizing.deinitConfig(self.allocator, &tts);
            try cfg.text_to_speech.registerConfig("vopr-tts", tts);
            return cfg;
        }

        fn start(self: *State, mode: Mode) !void {
            self.mode = mode;
            if (!isHttpMode(mode)) return self.runActivation(mode);

            self.server = try httpx.TestServer.start(self.allocator, self.sim.io(), routesFor(mode));
            var cfg = try self.makeConfig(self.server.?.baseUrl());
            defer cfg.deinit();

            var options = audio_runtime.ActiveRuntime.Options{};
            options.client.retry_policy = .noRetry();
            if (mode == .stt_timeout) options.client.timeouts.request_ms = 10;
            if (mode == .tts_retry) options.client.retry_policy = .{
                .max_retries = 1,
                .initial_delay_ms = 1,
                .max_delay_ms = 1,
                .retry_only_idempotent = false,
            };
            self.active = try audio_runtime.ActiveRuntime.initWithOptions(self.allocator, self.sim.io(), &cfg, options);
            self.active_live = true;
            _ = self.sim.io().async(requestTask, .{self});
            _ = self.sim.io().async(serverTask, .{self});
        }

        fn runActivation(self: *State, mode: Mode) !void {
            var cfg = try self.makeConfig("http://vopr.invalid");
            defer cfg.deinit();
            switch (mode) {
                .empty => {
                    var active = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), null);
                    active.deinit();
                },
                .configured => {
                    var active = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &cfg);
                    self.result_classified = transcribing.getActiveRuntime() != null and synthesizing.getActiveRuntime() != null;
                    active.deinit();
                },
                .nested => {
                    var outer = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &cfg);
                    const outer_stt = transcribing.getActiveRuntime();
                    var inner = try audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &cfg);
                    self.replacement_preserved = transcribing.getActiveRuntime() != outer_stt;
                    inner.deinit();
                    self.replacement_preserved = self.replacement_preserved and transcribing.getActiveRuntime() == outer_stt;
                    outer.deinit();
                },
                .partial_startup_rollback => {
                    var failing = config_mod.Config{
                        .registry = provider_registry.Registry.init(self.allocator),
                        .transcribers = transcribing.Registry.init(self.allocator),
                        .readers = readers.Registry.init(self.allocator),
                        .text_to_speech = synthesizing.Registry.init(self.allocator),
                    };
                    defer failing.deinit();
                    try failing.transcribers.registerConfig("valid-first", .{
                        .provider = .antfly,
                        .api_url = "http://vopr.invalid",
                        .model = "vopr-stt",
                    });
                    try failing.text_to_speech.registerConfig("unsupported-later", .{
                        .provider = .elevenlabs,
                        .voice_id = "vopr-voice",
                    });
                    var unexpected = audio_runtime.ActiveRuntime.init(self.allocator, self.sim.io(), &failing) catch |err| {
                        self.result_classified = err == error.UnsupportedSynthesizingProvider and
                            transcribing.getActiveRuntime() == self.original_stt and
                            synthesizing.getActiveRuntime() == self.original_tts;
                        self.finishRestoration();
                        self.complete = true;
                        return;
                    };
                    unexpected.deinit();
                    self.result_classified = false;
                },
                else => unreachable,
            }
            self.finishRestoration();
            self.complete = true;
        }

        fn requestTask(self: *State) void {
            defer self.request_done = true;
            const mode = self.mode.?;
            if (isTtsMode(mode)) {
                const runtime = if (self.active) |*active| &active.synthesizing_runtime.? else unreachable;
                const provider = runtime.get(null) catch |err| {
                    self.request_error = err;
                    return;
                };
                var result = provider.synthesize(self.allocator, .{ .text = "hello vopr", .format = .mp3 }) catch |err| {
                    self.request_error = err;
                    return;
                };
                defer synthesizing.deinitResult(self.allocator, &result);
                self.audio = self.allocator.dupe(u8, result.audio orelse "") catch |err| {
                    self.request_error = err;
                    return;
                };
                return;
            }

            const runtime = if (self.active) |*active| &active.transcribing_runtime.? else unreachable;
            const provider = runtime.get(null) catch |err| {
                self.request_error = err;
                return;
            };
            var response = provider.transcribe(self.allocator, .{ .url = "data:audio/wav;base64,ZmFrZQ==" }) catch |err| {
                self.request_error = err;
                return;
            };
            defer transcribing.deinitResponse(self.allocator, &response);
            self.transcript = self.allocator.dupe(u8, response.text orelse "") catch |err| {
                self.request_error = err;
                return;
            };
        }

        fn serverTask(self: *State) void {
            defer self.server_done = true;
            const requests: usize = if (self.mode.? == .tts_retry) 2 else 1;
            for (0..requests) |_| self.server.?.handleOne() catch |err| {
                self.server_error = err;
                return;
            };
        }

        fn shutdownTask(self: *State) void {
            self.active.?.deinit();
            self.active_live = false;
            self.shutdown_done = true;
        }

        fn replaceRuntime(self: *State) !void {
            var cfg = try self.makeConfig(self.server.?.baseUrl());
            defer cfg.deinit();
            const outer_stt = transcribing.getActiveRuntime();
            var inner = try audio_runtime.ActiveRuntime.initWithOptions(self.allocator, self.sim.io(), &cfg, .{});
            self.replacement_preserved = transcribing.getActiveRuntime() != outer_stt;
            inner.deinit();
            self.replacement_preserved = self.replacement_preserved and transcribing.getActiveRuntime() == outer_stt;
            self.replacement_done = true;
        }

        fn routeWasHit(self: *const State) bool {
            if (self.server) |*server| return server.routeHitCount(0) != 0;
            return false;
        }

        fn finalize(self: *State) void {
            const mode = self.mode.?;
            switch (mode) {
                .stt_success => self.result_classified = self.request_error == null and std.mem.eql(u8, self.transcript orelse "", "vopr transcript"),
                .tts_success => self.result_classified = self.request_error == null and std.mem.eql(u8, self.audio orelse "", "VOPR-AUDIO"),
                .stt_malformed, .tts_partial => self.result_classified = self.request_error != null and self.transcript == null and self.audio == null,
                .stt_timeout => {
                    self.timeout_propagated = self.request_error == error.Timeout;
                    self.result_classified = self.timeout_propagated;
                },
                .tts_retry => {
                    const server = &self.server.?;
                    self.retry_recovered = self.request_error == null and
                        std.mem.eql(u8, self.audio orelse "", "VOPR-AUDIO") and
                        server.routeHitCount(0) == 1 and server.routeHitCount(1) == 1;
                    self.result_classified = self.retry_recovered;
                },
                .stt_cancel_shutdown => {
                    self.cancellation_propagated = self.shutdown_started and self.shutdown_done and self.request_error == error.Cancelled;
                    self.result_classified = self.cancellation_propagated;
                },
                .stt_runtime_replacement => {
                    self.replacement_preserved = self.replacement_preserved and self.replacement_done and
                        self.request_error == null and std.mem.eql(u8, self.transcript orelse "", "vopr transcript");
                    self.result_classified = self.replacement_preserved;
                },
                else => unreachable,
            }

            if (self.active_live) {
                self.active.?.deinit();
                self.active_live = false;
            }
            if (self.server) |*server| server.deinit();
            self.server = null;
            self.finishRestoration();
            self.resources_cleaned = self.sim.tasks.activeTaskCount() == 0;
            self.complete = true;
        }

        fn finishRestoration(self: *State) void {
            self.resources_cleaned = self.resources_cleaned and
                transcribing.getActiveRuntime() == self.original_stt and
                synthesizing.getActiveRuntime() == self.original_tts;
        }

        fn cleanup(self: *State) void {
            if (self.transcript) |value| self.allocator.free(value);
            if (self.audio) |value| self.allocator.free(value);
            if (self.server) |*server| server.deinit();
            // Successful histories finalize before world teardown. Keeping this
            // assertion here turns a premature runner exit into a harness error
            // instead of risking provider-state destruction under a live task.
            std.debug.assert(!self.active_live);
            self.sim.deinit();
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4d45_4449_41,
                .tasks = .{ .stack_size = 4 * 1024 * 1024 },
                .network = .{ .max_sockets = 16 },
                .required = .of(&.{ .clock_read, .sockets, .task_scheduling, .synchronization, .sleep }),
            }),
            .original_stt = transcribing.getActiveRuntime(),
            .original_tts = synthesizing.getActiveRuntime(),
        };
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.cleanup();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.complete) return;
        if (state.mode == null) {
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = switch (mode) {
                    .empty, .configured, .stt_success, .tts_success => .workload,
                    .nested, .stt_runtime_replacement => .scheduler,
                    else => .fault,
                },
            });
            return;
        }
        if (!State.isHttpMode(state.mode.?)) return;

        if (state.mode.? == .stt_cancel_shutdown and state.routeWasHit() and !state.request_done and !state.shutdown_started) {
            try list.append(allocator, .{ .id = shutdown_transition_id, .name = name ++ ".begin-shutdown", .kind = .fault });
            return;
        }
        if (state.mode.? == .stt_runtime_replacement and state.routeWasHit() and !state.request_done and !state.replacement_done) {
            try list.append(allocator, .{ .id = replace_transition_id, .name = name ++ ".replace-runtime", .kind = .scheduler });
            return;
        }
        if (!state.sim.scheduler().quiescent()) {
            try state.sim.scheduler().enumerateReady(list, allocator);
            return;
        }
        try list.append(allocator, .{ .id = finalize_transition_id, .name = name ++ ".finalize", .kind = .maintenance });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                try state.start(mode);
                found = true;
            };
            if (!found) return error.InvalidMediaRuntimeTransition;
        } else if (selected.id == shutdown_transition_id) {
            state.shutdown_started = true;
            _ = state.sim.io().async(State.shutdownTask, .{state});
        } else if (selected.id == replace_transition_id) {
            try state.replaceRuntime();
        } else if (selected.id == finalize_transition_id) {
            state.finalize();
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        try events.emitNamed(allocator, .domain, selected.name, @intFromBool(state.request_done));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
        try builder.addNamed(allocator, name ++ ".request-done", @intFromBool(state.request_done));
        try builder.addNamed(allocator, name ++ ".server-done", @intFromBool(state.server_done));
        try builder.addNamed(allocator, name ++ ".shutdown-done", @intFromBool(state.shutdown_done));
        try builder.addNamed(allocator, name ++ ".replacement-done", @intFromBool(state.replacement_done));
        try builder.addNamed(allocator, name ++ ".active-tasks", state.sim.tasks.activeTaskCount());
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, restored_id, !state.complete or state.resources_cleaned);
        try sink.check(allocator, result_id, state.result_classified);
        try sink.check(allocator, timeout_id, state.timeout_propagated);
        try sink.check(allocator, retry_id, state.retry_recovered);
        try sink.check(allocator, cancellation_id, state.cancellation_propagated);
        try sink.check(allocator, replacement_id, state.replacement_preserved);
        try sink.check(allocator, cleanup_id, state.resources_cleaned);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = @intFromBool(state.complete),
            .cleanup_complete = state.complete and state.resources_cleaned,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "media provider VOPR exact replays production HTTP and lifecycle modes" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, index| {
        var targeted = vopr.choice.Starving.init(mode_id, 0, 0x4d45_4449_41 + index);
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, targeted.source(), .{
            .system = "antfly",
            .transition_budget = 512,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.harness_errors);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
