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
const readers = @import("antfly_readers");
const secrets = @import("secrets.zig");
const synthesizing = @import("antfly_synthesizing");

pub const ActiveRuntime = struct {
    pub const Options = struct {
        client: httpx.ClientConfig = .{},
        secret_store: ?*secrets.FileStore = null,
    };

    alloc: ?std.mem.Allocator = null,
    client: ?*httpx.Client = null,
    transcribing_runtime: ?*transcribing.Runtime = null,
    readers_runtime: ?*readers.Runtime = null,
    synthesizing_runtime: ?*synthesizing.Runtime = null,
    previous_transcribing_runtime: ?*const transcribing.Runtime = null,
    previous_readers_runtime: ?*const readers.Runtime = null,
    previous_synthesizing_runtime: ?*const synthesizing.Runtime = null,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        cfg: ?*const config_mod.Config,
    ) !ActiveRuntime {
        return initWithOptions(alloc, io, cfg, .{});
    }

    pub fn initWithOptions(
        alloc: std.mem.Allocator,
        io: std.Io,
        cfg: ?*const config_mod.Config,
        options: Options,
    ) !ActiveRuntime {
        var out = ActiveRuntime{ .alloc = alloc };
        const loaded = cfg orelse return out;
        const has_transcribing = loaded.transcribers.defaultProviderName() != null;
        const has_readers = loaded.readers.defaultProviderName() != null;
        const has_synthesizing = loaded.text_to_speech.defaultProviderName() != null;
        if (!has_transcribing and !has_readers and !has_synthesizing) return out;

        var client_config = options.client;
        client_config.keep_alive = false;
        // Provider implementations and their shared client are one lifetime.
        // Teardown must interrupt and drain requests before freeing either.
        client_config.cancel_in_flight_on_shutdown = true;
        out.client = try alloc.create(httpx.Client);
        out.client.?.* = httpx.Client.initWithConfig(alloc, io, client_config);
        errdefer if (out.client) |client| {
            client.deinit();
            alloc.destroy(client);
        };
        errdefer {
            if (out.synthesizing_runtime) |runtime| {
                runtime.deinit();
                alloc.destroy(runtime);
            }
            if (out.readers_runtime) |runtime| {
                runtime.deinit();
                alloc.destroy(runtime);
            }
            if (out.transcribing_runtime) |runtime| {
                runtime.deinit();
                alloc.destroy(runtime);
            }
        }

        if (has_transcribing) {
            out.transcribing_runtime = try alloc.create(transcribing.Runtime);
            out.transcribing_runtime.?.* = transcribing.Runtime.init(alloc);
            var resolved_registry = transcribing.Registry.init(alloc);
            defer resolved_registry.deinit();
            var it = loaded.transcribers.configs.iterator();
            while (it.next()) |entry| {
                if (needsNativeCredentials(entry.value_ptr.*, options.secret_store)) {
                    const provider = try NativeTranscriber.init(alloc, out.client.?, entry.value_ptr.*, options.secret_store.?);
                    errdefer provider.deinit();
                    try out.transcribing_runtime.?.registerOwnedTranscriber(entry.key_ptr.*, provider);
                    continue;
                }
                var resolved = try transcribing.cloneConfig(alloc, entry.value_ptr.*);
                defer transcribing.deinitConfig(alloc, &resolved);
                try resolveProviderDefaults(alloc, &resolved, options.secret_store);
                try resolved_registry.registerConfig(entry.key_ptr.*, resolved);
            }
            try out.transcribing_runtime.?.loadFromRegistry(out.client.?, &resolved_registry);
            if (loaded.transcribers.default_provider) |name| {
                const idx = out.transcribing_runtime.?.transcribers.getIndex(name) orelse return error.UnknownTranscribingProvider;
                out.transcribing_runtime.?.default_provider = out.transcribing_runtime.?.transcribers.keys()[idx];
            }
        }

        if (has_readers) {
            out.readers_runtime = try alloc.create(readers.Runtime);
            out.readers_runtime.?.* = readers.Runtime.init(alloc);
            var resolved_registry = readers.Registry.init(alloc);
            defer resolved_registry.deinit();
            var it = loaded.readers.configs.iterator();
            while (it.next()) |entry| {
                if (needsNativeCredentials(entry.value_ptr.*, options.secret_store)) {
                    const provider = try NativeReader.init(alloc, out.client.?, entry.value_ptr.*, options.secret_store.?);
                    errdefer provider.deinit();
                    try out.readers_runtime.?.registerOwnedReader(entry.key_ptr.*, provider);
                    continue;
                }
                var resolved = try readers.cloneConfig(alloc, entry.value_ptr.*);
                defer readers.deinitConfig(alloc, &resolved);
                try resolveProviderDefaults(alloc, &resolved, options.secret_store);
                try resolved_registry.registerConfig(entry.key_ptr.*, resolved);
            }
            try out.readers_runtime.?.loadFromRegistry(out.client.?, &resolved_registry);
            if (loaded.readers.default_provider) |name| {
                const idx = out.readers_runtime.?.readers.getIndex(name) orelse return error.UnknownReaderProvider;
                out.readers_runtime.?.default_provider = out.readers_runtime.?.readers.keys()[idx];
            }
        }

        if (has_synthesizing) {
            out.synthesizing_runtime = try alloc.create(synthesizing.Runtime);
            out.synthesizing_runtime.?.* = synthesizing.Runtime.init(alloc);
            var resolved_registry = synthesizing.Registry.init(alloc);
            defer resolved_registry.deinit();
            var it = loaded.text_to_speech.configs.iterator();
            while (it.next()) |entry| {
                if (needsNativeCredentials(entry.value_ptr.*, options.secret_store)) {
                    const provider = try NativeSynthesizer.init(alloc, out.client.?, entry.value_ptr.*, options.secret_store.?);
                    errdefer provider.deinit();
                    try out.synthesizing_runtime.?.registerOwnedSynthesizer(entry.key_ptr.*, provider);
                    continue;
                }
                var resolved = try synthesizing.cloneConfig(alloc, entry.value_ptr.*);
                defer synthesizing.deinitConfig(alloc, &resolved);
                try resolveProviderDefaults(alloc, &resolved, options.secret_store);
                try resolved_registry.registerConfig(entry.key_ptr.*, resolved);
            }
            try out.synthesizing_runtime.?.loadFromRegistry(out.client.?, &resolved_registry);
            if (loaded.text_to_speech.default_provider) |name| {
                const idx = out.synthesizing_runtime.?.synthesizers.getIndex(name) orelse return error.UnknownSynthesizingProvider;
                out.synthesizing_runtime.?.default_provider = out.synthesizing_runtime.?.synthesizers.keys()[idx];
            }
        }

        // Publish the new registry set as one final, non-failing phase. A
        // failure while loading any provider therefore leaves every previous
        // active runtime untouched.
        if (out.transcribing_runtime != null) {
            out.previous_transcribing_runtime = transcribing.getActiveRuntime();
            transcribing.setActiveRuntime(out.transcribing_runtime.?);
        }
        if (out.readers_runtime != null) {
            out.previous_readers_runtime = readers.getActiveRuntime();
            readers.setActiveRuntime(out.readers_runtime.?);
        }
        if (out.synthesizing_runtime != null) {
            out.previous_synthesizing_runtime = synthesizing.getActiveRuntime();
            synthesizing.setActiveRuntime(out.synthesizing_runtime.?);
        }

        return out;
    }

    pub fn deinit(self: *ActiveRuntime) void {
        // Close global admission first. Existing calls retain their provider
        // state until the shared client has cancelled and drained them.
        if (self.synthesizing_runtime != null) synthesizing.setActiveRuntime(self.previous_synthesizing_runtime);
        if (self.readers_runtime != null) readers.setActiveRuntime(self.previous_readers_runtime);
        if (self.transcribing_runtime != null) transcribing.setActiveRuntime(self.previous_transcribing_runtime);

        if (self.client) |client| client.shutdown();
        const alloc = self.alloc.?;
        if (self.synthesizing_runtime) |runtime| {
            runtime.deinit();
            alloc.destroy(runtime);
        }
        if (self.readers_runtime) |runtime| {
            runtime.deinit();
            alloc.destroy(runtime);
        }
        if (self.transcribing_runtime) |runtime| {
            runtime.deinit();
            alloc.destroy(runtime);
        }
        if (self.client) |client| {
            client.deinit();
            alloc.destroy(client);
        }
        self.* = undefined;
    }
};

test "audio runtime activates configured transcribing and synthesizing providers" {
    const alloc = std.testing.allocator;
    var io = std.Io.Threaded.init(alloc, .{});
    defer io.deinit();

    var cfg = config_mod.Config{
        .registry = @import("provider_registry.zig").Registry.init(alloc),
        .transcribers = transcribing.Registry.init(alloc),
        .readers = readers.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
    };
    defer cfg.deinit();

    var stt_cfg = transcribing.Config{
        .provider = .antfly,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:9090"),
        .model = try alloc.dupe(u8, "whisper-small"),
    };
    defer transcribing.deinitConfig(alloc, &stt_cfg);
    try cfg.transcribers.registerConfig("local-stt", stt_cfg);

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

test "audio runtime rolls back globals when a later provider fails to load" {
    const alloc = std.testing.allocator;
    var io = std.Io.Threaded.init(alloc, .{});
    defer io.deinit();

    var cfg = config_mod.Config{
        .registry = @import("provider_registry.zig").Registry.init(alloc),
        .transcribers = transcribing.Registry.init(alloc),
        .readers = readers.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
    };
    defer cfg.deinit();

    try cfg.transcribers.registerConfig("valid-first", .{
        .provider = .antfly,
        .api_url = "http://127.0.0.1:9090",
        .model = "vopr-stt",
    });
    try cfg.text_to_speech.registerConfig("unsupported-later", .{
        .provider = .elevenlabs,
        .voice_id = "vopr-voice",
    });

    const previous_stt = transcribing.getActiveRuntime();
    const previous_tts = synthesizing.getActiveRuntime();
    try std.testing.expectError(error.UnsupportedSynthesizingProvider, ActiveRuntime.init(alloc, io.io(), &cfg));
    try std.testing.expect(transcribing.getActiveRuntime() == previous_stt);
    try std.testing.expect(synthesizing.getActiveRuntime() == previous_tts);
}

// File and environment credentials are resolved at startup. Native-backed
// providers call this at request time, after their source has been attached.
// Keep server secret resolution outside the Apache-licensed adapters.
fn resolveProviderDefaults(alloc: std.mem.Allocator, cfg: anytype, store: ?*secrets.FileStore) !void {
    const Config = @TypeOf(cfg.*);
    const has_bearer = if (@hasField(Config, "bearer_token")) cfg.bearer_token != null else false;
    const provider = @tagName(cfg.provider);
    const env_name: ?[]const u8 = if (std.mem.eql(u8, provider, "openai"))
        "OPENAI_API_KEY"
    else if (std.mem.eql(u8, provider, "antfly"))
        "ANTFLY_INFERENCE_API_KEY"
    else
        null;
    if (!has_bearer) if (env_name) |name| {
        var source = try secrets.SecretValue.initConfigOrProviderDefault(alloc, cfg.api_key, name);
        defer source.deinit(alloc);
        const resolved = try source.resolveOwned(alloc, store);
        if (cfg.api_key) |old| alloc.free(old);
        cfg.api_key = resolved;
    };
    if (cfg.provider == .openai and cfg.base_url == null) {
        if (@hasField(Config, "url")) {
            if (cfg.resolvedUrl()) |url| cfg.base_url = try alloc.dupe(u8, url);
        }
        if (cfg.base_url == null) cfg.base_url = secrets.envValueOwned(alloc, "OPENAI_BASE_URL");
    }
}

test "audio runtime provider defaults resolve OpenAI credentials and endpoint aliases" {
    const alloc = std.testing.allocator;
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/test-audio-defaults-{d}.json", .{std.Io.Clock.awake.now(std.testing.io).nanoseconds});
    defer alloc.free(path);
    defer std.Io.Dir.cwd().deleteFile(std.testing.io, path) catch {};
    var store = try secrets.FileStore.init(alloc, path);
    defer store.deinit();
    var entry = try store.put(alloc, "openai.api_key", "test-key");
    defer entry.deinit(alloc);
    inline for (.{ transcribing, readers, synthesizing }) |lib| {
        var cfg = try lib.cloneConfig(alloc, .{ .provider = .openai });
        defer lib.deinitConfig(alloc, &cfg);
        try resolveProviderDefaults(alloc, &cfg, &store);
        try std.testing.expectEqualStrings("test-key", cfg.api_key.?);
    }
    var cfg = try transcribing.cloneConfig(alloc, .{ .provider = .openai, .api_key = "explicit", .url = "http://custom/v1" });
    defer transcribing.deinitConfig(alloc, &cfg);
    try resolveProviderDefaults(alloc, &cfg, &store);
    try std.testing.expectEqualStrings("explicit", cfg.api_key.?);
    try std.testing.expectEqualStrings("http://custom/v1", cfg.base_url.?);
}

// Native stores are attached after audio initialization and require running
// metadata services. Retain the credential source until a request can read it.
fn needsNativeCredentials(cfg: anytype, store: ?*secrets.FileStore) bool {
    const active_store = store orelse return false;
    if (active_store.native_config == null) return false;
    const Config = @TypeOf(cfg);
    if (@hasField(Config, "bearer_token")) if (cfg.bearer_token != null) return false;
    const provider = @tagName(cfg.provider);
    if (!std.mem.eql(u8, provider, "openai") and !std.mem.eql(u8, provider, "antfly")) return false;
    const key = cfg.api_key orelse return true;
    return std.mem.startsWith(u8, key, "${secret:");
}

const NativeTranscriber = NativeCredentialProvider(transcribing, transcribing.Transcriber, "transcribe", transcribing.Response);
const NativeReader = NativeCredentialProvider(readers, readers.Reader, "read", []readers.Result);
const NativeSynthesizer = NativeCredentialProvider(synthesizing, synthesizing.Synthesizer, "synthesize", synthesizing.SynthesisResult);

fn NativeCredentialProvider(comptime lib: type, comptime Adapter: type, comptime method: []const u8, comptime Result: type) type {
    return struct {
        const Self = @This();
        alloc: std.mem.Allocator,
        http: *httpx.Client,
        cfg: lib.Config,
        store: *secrets.FileStore,

        fn init(alloc: std.mem.Allocator, http: *httpx.Client, cfg: lib.Config, store: *secrets.FileStore) !Adapter {
            const self = try alloc.create(Self);
            errdefer alloc.destroy(self);
            self.* = .{ .alloc = alloc, .http = http, .cfg = try lib.cloneConfig(alloc, cfg), .store = store };
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable: Adapter.VTable = blk: {
            var value: Adapter.VTable = undefined;
            value.deinit = deinit;
            @field(value, method) = call;
            if (@hasField(Adapter.VTable, "read_reported")) value.read_reported = readReported;
            break :blk value;
        };

        fn load(self: *Self) !lib.Runtime {
            // Never send a bootstrap fallback credential before native storage
            // can enforce its precedence and freshness policy.
            if (self.store.native_source == null) return error.Unavailable;
            var cfg = try lib.cloneConfig(self.alloc, self.cfg);
            defer lib.deinitConfig(self.alloc, &cfg);
            try resolveProviderDefaults(self.alloc, &cfg, self.store);
            var registry = lib.Registry.init(self.alloc);
            defer registry.deinit();
            try registry.registerConfig("native", cfg);
            var runtime = lib.Runtime.init(self.alloc);
            errdefer runtime.deinit();
            try runtime.loadFromRegistry(self.http, &registry);
            return runtime;
        }

        fn call(ptr: *anyopaque, alloc: std.mem.Allocator, req: lib.Request) !Result {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var runtime = try self.load();
            defer runtime.deinit();
            const adapter = try runtime.get(null);
            return @call(.auto, @field(Adapter, method), .{ adapter, alloc, req });
        }

        fn readReported(ptr: *anyopaque, alloc: std.mem.Allocator, req: lib.Request) !readers.BatchResult {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var runtime = try self.load();
            defer runtime.deinit();
            return (try runtime.get(null)).readReported(alloc, req);
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            lib.deinitConfig(self.alloc, &self.cfg);
            self.alloc.destroy(self);
        }
    };
}

test "audio runtime native credentials resolve after attachment and rotate" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const contract = @import("secret_contract.zig");
    const Fake = struct {
        revision: u64 = 1,
        calls: usize = 0,
        fn resolve(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, key: []const u8, _: contract.ReadOptions) !contract.Lookup {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("openai.api_key", key);
            self.calls += 1;
            return .{ .revision = self.revision, .value = .{
                .revision = self.revision,
                .secret = .{ .bytes = try a.dupe(u8, if (self.revision == 1) "first-key" else "second-key") },
            } };
        }
        fn list(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: contract.ReadOptions) !contract.Listing {
            return error.Unexpected;
        }
        fn refresh(ptr: *anyopaque, _: []const u8) !contract.Health {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .revision = self.revision };
        }
        fn check(req: httpx.testing_mod.RequestInfo) !void {
            const expected = if (std.mem.indexOf(u8, req.body, "again") != null) "Bearer second-key" else "Bearer first-key";
            try std.testing.expectEqualStrings(expected, req.header("Authorization") orelse "");
        }
        fn serve(server: *httpx.TestServer, failure: *?anyerror) void {
            server.handleOne() catch |err| {
                failure.* = err;
                return;
            };
            server.handleOne() catch |err| {
                failure.* = err;
            };
        }
    };
    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/audio/speech", .assert_request = Fake.check, .respond = .{ .body = "AUDIO" } },
    });
    defer server.deinit();
    var store = try secrets.FileStore.initConfiguredWithIo(alloc, io, .{
        .native = .{ .backend = .distributed, .keyring_path = "unused-keyring" },
        .environment = false,
    });
    defer store.deinit();
    var cfg = config_mod.Config{
        .registry = @import("provider_registry.zig").Registry.init(alloc),
        .transcribers = transcribing.Registry.init(alloc),
        .readers = readers.Registry.init(alloc),
        .text_to_speech = synthesizing.Registry.init(alloc),
    };
    defer cfg.deinit();
    try cfg.transcribers.registerConfig("native-stt", .{ .provider = .openai, .api_key = "${secret:openai.api_key}" });
    try cfg.readers.registerConfig("native-reader", .{ .provider = .openai });
    try cfg.text_to_speech.registerConfig("native-tts", .{ .provider = .openai, .base_url = server.baseUrl() });
    // Preserve an explicit default when native and eagerly loaded providers mix.
    try cfg.text_to_speech.registerConfig("literal", .{ .provider = .openai, .api_key = "literal-key" });
    var runtime = try ActiveRuntime.initWithOptions(alloc, io, &cfg, .{ .secret_store = &store });
    defer runtime.deinit();
    const synth = try runtime.synthesizing_runtime.?.get(null);
    try std.testing.expectError(error.Unavailable, synth.synthesize(alloc, .{ .text = "too early", .format = .mp3 }));
    var fake = Fake{};
    store.attachNative(.{ .ptr = &fake, .vtable = &.{ .resolve = Fake.resolve, .list_metadata = Fake.list, .refresh = Fake.refresh } }, null);
    // Exercise all three lazy provider loaders after native attachment.
    const transcriber = try runtime.transcribing_runtime.?.get(null);
    const stt: *NativeTranscriber = @ptrCast(@alignCast(transcriber.ptr));
    var stt_runtime = try stt.load();
    stt_runtime.deinit();
    const reader = try runtime.readers_runtime.?.get(null);
    const ocr: *NativeReader = @ptrCast(@alignCast(reader.ptr));
    var reader_runtime = try ocr.load();
    reader_runtime.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("native-tts", runtime.synthesizing_runtime.?.default_provider.?);
    var failure: ?anyerror = null;
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Fake.serve, .{ &server, &failure });
    var first = try synth.synthesize(alloc, .{ .text = "hello", .format = .mp3 });
    defer synthesizing.deinitResult(alloc, &first);
    try std.testing.expectEqualStrings("AUDIO", first.audio.?);
    fake.revision = 2;
    var second = try synth.synthesize(alloc, .{ .text = "again", .format = .mp3 });
    defer synthesizing.deinitResult(alloc, &second);
    try std.testing.expectEqualStrings("AUDIO", second.audio.?);
    try group.await(io);
    if (failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
}
