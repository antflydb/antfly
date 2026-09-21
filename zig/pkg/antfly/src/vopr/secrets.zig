// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Replayable secret histories through the production Lite, file, and object
//! stores. Object publication faults exercise the shared collection contract;
//! quorum/leader routing is qualified separately with real distributed nodes.
const std = @import("std");
const vopr = @import("vopr");
const contract = @import("../common/secret_contract.zig");
const collection = @import("../common/secret_collection.zig");
const keyring = @import("../common/secret_keyring.zig");
const secrets = @import("../common/secrets.zig");
const objects = @import("../storage/object_storage.zig");
const serverless = @import("../serverless/secret_store.zig");
const lite = @import("../storage/lite/secret_store.zig");
const docs = @import("../storage/lite/docstore.zig");
const delivery = @import("../common/secret_delivery.zig");
const http = @import("../common/http/http_common.zig");
const Allocator = std.mem.Allocator;
const scope = "qualification";
const key = "provider.api_key";
const first_key = "{\"active\":\"one\",\"keys\":[{\"id\":\"one\",\"key\":\"1111111111111111111111111111111111111111111111111111111111111111\"}]}";
const rotated_key = "{\"active\":\"two\",\"keys\":[{\"id\":\"one\",\"key\":\"1111111111111111111111111111111111111111111111111111111111111111\"},{\"id\":\"two\",\"key\":\"2222222222222222222222222222222222222222222222222222222222222222\"}]}";

const FaultBackend = struct {
    inner: serverless.Backend,
    fault: *enum { none, before_commit, after_commit, unavailable },
    publications: *usize,
    pub fn read(self: *@This(), alloc: Allocator, namespace: []const u8) !collection.Snapshot {
        if (self.fault.* == .unavailable) return error.Unavailable;
        return self.inner.read(alloc, namespace);
    }
    pub fn publish(self: *@This(), alloc: Allocator, namespace: []const u8, previous: collection.Snapshot, bytes: []const u8) !void {
        self.publications.* += 1;
        if (self.fault.* == .before_commit) return error.OutcomeUnknown;
        try self.inner.publish(alloc, namespace, previous, bytes);
        if (self.fault.* == .after_commit) return error.OutcomeUnknown;
    }
};
const ObjectStore = collection.Store(FaultBackend);

/// The oracle stores plaintext and revisions independently of AFSC/AFSE framing.
const Model = struct {
    revision: u64 = 0,
    value: ?[]const u8 = null,
    fn put(self: *@This(), value: []const u8) void {
        self.revision += 1;
        self.value = value;
    }
    fn remove(self: *@This()) void {
        if (self.value != null) self.revision += 1;
        self.value = null;
    }
    fn check(self: @This(), alloc: Allocator, source: contract.Source) !void {
        var result = try source.resolve(alloc, scope, key, .{ .min_revision = self.revision });
        defer result.deinit(alloc);
        try std.testing.expectEqual(self.revision, result.revision);
        if (self.value) |value| {
            try std.testing.expect(result.value != null);
            try std.testing.expectEqualStrings(value, result.value.?.secret.bytes);
            try std.testing.expectEqual(self.revision, result.value.?.revision);
        } else try std.testing.expect(result.value == null);
        var listing = try source.listMetadata(alloc, scope, .{ .min_revision = self.revision });
        defer listing.deinit(alloc);
        try std.testing.expectEqual(self.revision, listing.revision);
        try std.testing.expectEqual(@as(usize, if (self.value != null) 1 else 0), listing.entries.len);
        if (self.value != null) try std.testing.expectEqualStrings(key, listing.entries[0].key);
        try std.testing.expectError(error.Unavailable, source.resolve(alloc, scope, key, .{ .min_revision = self.revision + 1 }));
        try std.testing.expectError(error.Unauthorized, source.resolve(alloc, "other", key, .{}));
    }
};

pub const Scenario = struct {
    pub const name: []const u8 = "native-secret-lifecycle";
    pub const version: u32 = 1;
    const sound = vopr.id.stable(name, "durability-authorization-and-cas");
    const complete = vopr.id.stable(name, "recovery-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound, .name = name ++ ".durability-authorization-and-cas", .kind = .always },
        .{ .id = complete, .name = name ++ ".recovery-completes", .kind = .reachable },
    };
    const Operation = enum { put_red, put_blue, remove, stale_writer, competing_writers, lost_before, lost_after, unavailable, delivery_replay_partition, rotate_keys, corrupt, crash, lite_sync_failure, file_fallback, finish };
    fn id(comptime operation: Operation) u64 {
        return vopr.id.stable(name, @tagName(operation));
    }
    const State = struct {
        alloc: Allocator,
        sim: vopr.vopr_io.VoprIo,
        keys: keyring.Keyring = undefined,
        fs: ?objects.FilesystemObjectStorage = null,
        object: ?ObjectStore = null,
        document: ?docs.Store = null,
        lite_store: ?lite.Store = null,
        file: ?secrets.FileStore = null,
        fault: @typeInfo(@FieldType(FaultBackend, "fault")).pointer.child = .none,
        publications: usize = 0,
        models: [2]Model = .{ .{}, .{} },
        steps: usize = 0,
        finished: bool = false,
        sound: bool = true,

        fn write(self: *@This(), path: []const u8, bytes: []const u8) !void {
            var file = try std.Io.Dir.cwd().createFile(self.sim.io(), path, .{ .truncate = true });
            defer file.close(self.sim.io());
            try file.writeStreamingAll(self.sim.io(), bytes);
            try file.sync(self.sim.io());
            try self.sim.files.syncNamespace();
        }
        fn open(self: *@This(), create: bool) !void {
            self.fs = try objects.FilesystemObjectStorage.initWithIo(self.alloc, "objects", self.sim.io());
            var client = self.fs.?.client();
            if (create) try client.makeBucket("secrets");
            self.object = try ObjectStore.init(self.alloc, self.sim.io(), scope, self.keys.provider(), .{
                .inner = .{ .client = client, .bucket = "secrets", .prefix = "native", .consistency = .linearizable_cas },
                .fault = &self.fault,
                .publications = &self.publications,
            });
            self.document = if (create)
                try docs.Store.createWithOptions(self.alloc, "native.aflite", .{ .io = self.sim.io(), .exclusive = true })
            else
                try docs.Store.openWithOptions(self.alloc, "native.aflite", .{ .io = self.sim.io() });
            self.lite_store = try lite.Store.init(self.alloc, &self.document.?, scope, self.keys.provider());
            self.file = try secrets.FileStore.initConfiguredWithIo(self.alloc, self.sim.io(), .{
                .native = .{ .path = "standalone.json" },
                .sources = &.{.{ .name = "mounted", .type = .file, .path = "fallback.json" }},
                .environment = false,
            });
        }
        fn close(self: *@This()) void {
            if (self.file) |*value| value.deinit();
            self.file = null;
            if (self.lite_store) |*value| value.deinit();
            self.lite_store = null;
            if (self.document) |*value| value.close();
            self.document = null;
            if (self.object) |*value| value.deinit();
            self.object = null;
            if (self.fs) |*value| value.deinit();
            self.fs = null;
        }
        fn handles(self: *@This()) [2]contract.NativeStore {
            return .{ self.object.?.nativeStore(), self.lite_store.?.nativeStore().? };
        }
        fn check(self: *@This()) !void {
            for (self.handles(), self.models) |handle, model| try model.check(self.alloc, handle.source);
            try self.sim.ensureNoCapabilityViolation();
        }
        fn run(self: *@This(), operation: Operation) !void {
            switch (operation) {
                .put_red, .put_blue => {
                    const value = if (operation == .put_red) "red-private-credential" else "blue-private-credential";
                    for (self.handles(), &self.models) |handle, *model| {
                        const result = try handle.writer.put(scope, key, value, if (model.value == null) .absent else .{ .exact = model.revision });
                        model.put(value);
                        try std.testing.expectEqual(model.revision, result.revision);
                    }
                },
                .remove => for (self.handles(), &self.models) |handle, *model| {
                    const present = model.value != null;
                    const result = try handle.writer.removeOverride(scope, key, .any);
                    model.remove();
                    try std.testing.expectEqual(present, result.changed);
                    try std.testing.expectEqual(model.revision, result.revision);
                },
                .stale_writer => for (self.handles(), self.models) |handle, model| {
                    try std.testing.expectError(error.Conflict, handle.writer.put(scope, key, "stale", .{ .exact = model.revision + 1 }));
                    try std.testing.expectError(error.Conflict, handle.writer.removeOverride(scope, key, .{ .exact = model.revision + 1 }));
                },
                .competing_writers => {
                    // Both writers prepare against the same authoritative head.
                    // Only one publication may win, including identical deletes.
                    var backend = self.object.?.backend.inner;
                    var old = try backend.read(self.alloc, scope);
                    defer old.deinit(self.alloc);
                    _ = try self.object.?.nativeStore().writer.put(scope, key, "winner", .any);
                    self.models[0].put("winner");
                    var winner = try backend.read(self.alloc, scope);
                    defer winner.deinit(self.alloc);
                    try std.testing.expectError(error.Conflict, backend.publish(self.alloc, scope, old, winner.bytes.?));
                    var competing = try lite.Store.init(self.alloc, &self.document.?, scope, self.keys.provider());
                    defer competing.deinit();
                    const expected: contract.ExpectedRevision = if (self.models[1].value != null) .{ .exact = self.models[1].revision } else .absent;
                    _ = try self.lite_store.?.nativeStore().?.writer.put(scope, key, "winner", expected);
                    self.models[1].put("winner");
                    try std.testing.expectError(error.Conflict, competing.nativeStore().?.writer.put(scope, key, "loser", expected));
                },
                .lost_before, .lost_after => {
                    self.fault = if (operation == .lost_before) .before_commit else .after_commit;
                    defer self.fault = .none;
                    const count = self.publications;
                    try std.testing.expectError(error.OutcomeUnknown, self.object.?.nativeStore().writer.put(scope, key, "ambiguous", .any));
                    // Ambiguity must never cause an automatic second mutation.
                    try std.testing.expectEqual(count + 1, self.publications);
                    if (operation == .lost_after) self.models[0].put("ambiguous");
                },
                .unavailable => {
                    var facade = try secrets.FileStore.initConfiguredWithIo(self.alloc, self.sim.io(), .{
                        .native = .{ .backend = .serverless, .path = "file://objects", .scope = scope, .keyring_path = "keys.json" },
                        .sources = &.{.{ .name = "mounted", .type = .file, .path = "fallback.json" }},
                        .environment = false,
                    });
                    defer facade.deinit();
                    facade.attachNative(self.object.?.source(), null);
                    self.fault = .unavailable;
                    defer self.fault = .none;
                    try std.testing.expectError(error.Unavailable, facade.getOwned(self.alloc, key));
                    try std.testing.expectError(error.Unavailable, facade.list(self.alloc));
                },
                .delivery_replay_partition => {
                    var facade = try secrets.FileStore.initConfiguredWithIo(self.alloc, self.sim.io(), .{
                        .native = .{ .backend = .distributed, .scope = scope, .keyring_path = "keys.json", .grants = &.{.{
                            .name = "reader",
                            .credential_path = "reader.key",
                            .keys = &.{key},
                        }} },
                        .environment = false,
                    });
                    defer facade.deinit();
                    facade.attachNative(self.object.?.source(), self.object.?.nativeStore().writer);
                    const Transport = struct {
                        store: *secrets.FileStore,
                        response: ?[]u8 = null,
                        replay: bool = false,
                        partitioned: bool = false,
                        fn execute(ptr: *anyopaque, alloc: Allocator, request: http.HttpRequest) !http.HttpResponse {
                            const transport: *@This() = @ptrCast(@alignCast(ptr));
                            if (transport.partitioned) return error.Unavailable;
                            if (transport.replay) return .{ .status = 200, .body = try alloc.dupe(u8, transport.response.?) };
                            const body = delivery.serve(alloc, transport.store, request.header("X-Antfly-Secret-Grant").?, request.body) catch |err| switch (err) {
                                error.Unauthorized => return .{ .status = 403 },
                                else => return err,
                            };
                            errdefer alloc.free(body);
                            if (transport.response) |old| alloc.free(old);
                            transport.response = try alloc.dupe(u8, body);
                            return .{ .status = 200, .body = body };
                        }
                    };
                    var transport = Transport{ .store = &facade };
                    defer if (transport.response) |bytes| self.alloc.free(bytes);
                    var remote = delivery.Remote{ .alloc = self.alloc, .io = self.sim.io(), .scope = scope, .config = .{ .name = "reader", .credential_path = "reader.key", .urls = &.{"http://metadata"} }, .executor = .{ .ptr = &transport, .vtable = &.{ .execute = Transport.execute } } };
                    var found = try remote.source().resolve(self.alloc, scope, key, .{});
                    defer found.deinit(self.alloc);
                    try std.testing.expectEqual(self.models[0].revision, found.revision);
                    transport.replay = true;
                    try std.testing.expectError(error.Unauthorized, remote.source().resolve(self.alloc, scope, key, .{}));
                    transport.replay = false;
                    try std.testing.expectError(error.Unauthorized, remote.source().resolve(self.alloc, scope, "not-granted", .{}));
                    transport.partitioned = true;
                    try std.testing.expectError(error.Unavailable, remote.source().resolve(self.alloc, scope, key, .{}));
                    transport.partitioned = false;
                    try self.models[0].check(self.alloc, remote.source());
                },
                .rotate_keys => {
                    try self.write("keys.json", rotated_key);
                    try self.check(); // retained wrapping keys decrypt existing entries
                    try self.write("keys.json", "{}");
                    for (self.handles(), self.models) |handle, model| {
                        try std.testing.expectError(error.CorruptInput, handle.writer.put(scope, key, "invalid-keyring", .any));
                        if (model.value != null) try std.testing.expectError(error.CorruptInput, handle.source.resolve(self.alloc, scope, key, .{}));
                    }
                    try self.write("keys.json", rotated_key);
                },
                .corrupt => {
                    // Tamper a persisted ciphertext byte while retaining framing.
                    _ = try self.object.?.nativeStore().writer.put(scope, key, "tamper-target", .any);
                    self.models[0].put("tamper-target");
                    var backend = self.object.?.backend.inner;
                    var old = try backend.read(self.alloc, scope);
                    defer old.deinit(self.alloc);
                    const damaged = try self.alloc.dupe(u8, old.bytes.?);
                    defer self.alloc.free(damaged);
                    damaged[damaged.len - 1] ^= 1;
                    try backend.publish(self.alloc, scope, old, damaged);
                    try std.testing.expectError(error.CorruptInput, self.object.?.source().resolve(self.alloc, scope, key, .{}));
                    var bad = try backend.read(self.alloc, scope);
                    defer bad.deinit(self.alloc);
                    try backend.publish(self.alloc, scope, bad, old.bytes.?);
                },
                .crash, .finish => {
                    self.close();
                    try self.sim.crashFileSystem();
                    try self.open(false);
                    if (operation == .finish) self.finished = true;
                },
                .lite_sync_failure => {
                    const before = self.models[1];
                    self.sim.files.faults.fail_next_sync = true;
                    try std.testing.expectError(error.OutcomeUnknown, self.lite_store.?.nativeStore().?.writer.put(scope, key, "uncertain-lite", .any));
                    try std.testing.expect(!self.sim.files.faults.fail_next_sync);
                    try std.testing.expectError(error.OutcomeUnknown, self.lite_store.?.source().resolve(self.alloc, scope, key, .{}));
                    self.close();
                    try self.sim.crashFileSystem();
                    try self.open(false);
                    var result = try self.lite_store.?.source().resolve(self.alloc, scope, key, .{});
                    defer result.deinit(self.alloc);
                    // An uncertain write may be absent or completely durable;
                    // it must never expose a torn value/head or erase prior data.
                    try std.testing.expect(result.revision == before.revision or result.revision == before.revision + 1);
                    if (result.revision == before.revision + 1) self.models[1].put("uncertain-lite");
                },
                .file_fallback => {
                    var listed = try self.file.?.put(self.alloc, key, "file-override");
                    listed.deinit(self.alloc);
                    const value = (try self.file.?.getOwned(self.alloc, key)).?;
                    defer self.alloc.free(value);
                    try std.testing.expectEqualStrings("file-override", value);
                    try std.testing.expect(try self.file.?.delete(key));
                    const fallback = (try self.file.?.getOwned(self.alloc, key)).?;
                    defer self.alloc.free(fallback);
                    try std.testing.expectEqualStrings("mounted", fallback);
                },
            }
            self.steps += 1;
            try self.check();
        }
    };
    pub const World = struct { state: *State };
    pub fn init(alloc: Allocator) !World {
        const state = try alloc.create(State);
        errdefer alloc.destroy(state);
        state.* = .{ .alloc = alloc, .sim = try vopr.vopr_io.VoprIo.init(.{
            .seed = 0x5ec7e7,
            .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }),
        }) };
        errdefer state.sim.deinit();
        errdefer state.close();
        state.keys = .{ .alloc = alloc, .io = state.sim.io(), .path = "keys.json" };
        try state.write("keys.json", first_key);
        try state.write("standalone.json", "{\"secrets\":[]}");
        try state.write("reader.key", "3333333333333333333333333333333333333333333333333333333333333333");
        try state.write("fallback.json", "{\"secrets\":[{\"key\":\"provider.api_key\",\"value\":\"mounted\",\"created_at_ns\":1,\"updated_at_ns\":1}]}");
        try state.open(true);
        return .{ .state = state };
    }
    pub fn deinit(world: *World, alloc: Allocator) void {
        world.state.close();
        world.state.sim.deinit();
        alloc.destroy(world.state);
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, alloc: Allocator) !void {
        if (world.state.finished) return;
        inline for (comptime std.meta.tags(Operation)) |operation| {
            if ((operation == .finish) == (world.state.steps >= 24)) try list.append(alloc, .{
                .id = id(operation),
                .name = name ++ "." ++ @tagName(operation),
                .kind = switch (operation) {
                    .finish => .quiescence,
                    .lost_before, .lost_after, .unavailable, .delivery_replay_partition, .corrupt, .crash, .lite_sync_failure => .fault,
                    .rotate_keys => .maintenance,
                    else => .workload,
                },
            });
        }
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, alloc: Allocator) !vopr.outcome.TransitionOutcome {
        inline for (comptime std.meta.tags(Operation)) |operation| {
            if (selected.id == id(operation)) {
                world.state.run(operation) catch |err| switch (err) {
                    error.TestExpectedEqual, error.TestUnexpectedResult, error.TestExpectedError => {
                        world.state.sound = false;
                        world.state.finished = true;
                    },
                    else => return err,
                };
                try events.emitNamed(alloc, .domain, selected.name, world.state.steps);
                return .applied();
            }
        }
        return error.InvalidSecretTransition;
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, alloc: Allocator) !void {
        try builder.addNamed(alloc, name ++ ".object_revision", @intCast(world.state.models[0].revision));
        try builder.addNamed(alloc, name ++ ".lite_revision", @intCast(world.state.models[1].revision));
        try builder.addNamed(alloc, name ++ ".steps", @intCast(world.state.steps));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, alloc: Allocator) !void {
        try sink.check(alloc, sound, world.state.sound);
        try sink.check(alloc, complete, world.state.finished);
    }
    pub fn done(world: *World) bool {
        return world.state.finished;
    }
};

test "secrets VOPR model exact replays publication faults rotation and crash recovery" {
    const ids = vopr.vopr_io.artifactBackendIds();
    // Force every fault at least once, then explore ordering with seeded runs.
    const prefix = comptime blk: {
        var selected: [14]u64 = undefined;
        for (std.meta.tags(Scenario.Operation)[0..14], 0..) |operation, i| selected[i] = Scenario.id(operation);
        break :blk selected;
    };
    var required = vopr.choice.PrefixedSeeded.init(&prefix, 0x5ec7e7);
    var required_trace = try vopr.runner.run(Scenario, std.testing.allocator, required.source(), .{
        .system = "antfly",
        .seed = 0x5ec7e7,
        .transition_budget = 32,
        .backend_ids = &ids,
    });
    defer required_trace.deinit();
    try std.testing.expectEqual(@as(u64, 0), required_trace.summary.?.property_failures);
    var required_replay = try vopr.replay.exact(Scenario, std.testing.allocator, &required_trace);
    required_replay.deinit();
    for (0..8) |seed| {
        var choices = vopr.choice.Seeded.init(seed);
        var trace = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .seed = seed,
            .transition_budget = 32,
            .backend_ids = &ids,
        });
        defer trace.deinit();
        try std.testing.expectEqual(@as(u64, 0), trace.summary.?.property_failures);
        var replay = try vopr.replay.exact(Scenario, std.testing.allocator, &trace);
        replay.deinit();
    }
}
