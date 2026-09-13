// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! C ABI handle, callback, restore, and generation ownership on borrowed I/O.

const std = @import("std");
const vopr = @import("vopr");
const capi_db = @import("antfly_capi");
const storage = @import("antfly_capi_storage_root");

const CApi = capi_db.ApiTypes;
const Runtime = storage.db.background_runtime.BackendRuntime;
const MaintenanceCancel = storage.storage_maintenance.CancelToken;

pub const Scenario = struct {
    pub const name: []const u8 = "capi-lite-lifecycle";
    pub const version: u32 = 1;
    const sound_id = vopr.id.stable(name, "callback-and-generation-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".callback-and-generation-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum { callback_lifetime, restore_cancel_and_activate };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "callback-lifetime"),
        vopr.id.stable(name, "restore-cancel-and-activate"),
    };
    const mode_names = [_][]const u8{
        name ++ ".callback-lifetime",
        name ++ ".restore-cancel-and-activate",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        const CallbackRecorder = struct {
            calls: u64 = 0,
            last_group: u64 = 0,
            last_context_len: usize = 0,

            fn callback(ctx: ?*anyopaque, group_id: u64, _: ?[*]const u8, context_len: usize) callconv(.c) CApi.ErrorCode {
                const self: *@This() = @ptrCast(@alignCast(ctx.?));
                self.calls += 1;
                self.last_group = group_id;
                self.last_context_len = context_len;
                return .ok;
            }
        };

        fn open(self: *@This(), runtime: *Runtime, path: []const u8, create: bool, read_only: bool) !*anyopaque {
            return try capi_db.openLiteHandleWithRuntime(
                std.heap.c_allocator,
                path,
                self.sim.io(),
                runtime,
                .{ .create = create, .read_only = read_only, .hosted = true },
            );
        }

        fn insert(handle: *anyopaque, key: []const u8, value: []const u8) !void {
            const body = try std.fmt.allocPrint(std.heap.c_allocator, "{{\"inserts\":{{\"{s}\":{s}}},\"sync_level\":\"write\"}}", .{ key, value });
            defer std.heap.c_allocator.free(body);
            var out: CApi.Buffer = .{};
            if (capi_db.antfly_db_batch_json(handle, .{ .ptr = body.ptr, .len = body.len }, &out) != .ok)
                return error.CapiBatchFailed;
            capi_db.antfly_buffer_free(&out);
        }

        fn lookup(handle: *anyopaque, key: []const u8) ![]u8 {
            var out: CApi.Buffer = .{};
            if (capi_db.antfly_db_lookup_json(handle, .{ .ptr = key.ptr, .len = key.len }, &out) != .ok)
                return error.CapiLookupFailed;
            defer capi_db.antfly_buffer_free(&out);
            return try std.heap.c_allocator.dupe(u8, out.ptr.?[0..out.len]);
        }

        fn backup(handle: *anyopaque) !CApi.Buffer {
            var out: CApi.Buffer = .{};
            if (capi_db.antfly_lite_backup(handle, &out) != .ok) return error.CapiBackupFailed;
            return out;
        }

        fn runCallback(self: *@This()) !void {
            var runtime = try Runtime.init(self.allocator, .{
                .backend = .manual,
                .borrowed_io = .{ .general = self.sim.io() },
            });
            defer runtime.deinit();
            const handle = try self.open(&runtime, "callback.aflite", true, false);
            defer capi_db.closeLiteRuntimeHandle(handle);
            try insert(handle, "doc:callback", "{\"title\":\"callback\"}");

            var recorder = CallbackRecorder{};
            if (capi_db.antfly_db_set_readable_lease_hook(handle, 73, &recorder, &CallbackRecorder.callback) != .ok)
                return error.CapiHookInstallFailed;
            const first = try lookup(handle, "doc:callback");
            defer std.heap.c_allocator.free(first);
            if (capi_db.antfly_db_set_readable_lease_hook(handle, 0, null, null) != .ok)
                return error.CapiHookRemoveFailed;
            const second = try lookup(handle, "doc:callback");
            defer std.heap.c_allocator.free(second);

            self.sound = recorder.calls == 1 and recorder.last_group == 73 and
                recorder.last_context_len > 0 and std.mem.eql(u8, first, second);
            self.progress += 4;
        }

        fn runRestore(self: *@This()) !void {
            var runtime = try Runtime.init(self.allocator, .{
                .backend = .manual,
                .borrowed_io = .{ .general = self.sim.io() },
            });
            defer runtime.deinit();

            const source = try self.open(&runtime, "source.aflite", true, false);
            try insert(source, "doc:v1", "{\"generation\":1}");
            var backup_v1 = try backup(source);
            defer capi_db.antfly_buffer_free(&backup_v1);
            capi_db.closeLiteRuntimeHandle(source);

            var canceled = MaintenanceCancel{};
            canceled.request();
            const canceled_result = capi_db.restorePortableBackupToLiteFileWithRuntime(
                std.heap.c_allocator,
                self.sim.io(),
                &runtime,
                "restored.aflite",
                backup_v1.ptr.?[0..backup_v1.len],
                false,
                &canceled,
            );
            if (canceled_result) |_| {
                self.sound = false;
                return;
            } else |err| if (err != error.MaintenanceCanceled) return err;

            try capi_db.restorePortableBackupToLiteFileWithRuntime(
                std.heap.c_allocator,
                self.sim.io(),
                &runtime,
                "restored.aflite",
                backup_v1.ptr.?[0..backup_v1.len],
                false,
                null,
            );
            const pinned = try self.open(&runtime, "restored.aflite", false, true);
            defer capi_db.closeLiteRuntimeHandle(pinned);

            const source_v2 = try self.open(&runtime, "source.aflite", false, false);
            try insert(source_v2, "doc:v2", "{\"generation\":2}");
            var backup_v2 = try backup(source_v2);
            defer capi_db.antfly_buffer_free(&backup_v2);
            capi_db.closeLiteRuntimeHandle(source_v2);

            try capi_db.restorePortableBackupToLiteFileWithRuntime(
                std.heap.c_allocator,
                self.sim.io(),
                &runtime,
                "restored.aflite",
                backup_v2.ptr.?[0..backup_v2.len],
                true,
                null,
            );
            const current = try self.open(&runtime, "restored.aflite", false, true);
            defer capi_db.closeLiteRuntimeHandle(current);
            const old_v1 = try lookup(pinned, "doc:v1");
            defer std.heap.c_allocator.free(old_v1);
            const new_v2 = try lookup(current, "doc:v2");
            defer std.heap.c_allocator.free(new_v2);

            self.sound = std.mem.indexOf(u8, old_v1, "generation") != null and
                std.mem.indexOf(u8, new_v2, "generation") != null;
            self.progress += 8;
        }

        fn run(self: *@This(), mode: Mode) !void {
            switch (mode) {
                .callback_lifetime => try self.runCallback(),
                .restore_cancel_and_activate => try self.runRestore(),
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0xca91_11fe,
                .required = .of(&.{ .files, .task_scheduling, .synchronization, .deterministic_entropy, .clock_read }),
            }),
        };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
            .id = id,
            .name = mode_name,
            .kind = if (mode == .restore_cancel_and_activate) .fault else .workload,
        });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidCapiLiteLifecycleTransition;
        try events.emitNamed(allocator, .domain, selected.name, world.state.progress);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".progress", @intCast(world.state.progress));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, sound_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
    }
    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return .{
            .progress_expected = true,
            .progress_units = world.state.progress,
            .active_tasks = world.state.sim.tasks.activeTaskCount(),
            .cleanup_complete = world.state.complete,
        };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "C API Lite lifecycle exact replay" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
