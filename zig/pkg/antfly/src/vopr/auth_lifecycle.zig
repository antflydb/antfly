// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production UserManager lifecycle histories. Deterministic randomness,
//! realtime, and mutation locking all come from the manager's borrowed
//! `std.Io`; VOPR owns only fault choice and semantic suspension identities.

const std = @import("std");
const casbin = @import("antfly_casbin");
const vopr = @import("vopr");
const usermgr = @import("../usermgr/user_manager.zig");

pub const Hook = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn lifecycle(self: *Hook) usermgr.AuthLifecycleHook {
        return .{ .ptr = self, .reach_fn = reach };
    }

    pub fn stableId(event: usermgr.AuthLifecycleEvent) vopr.id.StableId {
        var result = vopr.id.stable("auth-lifecycle.phase", @tagName(event.phase));
        result = vopr.id.derive("auth-lifecycle.user", result, vopr.id.digest(event.username));
        return vopr.id.derive("auth-lifecycle.key", result, vopr.id.digest(event.key_id));
    }

    fn reach(ptr: *anyopaque, event: usermgr.AuthLifecycleEvent) !void {
        const self: *Hook = @ptrCast(@alignCast(ptr));
        try self.vopr_io.safepoint(stableId(event));
    }
};

pub const Scenario = struct {
    pub const name: []const u8 = "auth-production-lifecycle";
    pub const version: u32 = 1;
    const durable_id = vopr.id.stable(name, "durable-memory-agreement");
    const revoked_id = vopr.id.stable(name, "revoked-key-stays-revoked");
    const policy_id = vopr.id.stable(name, "policy-and-filter-visible");
    const crash_id = vopr.id.stable(name, "partial-user-create-rolls-back");
    const seed_id = vopr.id.stable(name, "seed-capture-excludes-mutation");
    const complete_id = vopr.id.stable(name, "lifecycle-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = durable_id, .name = name ++ ".durable-memory-agreement", .kind = .always },
        .{ .id = revoked_id, .name = name ++ ".revoked-key-stays-revoked", .kind = .always },
        .{ .id = policy_id, .name = name ++ ".policy-and-filter-visible", .kind = .always },
        .{ .id = crash_id, .name = name ++ ".partial-user-create-rolls-back", .kind = .always },
        .{ .id = seed_id, .name = name ++ ".seed-capture-excludes-mutation", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".lifecycle-completes", .kind = .reachable },
    };

    const Mode = enum {
        password_rotate,
        api_key_rotate,
        permission_change,
        row_filter_change,
        revoke_with_stale_reader,
        durable_reload,
        crash_between_user_and_policy,
        seed_capture,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "password-rotate"),
        vopr.id.stable(name, "api-key-rotate"),
        vopr.id.stable(name, "permission-change"),
        vopr.id.stable(name, "row-filter-change"),
        vopr.id.stable(name, "revoke-with-stale-reader"),
        vopr.id.stable(name, "durable-reload"),
        vopr.id.stable(name, "crash-between-user-and-policy"),
        vopr.id.stable(name, "seed-capture"),
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        store: usermgr.MemoryStore,
        policy_store: casbin.MemoryAdapter,
        manager: usermgr.UserManager,
        mode: Mode = .password_rotate,
        complete: bool = false,
        durable_agreement: bool = true,
        revoked_safe: bool = true,
        policy_visible: bool = true,
        crash_rolled_back: bool = true,
        inject_user_crash: bool = false,
        lifecycle_calls: u32 = 0,
        seed_capture_acquired: bool = false,
        seed_capture_released: bool = false,
        seed_mutation_started: bool = false,
        seed_mutation_finished: bool = false,
        seed_mutation_escaped: bool = false,
        seed_task_error: ?anyerror = null,

        fn hook(self: *@This()) usermgr.AuthLifecycleHook {
            return .{ .ptr = self, .reach_fn = reach };
        }

        fn reach(ptr: *anyopaque, event: usermgr.AuthLifecycleEvent) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lifecycle_calls += 1;
            try self.sim.safepoint(Hook.stableId(event));
            if (self.inject_user_crash and event.phase == .user_persisted) {
                self.inject_user_crash = false;
                return error.InjectedAuthCrash;
            }
        }

        fn permission(self: *@This(), kind: usermgr.PermissionType) !usermgr.Permission {
            return try usermgr.Permission.initOwned(self.allocator, .table, "docs", kind);
        }

        fn rowFilter(self: *@This()) !usermgr.RowFilterEntry {
            return try usermgr.RowFilterEntry.initOwned(
                self.allocator,
                "docs",
                "{\"key\":\"tenant_id\",\"match\":\"acme\"}",
            );
        }

        fn durableContainsUser(self: *@This(), username: []const u8) !bool {
            const users = try self.store.iface().loadUsers(self.allocator);
            defer {
                for (users) |*user| user.deinit(self.allocator);
                self.allocator.free(users);
            }
            for (users) |user| if (std.mem.eql(u8, user.username, username)) return true;
            return false;
        }

        fn durableContainsKey(self: *@This(), key_id: []const u8) !bool {
            const keys = try self.store.iface().loadApiKeys(self.allocator);
            defer {
                for (keys) |*key| key.deinit(self.allocator);
                self.allocator.free(keys);
            }
            for (keys) |key| if (std.mem.eql(u8, key.key.key_id, key_id)) return true;
            return false;
        }

        fn createKey(self: *@This(), name_value: []const u8) !usermgr.CreatedApiKey {
            var permission_value = try self.permission(.read);
            defer permission_value.deinit(self.allocator);
            var filter = try self.rowFilter();
            defer filter.deinit(self.allocator);
            return try self.manager.createApiKey("alice", name_value, &.{permission_value}, &.{filter}, null);
        }

        fn seedCaptureTask(self: *@This()) void {
            var lease = self.manager.acquireSeedCaptureLease();
            self.seed_capture_acquired = true;
            self.sim.safepoint(vopr.id.stable(name, "seed-capture-held")) catch |err| {
                self.seed_task_error = err;
                lease.release();
                return;
            };
            if (self.seed_mutation_finished) self.seed_mutation_escaped = true;
            self.seed_capture_released = true;
            lease.release();
        }

        fn seedMutationTask(self: *@This()) void {
            self.seed_mutation_started = true;
            self.manager.updatePassword("alice", "after-seed") catch |err| {
                self.seed_task_error = err;
                return;
            };
            self.seed_mutation_finished = true;
            if (!self.seed_capture_released) self.seed_mutation_escaped = true;
        }

        fn runSeedCapture(self: *@This()) !void {
            const io = self.sim.io();
            _ = io.async(seedCaptureTask, .{self});
            _ = io.async(seedMutationTask, .{self});
            var enabled: vopr.transition.List = .{};
            defer enabled.deinit(self.allocator);
            var events: vopr.event.Sink = .{};
            defer events.deinit(self.allocator);
            const scheduler = self.sim.scheduler();

            try scheduler.enumerateReady(&enabled, self.allocator);
            if (enabled.items.items.len != 2) return error.InvalidSeedCaptureInitialSchedule;
            const capture_transition = enabled.items.items[0];
            const capture_actor = capture_transition.actor_id;
            try scheduler.executeReady(capture_transition.id, &events, self.allocator);
            if (!self.seed_capture_acquired) return error.SeedCaptureTaskDidNotAcquire;

            enabled.items.clearRetainingCapacity();
            try scheduler.enumerateReady(&enabled, self.allocator);
            try enabled.canonicalize();
            var mutation_transition: ?vopr.transition.Transition = null;
            for (enabled.items.items) |candidate| {
                if (candidate.actor_id != capture_actor and std.mem.eql(u8, candidate.name, "sim-io.task_resume")) {
                    mutation_transition = candidate;
                    break;
                }
            }
            try scheduler.executeReady((mutation_transition orelse return error.SeedMutationTaskNotRunnable).id, &events, self.allocator);
            if (!self.seed_mutation_started or self.seed_mutation_finished) return error.SeedMutationDidNotBlock;

            var transitions: usize = 0;
            while (!scheduler.quiescent()) : (transitions += 1) {
                if (transitions > 32) return error.SeedCaptureScheduleBudgetExceeded;
                enabled.items.clearRetainingCapacity();
                try scheduler.enumerateReady(&enabled, self.allocator);
                try enabled.canonicalize();
                if (enabled.items.items.len == 0) return error.SeedCaptureScheduleDeadlock;
                try scheduler.executeReady(enabled.items.items[0].id, &events, self.allocator);
            }
            if (self.seed_task_error) |err| return err;
            if (!self.seed_capture_released or !self.seed_mutation_finished) return error.SeedCaptureTasksIncomplete;
        }

        fn run(self: *@This()) !void {
            self.manager.setLifecycleHook(self.hook());
            switch (self.mode) {
                .password_rotate => {
                    try self.manager.updatePassword("alice", "rotated-secret");
                    var authenticated = try self.manager.authenticateUser("alice", "rotated-secret");
                    defer authenticated.deinit(self.allocator);
                    const users = try self.store.iface().loadUsers(self.allocator);
                    defer {
                        for (users) |*user| user.deinit(self.allocator);
                        self.allocator.free(users);
                    }
                    self.durable_agreement = users.len == 1 and
                        std.mem.eql(u8, users[0].password_hash, authenticated.password_hash);
                },
                .api_key_rotate => {
                    var first = try self.createKey("first");
                    defer first.deinit(self.allocator);
                    try self.manager.deleteApiKey("alice", first.key.key_id);
                    var second = try self.createKey("second");
                    defer second.deinit(self.allocator);
                    self.revoked_safe = self.manager.validateApiKey(first.key.key_id, first.key_secret) == error.ApiKeyNotFound;
                    var validated = try self.manager.validateApiKey(second.key.key_id, second.key_secret);
                    defer validated.deinit(self.allocator);
                    self.durable_agreement = !(try self.durableContainsKey(first.key.key_id)) and
                        try self.durableContainsKey(second.key.key_id);
                },
                .permission_change => {
                    var write = try self.permission(.write);
                    defer write.deinit(self.allocator);
                    try self.manager.addPermissionToUser("alice", write);
                    self.policy_visible = try self.manager.enforce("alice", .table, "docs", .write);
                },
                .row_filter_change => {
                    try self.manager.setRowFilter("alice", "docs", "{\"key\":\"tenant_id\",\"match\":\"acme\"}");
                    const filter = try self.manager.getRowFilter("alice", "docs");
                    defer self.allocator.free(filter);
                    self.policy_visible = std.mem.indexOf(u8, filter, "acme") != null;
                },
                .revoke_with_stale_reader => {
                    var created = try self.createKey("stale-reader");
                    defer created.deinit(self.allocator);
                    var reader_snapshot = try self.manager.validateApiKey(created.key.key_id, created.key_secret);
                    defer reader_snapshot.deinit(self.allocator);
                    try self.manager.deleteApiKey("alice", created.key.key_id);
                    self.revoked_safe = self.manager.validateApiKey(created.key.key_id, created.key_secret) == error.ApiKeyNotFound and
                        std.mem.eql(u8, reader_snapshot.username, "alice");
                },
                .durable_reload => {
                    var created = try self.createKey("reload");
                    defer created.deinit(self.allocator);
                    var reload_policy = casbin.MemoryAdapter.init(self.allocator);
                    defer reload_policy.deinit();
                    var reloaded = try usermgr.UserManager.initWithIo(
                        self.allocator,
                        self.sim.io(),
                        self.store.iface(),
                        try usermgr.initDefaultEnforcer(self.allocator, reload_policy.iface()),
                    );
                    defer reloaded.deinit();
                    var validated = try reloaded.validateApiKey(created.key.key_id, created.key_secret);
                    defer validated.deinit(self.allocator);
                    self.durable_agreement = std.mem.eql(u8, validated.username, "alice");
                },
                .crash_between_user_and_policy => {
                    self.inject_user_crash = true;
                    var read = try self.permission(.read);
                    defer read.deinit(self.allocator);
                    if (self.manager.createUser("bob", "secret", &.{read})) |*created| {
                        var owned = created.*;
                        owned.deinit(self.allocator);
                        self.crash_rolled_back = false;
                    } else |err| {
                        if (err != error.InjectedAuthCrash) return err;
                    }
                    self.crash_rolled_back = self.manager.getUser("bob") == error.UserNotFound and
                        !(try self.durableContainsUser("bob"));
                },
                .seed_capture => try self.runSeedCapture(),
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.allocator = allocator;
        state.sim = try vopr.vopr_io.VoprIo.init(.{ .seed = 0x41555448, .realtime_ns = 1_000_000 });
        errdefer state.sim.deinit();
        state.store = usermgr.MemoryStore.init(allocator);
        errdefer state.store.deinit();
        try state.store.users.append(allocator, .{
            .username = try allocator.dupe(u8, "alice"),
            .password_hash = try allocator.dupe(u8, "not-used-before-rotation"),
            .metadata_json = try allocator.dupe(u8, "{\"tenant_id\":\"acme\"}"),
        });
        state.policy_store = casbin.MemoryAdapter.init(allocator);
        errdefer state.policy_store.deinit();
        state.manager = try usermgr.UserManager.initWithIo(
            allocator,
            state.sim.io(),
            state.store.iface(),
            try usermgr.initDefaultEnforcer(allocator, state.policy_store.iface()),
        );
        errdefer state.manager.deinit();
        var read = try usermgr.Permission.initOwned(allocator, .table, "docs", .read);
        defer read.deinit(allocator);
        try state.manager.addPermissionToUser("alice", read);
        state.mode = .password_rotate;
        state.complete = false;
        state.durable_agreement = true;
        state.revoked_safe = true;
        state.policy_visible = true;
        state.crash_rolled_back = true;
        state.inject_user_crash = false;
        state.lifecycle_calls = 0;
        state.seed_capture_acquired = false;
        state.seed_capture_released = false;
        state.seed_mutation_started = false;
        state.seed_mutation_finished = false;
        state.seed_mutation_escaped = false;
        state.seed_task_error = null;
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        const state = world.state;
        state.manager.deinit();
        state.policy_store.deinit();
        state.store.deinit();
        state.sim.deinit();
        allocator.destroy(state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| try list.append(allocator, .{
            .id = id,
            .name = name ++ "." ++ @tagName(mode),
            .kind = if (mode == .crash_between_user_and_policy) .fault else .workload,
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            world.state.mode = mode;
            found = true;
        };
        if (!found) return error.InvalidAuthLifecycleTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.lifecycle_calls);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
        try builder.addNamed(allocator, name ++ ".lifecycle-calls", world.state.lifecycle_calls);
        try builder.addNamed(allocator, name ++ ".durable-agreement", @intFromBool(world.state.durable_agreement));
        try builder.addNamed(allocator, name ++ ".revoked-safe", @intFromBool(world.state.revoked_safe));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, durable_id, state.durable_agreement);
        try sink.check(allocator, revoked_id, state.revoked_safe);
        try sink.check(allocator, policy_id, state.policy_visible);
        try sink.check(allocator, crash_id, state.crash_rolled_back);
        try sink.check(allocator, seed_id, !state.seed_mutation_escaped and
            (state.mode != .seed_capture or (state.seed_capture_released and state.seed_mutation_finished)));
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = state.lifecycle_calls,
            .recovery_expected = state.mode == .durable_reload or state.mode == .crash_between_user_and_policy,
            .recovery_complete = state.complete and state.durable_agreement and state.crash_rolled_back,
            .consistency_valid = state.durable_agreement and state.revoked_safe and state.policy_visible and state.crash_rolled_back,
            .cleanup_complete = state.complete and !state.seed_mutation_escaped,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "user auth lifecycle VOPR exact replays rotate revoke reload and crash recovery" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
            .source_revision = "auth-lifecycle-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.property_failures);
        for (0..5) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
