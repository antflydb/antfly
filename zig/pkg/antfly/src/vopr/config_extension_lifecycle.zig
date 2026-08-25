// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Cold configuration, secret publication, remote-content refresh, and
//! extension lifecycle histories through production APIs on borrowed VoprIo.

const std = @import("std");
const scraping = @import("antfly_scraping");
const vopr = @import("vopr");
const remote_content_runtime = @import("../common/remote_content_runtime.zig");
const secrets = @import("../common/secrets.zig");
const extensions = @import("../extensions/mod.zig");
const extension_lifecycle = @import("../extensions/lifecycle.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const transition_state = @import("../metadata/transition_state.zig");

const primary_config =
    \\{"health_port":8081,"remote_content":{"default_s3":"primary","s3":{"primary":{"access_key_id":"${secret:primary.access}","secret_access_key":"${secret:primary.secret}"}}}}
;
const archive_config =
    \\{"health_port":8081,"remote_content":{"default_s3":"archive","s3":{"archive":{"access_key_id":"${secret:archive.access}","secret_access_key":"${secret:archive.secret}"}}}}
;

pub const Scenario = struct {
    pub const name: []const u8 = "config-extension-lifecycle";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "publication-remains-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".publication-remains-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        cold_valid_config,
        cold_malformed_config,
        cold_partial_config,
        secret_rotation_during_use,
        secret_config_crash_boundary,
        remote_refresh_rollback,
        extension_install_replace_activate,
        extension_package_recovery,
        wasm_failed_startup,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "cold-valid-config"),
        vopr.id.stable(name, "cold-malformed-config"),
        vopr.id.stable(name, "cold-partial-config"),
        vopr.id.stable(name, "secret-rotation-during-use"),
        vopr.id.stable(name, "secret-config-crash-boundary"),
        vopr.id.stable(name, "remote-refresh-rollback"),
        vopr.id.stable(name, "extension-install-replace-activate"),
        vopr.id.stable(name, "extension-package-recovery"),
        vopr.id.stable(name, "wasm-failed-startup"),
    };
    const mode_names = [_][]const u8{
        name ++ ".cold-valid-config",
        name ++ ".cold-malformed-config",
        name ++ ".cold-partial-config",
        name ++ ".secret-rotation-during-use",
        name ++ ".secret-config-crash-boundary",
        name ++ ".remote-refresh-rollback",
        name ++ ".extension-install-replace-activate",
        name ++ ".extension-package-recovery",
        name ++ ".wasm-failed-startup",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        sound: bool = true,
        complete: bool = false,
        progress: u64 = 0,

        fn path(self: *@This(), mode: Mode, leaf: []const u8) ![]u8 {
            return std.fmt.allocPrint(self.allocator, "vopr-cold/{s}/{s}", .{ @tagName(mode), leaf });
        }

        fn writeFile(self: *@This(), path_value: []const u8, bytes: []const u8, durable: bool) !void {
            if (std.fs.path.dirname(path_value)) |parent| try std.Io.Dir.cwd().createDirPath(self.sim.io(), parent);
            var file = try std.Io.Dir.cwd().createFile(self.sim.io(), path_value, .{ .truncate = true, .read = true });
            defer file.close(self.sim.io());
            try file.writeStreamingAll(self.sim.io(), bytes);
            if (durable) {
                try file.sync(self.sim.io());
                try self.sim.files.syncNamespace();
            }
        }

        fn put(store: *secrets.FileStore, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !void {
            var listed = try store.put(allocator, key, value);
            listed.deinit(allocator);
        }

        fn seedPrimarySecrets(self: *@This(), store: *secrets.FileStore) !void {
            try put(store, self.allocator, "primary.access", "PRIMARY-ACCESS");
            try put(store, self.allocator, "primary.secret", "PRIMARY-SECRET");
        }

        fn seedArchiveSecrets(self: *@This(), store: *secrets.FileStore) !void {
            try put(store, self.allocator, "archive.access", "ARCHIVE-ACCESS");
            try put(store, self.allocator, "archive.secret", "ARCHIVE-SECRET");
        }

        fn snapshotUses(
            self: *@This(),
            facade: *scraping.RemoteContentConfig,
            store: *secrets.FileStore,
            name_value: []const u8,
            access_reference: []const u8,
            resolved_access: []const u8,
        ) !bool {
            var snapshot = facade.acquire();
            defer snapshot.deinit();
            const actual_name = snapshot.config.default_s3 orelse "";
            if (!std.mem.eql(u8, actual_name, name_value)) return false;
            const credential = snapshot.config.getS3(name_value) orelse return false;
            const actual_access = credential.access_key_id orelse "";
            if (!std.mem.eql(u8, actual_access, access_reference)) return false;
            const resolved = try secrets.resolveReferenceOwned(self.allocator, store, actual_access);
            defer self.allocator.free(resolved);
            return std.mem.eql(u8, resolved, resolved_access);
        }

        fn coldValid(self: *@This(), mode: Mode) !void {
            const secret_path = try self.path(mode, "secrets.json");
            defer self.allocator.free(secret_path);
            const config_path = try self.path(mode, "config.json");
            defer self.allocator.free(config_path);
            var store = try secrets.FileStore.initWithIo(self.allocator, self.sim.io(), secret_path);
            defer store.deinit();
            try self.seedPrimarySecrets(&store);
            try self.writeFile(config_path, primary_config, true);
            var runtime = try remote_content_runtime.Runtime.initWithIo(self.allocator, self.sim.io(), config_path, &store, null);
            defer runtime.deinit();
            var facade = scraping.RemoteContentConfig{};
            runtime.attach(&facade);
            const health = runtime.health();
            const snapshot_sound = try self.snapshotUses(
                &facade,
                &store,
                "primary",
                "${secret:primary.access}",
                "PRIMARY-ACCESS",
            );
            self.sound = health.generation == 1 and !health.stale_snapshot and snapshot_sound;
            self.progress += 1;
        }

        fn coldRejected(self: *@This(), mode: Mode, bytes: []const u8, expected: anyerror) !void {
            const config_path = try self.path(mode, "config.json");
            defer self.allocator.free(config_path);
            try self.writeFile(config_path, bytes, true);
            if (remote_content_runtime.Runtime.initWithIo(self.allocator, self.sim.io(), config_path, null, null)) |runtime_value| {
                var runtime = runtime_value;
                runtime.deinit();
                self.sound = false;
            } else |err| {
                self.sound = err == expected;
                if (!self.sound) std.debug.print("cold config expected={} actual={}\n", .{ expected, err });
            }
            self.progress += 1;
        }

        fn rotateSecretDuringUse(self: *@This(), mode: Mode) !void {
            const secret_path = try self.path(mode, "secrets.json");
            defer self.allocator.free(secret_path);
            var store = try secrets.FileStore.initWithIo(self.allocator, self.sim.io(), secret_path);
            defer store.deinit();
            try put(&store, self.allocator, "provider.api_key", "KEY-ONE");
            var reference = (try secrets.SecretValue.initConfig(self.allocator, "${secret:provider.api_key}")).?;
            defer reference.deinit(self.allocator);
            var held = try reference.resolveOwnedWithGeneration(self.allocator, &store);
            defer held.deinit(self.allocator);
            try put(&store, self.allocator, "provider.api_key", "KEY-TWO");
            var rotated = try reference.resolveOwnedWithGeneration(self.allocator, &store);
            defer rotated.deinit(self.allocator);
            self.sound = std.mem.eql(u8, held.value, "KEY-ONE") and
                std.mem.eql(u8, rotated.value, "KEY-TWO") and
                rotated.generation > held.generation and
                store.healthSnapshot().entry_count == 1;
            self.progress += 2;
        }

        fn secretConfigCrash(self: *@This(), mode: Mode) !void {
            const secret_path = try self.path(mode, "secrets.json");
            defer self.allocator.free(secret_path);
            const config_path = try self.path(mode, "config.json");
            defer self.allocator.free(config_path);
            try self.writeFile(config_path, primary_config, true);
            {
                var store = try secrets.FileStore.initWithIo(self.allocator, self.sim.io(), secret_path);
                defer store.deinit();
                try self.seedPrimarySecrets(&store);
                var runtime = try remote_content_runtime.Runtime.initWithIo(self.allocator, self.sim.io(), config_path, &store, null);
                defer runtime.deinit();
                try self.seedArchiveSecrets(&store);
                // The next control-plane config image is visible but has not
                // crossed either its file or namespace durability boundary.
                try self.writeFile(config_path, archive_config, false);
            }
            try self.sim.crashFileSystem();
            var reopened_store = try secrets.FileStore.initWithIo(self.allocator, self.sim.io(), secret_path);
            defer reopened_store.deinit();
            var archive_access = try reopened_store.getOwnedWithGeneration(self.allocator, "archive.access");
            defer archive_access.deinit(self.allocator);
            var reopened_runtime = try remote_content_runtime.Runtime.initWithIo(self.allocator, self.sim.io(), config_path, &reopened_store, null);
            defer reopened_runtime.deinit();
            var facade = scraping.RemoteContentConfig{};
            reopened_runtime.attach(&facade);
            self.sound = std.mem.eql(u8, archive_access.value, "ARCHIVE-ACCESS") and
                try self.snapshotUses(
                    &facade,
                    &reopened_store,
                    "primary",
                    "${secret:primary.access}",
                    "PRIMARY-ACCESS",
                );
            self.progress += 2;
        }

        fn remoteRefreshRollback(self: *@This(), mode: Mode) !void {
            const secret_path = try self.path(mode, "secrets.json");
            defer self.allocator.free(secret_path);
            const config_path = try self.path(mode, "config.json");
            defer self.allocator.free(config_path);
            var store = try secrets.FileStore.initWithIo(self.allocator, self.sim.io(), secret_path);
            defer store.deinit();
            try self.seedPrimarySecrets(&store);
            try self.seedArchiveSecrets(&store);
            try self.writeFile(config_path, primary_config, true);
            var runtime = try remote_content_runtime.Runtime.initWithIo(self.allocator, self.sim.io(), config_path, &store, null);
            defer runtime.deinit();
            var facade = scraping.RemoteContentConfig{};
            runtime.attach(&facade);
            var held = facade.acquire();
            defer held.deinit();

            try self.writeFile(config_path, archive_config, true);
            const published = runtime.refreshIfChanged();
            const archive_visible = try self.snapshotUses(
                &facade,
                &store,
                "archive",
                "${secret:archive.access}",
                "ARCHIVE-ACCESS",
            );
            try self.writeFile(config_path,
                \\{"health_port":8081,"remote_content":{"default_s3":"missing","s3":{}}}
            , true);
            const rejected = !runtime.refreshIfChanged();
            const retained = try self.snapshotUses(
                &facade,
                &store,
                "archive",
                "${secret:archive.access}",
                "ARCHIVE-ACCESS",
            );
            const stale = runtime.health().stale_snapshot;
            try self.writeFile(config_path, primary_config, true);
            const recovered = runtime.refreshIfChanged();
            self.sound = published and archive_visible and rejected and retained and stale and recovered and
                try self.snapshotUses(
                    &facade,
                    &store,
                    "primary",
                    "${secret:primary.access}",
                    "PRIMARY-ACCESS",
                ) and
                std.mem.eql(u8, held.config.default_s3 orelse "", "primary") and
                !runtime.health().stale_snapshot;
            self.progress += 3;
        }

        const package_v1 = extensions.PackageManifest{
            .name = "memoryaf",
            .version = "1.0.0",
            .digest = "sha256:v1",
            .install = .{
                .scopes_supported = &.{.cluster},
                .objects = &.{.{ .kind = .mcp_tool, .name = "recall" }},
            },
        };
        const package_v2 = extensions.PackageManifest{
            .name = "memoryaf",
            .version = "1.1.0",
            .digest = "sha256:v2",
            .install = .{
                .scopes_supported = &.{.cluster},
                .objects = &.{
                    .{ .kind = .mcp_tool, .name = "recall" },
                    .{ .kind = .mcp_tool, .name = "remember" },
                },
            },
            .updates = &.{.{ .from_version = "1.0.0", .to_version = "1.1.0", .path = "updates/1.0.0-1.1.0.json" }},
        };

        const CatalogService = struct {
            package: extensions.PackageManifest = package_v1,
            proposals: usize = 0,
            empty_tables: [0]metadata_table_manager.TableRecord = .{},
            empty_ranges: [0]metadata_table_manager.RangeRecord = .{},
            empty_stores: [0]metadata_table_manager.StoreRecord = .{},
            empty_placements: [0]raft_reconciler.PlacementIntent = .{},
            empty_splits: [0]transition_state.SplitTransitionRecord = .{},
            empty_merges: [0]transition_state.MergeTransitionRecord = .{},

            fn packageSlice(self: *@This()) []extensions.PackageManifest {
                return @as([*]extensions.PackageManifest, @ptrCast(&self.package))[0..1];
            }

            pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
                return .{
                    .status = .{ .metadata_group_id = 1, .metrics = .{} },
                    .tables = self.empty_tables[0..],
                    .ranges = self.empty_ranges[0..],
                    .stores = self.empty_stores[0..],
                    .placement_intents = self.empty_placements[0..],
                    .extension_packages = self.packageSlice(),
                    .split_transitions = self.empty_splits[0..],
                    .merge_transitions = self.empty_merges[0..],
                };
            }

            pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

            pub fn proposeTransitionCommand(self: *@This(), command: anytype) !void {
                _ = command;
                self.proposals += 1;
            }
        };

        fn extensionLifecycle(self: *@This()) !void {
            var service = CatalogService{};
            var admin_installed = try extension_lifecycle.installOnServiceWithIo(&service, self.allocator, self.sim.io(), "memoryaf", .{
                .version = "1.0.0",
                .scope = .{ .kind = .cluster },
            });
            defer admin_installed.deinitOwned(self.allocator);
            var dry_run = try extension_lifecycle.installOnServiceWithIo(&service, self.allocator, self.sim.io(), "memoryaf", .{
                .version = "1.0.0",
                .scope = .{ .kind = .cluster },
                .dry_run = true,
            });
            defer dry_run.deinitOwned(self.allocator);

            var catalog = extensions.ExtensionCatalog.init(self.allocator);
            defer catalog.deinit();
            try catalog.registerPackage(package_v1);
            try catalog.registerPackage(package_v2);
            var installed = try catalog.installManifestOnly("memoryaf", "memoryaf", .{
                .version = "1.0.0",
                .scope = .{ .kind = .cluster },
            }, 7);
            defer installed.deinitOwned(self.allocator);
            try catalog.disableInstalled("memoryaf");
            var disabled = try catalog.getInstalledAlloc(self.allocator, "memoryaf");
            defer disabled.deinitOwned(self.allocator);
            try catalog.enableInstalled("memoryaf");
            try catalog.configureInstalled("memoryaf", .{ .config_json = "{\"retention\":42}" });
            var updated = try catalog.updateManifestOnly("memoryaf", .{ .target_version = "1.1.0" });
            defer updated.deinitOwned(self.allocator);
            const members = try catalog.listMembersForExtension(self.allocator, "memoryaf");
            defer catalog.freeMembers(self.allocator, members);
            self.sound = service.proposals == 1 and admin_installed.status == .ready and dry_run.status == .ready and
                disabled.status == .disabled and updated.status == .ready and
                std.mem.eql(u8, updated.package_version, "1.1.0") and
                std.mem.eql(u8, updated.config_json, "{\"retention\":42}") and members.len == 2;
            self.progress += 4;
        }

        fn packageRecovery(self: *@This(), mode: Mode) !void {
            const good_path = try self.path(mode, "extensions/memoryaf/extension.json");
            defer self.allocator.free(good_path);
            const bad_path = try self.path(mode, "extensions/broken/extension.json");
            defer self.allocator.free(bad_path);
            const root_path = try self.path(mode, "extensions");
            defer self.allocator.free(root_path);
            try self.writeFile(good_path,
                \\{"manifest_api_version":"extensions/v1","name":"memoryaf","version":"1.0.0","digest":"sha256:v1","install":{"scopes_supported":["cluster"],"objects":[{"kind":"mcp_tool","name":"recall"}]}}
            , true);
            try self.writeFile(bad_path, "{", true);
            if (extensions.scanPackageStoreAlloc(self.allocator, self.sim.io(), root_path)) |unexpected| {
                extensions.freePackageStoreEntries(self.allocator, unexpected);
                self.sound = false;
            } else |_| {}
            try std.Io.Dir.cwd().deleteFile(self.sim.io(), bad_path);
            const entries = try extensions.scanPackageStoreAlloc(self.allocator, self.sim.io(), root_path);
            defer extensions.freePackageStoreEntries(self.allocator, entries);
            self.sound = self.sound and entries.len == 1 and
                std.mem.eql(u8, entries[0].manifest.name, "memoryaf") and
                entries[0].layout == .canonical;
            self.progress += 2;
        }

        fn wasmFailedStartup(self: *@This(), mode: Mode) !void {
            const artifact_path = try self.path(mode, "extensions/memoryaf/module.wasm");
            defer self.allocator.free(artifact_path);
            const root_path = try self.path(mode, "extensions");
            defer self.allocator.free(root_path);
            try self.writeFile(artifact_path, "not-a-wasm-component", true);
            if (extensions.wasmtime_runtime.invokeExtensionWithOptions(self.allocator, .{
                .package_name = "memoryaf",
                .package_version = "1.0.0",
                .package_digest = "sha256:v1",
                .runtime_name = "memoryaf_wasm",
                .artifact = "module.wasm",
            }, "recall", "{}", .{
                .io = self.sim.io(),
                .package_store_root = root_path,
            })) |response| {
                self.allocator.free(response);
                self.sound = false;
            } else |err| {
                self.sound = err == error.WasmtimeUnsupportedArtifact;
            }
            self.progress += 1;
        }

        fn run(self: *@This(), mode: Mode) !void {
            switch (mode) {
                .cold_valid_config => try self.coldValid(mode),
                .cold_malformed_config => try self.coldRejected(mode, "{", error.UnexpectedEndOfInput),
                .cold_partial_config => try self.coldRejected(mode,
                    \\{"health_port":8081,"remote_content":{"default_s3":"missing","s3":{}}}
                , error.InvalidRemoteContentConfig),
                .secret_rotation_during_use => try self.rotateSecretDuringUse(mode),
                .secret_config_crash_boundary => try self.secretConfigCrash(mode),
                .remote_refresh_rollback => try self.remoteRefreshRollback(mode),
                .extension_install_replace_activate => try self.extensionLifecycle(),
                .extension_package_recovery => try self.packageRecovery(mode),
                .wasm_failed_startup => try self.wasmFailedStartup(mode),
            }
            try self.sim.ensureNoCapabilityViolation();
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
                .seed = 0xc01d_5eec_0e17,
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
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| {
            try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = switch (mode) {
                    .cold_valid_config, .secret_rotation_during_use, .extension_install_replace_activate => .workload,
                    else => .fault,
                },
            });
        }
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| {
            if (selected.id == id) {
                try world.state.run(mode);
                found = true;
            }
        }
        if (!found) return error.InvalidConfigExtensionLifecycleTransition;
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

test "config extension lifecycle VOPR exact replays cold start rotation refresh and activation" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        if (artifact.summary.?.property_failures != 0) {
            for (artifact.failures.items) |failure| std.debug.print(
                "config extension mode={s} failure={s} class={s}\n",
                .{ Scenario.mode_names[mode_ordinal], failure.identity, @tagName(failure.class) },
            );
        }
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
