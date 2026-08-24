// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Differential compatibility and migration histories. The suite opens real
//! golden storage bytes and serverless artifacts, exercises VOPR's canonical
//! trace/fixture/checkpoint contracts, and crashes the production atomic
//! data-directory admission path before retrying from durable state.

const std = @import("std");
const vopr = @import("vopr");
const data_format = @import("../common/data_format.zig");
const fs_paths = @import("../common/fs_paths.zig");
const storage_ha = @import("../storage/ha/mod.zig");
const head_coordination = @import("../serverless/head_coordination.zig");
const external_codec = @import("../serverless/external_source/codec.zig");
const external_types = @import("../serverless/external_source/types.zig");
const manifest_codec = @import("../serverless/manifest/codec.zig");
const manifest_types = @import("../serverless/manifest/types.zig");

fn CompatibilityScenario(comptime scenario_version: u32, comptime changed_semantics: bool) type {
    return struct {
        pub const name: []const u8 = "upgrade-compatibility-fixture";
        pub const version: u32 = scenario_version;
        const transition_id = vopr.id.stable(name, "advance");
        const bounded_id = vopr.id.stable(name, "bounded");
        pub const properties = &[_]vopr.property.Declaration{
            .{ .id = bounded_id, .name = name ++ ".bounded", .kind = .always },
        };
        pub const World = struct { value: u64 = 0 };

        pub fn init(_: std.mem.Allocator) !World {
            return .{};
        }
        pub fn deinit(_: *World, _: std.mem.Allocator) void {}
        pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
            if (world.value < 2) try list.append(allocator, .{ .id = transition_id, .name = name ++ ".advance", .kind = .workload });
        }
        pub fn execute(world: *World, _: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
            world.value += 1;
            try events.emitNamed(allocator, .state_change, name ++ ".advanced", world.value + @intFromBool(changed_semantics));
            return .applied();
        }
        pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
            try builder.addNamed(allocator, name ++ ".value", world.value + @intFromBool(changed_semantics));
        }
        pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
            try sink.check(allocator, bounded_id, world.value <= 2);
        }
        pub fn done(world: *World) bool {
            return world.value == 2;
        }
        pub fn snapshotAlloc(world: *const World, allocator: std.mem.Allocator) ![]u8 {
            const bytes = try allocator.alloc(u8, @sizeOf(u64));
            std.mem.writeInt(u64, bytes, world.value, .little);
            return bytes;
        }
        pub fn restoreSnapshot(world: *World, bytes: []const u8, _: std.mem.Allocator) !void {
            if (bytes.len != @sizeOf(u64)) return error.InvalidCompatibilitySnapshot;
            world.value = std.mem.readInt(u64, bytes[0..@sizeOf(u64)], .little);
        }
    };
}

const FixtureV1 = CompatibilityScenario(1, false);
const FixtureV2 = CompatibilityScenario(2, false);
const FixtureV2Changed = CompatibilityScenario(2, true);

fn runFromChoices(comptime ScenarioType: type, allocator: std.mem.Allocator, source: *const vopr.trace.Trace) !vopr.trace.Trace {
    var replay_source = vopr.choice.Replay{ .records = source.choices.items };
    return vopr.runner.run(ScenarioType, allocator, replay_source.source(), .{
        .system = source.header.system,
        .seed = source.config.seed,
        .transition_budget = source.config.transition_budget,
        .resource_budget = source.config.resource_budget,
        .fixture_hashes = source.config.fixture_hashes,
        .feature_flags = source.config.feature_flags,
        .backend_ids = source.config.backend_ids,
        .scenario_parameters = source.config.scenario_parameters,
        .source_revision = source.header.source_revision,
        .target = source.header.target,
        .optimize = source.header.optimize,
    });
}

const FixtureMigration = struct {
    fn replayV1(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
        return vopr.replay.exact(FixtureV1, allocator, artifact);
    }
    fn replayV2(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
        return vopr.replay.exact(FixtureV2, allocator, artifact);
    }
    fn replayV2Changed(allocator: std.mem.Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
        return vopr.replay.exact(FixtureV2Changed, allocator, artifact);
    }
    fn transformV2(allocator: std.mem.Allocator, source: *const vopr.trace.Trace) !vopr.trace.Trace {
        return runFromChoices(FixtureV2, allocator, source);
    }
    fn transformV2Changed(allocator: std.mem.Allocator, source: *const vopr.trace.Trace) !vopr.trace.Trace {
        return runFromChoices(FixtureV2Changed, allocator, source);
    }
};

fn newFixtureTrace(allocator: std.mem.Allocator) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(0xc0a7_0001);
    return vopr.runner.run(FixtureV1, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = 0xc0a7_0001,
        .transition_budget = 2,
        .source_revision = "upgrade-compatibility",
    });
}

fn minimalInventory(allocator: std.mem.Allocator) !external_types.Inventory {
    return .{
        .format = .iceberg,
        .source_id = try allocator.dupe(u8, "events"),
        .source_uri = try allocator.dupe(u8, "s3://vopr/events"),
        .snapshot_id = try allocator.dupe(u8, "snapshot-1"),
        .schema_fingerprint = try allocator.dupe(u8, "schema-1"),
        .files = try allocator.alloc(external_types.FileEntry, 0),
    };
}

fn minimalManifest() manifest_types.Manifest {
    return .{
        .namespace = "docs",
        .version = 7,
        .built_at_ns = 11,
        .wal_start_lsn = 3,
        .wal_end_lsn = 7,
        .stats = .{ .document_count = 2, .document_base_version = 7 },
        .artifacts = &.{},
    };
}

fn manifestV12FixtureAlloc(allocator: std.mem.Allocator) ![]u8 {
    const current = try manifest_codec.encodeAlloc(allocator, minimalManifest());
    defer allocator.free(current);
    // v13 inserted lineage_tracked:u8 + parent_version:u64 immediately after
    // the v12 fixed header. Keeping this explicit offset makes layout drift a
    // compatibility failure instead of silently regenerating a new fixture.
    const v12_header_len: usize = 201;
    const v13_lineage_len: usize = 9;
    if (current.len < v12_header_len + v13_lineage_len) return error.InvalidManifestFixture;
    const legacy = try allocator.alloc(u8, current.len - v13_lineage_len);
    @memcpy(legacy[0..v12_header_len], current[0..v12_header_len]);
    @memcpy(legacy[v12_header_len..], current[v12_header_len + v13_lineage_len ..]);
    std.mem.writeInt(u16, legacy[manifest_codec.wire_magic.len..][0..2], 12, .little);
    return legacy;
}

pub const Scenario = struct {
    pub const name: []const u8 = "upgrade-compatibility";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "safe-compatible-outcome");
    const complete_id = vopr.id.stable(name, "mode-completes");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".safe-compatible-outcome", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        storage_ha_v1_golden,
        data_dir_legacy_rejected,
        data_dir_future_rejected,
        data_dir_crash_then_forward,
        trace_v1_roundtrip,
        trace_future_format_rejected,
        trace_scenario_version_rejected,
        fixture_v1_to_v2_equivalent,
        fixture_semantic_change_rejected,
        checkpoint_restore,
        checkpoint_old_version_rejected,
        checkpoint_corruption_rejected,
        serverless_legacy_head_forward,
        serverless_future_head_rejected,
        serverless_inventory_v14_forward,
        serverless_inventory_future_rejected,
        serverless_manifest_v12_forward,
        serverless_manifest_future_rejected,
    };

    const mode_ids = idsForModes();
    const mode_names = namesForModes();

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
        sound: bool = true,
        progress: u64 = 0,
        complete: bool = false,

        fn run(self: *State, mode: Mode) !void {
            switch (mode) {
                .storage_ha_v1_golden => try storage_ha.compat.validateV1Fixtures(self.allocator),
                .data_dir_legacy_rejected => try self.legacyDataDirRejected(),
                .data_dir_future_rejected => try self.futureDataDirRejected(),
                .data_dir_crash_then_forward => try self.crashDataDirMigration(),
                .trace_v1_roundtrip => try self.traceRoundtrip(),
                .trace_future_format_rejected => try self.traceFutureRejected(),
                .trace_scenario_version_rejected => try self.traceScenarioRejected(),
                .fixture_v1_to_v2_equivalent => try self.fixtureMigrates(),
                .fixture_semantic_change_rejected => try self.fixtureChangeRejected(),
                .checkpoint_restore => try self.checkpointRestores(),
                .checkpoint_old_version_rejected => try self.checkpointVersionRejected(),
                .checkpoint_corruption_rejected => try self.checkpointCorruptionRejected(),
                .serverless_legacy_head_forward => try self.legacyHeadForwards(),
                .serverless_future_head_rejected => try self.futureHeadRejected(),
                .serverless_inventory_v14_forward => try self.inventoryV14Forwards(),
                .serverless_inventory_future_rejected => try self.inventoryFutureRejected(),
                .serverless_manifest_v12_forward => try self.manifestV12Forwards(),
                .serverless_manifest_future_rejected => try self.manifestFutureRejected(),
            }
            self.progress += 1;
            self.complete = true;
            try self.sim.ensureNoCapabilityViolation();
        }

        fn legacyDataDirRejected(self: *State) !void {
            try fs_paths.createDirPathPortable(self.sim.io(), "/upgrade/store/1/storage");
            data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade") catch |err| {
                self.sound = err == data_format.Error.IncompatibleAntflyDataDir and
                    (std.Io.Dir.cwd().access(self.sim.io(), "/upgrade/" ++ data_format.marker_file_name, .{}) catch null) == null;
                return;
            };
            self.sound = false;
        }

        fn futureDataDirRejected(self: *State) !void {
            try fs_paths.createDirPathPortable(self.sim.io(), "/upgrade");
            const future =
                \\{"product":"antfly","engine":"zig","storage_format":99,"min_reader_storage_format":99}
            ;
            try std.Io.Dir.cwd().writeFile(self.sim.io(), .{ .sub_path = "/upgrade/" ++ data_format.marker_file_name, .data = future });
            data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade") catch |err| {
                const after = try std.Io.Dir.cwd().readFileAlloc(self.sim.io(), "/upgrade/" ++ data_format.marker_file_name, self.allocator, .limited(1024));
                defer self.allocator.free(after);
                self.sound = err == data_format.Error.UnsupportedAntflyDataFormat and std.mem.eql(u8, future, after);
                return;
            };
            self.sound = false;
        }

        fn crashDataDirMigration(self: *State) !void {
            self.sim.failNextRename();
            if (data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade")) |_| {
                self.sound = false;
                return;
            } else |_| {}
            try self.sim.crashFileSystem();
            try data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade");
            try data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade");
        }

        fn traceRoundtrip(self: *State) !void {
            var source = try newFixtureTrace(self.allocator);
            defer source.deinit();
            const bytes = try source.renderAlloc(self.allocator);
            defer self.allocator.free(bytes);
            var parsed = try vopr.trace.parseAlloc(self.allocator, bytes);
            defer parsed.deinit();
            const canonical = try parsed.renderAlloc(self.allocator);
            defer self.allocator.free(canonical);
            self.sound = std.mem.eql(u8, bytes, canonical);
        }

        fn traceFutureRejected(self: *State) !void {
            var source = try newFixtureTrace(self.allocator);
            defer source.deinit();
            const bytes = try source.renderAlloc(self.allocator);
            defer self.allocator.free(bytes);
            const incompatible = try self.allocator.dupe(u8, bytes);
            defer self.allocator.free(incompatible);
            const marker = std.mem.indexOf(u8, incompatible, vopr.trace.format) orelse return error.MissingTraceFormat;
            incompatible[marker + vopr.trace.format.len - 1] = '9';
            if (vopr.trace.parseAlloc(self.allocator, incompatible)) |*unexpected| {
                unexpected.deinit();
                self.sound = false;
            } else |err| self.sound = err == error.IncompatibleTrace;
        }

        fn traceScenarioRejected(self: *State) !void {
            var source = try newFixtureTrace(self.allocator);
            defer source.deinit();
            source.header.scenario_version = 99;
            if (vopr.replay.exact(FixtureV1, self.allocator, &source)) |*unexpected| {
                unexpected.deinit();
                self.sound = false;
            } else |err| self.sound = err == error.IncompatibleScenarioVersion;
        }

        fn fixtureMigrates(self: *State) !void {
            var source = try newFixtureTrace(self.allocator);
            defer source.deinit();
            var migration = try vopr.fixture.migrate(
                self.allocator,
                &source,
                FixtureMigration.replayV1,
                FixtureMigration.transformV2,
                FixtureMigration.replayV2,
                .{},
            );
            defer migration.deinit();
            self.sound = migration.report.source_scenario_version == 1 and
                migration.report.migrated_scenario_version == 2 and
                migration.report.source_outcome_digest == migration.report.migrated_outcome_digest;
        }

        fn fixtureChangeRejected(self: *State) !void {
            var source = try newFixtureTrace(self.allocator);
            defer source.deinit();
            if (vopr.fixture.migrate(
                self.allocator,
                &source,
                FixtureMigration.replayV1,
                FixtureMigration.transformV2Changed,
                FixtureMigration.replayV2Changed,
                .{},
            )) |*unexpected| {
                unexpected.deinit();
                self.sound = false;
            } else |err| self.sound = err == error.FixtureMigrationFinalObservationChanged;
        }

        fn checkpointRestores(self: *State) !void {
            var world = FixtureV1.World{ .value = 2 };
            var checkpoint = try vopr.snapshot.capture(FixtureV1, self.allocator, &world, 2, 17);
            defer checkpoint.deinit(self.allocator);
            world.value = 0;
            try vopr.snapshot.restore(FixtureV1, &world, checkpoint, self.allocator);
            self.sound = world.value == 2;
        }

        fn checkpointVersionRejected(self: *State) !void {
            var world = FixtureV1.World{ .value = 2 };
            var checkpoint = try vopr.snapshot.capture(FixtureV1, self.allocator, &world, 2, 17);
            defer checkpoint.deinit(self.allocator);
            checkpoint.scenario_version = 0;
            if (vopr.snapshot.restore(FixtureV1, &world, checkpoint, self.allocator)) {
                self.sound = false;
            } else |err| self.sound = err == error.IncompatibleLogicalSnapshot;
        }

        fn checkpointCorruptionRejected(self: *State) !void {
            var world = FixtureV1.World{ .value = 2 };
            var checkpoint = try vopr.snapshot.capture(FixtureV1, self.allocator, &world, 2, 17);
            defer checkpoint.deinit(self.allocator);
            checkpoint.bytes[0] ^= 1;
            if (vopr.snapshot.restore(FixtureV1, &world, checkpoint, self.allocator)) {
                self.sound = false;
            } else |err| self.sound = err == error.CorruptLogicalSnapshot;
        }

        fn legacyHeadForwards(self: *State) !void {
            const legacy = head_coordination.Record.fromLegacyHead(7);
            const payload = try head_coordination.payloadAlloc(self.allocator, legacy);
            defer self.allocator.free(payload);
            const parsed_head = try std.fmt.parseInt(u64, std.mem.trim(u8, payload, " \t\r\n"), 10);
            const content_type = try head_coordination.contentTypeAlloc(self.allocator, legacy);
            defer self.allocator.free(content_type);
            var parsed = (try head_coordination.parseContentTypeAlloc(self.allocator, content_type)).?;
            defer parsed.deinit();
            self.sound = parsed_head == 7 and head_coordination.hasPayloadFingerprint(payload) and head_coordination.valid(parsed.value);
        }

        fn futureHeadRejected(self: *State) !void {
            const future = head_coordination.Record{ .format_version = head_coordination.format_version + 1 };
            const content_type = try head_coordination.contentTypeAlloc(self.allocator, future);
            defer self.allocator.free(content_type);
            var parsed = (try head_coordination.parseContentTypeAlloc(self.allocator, content_type)).?;
            defer parsed.deinit();
            self.sound = !head_coordination.valid(parsed.value);
        }

        fn inventoryV14Forwards(self: *State) !void {
            var inventory = try minimalInventory(self.allocator);
            defer inventory.deinit(self.allocator);
            const current = try external_codec.encodeAlloc(self.allocator, inventory);
            defer self.allocator.free(current);
            const legacy = try self.allocator.dupe(u8, current[0 .. current.len - @sizeOf(u32)]);
            defer self.allocator.free(legacy);
            std.mem.writeInt(u32, legacy[4..8], 14, .little);
            var decoded = try external_codec.decodeAlloc(self.allocator, legacy);
            defer decoded.deinit(self.allocator);
            self.sound = decoded.files.len == 0 and decoded.deleted_row_groups.len == 0 and std.mem.eql(u8, decoded.snapshot_id, "snapshot-1");
        }

        fn inventoryFutureRejected(self: *State) !void {
            var inventory = try minimalInventory(self.allocator);
            defer inventory.deinit(self.allocator);
            const encoded = try external_codec.encodeAlloc(self.allocator, inventory);
            defer self.allocator.free(encoded);
            std.mem.writeInt(u32, encoded[4..8], 99, .little);
            if (external_codec.decodeAlloc(self.allocator, encoded)) |*unexpected| {
                unexpected.deinit(self.allocator);
                self.sound = false;
            } else |err| self.sound = err == error.UnsupportedExternalSourceInventoryVersion;
        }

        fn manifestV12Forwards(self: *State) !void {
            const legacy = try manifestV12FixtureAlloc(self.allocator);
            defer self.allocator.free(legacy);
            var decoded = try manifest_codec.decodeAlloc(self.allocator, legacy);
            defer decoded.deinit(self.allocator);
            self.sound = decoded.version == 7 and
                decoded.stats.document_base_version == 7 and
                !decoded.publication_lineage_tracked and
                decoded.publication_parent_version == null;
        }

        fn manifestFutureRejected(self: *State) !void {
            const encoded = try manifest_codec.encodeAlloc(self.allocator, minimalManifest());
            defer self.allocator.free(encoded);
            std.mem.writeInt(u16, encoded[manifest_codec.wire_magic.len..][0..2], 99, .little);
            if (manifest_codec.decodeAlloc(self.allocator, encoded)) |*unexpected| {
                unexpected.deinit(self.allocator);
                self.sound = false;
            } else |err| self.sound = err == error.UnsupportedManifestVersion;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0xc0a7_2026,
                .required = .of(&.{ .files, .clock_read, .deterministic_entropy }),
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
            .kind = switch (mode) {
                .storage_ha_v1_golden, .trace_v1_roundtrip, .fixture_v1_to_v2_equivalent, .checkpoint_restore, .serverless_legacy_head_forward, .serverless_inventory_v14_forward, .serverless_manifest_v12_forward => .maintenance,
                else => .fault,
            },
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidUpgradeCompatibilityTransition;
        try events.emitNamed(allocator, .domain, selected.name, world.state.progress);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".sound", @intFromBool(world.state.sound));
        try builder.addNamed(allocator, name ++ ".progress", world.state.progress);
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, sound_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        return world.state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = world.state.progress,
            .cleanup_complete = world.state.complete,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "upgrade compatibility VOPR exact replays formats migrations and rejection" {
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
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.harness_errors);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
