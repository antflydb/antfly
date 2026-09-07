// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Differential product-format compatibility histories. The suite opens real
//! golden storage bytes and serverless artifacts and crashes the production
//! atomic data-directory admission path before retrying from durable state.
//! VOPR's own artifacts are deliberately forward-only and do not belong here.

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
    pub const version: u32 = 2;

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
            self.sim.failNextFileRename();
            if (data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade")) |_| {
                self.sound = false;
                return;
            } else |_| {}
            try self.sim.crashFileSystem();
            try data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade");
            try data_format.ensureCompatible(self.allocator, self.sim.io(), "/upgrade");
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
            if (external_codec.decodeAlloc(self.allocator, encoded)) |unexpected_value| {
                var unexpected = unexpected_value;
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
            if (manifest_codec.decodeAlloc(self.allocator, encoded)) |unexpected_value| {
                var unexpected = unexpected_value;
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
                .storage_ha_v1_golden, .serverless_legacy_head_forward, .serverless_inventory_v14_forward, .serverless_manifest_v12_forward => .maintenance,
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
        try builder.addNamed(allocator, name ++ ".progress", @intCast(world.state.progress));
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

test "upgrade compatibility VOPR exact replays product formats and rejection" {
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
