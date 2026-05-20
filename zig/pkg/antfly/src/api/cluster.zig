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
const common_secrets = @import("../common/secrets.zig");
const metadata_api = @import("../metadata/api.zig");

pub const ClusterHealth = enum {
    healthy,
    degraded,
    @"error",
};

pub const ClusterStatus = struct {
    health: ClusterHealth,
    message: ?[]u8 = null,
    auth_enabled: bool = false,
    swarm_mode: bool = false,
    secret_store: ?SecretStoreStatus = null,

    pub fn deinit(self: *ClusterStatus, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        self.* = undefined;
    }
};

pub const SecretStoreStatus = struct {
    stale: bool = false,
};

pub fn fromMetadataStatus(alloc: std.mem.Allocator, status: metadata_api.MetadataStatus) !ClusterStatus {
    if (status.projected_stores == 0 and (status.projected_ranges > 0 or status.projected_tables > 0 or status.projected_placement_intents > 0)) {
        return .{
            .health = .@"error",
            .message = try std.fmt.allocPrint(alloc, "metadata tracks {d} tables and {d} ranges but no stores", .{
                status.projected_tables,
                status.projected_ranges,
            }),
        };
    }
    if (status.repair_placement_groups > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} placement groups require repair", .{status.repair_placement_groups}),
        };
    }
    if (status.excluded_stores > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} stores are excluded from placement", .{status.excluded_stores}),
        };
    }
    if (status.overloaded_stores > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} stores are overloaded", .{status.overloaded_stores}),
        };
    }
    return .{
        .health = .healthy,
        .message = if (status.rebalance_placement_groups > 0)
            try std.fmt.allocPrint(alloc, "{d} placement groups are rebalancing", .{status.rebalance_placement_groups})
        else
            null,
    };
}

pub fn applySecretStoreHealth(status: *ClusterStatus, health: common_secrets.ReloadHealth) void {
    status.secret_store = .{
        .stale = health.stale_snapshot,
    };
}

test "cluster status derives degraded and error states from metadata status" {
    var error_status = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_tables = 1,
        .projected_ranges = 1,
        .projected_stores = 0,
        .projected_placement_intents = 1,
    });
    defer error_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.@"error", error_status.health);

    var degraded = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .repair_placement_groups = 2,
    });
    defer degraded.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.degraded, degraded.health);

    var healthy = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .rebalance_placement_groups = 1,
    });
    defer healthy.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.healthy, healthy.health);
}

test "cluster status carries non-secret secret store health" {
    var status = ClusterStatus{ .health = .healthy };
    applySecretStoreHealth(&status, .{
        .generation = 7,
        .entry_count = 3,
        .last_reload_failed = true,
        .stale_snapshot = true,
        .reload_successes = 2,
        .reload_failures = 1,
        .last_success_ns = 123,
        .last_failure_ns = 456,
    });
    const secret_store = status.secret_store orelse return error.TestUnexpectedResult;
    try std.testing.expect(secret_store.stale);
}
