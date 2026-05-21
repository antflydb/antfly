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

    pub fn deinit(self: *ClusterStatus, alloc: std.mem.Allocator) void {
        if (self.message) |message| alloc.free(message);
        self.* = undefined;
    }
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
    if (status.projected_doc_identity_lifecycle_rebuild_required > 0) {
        return .{
            .health = .degraded,
            .message = try std.fmt.allocPrint(alloc, "{d} ranges require document identity rebuild", .{status.projected_doc_identity_lifecycle_rebuild_required}),
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
        .message = if (status.projected_doc_identity_lifecycle_reassigning > 0)
            try std.fmt.allocPrint(alloc, "{d} ranges are reassigning document identity", .{status.projected_doc_identity_lifecycle_reassigning})
        else if (status.rebalance_placement_groups > 0)
            try std.fmt.allocPrint(alloc, "{d} placement groups are rebalancing", .{status.rebalance_placement_groups})
        else
            null,
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

    var rebuild_required = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .projected_doc_identity_lifecycle_rebuild_required = 1,
    });
    defer rebuild_required.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.degraded, rebuild_required.health);

    var reassigning = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .projected_doc_identity_lifecycle_reassigning = 1,
    });
    defer reassigning.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.healthy, reassigning.health);

    var healthy = try fromMetadataStatus(std.testing.allocator, .{
        .metadata_group_id = 1,
        .metrics = .{},
        .projected_stores = 3,
        .rebalance_placement_groups = 1,
    });
    defer healthy.deinit(std.testing.allocator);
    try std.testing.expectEqual(ClusterHealth.healthy, healthy.health);
}
