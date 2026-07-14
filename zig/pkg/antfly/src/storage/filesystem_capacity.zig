// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

const std = @import("std");
const platform = @import("antfly_platform");
const resource_manager = @import("resource_manager.zig");

pub const supported = platform.filesystem.capacity_supported;

/// A cheap, live capacity source for a storage root. The ResourceManager owns
/// admission policy and reservations; this storage-owned probe only supplies
/// physical filesystem facts.
pub const Probe = struct {
    path: []const u8,
    domain_id: resource_manager.CapacityDomainId,

    pub fn init(path: []const u8, domain_id: resource_manager.CapacityDomainId) Probe {
        return .{ .path = path, .domain_id = domain_id };
    }

    pub fn source(self: *Probe) resource_manager.CapacitySource {
        return .{
            .ptr = self,
            .domain_id = self.domain_id,
            .observe = observeOpaque,
        };
    }

    pub fn observe(self: *const Probe) !resource_manager.CapacityObservation {
        const observed = try platform.filesystem.capacity(self.path);
        return .{
            .available_bytes = observed.available_bytes,
            .capacity_bytes = observed.total_bytes,
            .observed_at_ns = platform.time.monotonicNs(),
            // Every repair boundary refreshes synchronously. The TTL prevents
            // accidentally retaining this observation in a future caller.
            .valid_for_ns = 5 * std.time.ns_per_s,
        };
    }

    fn observeOpaque(ptr: *anyopaque) anyerror!resource_manager.CapacityObservation {
        const self: *Probe = @ptrCast(@alignCast(ptr));
        return try self.observe();
    }
};

test "filesystem capacity probe reports the test volume" {
    if (!supported) return error.SkipZigTest;
    var probe = Probe.init(".", 1);
    const observation = try probe.observe();
    try std.testing.expect(observation.capacity_bytes.? > 0);
    try std.testing.expect(observation.available_bytes.? <= observation.capacity_bytes.?);
    try std.testing.expect(!observation.stale(platform.time.monotonicNs()));
}
