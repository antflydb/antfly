// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Read-indexed predecessor expectations for a generation publication plan.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const topology = local.storage_db_relational_integrity_topology_contract;

pub const Preview = struct { fence: topology.Fence, handoff: stages.GenerationHandoffRange };

pub const Source = struct {
    ptr: *const anyopaque,
    get: *const fn (*const anyopaque, std.mem.Allocator, stages.Id, []const u8, local.common_topology_records.RangeRecord, u64) anyerror!Preview,

    pub fn preview(self: Source, a: std.mem.Allocator, id: stages.Id, table_name: []const u8, old: local.common_topology_records.RangeRecord, group: u64) !Preview {
        return self.get(self.ptr, a, id, table_name, old, group);
    }
};

pub const Routed = struct {
    reads: local.api_table_read_source.TableReadSource,
    context: local.api_operation.RequestContext,

    pub fn source(self: *const Routed) Source {
        return .{ .ptr = self, .get = previewRouted };
    }

    fn previewRouted(ptr: *const anyopaque, a: std.mem.Allocator, id: stages.Id, table_name: []const u8, old: local.common_topology_records.RangeRecord, group: u64) !Preview {
        const self: *const Routed = @ptrCast(@alignCast(ptr));
        return self.preview(a, id, table_name, old, group);
    }

    pub fn preview(self: Routed, a: std.mem.Allocator, id: stages.Id, table_name: []const u8, old: local.common_topology_records.RangeRecord, group: u64) !Preview {
        try self.context.ensureActive();
        var response = (try self.reads.lookup(a, table_name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns, .cancellation = self.context.cancellation }, .read_index)) orelse return error.TableNotFound;
        defer response.deinit(a);
        const Native = topology.Identity;
        const native = try std.json.parseFromSliceLeaky(Native, a, response.json, .{ .ignore_unknown_fields = true });
        if (native.namespace.table_id != old.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(old) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(old)) return error.TableGenerationChanged;
        try native.requireGenerationHandoffReceipts();
        const fence: topology.Fence = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = std.mem.readInt(u64, id[8..16], .little), .admission_epoch = native.next_epoch, .owner_group_id = old.group_id, .peer_group_id = group, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
        // A linearizable preview becomes an immutable Plan assertion, not
        // authority by itself. The coordinator will fence this source and
        // compare a second read-indexed summary before irreversible
        // cutover, catching any intervening FK admission or retirement.
        var summary_response = (try self.reads.lookup(a, table_name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"generation_handoff_summary\"}", .execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns, .cancellation = self.context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
        defer summary_response.deinit(a);
        const summary = try std.json.parseFromSliceLeaky(local.storage_db_empty_generation_handoff.Summary, a, summary_response.json, .{ .allocate = .alloc_always });
        if (!summary.namespace.eql(native.namespace) or summary.intent != null or summary.seal != null) return error.TableGenerationChanged;
        return .{ .fence = fence, .handoff = .{
            .source_group_id = old.group_id,
            .target_group_id = group,
            .source_namespace = summary.namespace,
            .admissions = summary.admissions,
            .admissions_digest = summary.admissions_digest,
            .retired_digest = summary.retired_digest,
            .retired_count = summary.retired_count,
        } };
    }
};
