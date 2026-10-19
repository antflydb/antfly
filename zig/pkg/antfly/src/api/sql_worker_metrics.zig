// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Node SQL executor ownership, distinct from runnable CPU admission.
const std = @import("std");
const Runtime = @import("antfly_local_sources").storage_background_runtime.BackendRuntime;

pub fn append(writer: *std.Io.Writer, runtime: *const Runtime) !void {
    if (runtime.runtimeMemoryStats()) |memory| {
        inline for (.{
            .{ "memory_reserved_bytes", "gauge", "Prepaid control executor metadata and guarded stack ceiling", memory.control_reserved_bytes },
            .{ "heap_bytes", "gauge", "Live control executor heap bytes", memory.control_heap.used },
            .{ "heap_peak_bytes", "gauge", "Peak control executor heap bytes", memory.control_heap.peak },
            .{ "heap_rejections_total", "counter", "Control executor heap allocation refusals", memory.control_heap.rejected },
            .{ "stack_mapping_bytes", "gauge", "Live guarded control worker mappings", memory.control_stacks.used },
            .{ "stack_peak_bytes", "gauge", "Peak guarded control worker mappings", memory.control_stacks.peak },
            .{ "stack_rejections_total", "counter", "Control worker mapping allocation refusals", memory.control_stacks.rejected },
        }) |metric| {
            const name = "antfly_executor_control_" ++ metric[0];
            try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n{s} {d}\n", .{ name, metric[2], name, metric[1], name, metric[3] });
        }
        inline for (.{ .{ "inbound", memory.raft_inbound }, .{ "outbound", memory.raft_outbound } }) |lane| {
            inline for (.{
                .{ "memory_reserved_bytes", "gauge", "Prepaid Raft executor heap and guarded stack ceiling", lane[1].reserved_bytes },
                .{ "heap_bytes", "gauge", "Live Raft executor heap bytes", lane[1].heap.used },
                .{ "heap_peak_bytes", "gauge", "Peak Raft executor heap bytes", lane[1].heap.peak },
                .{ "heap_rejections_total", "counter", "Raft executor heap allocation refusals", lane[1].heap.rejected },
                .{ "stack_mapping_bytes", "gauge", "Live guarded Raft worker mappings", lane[1].stacks.used },
                .{ "stack_peak_bytes", "gauge", "Peak guarded Raft worker mappings", lane[1].stacks.peak },
                .{ "stack_rejections_total", "counter", "Raft worker mapping allocation refusals", lane[1].stacks.rejected },
            }) |metric| {
                const name = "antfly_executor_raft_" ++ lane[0] ++ "_" ++ metric[0];
                try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n{s} {d}\n", .{ name, metric[2], name, metric[1], name, metric[3] });
            }
        }
    }
    const roles = [_]Runtime.SharedSqlRole{ .execution, .control, .cleanup, .bounded_read };
    var snapshots: [roles.len]Runtime.SharedSqlStats = undefined;
    for (roles, 0..) |role, i| snapshots[i] = runtime.sharedSqlStats(role);
    inline for (.{
        .{ "capacity", "gauge", "Configured maximum SQL executor slots for this role" },
        .{ "retained_capacity", "gauge", "SQL executor slots retained through physical retirement" },
        .{ "borrowers", "gauge", "Consumers retaining the shared SQL executor" },
        .{ "peak_borrowers", "gauge", "Peak consumers retaining the shared SQL executor" },
        .{ "retiring", "gauge", "Whether the SQL executor is physically retiring" },
        .{ "acquisitions_total", "counter", "Successful shared SQL executor lease acquisitions" },
        .{ "rejections_total", "counter", "Rejected shared SQL executor lease acquisitions" },
    }) |metric| {
        const name = "antfly_executor_sql_" ++ metric[0];
        try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n", .{ name, metric[2], name, metric[1] });
        for (roles, snapshots) |role, stats| {
            const value = if (comptime std.mem.eql(u8, metric[0], "retiring")) @intFromBool(stats.retiring) else @field(stats, metric[0]);
            try writer.print("{s}{{role=\"{s}\"}} {d}\n", .{ name, @tagName(role), value });
        }
    }
}
