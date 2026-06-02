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
const builtin = @import("builtin");

pub const Stats = struct {
    available: bool = false,
    resident_bytes: u64 = 0,
    footprint_bytes: u64 = 0,
    wired_bytes: u64 = 0,
    pageins: u64 = 0,
    malloc_available: bool = false,
    malloc_allocated_bytes: u64 = 0,
    malloc_zone_bytes: u64 = 0,
};

pub fn snapshot() Stats {
    if (builtin.os.tag != .macos) return .{};

    var info: darwin.rusage_info_current = std.mem.zeroes(darwin.rusage_info_current);
    const rc = darwin.proc_pid_rusage(darwin.getpid(), darwin.RUSAGE_INFO_CURRENT, @ptrCast(&info));
    if (rc != 0) return .{};

    var stats = Stats{
        .available = true,
        .resident_bytes = info.ri_resident_size,
        .footprint_bytes = info.ri_phys_footprint,
        .wired_bytes = info.ri_wired_size,
        .pageins = info.ri_pageins,
    };
    const malloc_stats = darwin.mallocStats();
    stats.malloc_available = malloc_stats.available;
    stats.malloc_allocated_bytes = malloc_stats.allocated_bytes;
    stats.malloc_zone_bytes = malloc_stats.zone_bytes;
    return stats;
}

const darwin = if (builtin.os.tag == .macos) struct {
    const darwin_c_int = i32;
    const mach_port_t = u32;
    const kern_return_t = i32;
    const vm_address_t = usize;
    const vm_size_t = usize;
    const memory_reader_t = ?*const fn (mach_port_t, vm_address_t, vm_size_t, *?*anyopaque) callconv(.c) kern_return_t;

    pub const RUSAGE_INFO_CURRENT: darwin_c_int = 6;

    pub const rusage_info_current = extern struct {
        ri_uuid: [16]u8,
        ri_user_time: u64,
        ri_system_time: u64,
        ri_pkg_idle_wkups: u64,
        ri_interrupt_wkups: u64,
        ri_pageins: u64,
        ri_wired_size: u64,
        ri_resident_size: u64,
        ri_phys_footprint: u64,
        ri_proc_start_abstime: u64,
        ri_proc_exit_abstime: u64,
        ri_child_user_time: u64,
        ri_child_system_time: u64,
        ri_child_pkg_idle_wkups: u64,
        ri_child_interrupt_wkups: u64,
        ri_child_pageins: u64,
        ri_child_elapsed_abstime: u64,
        ri_diskio_bytesread: u64,
        ri_diskio_byteswritten: u64,
        ri_cpu_time_qos_default: u64,
        ri_cpu_time_qos_maintenance: u64,
        ri_cpu_time_qos_background: u64,
        ri_cpu_time_qos_utility: u64,
        ri_cpu_time_qos_legacy: u64,
        ri_cpu_time_qos_user_initiated: u64,
        ri_cpu_time_qos_user_interactive: u64,
        ri_billed_system_time: u64,
        ri_serviced_system_time: u64,
        ri_logical_writes: u64,
        ri_lifetime_max_phys_footprint: u64,
        ri_instructions: u64,
        ri_cycles: u64,
        ri_billed_energy: u64,
        ri_serviced_energy: u64,
        ri_interval_max_phys_footprint: u64,
        ri_runnable_time: u64,
        ri_flags: u64,
        ri_user_ptime: u64,
        ri_system_ptime: u64,
        ri_pinstructions: u64,
        ri_pcycles: u64,
        ri_energy_nj: u64,
        ri_penergy_nj: u64,
        ri_secure_time_in_system: u64,
        ri_secure_ptime_in_system: u64,
        ri_reserved: [12]u64,
    };

    const malloc_statistics_t = extern struct {
        blocks_in_use: c_uint,
        size_in_use: usize,
        max_size_in_use: usize,
        size_allocated: usize,
    };

    const MallocStats = struct {
        available: bool = false,
        allocated_bytes: u64 = 0,
        zone_bytes: u64 = 0,
    };

    fn mallocStats() MallocStats {
        var zones: [*]vm_address_t = undefined;
        var zone_count: c_uint = 0;
        if (malloc_get_all_zones(mach_task_self_, null, &zones, &zone_count) != 0) {
            return mallocStatsForDefaultZone();
        }

        var out: MallocStats = .{ .available = true };
        for (zones[0..zone_count]) |zone_addr| {
            var zone_stats: malloc_statistics_t = std.mem.zeroes(malloc_statistics_t);
            const zone: *anyopaque = @ptrFromInt(zone_addr);
            malloc_zone_statistics(zone, &zone_stats);
            out.allocated_bytes +|= @intCast(zone_stats.size_in_use);
            out.zone_bytes +|= @intCast(zone_stats.size_allocated);
        }
        return out;
    }

    fn mallocStatsForDefaultZone() MallocStats {
        const zone = malloc_default_zone() orelse return .{};
        var zone_stats: malloc_statistics_t = std.mem.zeroes(malloc_statistics_t);
        malloc_zone_statistics(zone, &zone_stats);
        return .{
            .available = true,
            .allocated_bytes = @intCast(zone_stats.size_in_use),
            .zone_bytes = @intCast(zone_stats.size_allocated),
        };
    }

    extern "c" fn proc_pid_rusage(pid: darwin_c_int, flavor: darwin_c_int, buffer: *rusage_info_current) darwin_c_int;
    extern "c" fn getpid() darwin_c_int;
    extern "c" var mach_task_self_: mach_port_t;
    extern "c" fn malloc_get_all_zones(task: mach_port_t, reader: memory_reader_t, addresses: *[*]vm_address_t, count: *c_uint) kern_return_t;
    extern "c" fn malloc_default_zone() ?*anyopaque;
    extern "c" fn malloc_zone_statistics(zone: *anyopaque, stats: *malloc_statistics_t) void;
} else struct {};
