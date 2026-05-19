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
};

pub fn snapshot() Stats {
    if (builtin.os.tag != .macos) return .{};

    var info: darwin.rusage_info_current = std.mem.zeroes(darwin.rusage_info_current);
    const rc = darwin.proc_pid_rusage(darwin.getpid(), darwin.RUSAGE_INFO_CURRENT, @ptrCast(&info));
    if (rc != 0) return .{};

    return .{
        .available = true,
        .resident_bytes = info.ri_resident_size,
        .footprint_bytes = info.ri_phys_footprint,
        .wired_bytes = info.ri_wired_size,
        .pageins = info.ri_pageins,
    };
}

const darwin = if (builtin.os.tag == .macos) struct {
    const darwin_c_int = i32;

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

    extern "c" fn proc_pid_rusage(pid: darwin_c_int, flavor: darwin_c_int, buffer: *rusage_info_current) darwin_c_int;
    extern "c" fn getpid() darwin_c_int;
} else struct {};
