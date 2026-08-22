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
    cpu_available: bool = false,
    user_cpu_ns: u64 = 0,
    system_cpu_ns: u64 = 0,
    available: bool = false,
    resident_bytes: u64 = 0,
    peak_resident_bytes: u64 = 0,
    anonymous_bytes: u64 = 0,
    private_dirty_bytes: u64 = 0,
    footprint_bytes: u64 = 0,
    peak_footprint_bytes: u64 = 0,
    wired_bytes: u64 = 0,
    pageins: u64 = 0,
    malloc_available: bool = false,
    malloc_allocated_bytes: u64 = 0,
    malloc_zone_bytes: u64 = 0,
};

pub const EnvelopeSource = enum {
    cgroup_v2,
    cgroup_v1,
    host,
    unavailable,
};

pub const Envelope = struct {
    limit_bytes: u64 = 0,
    source: EnvelopeSource = .unavailable,
};

const CgroupVersion = enum { v1, v2 };

const CgroupPaths = struct {
    v2: ?[]const u8 = null,
    v1_memory: ?[]const u8 = null,
};

const CgroupMount = struct {
    root_storage: [std.fs.max_path_bytes]u8 = undefined,
    root_len: usize = 0,
    mount_point_storage: [std.fs.max_path_bytes]u8 = undefined,
    mount_point_len: usize = 0,

    fn root(self: *const CgroupMount) []const u8 {
        return self.root_storage[0..self.root_len];
    }

    fn mountPoint(self: *const CgroupMount) []const u8 {
        return self.mount_point_storage[0..self.mount_point_len];
    }

    fn valid(self: *const CgroupMount) bool {
        return self.root_len != 0 and self.mount_point_len != 0;
    }
};

const CgroupMounts = struct {
    v2: CgroupMount = .{},
    v1_memory: CgroupMount = .{},
};

/// Resolve the finite physical envelope visible to this process. Linux walks
/// the process's actual cgroup leaf and every ancestor, including noncanonical
/// and subtree mounts; a host fallback is used only when no finite controller
/// limit is visible.
pub fn systemEnvelope() Envelope {
    if (builtin.os.tag == .linux) {
        if (linuxCgroupEnvelope()) |envelope| return envelope;
    }
    const total = std.process.totalSystemMemory() catch return .{};
    if (total == 0) return .{};
    return .{ .limit_bytes = total, .source = .host };
}

fn linuxCgroupEnvelope() ?Envelope {
    var cgroup_buffer: [4096]u8 = undefined;
    const cgroup_bytes = readSmallLinuxFile("/proc/self/cgroup", &cgroup_buffer) orelse
        return null;
    const paths = parseCgroupPaths(cgroup_bytes);

    if (paths.v2) |path| {
        const probe = readCgroupHierarchyLimit(
            "/sys/fs/cgroup",
            "/",
            path,
            "memory.max",
            .v2,
        );
        if (probe.leaf_present) {
            if (probe.limit_bytes) |limit|
                return .{ .limit_bytes = limit, .source = .cgroup_v2 };
            return null;
        }
    }
    if (paths.v1_memory) |path| {
        const probe = readCgroupHierarchyLimit(
            "/sys/fs/cgroup/memory",
            "/",
            path,
            "memory.limit_in_bytes",
            .v1,
        );
        if (probe.leaf_present) {
            if (probe.limit_bytes) |limit|
                return .{ .limit_bytes = limit, .source = .cgroup_v1 };
            return null;
        }
    }
    return probeCgroupMountLimits(paths);
}

fn controllerListContains(controllers: []const u8, expected: []const u8) bool {
    var it = std.mem.splitScalar(u8, controllers, ',');
    while (it.next()) |controller| {
        if (std.mem.eql(u8, controller, expected)) return true;
    }
    return false;
}

fn isSafeAbsoluteCgroupPath(path: []const u8) bool {
    if (path.len == 0 or path[0] != '/' or std.mem.indexOfScalar(u8, path, 0) != null)
        return false;
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, ".."))
            return false;
    }
    return true;
}

fn parseCgroupPaths(bytes: []const u8) CgroupPaths {
    var result = CgroupPaths{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const first = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const second = std.mem.indexOfScalarPos(u8, line, first + 1, ':') orelse continue;
        const hierarchy = line[0..first];
        const controllers = line[first + 1 .. second];
        const path = line[second + 1 ..];
        if (!isSafeAbsoluteCgroupPath(path)) continue;
        if (std.mem.eql(u8, hierarchy, "0") and controllers.len == 0) {
            result.v2 = path;
        } else if (controllerListContains(controllers, "memory")) {
            result.v1_memory = path;
        }
    }
    return result;
}

const LinuxLimit = struct {
    present: bool = false,
    finite_bytes: ?u64 = null,
};

fn readLinuxLimit(path: []const u8, version: CgroupVersion) LinuxLimit {
    var buffer: [128]u8 = undefined;
    const bytes = readSmallLinuxFile(path, &buffer) orelse return .{};
    const raw = std.mem.trim(u8, bytes, " \t\r\n");
    if (raw.len == 0 or std.mem.eql(u8, raw, "max")) return .{ .present = true };
    const value = std.fmt.parseUnsigned(u64, raw, 10) catch return .{ .present = true };
    if (value == 0) return .{ .present = true };
    if (version == .v1 and value >= std.math.maxInt(u64) / 2)
        return .{ .present = true };
    return .{ .present = true, .finite_bytes = value };
}

fn decodeMountInfoPath(destination: []u8, encoded: []const u8) ?usize {
    if (encoded.len == 0 or encoded[0] != '/') return null;
    var source_index: usize = 0;
    var destination_index: usize = 0;
    while (source_index < encoded.len) {
        if (destination_index == destination.len) return null;
        if (encoded[source_index] == '\\' and source_index + 3 < encoded.len) {
            const digits = encoded[source_index + 1 .. source_index + 4];
            if (digits[0] >= '0' and digits[0] <= '7' and
                digits[1] >= '0' and digits[1] <= '7' and
                digits[2] >= '0' and digits[2] <= '7')
            {
                destination[destination_index] = @intCast(
                    @as(u16, digits[0] - '0') * 64 +
                        @as(u16, digits[1] - '0') * 8 +
                        @as(u16, digits[2] - '0'),
                );
                source_index += 4;
                destination_index += 1;
                continue;
            }
        }
        destination[destination_index] = encoded[source_index];
        source_index += 1;
        destination_index += 1;
    }
    const decoded = destination[0..destination_index];
    var components = std.mem.splitScalar(u8, decoded, '/');
    while (components.next()) |component| {
        if (std.mem.eql(u8, component, "..")) return null;
    }
    return destination_index;
}

fn parseCgroupMountInfoLine(line: []const u8, mounts: *CgroupMounts) void {
    const separator = std.mem.indexOf(u8, line, " - ") orelse return;
    var before = std.mem.tokenizeScalar(u8, line[0..separator], ' ');
    _ = before.next() orelse return;
    _ = before.next() orelse return;
    _ = before.next() orelse return;
    const encoded_root = before.next() orelse return;
    const encoded_mount_point = before.next() orelse return;

    var after = std.mem.tokenizeScalar(u8, line[separator + 3 ..], ' ');
    const filesystem_type = after.next() orelse return;
    _ = after.next() orelse return;
    const super_options = after.next() orelse "";
    const is_v2 = std.mem.eql(u8, filesystem_type, "cgroup2");
    const is_v1_memory = std.mem.eql(u8, filesystem_type, "cgroup") and
        controllerListContains(super_options, "memory");
    if (!is_v2 and !is_v1_memory) return;

    const mount = if (is_v2) &mounts.v2 else &mounts.v1_memory;
    mount.root_len = decodeMountInfoPath(&mount.root_storage, encoded_root) orelse return;
    mount.mount_point_len = decodeMountInfoPath(
        &mount.mount_point_storage,
        encoded_mount_point,
    ) orelse return;
}

fn cgroupPathRelativeToMount(process_path: []const u8, mount_root: []const u8) ?[]const u8 {
    if (!isSafeAbsoluteCgroupPath(process_path) or !isSafeAbsoluteCgroupPath(mount_root))
        return null;
    if (std.mem.eql(u8, process_path, "/")) return process_path;
    if (std.mem.eql(u8, mount_root, "/")) return process_path;
    if (std.mem.eql(u8, process_path, mount_root)) return "/";
    if (std.mem.startsWith(u8, process_path, mount_root) and
        process_path.len > mount_root.len and process_path[mount_root.len] == '/')
        return process_path[mount_root.len..];
    return process_path;
}

fn cgroupDirectoryPath(buffer: []u8, mount_point: []const u8, relative: []const u8) ?[]u8 {
    if (!isSafeAbsoluteCgroupPath(mount_point) or !isSafeAbsoluteCgroupPath(relative))
        return null;
    const trimmed_mount = std.mem.trimEnd(u8, mount_point, "/");
    if (trimmed_mount.len == 0) return std.fmt.bufPrint(buffer, "{s}", .{relative}) catch null;
    return if (std.mem.eql(u8, relative, "/"))
        std.fmt.bufPrint(buffer, "{s}", .{trimmed_mount}) catch null
    else
        std.fmt.bufPrint(buffer, "{s}{s}", .{ trimmed_mount, relative }) catch null;
}

const CgroupLimitProbe = struct {
    limit_bytes: ?u64 = null,
    leaf_present: bool = false,
};

fn readCgroupHierarchyLimit(
    mount_point: []const u8,
    mount_root: []const u8,
    process_path: []const u8,
    limit_filename: []const u8,
    version: CgroupVersion,
) CgroupLimitProbe {
    const relative = cgroupPathRelativeToMount(process_path, mount_root) orelse return .{};
    var directory_buffer: [std.fs.max_path_bytes]u8 = undefined;
    var directory = cgroupDirectoryPath(&directory_buffer, mount_point, relative) orelse return .{};
    const leaf_len = directory.len;
    const hierarchy_root_len = @max(@as(usize, 1), std.mem.trimEnd(u8, mount_point, "/").len);
    var result = CgroupLimitProbe{};
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    while (directory.len >= hierarchy_root_len) {
        const path = std.fmt.bufPrint(&path_buffer, "{s}/{s}", .{ directory, limit_filename }) catch break;
        const limit = readLinuxLimit(path, version);
        if (directory.len == leaf_len) result.leaf_present = limit.present;
        if (limit.finite_bytes) |value| {
            result.limit_bytes = if (result.limit_bytes) |existing| @min(existing, value) else value;
        }
        if (directory.len == hierarchy_root_len) break;
        const parent = std.fs.path.dirname(directory) orelse break;
        if (parent.len < hierarchy_root_len) break;
        directory = directory_buffer[0..parent.len];
    }
    return result;
}

fn probeCgroupMountInfoLine(line: []const u8, paths: CgroupPaths, best: *?Envelope) void {
    var mounts = CgroupMounts{};
    parseCgroupMountInfoLine(line, &mounts);
    if (paths.v2) |path| {
        if (mounts.v2.valid()) {
            const probe = readCgroupHierarchyLimit(
                mounts.v2.mountPoint(),
                mounts.v2.root(),
                path,
                "memory.max",
                .v2,
            );
            if (probe.leaf_present) if (probe.limit_bytes) |limit| {
                if (best.* == null or limit < best.*.?.limit_bytes)
                    best.* = .{ .limit_bytes = limit, .source = .cgroup_v2 };
            };
        }
    }
    if (paths.v1_memory) |path| {
        if (mounts.v1_memory.valid()) {
            const probe = readCgroupHierarchyLimit(
                mounts.v1_memory.mountPoint(),
                mounts.v1_memory.root(),
                path,
                "memory.limit_in_bytes",
                .v1,
            );
            if (probe.leaf_present) if (probe.limit_bytes) |limit| {
                if (best.* == null or limit < best.*.?.limit_bytes)
                    best.* = .{ .limit_bytes = limit, .source = .cgroup_v1 };
            };
        }
    }
}

fn probeCgroupMountLimits(paths: CgroupPaths) ?Envelope {
    if (builtin.os.tag != .linux) return null;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        "/proc/self/mountinfo",
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return null;
    defer _ = std.posix.system.close(fd);

    var best: ?Envelope = null;
    var read_buffer: [4096]u8 = undefined;
    var line_buffer: [8192]u8 = undefined;
    var line_len: usize = 0;
    var discard_line = false;
    while (true) {
        const count = std.posix.read(fd, &read_buffer) catch return best;
        if (count == 0) break;
        for (read_buffer[0..count]) |byte| {
            if (byte == '\n') {
                if (!discard_line and line_len != 0)
                    probeCgroupMountInfoLine(line_buffer[0..line_len], paths, &best);
                line_len = 0;
                discard_line = false;
            } else if (!discard_line) {
                if (line_len == line_buffer.len) {
                    line_len = 0;
                    discard_line = true;
                } else {
                    line_buffer[line_len] = byte;
                    line_len += 1;
                }
            }
        }
    }
    if (!discard_line and line_len != 0)
        probeCgroupMountInfoLine(line_buffer[0..line_len], paths, &best);
    return best;
}

/// Memory that contributes to process pressure rather than clean file-backed
/// mappings. RSS is still reported separately for diagnostics, but durable
/// stores can map large files whose resident clean pages are reclaimable.
pub fn pressureWorkingSetBytes(stats: Stats) u64 {
    if (builtin.os.tag == .macos and stats.footprint_bytes != 0) return stats.footprint_bytes;
    if (builtin.os.tag == .linux) {
        const private_bytes = @max(stats.anonymous_bytes, stats.private_dirty_bytes);
        if (private_bytes != 0) return private_bytes;
    }
    if (stats.footprint_bytes != 0) return stats.footprint_bytes;
    return stats.resident_bytes;
}

pub fn snapshot() Stats {
    var stats = pressureSnapshot();
    const cpu = processCpuSnapshot();
    stats.cpu_available = cpu.available;
    stats.user_cpu_ns = cpu.user_ns;
    stats.system_cpu_ns = cpu.system_ns;
    if (!stats.available or builtin.os.tag != .macos) return stats;

    const malloc_stats = darwin.mallocStats();
    stats.malloc_available = malloc_stats.available;
    stats.malloc_allocated_bytes = malloc_stats.allocated_bytes;
    stats.malloc_zone_bytes = malloc_stats.zone_bytes;
    return stats;
}

const CpuStats = struct {
    available: bool = false,
    user_ns: u64 = 0,
    system_ns: u64 = 0,
};

fn processCpuSnapshot() CpuStats {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return .{};
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    return .{
        .available = true,
        .user_ns = timevalNs(usage.utime),
        .system_ns = timevalNs(usage.stime),
    };
}

fn timevalNs(value: anytype) u64 {
    if (value.sec < 0 or value.usec < 0) return 0;
    const seconds: u64 = @intCast(value.sec);
    const microseconds: u64 = @intCast(value.usec);
    const seconds_ns = std.math.mul(u64, seconds, std.time.ns_per_s) catch std.math.maxInt(u64);
    const microseconds_ns = std.math.mul(u64, microseconds, std.time.ns_per_us) catch std.math.maxInt(u64);
    return seconds_ns +| microseconds_ns;
}

/// Low-overhead OS memory-pressure sample for hot qualification loops. This
/// intentionally excludes allocator-zone enumeration, which is expensive and
/// can double-count allocations when zones overlap.
pub fn pressureSnapshot() Stats {
    if (builtin.os.tag == .linux) return linuxSnapshot();
    if (builtin.os.tag != .macos) return .{};

    var info: darwin.rusage_info_current = std.mem.zeroes(darwin.rusage_info_current);
    const rc = darwin.proc_pid_rusage(darwin.getpid(), darwin.RUSAGE_INFO_CURRENT, @ptrCast(&info));
    if (rc != 0) return .{};

    return .{
        .available = true,
        .resident_bytes = info.ri_resident_size,
        .peak_resident_bytes = processPeakResidentBytes(),
        .footprint_bytes = info.ri_phys_footprint,
        .peak_footprint_bytes = info.ri_lifetime_max_phys_footprint,
        .wired_bytes = info.ri_wired_size,
        .pageins = info.ri_pageins,
    };
}

fn linuxSnapshot() Stats {
    var out = linuxStatusSnapshot() orelse return .{};
    out.private_dirty_bytes = linuxStatusValueBytes("/proc/self/smaps_rollup", "Private_Dirty:") orelse 0;
    return out;
}

fn linuxStatusSnapshot() ?Stats {
    var buf: [16 * 1024]u8 = undefined;
    const contents = readProcFile("/proc/self/status", &buf) orelse return null;
    const resident_bytes = parseProcStatusBytes(contents, "VmRSS:") orelse return null;
    const anonymous_bytes = parseProcStatusBytes(contents, "RssAnon:") orelse 0;
    return .{
        .available = true,
        .resident_bytes = resident_bytes,
        .peak_resident_bytes = processPeakResidentBytes(),
        .anonymous_bytes = anonymous_bytes,
        .footprint_bytes = resident_bytes,
        .peak_footprint_bytes = processPeakResidentBytes(),
    };
}

fn processPeakResidentBytes() u64 {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return 0;
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;
    const maxrss: u64 = @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(u64, maxrss, 1024) catch std.math.maxInt(u64),
        else => maxrss,
    };
}

fn linuxStatusValueBytes(path: []const u8, key: []const u8) ?u64 {
    var buf: [64 * 1024]u8 = undefined;
    const contents = readProcFile(path, &buf) orelse return null;
    return parseProcStatusBytes(contents, key);
}

fn readSmallLinuxFile(path: []const u8, buffer: []u8) ?[]const u8 {
    if (builtin.os.tag != .linux or buffer.len == 0) return null;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return null;
    defer _ = std.posix.system.close(fd);

    var used: usize = 0;
    while (used < buffer.len) {
        const count = std.posix.read(fd, buffer[used..]) catch return null;
        if (count == 0) break;
        used += count;
    }
    if (used == 0) return null;
    return buffer[0..used];
}

fn readProcFile(path: []const u8, buf: []u8) ?[]const u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var file = std.Io.Dir.openFileAbsolute(io_impl.io(), path, .{}) catch return null;
    defer file.close(io_impl.io());
    var reader = file.reader(io_impl.io(), &.{});
    const n = reader.interface.readSliceShort(buf) catch return null;
    return buf[0..n];
}

fn parseProcStatusBytes(contents: []const u8, key: []const u8) ?u64 {
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, key)) continue;
        var fields = std.mem.tokenizeAny(u8, line[key.len..], " \t");
        const raw_value = fields.next() orelse return null;
        const value = std.fmt.parseInt(u64, raw_value, 10) catch return null;
        const unit = fields.next() orelse return value;
        if (std.mem.eql(u8, unit, "kB")) return value * 1024;
        return value;
    }
    return null;
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

test "cgroup paths reject traversal while preserving dotted names" {
    const paths = parseCgroupPaths(
        \\0::/system.slice/antfly.service
        \\7:cpu,cpuacct:/system.slice/antfly.service
        \\6:memory:/production/antfly
        \\
    );
    try std.testing.expectEqualStrings("/system.slice/antfly.service", paths.v2.?);
    try std.testing.expectEqualStrings("/production/antfly", paths.v1_memory.?);
    try std.testing.expectEqualStrings(
        "/system.slice/worker..scope",
        parseCgroupPaths("0::/system.slice/worker..scope\n").v2.?,
    );
    try std.testing.expect(parseCgroupPaths("0::/safe/../escape\n").v2 == null);
}

test "cgroup mountinfo resolves roots and escaped paths" {
    var mounts = CgroupMounts{};
    parseCgroupMountInfoLine(
        "36 29 0:32 /kubepods.slice /run/antfly\\040cg rw,nosuid,nodev - cgroup2 cgroup rw",
        &mounts,
    );
    parseCgroupMountInfoLine(
        "44 29 0:40 /production /run/cgroup/memory rw - cgroup memory rw,memory",
        &mounts,
    );

    try std.testing.expect(mounts.v2.valid());
    try std.testing.expectEqualStrings("/kubepods.slice", mounts.v2.root());
    try std.testing.expectEqualStrings("/run/antfly cg", mounts.v2.mountPoint());
    try std.testing.expect(mounts.v1_memory.valid());
    try std.testing.expectEqualStrings("/production", mounts.v1_memory.root());
    try std.testing.expectEqualStrings("/run/cgroup/memory", mounts.v1_memory.mountPoint());
    try std.testing.expectEqualStrings(
        "/pod-a/container-b",
        cgroupPathRelativeToMount(
            "/kubepods.slice/pod-a/container-b",
            mounts.v2.root(),
        ).?,
    );
    try std.testing.expectEqualStrings(
        "/",
        cgroupPathRelativeToMount("/", mounts.v2.root()).?,
    );
}
