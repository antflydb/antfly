// Copyright 2026 Antfly, Inc.
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

//! Shared runtime-kernel JIT contracts.
//!
//! Backend compilers own source compilation and executable handles. This
//! module owns only the stable configuration, cache identity, qualification,
//! and persistence rules used by both Metal and CUDA.

const std = @import("std");
const builtin = @import("builtin");
const workload_profile_policy = @import("../workload_profile_policy.zig");

pub const schema = "antfly.kernel_jit.v1";
pub const codegen_abi_version: u32 = 3;
pub const qualification_policy_version: u32 = 4;
/// Bump the policy version and update this identity whenever correctness
/// tolerances, fixtures, warmup/measurement procedure, or eligibility gates
/// change. It is part of every artifact key, so stale approvals cannot cross
/// a qualification-policy change.
pub const qualification_policy_identity =
    "correctness-first/v4;matrix-fp16-abs=q4k0.002,q6k0.003;min-speedup=1.02;warmups=2;paired-repeats=5;worst-repeat-gate=true;workload-coverage=qualified-weight-80,min-share-2,max-attempts-5";
pub const minimum_speedup: f64 = 1.02;
pub const maximum_candidates: usize = 8;
pub const warmup_repeats: usize = 2;
pub const measurement_repeats: usize = 5;
pub const maximum_measurement_repeats: usize = 31;
pub const maximum_source_bytes: usize = 16 * 1024 * 1024;
pub const maximum_artifact_bytes: usize = 64 * 1024 * 1024;
pub const maximum_log_bytes: usize = 64 * 1024;

pub const Mode = enum {
    off,
    shadow,
    on,
    required,

    pub fn compiles(self: Mode) bool {
        return self != .off;
    }

    pub fn activates(self: Mode) bool {
        return self == .on or self == .required;
    }

    pub fn failClosed(self: Mode) bool {
        return self == .required;
    }
};

/// A workload census must observe bundled production kernels. Activating a
/// generated route while capturing would feed its timings back as the
/// baseline and can make the exact-signature selector silently empty.
pub fn resolveProfileCaptureMode(
    mode: Mode,
    mode_explicit: bool,
    capture_requested: bool,
    qualified_profile_requested: bool,
) !Mode {
    if (!capture_requested) return mode;
    if (qualified_profile_requested) return error.KernelJitQualifiedProfileCaptureConflict;
    if (!mode_explicit and mode == .off) return .shadow;
    if (mode != .shadow) return error.KernelJitProfileCaptureRequiresShadow;
    return mode;
}

pub fn validateMetalProfileBackend(
    is_metal_backend: bool,
    capture_requested: bool,
    qualified_profile_requested: bool,
) !void {
    if ((capture_requested or qualified_profile_requested) and !is_metal_backend) {
        return error.KernelJitProfileRequiresMetalBackend;
    }
}

/// Live compiler and benchmark work is permitted only while the caller owns
/// an exclusive pre-serving startup phase. Dynamic loads may overlap requests
/// for other models and therefore must remain on bundled kernels.
pub const LoadContext = enum {
    dynamic,
    startup_preload,

    pub fn allowsQualification(self: LoadContext) bool {
        return self == .startup_preload;
    }
};

/// Required mode must never silently fall through to another backend after a
/// JIT contract failure. Other backend/model-loading failures retain the
/// normal fallback policy.
pub fn isRequiredFailure(mode: Mode, err: anyerror) bool {
    if (!mode.failClosed()) return false;
    return switch (err) {
        error.CudaJitRequiredRouteFailed,
        error.CudaJitRequiredRouteUnqualified,
        error.MetalJitRequiredRouteFailed,
        error.MetalKernelJitConfigConflict,
        error.KernelJitRequiredFailure,
        error.KernelJitRequiredBackendUnavailable,
        error.KernelJitRequiredDynamicLoad,
        error.KernelJitRequiredPreloadMissing,
        error.KernelJitRequiredPreloadUnmaterialized,
        error.KernelJitRequiredOptionalSessionUnmaterialized,
        error.KernelJitUnsupportedPlatform,
        error.KernelJitPersistentCacheUnsupported,
        error.InvalidKernelJitCacheDir,
        error.InvalidKernelJitQualifiedProfilePath,
        error.KernelJitQualifiedProfileRequiresCache,
        error.InvalidKernelJitPreloadBudget,
        error.InvalidKernelJitCacheBudget,
        => true,
        else => false,
    };
}

pub const Config = struct {
    mode: Mode = .off,
    cache_dir: ?[]const u8 = null,
    /// Workload-census sessions need the compiler context and persistent cache
    /// for exact-signature tuning, but must not compile general routes before
    /// the census establishes which signatures matter.
    profile_capture_only: bool = false,
    /// Canonical workload profile containing exact qualified winner keys. The
    /// corresponding qualification records must already exist in `cache_dir`
    /// (directly or via `antfly-kernel-jit-package import`).
    qualified_profile_path: ?[]const u8 = null,
    max_cache_bytes_mb: usize = 1024,
    /// Start budget for best-effort pre-publication work. One operation may
    /// overrun once started; required mode intentionally completes or fails.
    preload_budget_ms: u64 = 300_000,

    pub fn validate(self: Config) !void {
        if (self.profile_capture_only) {
            if (self.qualified_profile_path != null) {
                return error.KernelJitQualifiedProfileCaptureConflict;
            }
            if (self.mode != .shadow) return error.KernelJitProfileCaptureRequiresShadow;
            if (self.max_cache_bytes_mb == 0) return error.KernelJitProfileCaptureRequiresCache;
        }
        if (self.mode.compiles() and !runtimeJitSupported()) {
            return error.KernelJitUnsupportedPlatform;
        }
        if (self.cache_dir) |path| {
            if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) {
                return error.InvalidKernelJitCacheDir;
            }
            if (self.mode.compiles() and !persistentCacheSupported() and self.max_cache_bytes_mb != 0) {
                return error.KernelJitPersistentCacheUnsupported;
            }
        }
        if (self.qualified_profile_path) |path| {
            if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) {
                return error.InvalidKernelJitQualifiedProfilePath;
            }
            if (!self.mode.compiles() or self.max_cache_bytes_mb == 0) {
                return error.KernelJitQualifiedProfileRequiresCache;
            }
        }
        if (self.preload_budget_ms < 1_000 or self.preload_budget_ms > 3_600_000) {
            return error.InvalidKernelJitPreloadBudget;
        }
        const max_cache_mb: usize = 1024 * 1024;
        if (self.max_cache_bytes_mb > max_cache_mb) return error.InvalidKernelJitCacheBudget;
    }

    pub fn maxCacheBytes(self: Config) !usize {
        try self.validate();
        return std.math.mul(usize, self.max_cache_bytes_mb, 1024 * 1024) catch
            error.InvalidKernelJitCacheBudget;
    }

    /// Returns an owned cache path, or null when persistence is disabled or
    /// no explicit path/HOME is available. Callers may still JIT in memory.
    pub fn resolveCacheDir(
        self: Config,
        allocator: std.mem.Allocator,
        home: ?[]const u8,
    ) !?[]u8 {
        if (try self.maxCacheBytes() == 0) return null;
        if (!persistentCacheSupported()) return null;
        if (self.cache_dir) |path| return try allocator.dupe(u8, path);
        const home_dir = home orelse return null;
        if (home_dir.len == 0 or std.mem.indexOfScalar(u8, home_dir, 0) != null) return null;
        return try std.fs.path.join(allocator, &.{ home_dir, ".antfly", "inference", "jit" });
    }
};

/// Model-bound profile paths for a multimodel bundle. The primary path is
/// copied into `Config.qualified_profile_path`; optional sessions select only
/// their matching member when they are materialized.
pub const QualifiedProfileBundle = struct {
    primary: []const u8,
    vision: ?[]const u8 = null,
    audio: ?[]const u8 = null,
    text_projection: ?[]const u8 = null,
    visual_projection: ?[]const u8 = null,
    audio_projection: ?[]const u8 = null,
};

pub fn persistentCacheSupported() bool {
    return builtin.os.tag == .linux or builtin.os.tag == .macos;
}

pub fn runtimeJitSupported() bool {
    return builtin.os.tag == .linux or builtin.os.tag == .macos;
}

pub const Backend = enum(u8) {
    metal,
    cuda,
};

pub const RouteState = enum(u8) {
    missing,
    queued,
    compiling,
    validating,
    tuning,
    eligible,
    active,
    rejected,
    quarantined,
};

pub const CandidateEvidence = struct {
    candidate_index: usize,
    correctness_passed: bool,
    measured_speedup: f64,
    minimum_repeat_speedup: f64,
};

pub fn qualifies(evidence: CandidateEvidence) bool {
    return evidence.correctness_passed and
        std.math.isFinite(evidence.measured_speedup) and
        std.math.isFinite(evidence.minimum_repeat_speedup) and
        evidence.measured_speedup >= minimum_speedup and
        evidence.minimum_repeat_speedup >= minimum_speedup;
}

pub fn selectWinner(evidence: []const CandidateEvidence) ?CandidateEvidence {
    var winner: ?CandidateEvidence = null;
    for (evidence[0..@min(evidence.len, maximum_candidates)]) |candidate| {
        if (!qualifies(candidate)) continue;
        if (winner == null or candidate.measured_speedup > winner.?.measured_speedup) {
            winner = candidate;
        }
    }
    return winner;
}

pub fn evidenceFromPairedNanos(
    candidate_index: usize,
    correctness_passed: bool,
    baseline_nanos: []const u64,
    candidate_nanos: []const u64,
) !CandidateEvidence {
    if (baseline_nanos.len != candidate_nanos.len or
        baseline_nanos.len < measurement_repeats or
        baseline_nanos.len > maximum_measurement_repeats)
    {
        return error.InvalidKernelJitMeasurements;
    }
    var ratios: [maximum_measurement_repeats]f64 = undefined;
    var minimum = std.math.inf(f64);
    for (baseline_nanos, candidate_nanos, 0..) |baseline, candidate, index| {
        if (baseline == 0 or candidate == 0) return error.InvalidKernelJitMeasurements;
        const ratio = @as(f64, @floatFromInt(baseline)) / @as(f64, @floatFromInt(candidate));
        if (!std.math.isFinite(ratio) or ratio <= 0) return error.InvalidKernelJitMeasurements;
        ratios[index] = ratio;
        minimum = @min(minimum, ratio);
    }
    const measured = ratios[0..baseline_nanos.len];
    std.sort.heap(f64, measured, {}, std.sort.asc(f64));
    const middle = measured.len / 2;
    const median = if (measured.len % 2 == 1)
        measured[middle]
    else
        (measured[middle - 1] + measured[middle]) / 2.0;
    return .{
        .candidate_index = candidate_index,
        .correctness_passed = correctness_passed,
        .measured_speedup = median,
        .minimum_repeat_speedup = minimum,
    };
}

pub const QualificationRecord = struct {
    candidate_index: u32,
    repeat_count: u32,
    correctness_passed: bool,
    measured_speedup: f64,
    minimum_repeat_speedup: f64,
    max_absolute_error: f64,
    max_relative_error: f64,

    pub fn validate(self: QualificationRecord) !void {
        if (self.candidate_index >= maximum_candidates or
            self.repeat_count < measurement_repeats or
            self.repeat_count > maximum_measurement_repeats)
        {
            return error.InvalidKernelJitQualification;
        }
        if (!std.math.isFinite(self.measured_speedup) or self.measured_speedup <= 0 or
            !std.math.isFinite(self.minimum_repeat_speedup) or self.minimum_repeat_speedup <= 0 or
            !std.math.isFinite(self.max_absolute_error) or self.max_absolute_error < 0 or
            !std.math.isFinite(self.max_relative_error) or self.max_relative_error < 0)
        {
            return error.InvalidKernelJitQualification;
        }
    }

    pub fn evidence(self: QualificationRecord) !CandidateEvidence {
        try self.validate();
        return .{
            .candidate_index = self.candidate_index,
            .correctness_passed = self.correctness_passed,
            .measured_speedup = self.measured_speedup,
            .minimum_repeat_speedup = self.minimum_repeat_speedup,
        };
    }

    pub fn eligible(self: QualificationRecord) bool {
        return qualifies(self.evidence() catch return false);
    }

    pub fn encode(self: QualificationRecord) ![qualification_record_bytes]u8 {
        try self.validate();
        var out: [qualification_record_bytes]u8 = @splat(0);
        @memcpy(out[0..8], qualification_magic);
        std.mem.writeInt(u32, out[8..12], qualification_version, .little);
        std.mem.writeInt(u32, out[12..16], self.candidate_index, .little);
        std.mem.writeInt(u32, out[16..20], self.repeat_count, .little);
        out[20] = @intFromBool(self.correctness_passed);
        std.mem.writeInt(u64, out[24..32], @bitCast(self.measured_speedup), .little);
        std.mem.writeInt(u64, out[32..40], @bitCast(self.minimum_repeat_speedup), .little);
        std.mem.writeInt(u64, out[40..48], @bitCast(self.max_absolute_error), .little);
        std.mem.writeInt(u64, out[48..56], @bitCast(self.max_relative_error), .little);
        return out;
    }

    pub fn decode(bytes: []const u8) !QualificationRecord {
        if (bytes.len != qualification_record_bytes or
            !std.mem.eql(u8, bytes[0..8], qualification_magic) or
            std.mem.readInt(u32, bytes[8..12], .little) != qualification_version or
            bytes[20] > 1)
        {
            return error.InvalidKernelJitQualification;
        }
        const record = QualificationRecord{
            .candidate_index = std.mem.readInt(u32, bytes[12..16], .little),
            .repeat_count = std.mem.readInt(u32, bytes[16..20], .little),
            .correctness_passed = bytes[20] == 1,
            .measured_speedup = @bitCast(std.mem.readInt(u64, bytes[24..32], .little)),
            .minimum_repeat_speedup = @bitCast(std.mem.readInt(u64, bytes[32..40], .little)),
            .max_absolute_error = @bitCast(std.mem.readInt(u64, bytes[40..48], .little)),
            .max_relative_error = @bitCast(std.mem.readInt(u64, bytes[48..56], .little)),
        };
        try record.validate();
        return record;
    }
};

const qualification_magic = "AFJITQ01";
const qualification_version: u32 = 1;
const qualification_record_bytes: usize = 56;

pub const ArtifactKeyInput = struct {
    backend: Backend,
    semantic_identity: []const u8,
    runtime_identity: []const u8,
    target_identity: []const u8,
    device_identity: []const u8,
    schedule_identity: []const u8,
    baseline_identity: []const u8,
    compiler_identity: []const u8,
    compiler_options: []const u8,
    source: []const u8,
};

pub const ArtifactKey = [std.crypto.hash.sha2.Sha256.digest_length]u8;

fn hashBytes(hasher: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, bytes.len, .little);
    hasher.update(&len);
    hasher.update(bytes);
}

pub fn artifactKey(input: ArtifactKeyInput) ArtifactKey {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashBytes(&hasher, schema);
    var abi: [4]u8 = undefined;
    std.mem.writeInt(u32, &abi, codegen_abi_version, .little);
    hasher.update(&abi);
    hashBytes(&hasher, "qualification");
    var policy: [4]u8 = undefined;
    std.mem.writeInt(u32, &policy, qualification_policy_version, .little);
    hasher.update(&policy);
    hashBytes(&hasher, qualification_policy_identity);
    hasher.update(&.{@backingInt(input.backend)});
    hashBytes(&hasher, input.semantic_identity);
    hashBytes(&hasher, input.runtime_identity);
    hashBytes(&hasher, input.target_identity);
    hashBytes(&hasher, input.device_identity);
    hashBytes(&hasher, input.schedule_identity);
    hashBytes(&hasher, input.baseline_identity);
    hashBytes(&hasher, input.compiler_identity);
    hashBytes(&hasher, input.compiler_options);
    hashBytes(&hasher, input.source);
    var digest: ArtifactKey = undefined;
    hasher.final(&digest);
    return digest;
}

/// Cache identity for compiler output only. Qualification policy, fixtures,
/// device-specific evidence, and baseline implementation deliberately do not
/// participate so reusable CUDA PTX is not recompiled for every model shape
/// or qualification-policy edit.
pub fn compileArtifactKey(input: ArtifactKeyInput) ArtifactKey {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashBytes(&hasher, schema);
    var abi: [4]u8 = undefined;
    std.mem.writeInt(u32, &abi, codegen_abi_version, .little);
    hasher.update(&abi);
    hashBytes(&hasher, "compile");
    hasher.update(&.{@backingInt(input.backend)});
    hashBytes(&hasher, input.semantic_identity);
    hashBytes(&hasher, input.runtime_identity);
    hashBytes(&hasher, input.target_identity);
    hashBytes(&hasher, input.schedule_identity);
    hashBytes(&hasher, input.compiler_identity);
    hashBytes(&hasher, input.compiler_options);
    hashBytes(&hasher, input.source);
    var digest: ArtifactKey = undefined;
    hasher.final(&digest);
    return digest;
}

pub fn artifactKeyHex(key: ArtifactKey) [std.crypto.hash.sha2.Sha256.digest_length * 2]u8 {
    return std.fmt.bytesToHex(key, .lower);
}

pub const CacheArtifactKind = enum(u8) {
    cuda_ptx,
    qualification,

    fn suffix(self: CacheArtifactKind) []const u8 {
        return switch (self) {
            .cuda_ptx => "ptx",
            .qualification => "qualification",
        };
    }
};

pub const CacheStats = struct {
    hits: usize = 0,
    misses: usize = 0,
    corruptions: usize = 0,
    stores: usize = 0,
    evictions: usize = 0,
    stale_temps_removed: usize = 0,
    temporary_bytes: u64 = 0,
};

const cache_magic = "AFJIT001";
const cache_header_bytes = 88;
const stale_temp_age_ns: i96 = 24 * std.time.ns_per_hour;
const cache_key_hex_bytes = std.crypto.hash.sha2.Sha256.digest_length * 2;

fn isCacheKeyHex(bytes: []const u8) bool {
    if (bytes.len != cache_key_hex_bytes) return false;
    for (bytes) |byte| switch (byte) {
        '0'...'9', 'a'...'f' => {},
        else => return false,
    };
    return true;
}

fn isCacheArtifactName(name: []const u8) bool {
    if (name.len <= cache_key_hex_bytes or !isCacheKeyHex(name[0..cache_key_hex_bytes])) return false;
    const suffix = name[cache_key_hex_bytes..];
    return std.mem.eql(u8, suffix, ".ptx.jit") or
        std.mem.eql(u8, suffix, ".qualification.jit");
}

fn isCacheTemporaryName(name: []const u8) bool {
    if (name.len < 2 or name[0] != '.') return false;
    const marker = ".tmp-";
    const marker_index = std.mem.lastIndexOf(u8, name, marker) orelse return false;
    return marker_index + marker.len < name.len and
        isCacheArtifactName(name[1..marker_index]);
}

fn privateCachePermissions(comptime directory: bool) std.Io.File.Permissions {
    if (std.Io.File.Permissions.has_executable_bit) {
        return .fromMode(if (directory) 0o700 else 0o600);
    }
    return if (directory) .default_dir else .default_file;
}

fn permissionsRejectForeignWriters(permissions: std.Io.File.Permissions) bool {
    if (!std.Io.File.Permissions.has_executable_bit) return false;
    return permissions.toMode() & 0o022 == 0;
}

/// Executable cache entries are a trust boundary, not ordinary disposable
/// data. Verify ownership through the already-open handle so a path swap
/// cannot race this check. Unsupported platforms fail closed and simply run
/// without a persistent cache.
fn handleOwnedByEffectiveUser(handle: std.Io.File.Handle) !bool {
    return switch (builtin.os.tag) {
        .linux => blk: {
            var stat: std.os.linux.Statx = std.mem.zeroes(std.os.linux.Statx);
            const result = std.os.linux.statx(
                handle,
                "",
                std.os.linux.AT.EMPTY_PATH | std.os.linux.AT.SYMLINK_NOFOLLOW,
                std.os.linux.STATX.BASIC_STATS,
                &stat,
            );
            if (std.os.linux.errno(result) != .SUCCESS) return error.KernelJitCacheStatFailed;
            break :blk stat.uid == std.os.linux.geteuid();
        },
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => blk: {
            var stat: std.c.Stat = undefined;
            if (std.c.fstat(handle, &stat) != 0) return error.KernelJitCacheStatFailed;
            break :blk stat.uid == std.c.geteuid();
        },
        else => error.KernelJitCacheOwnershipUnsupported,
    };
}

fn validateTrustedDirectory(dir: std.Io.Dir, io: std.Io) !void {
    const stat = try dir.stat(io);
    if (stat.kind != .directory or
        !permissionsRejectForeignWriters(stat.permissions) or
        !try handleOwnedByEffectiveUser(dir.handle))
    {
        return error.UntrustedKernelJitCacheDirectory;
    }
}

fn validateTrustedFile(file: std.Io.File, io: std.Io) !void {
    _ = try trustedFileStat(file, io);
}

fn trustedFileStat(file: std.Io.File, io: std.Io) !std.Io.File.Stat {
    const stat = try file.stat(io);
    if (stat.kind != .file or
        !permissionsRejectForeignWriters(stat.permissions) or
        !try handleOwnedByEffectiveUser(file.handle))
    {
        return error.UntrustedKernelJitCacheEntry;
    }
    return stat;
}

fn syncCacheDirectory(dir: std.Io.Dir, io: std.Io) !void {
    switch (builtin.os.tag) {
        .linux, .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => {
            const file: std.Io.File = .{
                .handle = dir.handle,
                .flags = .{ .nonblocking = false },
            };
            try file.sync(io);
        },
        else => {},
    }
}

/// Small content-addressed disk cache shared by backend JIT owners. Entries
/// are checksummed, written through a same-directory temporary file, fsynced,
/// and atomically renamed. A corrupt entry is deleted and treated as a miss;
/// generated code is never loaded from unchecked bytes.
pub const ArtifactCache = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    owns_dir: bool,
    max_bytes: usize,
    mutex: std.Io.Mutex = .init,
    stats: CacheStats = .{},
    temp_sequence: u64 = 0,

    pub fn initPath(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        max_bytes: usize,
    ) !ArtifactCache {
        if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) {
            return error.InvalidKernelJitCacheDir;
        }
        const dir = try std.Io.Dir.cwd().createDirPathOpen(io, path, .{
            .permissions = privateCachePermissions(true),
            .open_options = .{
                .iterate = true,
                .follow_symlinks = false,
            },
        });
        return initOpenedDir(allocator, io, dir, true, max_bytes);
    }

    /// Test and embedding hook for a caller-owned directory handle.
    pub fn initDir(
        allocator: std.mem.Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        max_bytes: usize,
    ) !ArtifactCache {
        return initOpenedDir(allocator, io, dir, false, max_bytes);
    }

    fn initOpenedDir(
        allocator: std.mem.Allocator,
        io: std.Io,
        dir: std.Io.Dir,
        owns_dir: bool,
        max_bytes: usize,
    ) !ArtifactCache {
        var cache = ArtifactCache{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .owns_dir = owns_dir,
            .max_bytes = max_bytes,
        };
        errdefer if (owns_dir) dir.close(io);
        try validateTrustedDirectory(dir, io);
        if (max_bytes != 0) try cache.pruneUnlocked();
        return cache;
    }

    pub fn deinit(self: *ArtifactCache) void {
        if (self.owns_dir) self.dir.close(self.io);
        self.* = undefined;
    }

    pub fn snapshotStats(self: *ArtifactCache) CacheStats {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.stats;
    }

    pub fn load(
        self: *ArtifactCache,
        kind: CacheArtifactKind,
        key: ArtifactKey,
    ) !?[]u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.max_bytes == 0) {
            self.stats.misses += 1;
            return null;
        }

        try validateTrustedDirectory(self.dir, self.io);

        const name = try cacheEntryName(self.allocator, kind, key);
        defer self.allocator.free(name);
        var file = self.dir.openFile(self.io, name, .{
            .allow_directory = false,
            .follow_symlinks = false,
            .resolve_beneath = true,
        }) catch |err| switch (err) {
            error.FileNotFound => {
                self.stats.misses += 1;
                return null;
            },
            error.SymLinkLoop => {
                self.stats.corruptions += 1;
                self.stats.misses += 1;
                return null;
            },
            else => return err,
        };
        defer file.close(self.io);
        try validateTrustedFile(file, self.io);
        var reader = file.reader(self.io, &.{});
        const encoded = reader.interface.allocRemaining(
            self.allocator,
            .limited(cache_header_bytes + maximum_artifact_bytes),
        ) catch |err| switch (err) {
            error.StreamTooLong => {
                self.dir.deleteFile(self.io, name) catch {};
                self.stats.corruptions += 1;
                self.stats.misses += 1;
                return null;
            },
            error.ReadFailed => return reader.err.?,
            error.OutOfMemory => return error.OutOfMemory,
        };
        defer self.allocator.free(encoded);

        const payload = validateCacheEntry(encoded, kind, key) catch {
            self.dir.deleteFile(self.io, name) catch {};
            self.stats.corruptions += 1;
            self.stats.misses += 1;
            return null;
        };
        const owned = try self.allocator.dupe(u8, payload);
        file.setTimestampsNow(self.io) catch {};
        self.stats.hits += 1;
        return owned;
    }

    pub fn store(
        self: *ArtifactCache,
        kind: CacheArtifactKind,
        key: ArtifactKey,
        payload: []const u8,
    ) !void {
        if (payload.len == 0 or payload.len > maximum_artifact_bytes) {
            return error.InvalidKernelJitArtifactSize;
        }
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.max_bytes == 0) return;
        try validateTrustedDirectory(self.dir, self.io);

        const name = try cacheEntryName(self.allocator, kind, key);
        defer self.allocator.free(name);
        const encoded = try encodeCacheEntry(self.allocator, kind, key, payload);
        defer self.allocator.free(encoded);

        try self.writeFileAtomicUnlocked(name, encoded);
        self.stats.stores += 1;
        try self.pruneUnlocked();
    }

    fn writeFileAtomicUnlocked(self: *ArtifactCache, name: []const u8, data: []const u8) !void {
        var tmp_name: ?[]u8 = null;
        defer if (tmp_name) |value| self.allocator.free(value);
        var tmp_file: ?std.Io.File = null;
        var attempts: usize = 0;
        while (attempts < 16 and tmp_file == null) : (attempts += 1) {
            self.temp_sequence +%= 1;
            const candidate = try std.fmt.allocPrint(
                self.allocator,
                ".{s}.tmp-{x}-{x}-{x}",
                .{ name, @intFromPtr(self), std.Thread.getCurrentId(), self.temp_sequence },
            );
            const opened = self.dir.createFile(self.io, candidate, .{
                .truncate = false,
                .exclusive = true,
                .permissions = privateCachePermissions(false),
                .resolve_beneath = true,
            }) catch |err| switch (err) {
                error.PathAlreadyExists => {
                    self.allocator.free(candidate);
                    continue;
                },
                else => {
                    self.allocator.free(candidate);
                    return err;
                },
            };
            tmp_name = candidate;
            tmp_file = opened;
        }
        const owned_tmp_name = tmp_name orelse return error.KernelJitCacheTempCollision;
        var file = tmp_file.?;
        var file_open = true;
        defer if (file_open) file.close(self.io);
        errdefer self.dir.deleteFile(self.io, owned_tmp_name) catch {};
        try file.writeStreamingAll(self.io, data);
        try file.sync(self.io);
        file.close(self.io);
        file_open = false;
        try std.Io.Dir.rename(self.dir, owned_tmp_name, self.dir, name, self.io);
        try syncCacheDirectory(self.dir, self.io);
    }

    fn pruneUnlocked(self: *ArtifactCache) !void {
        const Entry = struct {
            name: []u8,
            size: u64,
            mtime_ns: i128,

            fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
                if (lhs.mtime_ns != rhs.mtime_ns) return lhs.mtime_ns < rhs.mtime_ns;
                return std.mem.order(u8, lhs.name, rhs.name) == .lt;
            }
        };

        var entries: std.ArrayListUnmanaged(Entry) = .empty;
        defer {
            for (entries.items) |entry| self.allocator.free(entry.name);
            entries.deinit(self.allocator);
        }
        var total: u64 = 0;
        var temporary_bytes: u64 = 0;
        var directory_changed = false;
        const now = std.Io.Timestamp.now(self.io, .real);
        var iterator = self.dir.iterate();
        while (try iterator.next(self.io)) |entry| {
            const is_temporary = isCacheTemporaryName(entry.name);
            const is_artifact = isCacheArtifactName(entry.name);
            if (!is_temporary and !is_artifact) continue;
            const stat = try self.trustedRegularFileStat(entry.name) orelse continue;
            if (is_temporary) {
                // A recent temp file may belong to another cache instance or
                // process. Reclaim only our exact pattern after a full day.
                const age_ns = stat.mtime.durationTo(now).nanoseconds;
                if (age_ns >= stale_temp_age_ns) {
                    if (try self.deleteFileIfPresent(entry.name)) {
                        self.stats.stale_temps_removed += 1;
                        directory_changed = true;
                    }
                    continue;
                }
                temporary_bytes = std.math.add(u64, temporary_bytes, stat.size) catch std.math.maxInt(u64);
                total = std.math.add(u64, total, stat.size) catch std.math.maxInt(u64);
                continue;
            }
            try entries.append(self.allocator, .{
                .name = try self.allocator.dupe(u8, entry.name),
                .size = stat.size,
                .mtime_ns = stat.mtime.toNanoseconds(),
            });
            total = std.math.add(u64, total, stat.size) catch std.math.maxInt(u64);
        }
        self.stats.temporary_bytes = temporary_bytes;
        if (total <= self.max_bytes) {
            if (directory_changed) try syncCacheDirectory(self.dir, self.io);
            return;
        }
        std.sort.heap(Entry, entries.items, {}, Entry.lessThan);
        for (entries.items) |entry| {
            if (total <= self.max_bytes) break;
            const deleted = try self.deleteFileIfPresent(entry.name);
            total -|= entry.size;
            if (deleted) {
                self.stats.evictions += 1;
                directory_changed = true;
            }
        }
        if (directory_changed) try syncCacheDirectory(self.dir, self.io);
    }

    fn trustedRegularFileStat(self: *ArtifactCache, name: []const u8) !?std.Io.File.Stat {
        var file = self.dir.openFile(self.io, name, .{
            .allow_directory = false,
            .path_only = true,
            .follow_symlinks = false,
            .resolve_beneath = true,
        }) catch |err| switch (err) {
            error.FileNotFound, error.SymLinkLoop, error.IsDir => return null,
            else => return err,
        };
        defer file.close(self.io);
        return try trustedFileStat(file, self.io);
    }

    fn deleteFileIfPresent(self: *ArtifactCache, name: []const u8) !bool {
        self.dir.deleteFile(self.io, name) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
    }
};

fn cacheEntryName(
    allocator: std.mem.Allocator,
    kind: CacheArtifactKind,
    key: ArtifactKey,
) ![]u8 {
    const hex = artifactKeyHex(key);
    return std.fmt.allocPrint(allocator, "{s}.{s}.jit", .{ &hex, kind.suffix() });
}

fn encodeCacheEntry(
    allocator: std.mem.Allocator,
    kind: CacheArtifactKind,
    key: ArtifactKey,
    payload: []const u8,
) ![]u8 {
    const total = std.math.add(usize, cache_header_bytes, payload.len) catch
        return error.InvalidKernelJitArtifactSize;
    const encoded = try allocator.alloc(u8, total);
    @memset(encoded[0..cache_header_bytes], 0);
    @memcpy(encoded[0..cache_magic.len], cache_magic);
    std.mem.writeInt(u32, encoded[8..12], codegen_abi_version, .little);
    encoded[12] = @backingInt(kind);
    std.mem.writeInt(u64, encoded[16..24], payload.len, .little);
    @memcpy(encoded[24..56], &key);
    var digest: ArtifactKey = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    @memcpy(encoded[56..88], &digest);
    @memcpy(encoded[cache_header_bytes..], payload);
    return encoded;
}

fn validateCacheEntry(
    encoded: []const u8,
    expected_kind: CacheArtifactKind,
    expected_key: ArtifactKey,
) ![]const u8 {
    if (encoded.len < cache_header_bytes) return error.InvalidKernelJitCacheEntry;
    if (!std.mem.eql(u8, encoded[0..cache_magic.len], cache_magic)) return error.InvalidKernelJitCacheEntry;
    if (std.mem.readInt(u32, encoded[8..12], .little) != codegen_abi_version) return error.InvalidKernelJitCacheEntry;
    if (encoded[12] != @backingInt(expected_kind)) return error.InvalidKernelJitCacheEntry;
    const payload_len = std.mem.readInt(u64, encoded[16..24], .little);
    if (payload_len == 0 or payload_len > maximum_artifact_bytes) return error.InvalidKernelJitCacheEntry;
    if (payload_len != encoded.len - cache_header_bytes) return error.InvalidKernelJitCacheEntry;
    if (!std.mem.eql(u8, encoded[24..56], &expected_key)) return error.InvalidKernelJitCacheEntry;
    const payload = encoded[cache_header_bytes..];
    var digest: ArtifactKey = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    if (!std.mem.eql(u8, encoded[56..88], &digest)) return error.InvalidKernelJitCacheEntry;
    return payload;
}

pub const qualification_package_schema = "antfly.kernel_jit.package.v2";
pub const workload_profile_schema = workload_profile_policy.schema_name;
pub const qualification_package_manifest_name = "manifest.json";
pub const maximum_qualification_package_entries: usize = 4096;
pub const maximum_qualification_package_profiles: usize = 6;

const maximum_qualification_package_manifest_bytes: usize = 1024 * 1024;
const maximum_qualification_package_profile_bytes: usize = 16 * 1024 * 1024;
const qualification_package_cache_budget =
    maximum_qualification_package_entries * (cache_header_bytes + qualification_record_bytes);
const qualification_entry_suffix = ".qualification.jit";
const qualification_profile_prefix = "profile-";
const qualification_profile_suffix = ".json";

pub const QualificationPackageSummary = struct {
    entries: usize,
    profiles: usize,
};

/// Canonical profile material and the exact qualification records it owns.
/// Callers must derive these fields from one validated workload profile; raw,
/// unbound key lists are intentionally not accepted by package v2.
pub const QualificationPackageProfileInput = struct {
    canonical_profile: []const u8,
    device_registry_id: u64,
    compiler_fingerprint: []const u8,
    model_scope_fingerprint: []const u8,
    keys: []const ArtifactKey,
};

const QualificationPackageProfileKeyClaim = workload_profile_policy.QualifiedKernel;

const QualificationPackageRecordClaim = struct {
    key: ArtifactKey,
    candidate_index: u32,
    measured_speedup: f64,
    minimum_repeat_speedup: f64,
};

/// Parse the full canonical profile rather than a count-only projection. This
/// lets package verification replay the same ranked weighted-coverage policy
/// used before export and reject forged coverage claims before cache import.
const QualificationPackageProfileBinding = workload_profile_policy.ProfileReport;

const QualificationPackageManifestEntry = struct {
    key: []const u8,
};

const QualificationPackageManifestProfile = struct {
    canonical_profile_sha256: []const u8,
    model_scope_fingerprint: []const u8,
    entries: []const QualificationPackageManifestEntry,
};

const QualificationPackageManifest = struct {
    schema: []const u8,
    codegen_abi_version: u32,
    qualification_policy_version: u32,
    qualification_policy_identity: []const u8,
    device_registry_id: u64,
    compiler_fingerprint: []const u8,
    profiles: []const QualificationPackageManifestProfile,
};

const LoadedQualificationPackageManifest = struct {
    allocator: std.mem.Allocator,
    keys: []ArtifactKey,
    claims: []QualificationPackageRecordClaim,
    profile_digests: []ArtifactKey,

    fn deinit(self: *LoadedQualificationPackageManifest) void {
        self.allocator.free(self.profile_digests);
        self.allocator.free(self.claims);
        self.allocator.free(self.keys);
        self.* = undefined;
    }
};

const LoadedQualificationPackage = struct {
    allocator: std.mem.Allocator,
    keys: []ArtifactKey,
    payloads: [][]u8,
    profile_count: usize,

    fn deinit(self: *LoadedQualificationPackage) void {
        for (self.payloads) |payload| self.allocator.free(payload);
        self.allocator.free(self.payloads);
        self.allocator.free(self.keys);
        self.* = undefined;
    }
};

pub fn parseArtifactKeyHex(hex: []const u8) !ArtifactKey {
    if (!isCacheKeyHex(hex)) return error.InvalidKernelJitArtifactKey;
    var key: ArtifactKey = undefined;
    for (&key, 0..) |*byte, index| {
        byte.* = std.fmt.parseUnsigned(u8, hex[index * 2 .. index * 2 + 2], 16) catch
            return error.InvalidKernelJitArtifactKey;
    }
    return key;
}

fn artifactKeyLessThan(_: void, lhs: ArtifactKey, rhs: ArtifactKey) bool {
    return std.mem.order(u8, &lhs, &rhs) == .lt;
}

fn qualificationPackageClaimLessThan(
    _: void,
    lhs: QualificationPackageRecordClaim,
    rhs: QualificationPackageRecordClaim,
) bool {
    return std.mem.order(u8, &lhs.key, &rhs.key) == .lt;
}

fn qualificationPackageClaimsEqual(
    lhs: QualificationPackageRecordClaim,
    rhs: QualificationPackageRecordClaim,
) bool {
    return std.mem.eql(u8, &lhs.key, &rhs.key) and
        lhs.candidate_index == rhs.candidate_index and
        lhs.measured_speedup == rhs.measured_speedup and
        lhs.minimum_repeat_speedup == rhs.minimum_repeat_speedup;
}

fn qualificationRecordMatchesClaim(
    record: QualificationRecord,
    claim: QualificationPackageRecordClaim,
) bool {
    return record.candidate_index == claim.candidate_index and
        record.measured_speedup == claim.measured_speedup and
        record.minimum_repeat_speedup == claim.minimum_repeat_speedup;
}

fn sortedArtifactKeysContain(keys: []const ArtifactKey, key: ArtifactKey) bool {
    return sortedArtifactKeyIndex(keys, key) != null;
}

fn sortedArtifactKeyIndex(keys: []const ArtifactKey, key: ArtifactKey) ?usize {
    var low: usize = 0;
    var high = keys.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        switch (std.mem.order(u8, &keys[middle], &key)) {
            .lt => low = middle + 1,
            .eq => return middle,
            .gt => high = middle,
        }
    }
    return null;
}

fn validateQualificationPackageProfileBinding(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    expected_device_registry_id: u64,
    expected_compiler_fingerprint: []const u8,
    expected_model_scope_fingerprint: []const u8,
    expected_keys: []const ArtifactKey,
) ![]QualificationPackageRecordClaim {
    var parsed = std.json.parseFromSlice(
        QualificationPackageProfileBinding,
        allocator,
        bytes,
        .{},
    ) catch return error.InvalidKernelJitPackageProfile;
    defer parsed.deinit();
    const profile = parsed.value;
    workload_profile_policy.validateStructure(profile, minimum_speedup, maximum_candidates) catch
        return error.InvalidKernelJitPackageProfile;
    workload_profile_policy.validateQualifiedCoverage(profile) catch
        return error.InvalidKernelJitPackageProfile;
    const empty_bundle_member = expected_keys.len == 0 and
        profile.qualified_kernels.len == 0 and
        profile.tuning_selected_signatures == 0 and
        profile.tuning_qualified_signatures == 0 and
        profile.tuning_failed_signatures == 0;
    if (!std.mem.eql(u8, profile.schema, workload_profile_schema) or
        !profile.complete or
        profile.dropped_signatures != 0 or
        profile.dropped_dispatches != 0 or
        profile.device_registry_id != expected_device_registry_id or
        !std.mem.eql(u8, profile.compiler_fingerprint, expected_compiler_fingerprint) or
        !std.mem.eql(u8, profile.model_fingerprint, expected_model_scope_fingerprint) or
        !isSha256Fingerprint(profile.compiler_fingerprint) or
        !isSha256Fingerprint(profile.model_fingerprint) or
        profile.qualified_kernels.len != profile.tuning_qualified_signatures or
        @as(u16, profile.tuning_qualified_signatures) + profile.tuning_failed_signatures !=
            profile.tuning_selected_signatures or
        (!profile.tuning_coverage_complete and !empty_bundle_member))
    {
        return error.InvalidKernelJitPackageProfile;
    }

    const claims = try allocator.alloc(QualificationPackageRecordClaim, profile.qualified_kernels.len);
    errdefer allocator.free(claims);
    for (profile.qualified_kernels, claims) |claim, *record_claim| {
        const key = parseArtifactKeyHex(claim.artifact_key) catch
            return error.InvalidKernelJitPackageProfile;
        if (claim.candidate_index >= maximum_candidates or
            !std.math.isFinite(claim.measured_speedup) or
            !std.math.isFinite(claim.minimum_repeat_speedup) or
            claim.measured_speedup < minimum_speedup or
            claim.minimum_repeat_speedup < minimum_speedup)
        {
            return error.InvalidKernelJitPackageProfile;
        }
        record_claim.* = .{
            .key = key,
            .candidate_index = claim.candidate_index,
            .measured_speedup = claim.measured_speedup,
            .minimum_repeat_speedup = claim.minimum_repeat_speedup,
        };
    }
    std.sort.heap(QualificationPackageRecordClaim, claims, {}, qualificationPackageClaimLessThan);
    if (claims.len > 1) {
        for (claims[1..], claims[0 .. claims.len - 1]) |claim, previous| {
            if (std.mem.eql(u8, &claim.key, &previous.key)) return error.InvalidKernelJitPackageProfile;
        }
    }
    const sorted_expected = try allocator.dupe(ArtifactKey, expected_keys);
    defer allocator.free(sorted_expected);
    std.sort.heap(ArtifactKey, sorted_expected, {}, artifactKeyLessThan);
    if (claims.len != sorted_expected.len) return error.InvalidKernelJitPackageProfile;
    for (claims, sorted_expected) |claim, expected| {
        if (!std.mem.eql(u8, &claim.key, &expected)) return error.InvalidKernelJitPackageProfile;
    }
    return claims;
}

fn mergeQualificationPackageClaims(
    aggregate: []QualificationPackageRecordClaim,
    seen: []bool,
    sorted_keys: []const ArtifactKey,
    claims: []const QualificationPackageRecordClaim,
) !void {
    if (aggregate.len != sorted_keys.len or seen.len != sorted_keys.len) {
        return error.InvalidKernelJitPackageProfile;
    }
    for (claims) |claim| {
        const index = sortedArtifactKeyIndex(sorted_keys, claim.key) orelse
            return error.InvalidKernelJitPackageProfile;
        if (seen[index]) {
            if (!qualificationPackageClaimsEqual(aggregate[index], claim)) {
                return error.InvalidKernelJitPackageProfile;
            }
        } else {
            aggregate[index] = claim;
            seen[index] = true;
        }
    }
}

fn qualificationEntryKey(name: []const u8) !ArtifactKey {
    if (name.len != cache_key_hex_bytes + qualification_entry_suffix.len or
        !std.mem.endsWith(u8, name, qualification_entry_suffix))
    {
        return error.InvalidKernelJitPackageContents;
    }
    return parseArtifactKeyHex(name[0..cache_key_hex_bytes]) catch
        error.InvalidKernelJitPackageContents;
}

fn isSha256Fingerprint(value: []const u8) bool {
    const prefix = "sha256:";
    return value.len == prefix.len + cache_key_hex_bytes and
        std.mem.startsWith(u8, value, prefix) and
        isCacheKeyHex(value[prefix.len..]);
}

fn qualificationProfileName(
    allocator: std.mem.Allocator,
    digest: ArtifactKey,
) ![]u8 {
    const hex = artifactKeyHex(digest);
    return std.fmt.allocPrint(
        allocator,
        "{s}{s}{s}",
        .{ qualification_profile_prefix, &hex, qualification_profile_suffix },
    );
}

fn qualificationProfileDigest(name: []const u8) !ArtifactKey {
    const expected_len = qualification_profile_prefix.len + cache_key_hex_bytes + qualification_profile_suffix.len;
    if (name.len != expected_len or
        !std.mem.startsWith(u8, name, qualification_profile_prefix) or
        !std.mem.endsWith(u8, name, qualification_profile_suffix))
    {
        return error.InvalidKernelJitPackageContents;
    }
    const start = qualification_profile_prefix.len;
    return parseArtifactKeyHex(name[start .. start + cache_key_hex_bytes]) catch
        error.InvalidKernelJitPackageContents;
}

fn profileIndexLessThan(digests: []const ArtifactKey, lhs: usize, rhs: usize) bool {
    return std.mem.order(u8, &digests[lhs], &digests[rhs]) == .lt;
}

fn packageProvenanceFromManifest(
    allocator: std.mem.Allocator,
    manifest: QualificationPackageManifest,
) !LoadedQualificationPackageManifest {
    if (!std.mem.eql(u8, manifest.schema, qualification_package_schema)) {
        return error.StaleKernelJitPackage;
    }
    if (manifest.codegen_abi_version != codegen_abi_version or
        manifest.qualification_policy_version != qualification_policy_version or
        !std.mem.eql(u8, manifest.qualification_policy_identity, qualification_policy_identity))
    {
        return error.StaleKernelJitPackage;
    }
    if (!isSha256Fingerprint(manifest.compiler_fingerprint) or
        manifest.profiles.len == 0 or
        manifest.profiles.len > maximum_qualification_package_profiles)
    {
        return error.InvalidKernelJitPackageManifest;
    }

    const profile_digests = try allocator.alloc(ArtifactKey, manifest.profiles.len);
    errdefer allocator.free(profile_digests);
    var referenced_keys: std.ArrayListUnmanaged(ArtifactKey) = .empty;
    defer referenced_keys.deinit(allocator);
    for (manifest.profiles, 0..) |profile, profile_index| {
        profile_digests[profile_index] = parseArtifactKeyHex(profile.canonical_profile_sha256) catch
            return error.InvalidKernelJitPackageManifest;
        if (profile_index != 0 and
            std.mem.order(u8, &profile_digests[profile_index - 1], &profile_digests[profile_index]) != .lt)
        {
            return error.InvalidKernelJitPackageManifest;
        }
        if (!isSha256Fingerprint(profile.model_scope_fingerprint) or
            profile.entries.len > maximum_qualification_package_entries)
        {
            return error.InvalidKernelJitPackageManifest;
        }
        for (profile.entries, 0..) |entry, entry_index| {
            const key = parseArtifactKeyHex(entry.key) catch
                return error.InvalidKernelJitPackageManifest;
            if (entry_index != 0) {
                const previous = parseArtifactKeyHex(profile.entries[entry_index - 1].key) catch
                    return error.InvalidKernelJitPackageManifest;
                if (std.mem.order(u8, &previous, &key) != .lt) {
                    return error.InvalidKernelJitPackageManifest;
                }
            }
            try referenced_keys.append(allocator, key);
        }
    }
    if (referenced_keys.items.len == 0) return error.InvalidKernelJitPackageManifest;
    std.sort.heap(ArtifactKey, referenced_keys.items, {}, artifactKeyLessThan);
    var unique_count: usize = 0;
    for (referenced_keys.items) |key| {
        if (unique_count != 0 and std.mem.eql(u8, &referenced_keys.items[unique_count - 1], &key)) continue;
        referenced_keys.items[unique_count] = key;
        unique_count += 1;
    }
    if (unique_count > maximum_qualification_package_entries) {
        return error.InvalidKernelJitPackageManifest;
    }
    const keys = try allocator.dupe(ArtifactKey, referenced_keys.items[0..unique_count]);
    errdefer allocator.free(keys);
    const claims = try allocator.alloc(QualificationPackageRecordClaim, keys.len);
    errdefer allocator.free(claims);
    return .{
        .allocator = allocator,
        .keys = keys,
        .claims = claims,
        .profile_digests = profile_digests,
    };
}

fn readQualificationPackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
) !LoadedQualificationPackageManifest {
    try validateTrustedDirectory(dir, io);
    var file = dir.openFile(io, qualification_package_manifest_name, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.UnfinalizedKernelJitPackage,
        error.SymLinkLoop => return error.UntrustedKernelJitPackageManifest,
        else => return err,
    };
    defer file.close(io);
    validateTrustedFile(file, io) catch return error.UntrustedKernelJitPackageManifest;

    var reader = file.reader(io, &.{});
    const bytes = reader.interface.allocRemaining(
        allocator,
        .limited(maximum_qualification_package_manifest_bytes),
    ) catch |err| switch (err) {
        error.StreamTooLong => return error.InvalidKernelJitPackageManifest,
        error.ReadFailed => return reader.err.?,
        error.OutOfMemory => return error.OutOfMemory,
    };
    defer allocator.free(bytes);
    var header = std.json.parseFromSlice(
        struct { schema: []const u8 },
        allocator,
        bytes,
        .{ .ignore_unknown_fields = true },
    ) catch return error.InvalidKernelJitPackageManifest;
    defer header.deinit();
    if (!std.mem.eql(u8, header.value.schema, qualification_package_schema)) {
        return error.StaleKernelJitPackage;
    }
    var parsed = std.json.parseFromSlice(QualificationPackageManifest, allocator, bytes, .{}) catch
        return error.InvalidKernelJitPackageManifest;
    defer parsed.deinit();
    var provenance = try packageProvenanceFromManifest(allocator, parsed.value);
    errdefer provenance.deinit();
    try validateQualificationPackageLayout(io, dir, provenance.keys, provenance.profile_digests);
    const claim_seen = try allocator.alloc(bool, provenance.keys.len);
    defer allocator.free(claim_seen);
    @memset(claim_seen, false);
    for (parsed.value.profiles, provenance.profile_digests) |profile, digest| {
        const profile_keys = try allocator.alloc(ArtifactKey, profile.entries.len);
        defer allocator.free(profile_keys);
        for (profile.entries, profile_keys) |entry, *key| {
            key.* = parseArtifactKeyHex(entry.key) catch
                return error.InvalidKernelJitPackageManifest;
        }
        const claims = try readQualificationPackageProfile(
            allocator,
            io,
            dir,
            digest,
            parsed.value.device_registry_id,
            parsed.value.compiler_fingerprint,
            profile.model_scope_fingerprint,
            profile_keys,
        );
        defer allocator.free(claims);
        try mergeQualificationPackageClaims(
            provenance.claims,
            claim_seen,
            provenance.keys,
            claims,
        );
    }
    for (claim_seen) |seen| {
        if (!seen) return error.InvalidKernelJitPackageProfile;
    }
    return provenance;
}

fn validateQualificationPackageLayout(
    io: std.Io,
    dir: std.Io.Dir,
    keys: []const ArtifactKey,
    profile_digests: []const ArtifactKey,
) !void {
    var manifest_seen = false;
    var entry_count: usize = 0;
    var profile_count: usize = 0;
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (std.mem.eql(u8, entry.name, qualification_package_manifest_name)) {
            manifest_seen = true;
            continue;
        }
        if (std.mem.endsWith(u8, entry.name, qualification_entry_suffix)) {
            const key = qualificationEntryKey(entry.name) catch
                return error.InvalidKernelJitPackageContents;
            if (!sortedArtifactKeysContain(keys, key)) return error.InvalidKernelJitPackageContents;
            entry_count += 1;
            continue;
        }
        const digest = qualificationProfileDigest(entry.name) catch
            return error.InvalidKernelJitPackageContents;
        if (!sortedArtifactKeysContain(profile_digests, digest)) return error.InvalidKernelJitPackageContents;
        profile_count += 1;
    }
    if (!manifest_seen or entry_count != keys.len or profile_count != profile_digests.len) {
        return error.InvalidKernelJitPackageContents;
    }
}

fn readQualificationPackageProfile(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    expected_digest: ArtifactKey,
    expected_device_registry_id: u64,
    expected_compiler_fingerprint: []const u8,
    expected_model_scope_fingerprint: []const u8,
    expected_keys: []const ArtifactKey,
) ![]QualificationPackageRecordClaim {
    const name = try qualificationProfileName(allocator, expected_digest);
    defer allocator.free(name);
    var file = dir.openFile(io, name, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch return error.InvalidKernelJitPackageProfile;
    defer file.close(io);
    validateTrustedFile(file, io) catch return error.InvalidKernelJitPackageProfile;
    var reader = file.reader(io, &.{});
    const bytes = reader.interface.allocRemaining(
        allocator,
        .limited(maximum_qualification_package_profile_bytes),
    ) catch |err| switch (err) {
        error.ReadFailed => return reader.err.?,
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidKernelJitPackageProfile,
    };
    defer allocator.free(bytes);
    if (bytes.len == 0) return error.InvalidKernelJitPackageProfile;
    var digest: ArtifactKey = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &expected_digest)) return error.InvalidKernelJitPackageProfile;
    return validateQualificationPackageProfileBinding(
        allocator,
        bytes,
        expected_device_registry_id,
        expected_compiler_fingerprint,
        expected_model_scope_fingerprint,
        expected_keys,
    );
}

fn loadQualificationPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
) !LoadedQualificationPackage {
    var manifest = try readQualificationPackageManifest(allocator, io, dir);
    defer manifest.deinit();

    const payloads = try allocator.alloc([]u8, manifest.keys.len);
    var loaded: usize = 0;
    errdefer {
        for (payloads[0..loaded]) |payload| allocator.free(payload);
        allocator.free(payloads);
    }
    for (manifest.keys, manifest.claims, 0..) |key, claim, index| {
        const payload = try readQualificationPackageEntry(allocator, io, dir, key);
        errdefer allocator.free(payload);
        const record = QualificationRecord.decode(payload) catch
            return error.InvalidKernelJitPackageEntry;
        if (!record.eligible()) return error.UnqualifiedKernelJitPackageEntry;
        if (!qualificationRecordMatchesClaim(record, claim)) {
            return error.InvalidKernelJitPackageEntry;
        }
        payloads[index] = payload;
        loaded += 1;
    }
    const keys = try allocator.dupe(ArtifactKey, manifest.keys);
    errdefer allocator.free(keys);
    return .{
        .allocator = allocator,
        .keys = keys,
        .payloads = payloads,
        .profile_count = manifest.profile_digests.len,
    };
}

/// Package verification is evidence inspection, so it must never prune,
/// delete, or touch the package being inspected.
fn readQualificationPackageEntry(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    key: ArtifactKey,
) ![]u8 {
    const name = try cacheEntryName(allocator, .qualification, key);
    defer allocator.free(name);
    var file = dir.openFile(io, name, .{
        .allow_directory = false,
        .follow_symlinks = false,
        .resolve_beneath = true,
    }) catch return error.InvalidKernelJitPackageEntry;
    defer file.close(io);
    validateTrustedFile(file, io) catch return error.InvalidKernelJitPackageEntry;
    var reader = file.reader(io, &.{});
    const encoded = reader.interface.allocRemaining(
        allocator,
        .limited(cache_header_bytes + maximum_artifact_bytes),
    ) catch |err| switch (err) {
        error.ReadFailed => return reader.err.?,
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidKernelJitPackageEntry,
    };
    defer allocator.free(encoded);
    const payload = validateCacheEntry(encoded, .qualification, key) catch
        return error.InvalidKernelJitPackageEntry;
    return allocator.dupe(u8, payload);
}

fn ensureEmptyPackageDirectory(io: std.Io, dir: std.Io.Dir) !void {
    try validateTrustedDirectory(dir, io);
    var iterator = dir.iterate();
    if (try iterator.next(io) != null) return error.KernelJitPackageDestinationNotEmpty;
}

/// Exports only already-qualified cache records. The manifest is written last,
/// so interrupted exports remain unfinalized and cannot be imported.
pub fn exportQualificationPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    source_cache: *ArtifactCache,
    package_dir: std.Io.Dir,
    requested_profiles: []const QualificationPackageProfileInput,
) !QualificationPackageSummary {
    if (requested_profiles.len == 0 or requested_profiles.len > maximum_qualification_package_profiles) {
        return error.InvalidKernelJitPackageProfileCount;
    }

    const device_registry_id = requested_profiles[0].device_registry_id;
    const compiler_fingerprint = requested_profiles[0].compiler_fingerprint;
    if (!isSha256Fingerprint(compiler_fingerprint)) return error.InvalidKernelJitPackageProfile;
    const profile_digests = try allocator.alloc(ArtifactKey, requested_profiles.len);
    defer allocator.free(profile_digests);
    const profile_keys = try allocator.alloc([]ArtifactKey, requested_profiles.len);
    defer allocator.free(profile_keys);
    var initialized_profile_keys: usize = 0;
    defer for (profile_keys[0..initialized_profile_keys]) |keys| allocator.free(keys);
    const profile_claims = try allocator.alloc([]QualificationPackageRecordClaim, requested_profiles.len);
    defer allocator.free(profile_claims);
    var initialized_profile_claims: usize = 0;
    defer for (profile_claims[0..initialized_profile_claims]) |claims| allocator.free(claims);
    var referenced_keys: std.ArrayListUnmanaged(ArtifactKey) = .empty;
    defer referenced_keys.deinit(allocator);
    for (requested_profiles, 0..) |profile, index| {
        if (profile.canonical_profile.len == 0 or
            profile.canonical_profile.len > maximum_qualification_package_profile_bytes or
            profile.device_registry_id != device_registry_id or
            !std.mem.eql(u8, profile.compiler_fingerprint, compiler_fingerprint) or
            !isSha256Fingerprint(profile.model_scope_fingerprint) or
            profile.keys.len > maximum_qualification_package_entries)
        {
            return error.InvalidKernelJitPackageProfile;
        }
        std.crypto.hash.sha2.Sha256.hash(profile.canonical_profile, &profile_digests[index], .{});
        profile_keys[index] = try allocator.dupe(ArtifactKey, profile.keys);
        initialized_profile_keys += 1;
        std.sort.heap(ArtifactKey, profile_keys[index], {}, artifactKeyLessThan);
        if (profile_keys[index].len > 1) {
            for (profile_keys[index][1..], profile_keys[index][0 .. profile_keys[index].len - 1]) |key, previous| {
                if (std.mem.eql(u8, &key, &previous)) return error.DuplicateKernelJitPackageEntry;
            }
        }
        profile_claims[index] = try validateQualificationPackageProfileBinding(
            allocator,
            profile.canonical_profile,
            profile.device_registry_id,
            profile.compiler_fingerprint,
            profile.model_scope_fingerprint,
            profile_keys[index],
        );
        initialized_profile_claims += 1;
        try referenced_keys.appendSlice(allocator, profile_keys[index]);
    }
    if (referenced_keys.items.len == 0) return error.InvalidKernelJitPackageEntryCount;
    std.sort.heap(ArtifactKey, referenced_keys.items, {}, artifactKeyLessThan);
    var unique_count: usize = 0;
    for (referenced_keys.items) |key| {
        if (unique_count != 0 and std.mem.eql(u8, &referenced_keys.items[unique_count - 1], &key)) continue;
        referenced_keys.items[unique_count] = key;
        unique_count += 1;
    }
    if (unique_count > maximum_qualification_package_entries) return error.InvalidKernelJitPackageEntryCount;
    const keys = referenced_keys.items[0..unique_count];
    const claims = try allocator.alloc(QualificationPackageRecordClaim, keys.len);
    defer allocator.free(claims);
    const claim_seen = try allocator.alloc(bool, keys.len);
    defer allocator.free(claim_seen);
    @memset(claim_seen, false);
    for (profile_claims) |profile_claim_set| {
        try mergeQualificationPackageClaims(claims, claim_seen, keys, profile_claim_set);
    }
    for (claim_seen) |seen| {
        if (!seen) return error.InvalidKernelJitPackageProfile;
    }

    const profile_order = try allocator.alloc(usize, requested_profiles.len);
    defer allocator.free(profile_order);
    for (profile_order, 0..) |*slot, index| slot.* = index;
    std.sort.heap(usize, profile_order, profile_digests, profileIndexLessThan);
    for (profile_order[1..], profile_order[0 .. profile_order.len - 1]) |index, previous| {
        if (std.mem.eql(u8, &profile_digests[index], &profile_digests[previous])) {
            return error.DuplicateKernelJitPackageProfile;
        }
    }

    try ensureEmptyPackageDirectory(io, package_dir);

    const payloads = try allocator.alloc([]u8, keys.len);
    defer allocator.free(payloads);
    var loaded: usize = 0;
    defer for (payloads[0..loaded]) |payload| allocator.free(payload);
    for (keys, claims, 0..) |key, claim, index| {
        const payload = (try source_cache.load(.qualification, key)) orelse
            return error.MissingKernelJitQualification;
        errdefer allocator.free(payload);
        const record = QualificationRecord.decode(payload) catch
            return error.InvalidKernelJitQualification;
        if (!record.eligible()) return error.UnqualifiedKernelJitPackageEntry;
        if (!qualificationRecordMatchesClaim(record, claim)) {
            return error.InvalidKernelJitQualification;
        }
        payloads[index] = payload;
        loaded += 1;
    }

    var package_cache = try ArtifactCache.initDir(
        allocator,
        io,
        package_dir,
        qualification_package_cache_budget,
    );
    defer package_cache.deinit();
    for (keys, payloads) |key, payload| {
        try package_cache.store(.qualification, key, payload);
    }

    var referenced_key_count: usize = 0;
    for (profile_keys) |profile_key_set| {
        referenced_key_count = std.math.add(usize, referenced_key_count, profile_key_set.len) catch
            return error.InvalidKernelJitPackageManifest;
    }
    const key_hex = try allocator.alloc([cache_key_hex_bytes]u8, referenced_key_count);
    defer allocator.free(key_hex);
    const entries = try allocator.alloc(QualificationPackageManifestEntry, referenced_key_count);
    defer allocator.free(entries);
    const digest_hex = try allocator.alloc([cache_key_hex_bytes]u8, requested_profiles.len);
    defer allocator.free(digest_hex);
    const manifest_profiles = try allocator.alloc(QualificationPackageManifestProfile, requested_profiles.len);
    defer allocator.free(manifest_profiles);
    var entry_cursor: usize = 0;
    for (profile_order, 0..) |profile_index, manifest_index| {
        const start = entry_cursor;
        for (profile_keys[profile_index]) |key| {
            key_hex[entry_cursor] = artifactKeyHex(key);
            entries[entry_cursor] = .{ .key = &key_hex[entry_cursor] };
            entry_cursor += 1;
        }
        digest_hex[manifest_index] = artifactKeyHex(profile_digests[profile_index]);
        manifest_profiles[manifest_index] = .{
            .canonical_profile_sha256 = &digest_hex[manifest_index],
            .model_scope_fingerprint = requested_profiles[profile_index].model_scope_fingerprint,
            .entries = entries[start..entry_cursor],
        };
    }
    const manifest = QualificationPackageManifest{
        .schema = qualification_package_schema,
        .codegen_abi_version = codegen_abi_version,
        .qualification_policy_version = qualification_policy_version,
        .qualification_policy_identity = qualification_policy_identity,
        .device_registry_id = device_registry_id,
        .compiler_fingerprint = compiler_fingerprint,
        .profiles = manifest_profiles,
    };
    const manifest_bytes = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(manifest_bytes);
    if (manifest_bytes.len > maximum_qualification_package_manifest_bytes) {
        return error.InvalidKernelJitPackageManifest;
    }

    package_cache.mutex.lockUncancelable(io);
    defer package_cache.mutex.unlock(io);
    try validateTrustedDirectory(package_dir, io);
    for (profile_order) |profile_index| {
        const name = try qualificationProfileName(allocator, profile_digests[profile_index]);
        defer allocator.free(name);
        try package_cache.writeFileAtomicUnlocked(name, requested_profiles[profile_index].canonical_profile);
    }
    try package_cache.writeFileAtomicUnlocked(qualification_package_manifest_name, manifest_bytes);
    return .{ .entries = keys.len, .profiles = requested_profiles.len };
}

pub fn verifyQualificationPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_dir: std.Io.Dir,
) !QualificationPackageSummary {
    var package = try loadQualificationPackage(allocator, io, package_dir);
    defer package.deinit();
    return .{ .entries = package.keys.len, .profiles = package.profile_count };
}

pub fn importQualificationPackage(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_dir: std.Io.Dir,
    destination_cache: *ArtifactCache,
) !QualificationPackageSummary {
    // Validate and materialize the entire package before touching the target.
    // Cache files are then committed atomically per content-addressed entry; a
    // storage failure may leave a safe imported prefix, never an unvalidated
    // or partially written record.
    var package = try loadQualificationPackage(allocator, io, package_dir);
    defer package.deinit();
    for (package.keys, package.payloads) |key, payload| {
        try destination_cache.store(.qualification, key, payload);
    }
    return .{ .entries = package.keys.len, .profiles = package.profile_count };
}

fn openQualificationPackagePath(io: std.Io, path: []const u8) !std.Io.Dir {
    if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) {
        return error.InvalidKernelJitPackagePath;
    }
    return std.Io.Dir.cwd().openDir(io, path, .{
        .iterate = true,
        .follow_symlinks = false,
    });
}

pub fn exportQualificationPackagePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    source_cache: *ArtifactCache,
    package_path: []const u8,
    profiles: []const QualificationPackageProfileInput,
) !QualificationPackageSummary {
    if (package_path.len == 0 or std.mem.indexOfScalar(u8, package_path, 0) != null) {
        return error.InvalidKernelJitPackagePath;
    }
    var dir = try std.Io.Dir.cwd().createDirPathOpen(io, package_path, .{
        .permissions = privateCachePermissions(true),
        .open_options = .{
            .iterate = true,
            .follow_symlinks = false,
        },
    });
    defer dir.close(io);
    return exportQualificationPackage(allocator, io, source_cache, dir, profiles);
}

pub fn verifyQualificationPackagePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_path: []const u8,
) !QualificationPackageSummary {
    var dir = try openQualificationPackagePath(io, package_path);
    defer dir.close(io);
    return verifyQualificationPackage(allocator, io, dir);
}

pub fn importQualificationPackagePath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_path: []const u8,
    destination_cache: *ArtifactCache,
) !QualificationPackageSummary {
    var dir = try openQualificationPackagePath(io, package_path);
    defer dir.close(io);
    return importQualificationPackage(allocator, io, dir, destination_cache);
}

test "kernel JIT defaults are dormant" {
    const config = Config{};
    try config.validate();
    try std.testing.expect(!config.mode.compiles());
    try std.testing.expect(!config.mode.activates());
    try std.testing.expectEqual(@as(usize, 1024 * 1024 * 1024), try config.maxCacheBytes());
    try std.testing.expect(!LoadContext.dynamic.allowsQualification());
    try std.testing.expect(LoadContext.startup_preload.allowsQualification());
}

test "kernel JIT profile capture preserves bundled baselines" {
    try std.testing.expectEqual(Mode.shadow, try resolveProfileCaptureMode(.off, false, true, false));
    try std.testing.expectEqual(Mode.shadow, try resolveProfileCaptureMode(.shadow, true, true, false));
    try std.testing.expectError(
        error.KernelJitProfileCaptureRequiresShadow,
        resolveProfileCaptureMode(.off, true, true, false),
    );
    try std.testing.expectError(
        error.KernelJitProfileCaptureRequiresShadow,
        resolveProfileCaptureMode(.on, true, true, false),
    );
    try std.testing.expectError(
        error.KernelJitProfileCaptureRequiresShadow,
        resolveProfileCaptureMode(.required, true, true, false),
    );
    try std.testing.expectError(
        error.KernelJitQualifiedProfileCaptureConflict,
        resolveProfileCaptureMode(.shadow, true, true, true),
    );
    try validateMetalProfileBackend(true, true, false);
    try std.testing.expectError(
        error.KernelJitProfileRequiresMetalBackend,
        validateMetalProfileBackend(false, false, true),
    );

    try (Config{ .mode = .shadow, .profile_capture_only = true }).validate();
    try std.testing.expectError(
        error.KernelJitProfileCaptureRequiresShadow,
        (Config{ .mode = .on, .profile_capture_only = true }).validate(),
    );
    try std.testing.expectError(
        error.KernelJitQualifiedProfileCaptureConflict,
        (Config{
            .mode = .shadow,
            .profile_capture_only = true,
            .qualified_profile_path = "/tmp/qualified-profile.json",
        }).validate(),
    );
    try std.testing.expectError(
        error.KernelJitProfileCaptureRequiresCache,
        (Config{ .mode = .shadow, .profile_capture_only = true, .max_cache_bytes_mb = 0 }).validate(),
    );
}

test "kernel JIT required failures cannot be hidden by backend fallback" {
    try std.testing.expect(!isRequiredFailure(.on, error.CudaJitRequiredRouteFailed));
    try std.testing.expect(isRequiredFailure(.required, error.CudaJitRequiredRouteFailed));
    try std.testing.expect(isRequiredFailure(.required, error.MetalJitRequiredRouteFailed));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitRequiredBackendUnavailable));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitRequiredDynamicLoad));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitRequiredPreloadMissing));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitRequiredPreloadUnmaterialized));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitRequiredOptionalSessionUnmaterialized));
    try std.testing.expect(isRequiredFailure(.required, error.KernelJitUnsupportedPlatform));
    try std.testing.expect(isRequiredFailure(.required, error.InvalidKernelJitPreloadBudget));
    try std.testing.expect(!isRequiredFailure(.required, error.InvalidModel));
}

test "kernel JIT config rejects unsafe values" {
    try std.testing.expectError(error.InvalidKernelJitCacheDir, (Config{ .cache_dir = "" }).validate());
    try std.testing.expectError(error.InvalidKernelJitQualifiedProfilePath, (Config{
        .mode = .shadow,
        .qualified_profile_path = "",
    }).validate());
    try std.testing.expectError(error.KernelJitQualifiedProfileRequiresCache, (Config{
        .qualified_profile_path = "/tmp/profile.json",
    }).validate());
    try std.testing.expectError(error.KernelJitQualifiedProfileRequiresCache, (Config{
        .mode = .on,
        .qualified_profile_path = "/tmp/profile.json",
        .max_cache_bytes_mb = 0,
    }).validate());
    try std.testing.expectError(error.InvalidKernelJitPreloadBudget, (Config{ .preload_budget_ms = 999 }).validate());
    try std.testing.expectError(error.InvalidKernelJitPreloadBudget, (Config{ .preload_budget_ms = 3_600_001 }).validate());
    try std.testing.expectError(error.InvalidKernelJitCacheBudget, (Config{ .max_cache_bytes_mb = 1024 * 1024 + 1 }).validate());
}

test "kernel JIT cache directory resolution is owned explicit and default-off safe" {
    const explicit = (try (Config{ .cache_dir = "/var/cache/antfly-jit" }).resolveCacheDir(std.testing.allocator, "/ignored")).?;
    defer std.testing.allocator.free(explicit);
    try std.testing.expectEqualStrings("/var/cache/antfly-jit", explicit);

    const implicit = (try (Config{}).resolveCacheDir(std.testing.allocator, "/home/antfly")).?;
    defer std.testing.allocator.free(implicit);
    try std.testing.expectEqualStrings("/home/antfly/.antfly/inference/jit", implicit);
    try std.testing.expectEqual(@as(?[]u8, null), try (Config{}).resolveCacheDir(std.testing.allocator, null));
    try std.testing.expectEqual(@as(?[]u8, null), try (Config{ .max_cache_bytes_mb = 0 }).resolveCacheDir(std.testing.allocator, "/home/antfly"));
}

test "kernel JIT winner requires correctness and stable two percent speedup" {
    var candidates = [_]CandidateEvidence{
        .{ .candidate_index = 0, .correctness_passed = true, .measured_speedup = 1.40, .minimum_repeat_speedup = 1.019 },
        .{ .candidate_index = 1, .correctness_passed = false, .measured_speedup = 2.0, .minimum_repeat_speedup = 2.0 },
        .{ .candidate_index = 2, .correctness_passed = true, .measured_speedup = 1.20, .minimum_repeat_speedup = 1.021 },
        .{ .candidate_index = 3, .correctness_passed = true, .measured_speedup = 1.30, .minimum_repeat_speedup = 1.02 },
    };
    try std.testing.expect(!qualifies(.{ .candidate_index = 4, .correctness_passed = true, .measured_speedup = 1.019, .minimum_repeat_speedup = 2.0 }));
    try std.testing.expect(!qualifies(.{ .candidate_index = 5, .correctness_passed = true, .measured_speedup = 2.0, .minimum_repeat_speedup = 1.019 }));
    try std.testing.expect(qualifies(.{ .candidate_index = 6, .correctness_passed = true, .measured_speedup = 1.02, .minimum_repeat_speedup = 1.02 }));
    const winner = selectWinner(&candidates) orelse return error.MissingKernelJitWinner;
    try std.testing.expectEqual(@as(usize, 3), winner.candidate_index);

    candidates[winner.candidate_index].correctness_passed = false;
    const fallback = selectWinner(&candidates) orelse return error.MissingKernelJitWinner;
    try std.testing.expectEqual(@as(usize, 2), fallback.candidate_index);
}

test "kernel JIT evidence uses paired median and worst repeat" {
    const evidence = try evidenceFromPairedNanos(
        2,
        true,
        &.{ 120, 110, 140, 130, 150 },
        &.{ 100, 100, 100, 100, 100 },
    );
    try std.testing.expectApproxEqAbs(@as(f64, 1.3), evidence.measured_speedup, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.1), evidence.minimum_repeat_speedup, 0.000001);
    try std.testing.expect(qualifies(evidence));
    try std.testing.expectError(
        error.InvalidKernelJitMeasurements,
        evidenceFromPairedNanos(0, true, &.{ 1, 2 }, &.{ 1, 2 }),
    );
    try std.testing.expectError(
        error.InvalidKernelJitMeasurements,
        evidenceFromPairedNanos(0, true, &.{ 1, 2, 3, 4, 5 }, &.{ 1, 2, 0, 4, 5 }),
    );
}

test "kernel JIT qualification records are stable validated cache payloads" {
    const record = QualificationRecord{
        .candidate_index = 3,
        .repeat_count = measurement_repeats,
        .correctness_passed = true,
        .measured_speedup = 1.24,
        .minimum_repeat_speedup = 1.12,
        .max_absolute_error = 0.0001,
        .max_relative_error = 0.001,
    };
    const encoded = try record.encode();
    const decoded = try QualificationRecord.decode(&encoded);
    try std.testing.expect(decoded.eligible());
    try std.testing.expectEqual(record.candidate_index, decoded.candidate_index);
    try std.testing.expectEqual(record.repeat_count, decoded.repeat_count);
    try std.testing.expectEqual(record.measured_speedup, decoded.measured_speedup);

    var corrupted = encoded;
    corrupted[8] +%= 1;
    try std.testing.expectError(error.InvalidKernelJitQualification, QualificationRecord.decode(&corrupted));
    var too_short = record;
    too_short.repeat_count = measurement_repeats - 1;
    try std.testing.expectError(error.InvalidKernelJitQualification, too_short.encode());
    var too_long = record;
    too_long.repeat_count = maximum_measurement_repeats + 1;
    try std.testing.expectError(error.InvalidKernelJitQualification, too_long.encode());
    var invalid_candidate = record;
    invalid_candidate.candidate_index = maximum_candidates;
    try std.testing.expectError(error.InvalidKernelJitQualification, invalid_candidate.encode());
}

test "kernel JIT artifact identity includes source target compiler and baseline" {
    const base = ArtifactKeyInput{
        .backend = .cuda,
        .semantic_identity = "q4_0/mmv/none",
        .runtime_identity = "rows=1,in=1536,out=6144",
        .target_identity = "sm_89",
        .device_identity = "gpu-0",
        .schedule_identity = "threads=256,cols=4",
        .baseline_identity = "termite_q4_0_tile4/v1",
        .compiler_identity = "nvrtc/13.2",
        .compiler_options = "--gpu-architecture=compute_89",
        .source = "extern \"C\" __global__ void k() {}",
    };
    const first = artifactKey(base);
    const repeated = artifactKey(base);
    try std.testing.expectEqualSlices(u8, &first, &repeated);

    var changed = base;
    changed.source = "extern \"C\" __global__ void k2() {}";
    const source_changed = artifactKey(changed);
    try std.testing.expect(!std.mem.eql(u8, &first, &source_changed));
    changed = base;
    changed.baseline_identity = "termite_q4_0_tile4/v2";
    const baseline_changed = artifactKey(changed);
    try std.testing.expect(!std.mem.eql(u8, &first, &baseline_changed));

    const compile_key = compileArtifactKey(base);
    changed = base;
    changed.baseline_identity = "termite_q4_0_tile4/v2";
    changed.device_identity = "gpu-1";
    const compile_evidence_changed = compileArtifactKey(changed);
    try std.testing.expectEqualSlices(u8, &compile_key, &compile_evidence_changed);
    changed.source = "extern \"C\" __global__ void k2() {}";
    const compile_source_changed = compileArtifactKey(changed);
    try std.testing.expect(!std.mem.eql(u8, &compile_key, &compile_source_changed));
    try std.testing.expect(!std.mem.eql(u8, &first, &compile_key));

    const hex = artifactKeyHex(first);
    try std.testing.expectEqual(@as(usize, 64), hex.len);
}

fn testArtifactKey(source: []const u8) ArtifactKey {
    return artifactKey(.{
        .backend = .cuda,
        .semantic_identity = "test",
        .runtime_identity = "rows=1",
        .target_identity = "sm_test",
        .device_identity = "device",
        .schedule_identity = "canonical",
        .baseline_identity = "baseline",
        .compiler_identity = "compiler",
        .compiler_options = "options",
        .source = source,
    });
}

test "kernel JIT cache round trips only checksummed content-addressed entries" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer cache.deinit();

    const key = testArtifactKey("kernel-a");
    try std.testing.expectEqual(@as(?[]u8, null), try cache.load(.cuda_ptx, key));
    try cache.store(.cuda_ptx, key, "ptx-data\x00");
    const loaded = (try cache.load(.cuda_ptx, key)) orelse return error.MissingKernelJitCacheEntry;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("ptx-data\x00", loaded);

    const stats = cache.snapshotStats();
    try std.testing.expectEqual(@as(usize, 1), stats.misses);
    try std.testing.expectEqual(@as(usize, 1), stats.stores);
    try std.testing.expectEqual(@as(usize, 1), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.corruptions);
}

test "kernel JIT cache rejects directories writable by another principal" {
    if (comptime !std.Io.File.Permissions.has_executable_bit or
        (builtin.os.tag != .linux and builtin.os.tag != .macos)) return;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try tmp.dir.setPermissions(std.testing.io, .fromMode(0o777));
    defer tmp.dir.setPermissions(std.testing.io, .fromMode(0o700)) catch {};

    try std.testing.expectError(
        error.UntrustedKernelJitCacheDirectory,
        ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024),
    );
}

test "kernel JIT cache rejects and removes corrupted bytes" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer cache.deinit();

    const key = testArtifactKey("kernel-corrupt");
    try cache.store(.cuda_ptx, key, "valid");
    const name = try cacheEntryName(std.testing.allocator, .cuda_ptx, key);
    defer std.testing.allocator.free(name);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = "not-a-cache-entry" });
    try std.testing.expectEqual(@as(?[]u8, null), try cache.load(.cuda_ptx, key));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, name, .{}));
    const stats = cache.snapshotStats();
    try std.testing.expectEqual(@as(usize, 1), stats.corruptions);
}

test "kernel JIT cache rejects and removes oversized entries" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer cache.deinit();

    const key = testArtifactKey("kernel-oversized");
    const name = try cacheEntryName(std.testing.allocator, .cuda_ptx, key);
    defer std.testing.allocator.free(name);
    var file = try tmp.dir.createFile(std.testing.io, name, .{});
    {
        defer file.close(std.testing.io);
        var buffer: [1]u8 = undefined;
        var writer = file.writer(std.testing.io, &buffer);
        try writer.seekTo(cache_header_bytes + maximum_artifact_bytes);
        try writer.interface.writeAll(&.{0});
        try writer.end();
    }

    try std.testing.expectEqual(@as(?[]u8, null), try cache.load(.cuda_ptx, key));
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, name, .{}));
    try std.testing.expectEqual(@as(usize, 1), cache.snapshotStats().corruptions);
}

test "kernel JIT cache enforces its byte budget and zero disables persistence" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const one_entry_budget = cache_header_bytes + 8;
    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, one_entry_budget);
    defer cache.deinit();
    try cache.store(.cuda_ptx, testArtifactKey("one"), "12345678");
    try cache.store(.cuda_ptx, testArtifactKey("two"), "abcdefgh");
    try std.testing.expect(cache.snapshotStats().evictions >= 1);

    var disabled_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer disabled_tmp.cleanup();
    var disabled = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, disabled_tmp.dir, 0);
    defer disabled.deinit();
    const key = testArtifactKey("disabled");
    try disabled.store(.cuda_ptx, key, "ignored");
    try std.testing.expectEqual(@as(?[]u8, null), try disabled.load(.cuda_ptx, key));
    try std.testing.expectEqual(@as(usize, 0), disabled.snapshotStats().stores);
}

test "kernel JIT cache enforces a reduced byte budget when opened" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const first_key = testArtifactKey("preexisting-one");
    const second_key = testArtifactKey("preexisting-two");
    var writer = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    try writer.store(.cuda_ptx, first_key, "12345678");
    try writer.store(.cuda_ptx, second_key, "abcdefgh");
    writer.deinit();

    var reopened = try ArtifactCache.initDir(
        std.testing.allocator,
        std.testing.io,
        tmp.dir,
        cache_header_bytes + 8,
    );
    defer reopened.deinit();
    try std.testing.expectEqual(@as(usize, 1), reopened.snapshotStats().evictions);
    const first = try reopened.load(.cuda_ptx, first_key);
    defer if (first) |value| std.testing.allocator.free(value);
    const second = try reopened.load(.cuda_ptx, second_key);
    defer if (second) |value| std.testing.allocator.free(value);
    try std.testing.expect((first == null) != (second == null));
}

test "kernel JIT cache cleanup ignores unrelated jit files" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "unrelated.jit",
        .data = "not an Antfly cache artifact",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = ".unrelated.jit.tmp-abandoned",
        .data = "not an Antfly cache temporary",
    });

    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1);
    defer cache.deinit();
    _ = try tmp.dir.statFile(std.testing.io, "unrelated.jit", .{});
    _ = try tmp.dir.statFile(std.testing.io, ".unrelated.jit.tmp-abandoned", .{});
    try std.testing.expectEqual(@as(usize, 0), cache.snapshotStats().evictions);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshotStats().stale_temps_removed);
}

test "kernel JIT cache never removes another writer's temporary file" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const key = testArtifactKey("shared-key");
    const entry_name = try cacheEntryName(std.testing.allocator, .cuda_ptx, key);
    defer std.testing.allocator.free(entry_name);
    const foreign_temp = try std.fmt.allocPrint(std.testing.allocator, ".{s}.tmp-live-writer", .{entry_name});
    defer std.testing.allocator.free(foreign_temp);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = foreign_temp,
        .data = "in-progress",
    });
    var first = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer first.deinit();
    var second = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer second.deinit();
    try first.store(.cuda_ptx, key, "first\x00");
    try second.store(.cuda_ptx, key, "second\x00");
    const loaded = (try first.load(.cuda_ptx, key)) orelse return error.MissingKernelJitCacheEntry;
    defer std.testing.allocator.free(loaded);
    try std.testing.expectEqualStrings("second\x00", loaded);
    _ = try tmp.dir.statFile(std.testing.io, foreign_temp, .{});
}

test "kernel JIT cache reclaims stale temporary files when opened" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const entry_name = try cacheEntryName(std.testing.allocator, .cuda_ptx, testArtifactKey("stale"));
    defer std.testing.allocator.free(entry_name);
    const stale_name = try std.fmt.allocPrint(std.testing.allocator, ".{s}.tmp-abandoned", .{entry_name});
    defer std.testing.allocator.free(stale_name);
    var stale = try tmp.dir.createFile(std.testing.io, stale_name, .{});
    try stale.writeStreamingAll(std.testing.io, "stale");
    const old = std.Io.Timestamp.now(std.testing.io, .real).subDuration(.{
        .nanoseconds = stale_temp_age_ns + std.time.ns_per_hour,
    });
    try stale.setTimestamps(std.testing.io, .{
        .access_timestamp = .{ .new = old },
        .modify_timestamp = .{ .new = old },
    });
    stale.close(std.testing.io);

    var cache = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, tmp.dir, 1024 * 1024);
    defer cache.deinit();

    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(std.testing.io, stale_name, .{}));
    try std.testing.expectEqual(@as(usize, 1), cache.snapshotStats().stale_temps_removed);
}

fn testEligibleQualificationRecord() QualificationRecord {
    return .{
        .candidate_index = 0,
        .repeat_count = measurement_repeats,
        .correctness_passed = true,
        .measured_speedup = minimum_speedup + 0.1,
        .minimum_repeat_speedup = minimum_speedup + 0.01,
        .max_absolute_error = 0.0001,
        .max_relative_error = 0.001,
    };
}

fn storeTestQualification(cache: *ArtifactCache, key: ArtifactKey) !void {
    try storeTestQualificationRecord(cache, key, testEligibleQualificationRecord());
}

fn storeTestQualificationRecord(
    cache: *ArtifactCache,
    key: ArtifactKey,
    record: QualificationRecord,
) !void {
    const encoded = try record.encode();
    try cache.store(.qualification, key, &encoded);
}

const test_package_compiler_fingerprint = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const test_package_model_fingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222";

const TestQualificationPackageProfile = struct {
    json: []u8,
    input: QualificationPackageProfileInput,

    fn deinit(self: *TestQualificationPackageProfile) void {
        std.testing.allocator.free(self.json);
        self.* = undefined;
    }
};

fn testQualificationPackageProfileForModel(
    keys: []const ArtifactKey,
    model_fingerprint: []const u8,
) !TestQualificationPackageProfile {
    const key_hex = try std.testing.allocator.alloc([cache_key_hex_bytes]u8, keys.len);
    defer std.testing.allocator.free(key_hex);
    const entries = try std.testing.allocator.alloc(workload_profile_policy.ProfileEntry, keys.len);
    defer std.testing.allocator.free(entries);
    const claims = try std.testing.allocator.alloc(QualificationPackageProfileKeyClaim, keys.len);
    defer std.testing.allocator.free(claims);
    const record = testEligibleQualificationRecord();
    for (keys, key_hex, entries, claims, 0..) |key, *hex, *entry, *claim, index| {
        hex.* = artifactKeyHex(key);
        entry.* = .{
            .regime = "encoder",
            .quant_format = "q4_0",
            .dispatch = "small_batch",
            .implementation = "bundled",
            .rows = 2,
            .in_dim = 256,
            .out_dim = @intCast(512 + index),
            .input_encoding = "f32",
            .output_encoding = "f32",
            .epilogue = "none",
            .fusion = "none",
            .layout = "row_major",
            .call_count = 1,
            .logical_bytes = 1,
            .macs = 1,
            .estimated_gpu_nanos = 100,
            .calibrated = true,
        };
        claim.* = .{
            .artifact_key = hex,
            .regime = entry.regime,
            .quant_format = entry.quant_format,
            .dispatch = entry.dispatch,
            .implementation = entry.implementation,
            .rows = entry.rows,
            .in_dim = entry.in_dim,
            .out_dim = entry.out_dim,
            .input_encoding = entry.input_encoding,
            .output_encoding = entry.output_encoding,
            .epilogue = entry.epilogue,
            .fusion = entry.fusion,
            .layout = entry.layout,
            .candidate_index = @intCast(record.candidate_index),
            .threads_per_threadgroup = 32,
            .cols_per_threadgroup = 1,
            .reduction = "simd_sum",
            .measured_speedup = record.measured_speedup,
            .minimum_repeat_speedup = record.minimum_repeat_speedup,
        };
    }
    const count = std.math.cast(u8, keys.len) orelse return error.InvalidKernelJitPackageEntryCount;
    const json = try std.json.Stringify.valueAlloc(
        std.testing.allocator,
        QualificationPackageProfileBinding{
            .schema = workload_profile_schema,
            .complete = true,
            .device_registry_id = 42,
            .compiler_fingerprint = test_package_compiler_fingerprint,
            .model_fingerprint = model_fingerprint,
            .entries = entries,
            .qualified_kernels = claims,
            .tuning_selected_signatures = count,
            .tuning_qualified_signatures = count,
            .tuning_coverage_complete = count != 0,
        },
        .{ .whitespace = .indent_2 },
    );
    return .{
        .json = json,
        .input = .{
            .canonical_profile = json,
            .device_registry_id = 42,
            .compiler_fingerprint = test_package_compiler_fingerprint,
            .model_scope_fingerprint = model_fingerprint,
            .keys = keys,
        },
    };
}

fn testQualificationPackageProfile(keys: []const ArtifactKey) !TestQualificationPackageProfile {
    return testQualificationPackageProfileForModel(keys, test_package_model_fingerprint);
}

fn testWeightedPackageEntry(cost: u64, out_dim: u32) workload_profile_policy.ProfileEntry {
    return .{
        .regime = "encoder",
        .quant_format = "q4_0",
        .dispatch = "small_batch",
        .implementation = "bundled",
        .rows = 2,
        .in_dim = 256,
        .out_dim = out_dim,
        .input_encoding = "f32",
        .output_encoding = "f32",
        .epilogue = "none",
        .fusion = "none",
        .layout = "row_major",
        .call_count = 1,
        .logical_bytes = 1,
        .macs = 1,
        .estimated_gpu_nanos = cost,
        .calibrated = true,
    };
}

fn testWeightedPackageClaim(
    entry: workload_profile_policy.ProfileEntry,
    artifact_key: []const u8,
) QualificationPackageProfileKeyClaim {
    const record = testEligibleQualificationRecord();
    return .{
        .artifact_key = artifact_key,
        .regime = entry.regime,
        .quant_format = entry.quant_format,
        .dispatch = entry.dispatch,
        .implementation = entry.implementation,
        .rows = entry.rows,
        .in_dim = entry.in_dim,
        .out_dim = entry.out_dim,
        .input_encoding = entry.input_encoding,
        .output_encoding = entry.output_encoding,
        .epilogue = entry.epilogue,
        .fusion = entry.fusion,
        .layout = entry.layout,
        .candidate_index = @intCast(record.candidate_index),
        .threads_per_threadgroup = 32,
        .cols_per_threadgroup = 1,
        .reduction = "simd_sum",
        .measured_speedup = record.measured_speedup,
        .minimum_repeat_speedup = record.minimum_repeat_speedup,
    };
}

fn testQualificationPackageProfileDigest(profile: QualificationPackageProfileInput) ArtifactKey {
    var digest: ArtifactKey = undefined;
    std.crypto.hash.sha2.Sha256.hash(profile.canonical_profile, &digest, .{});
    return digest;
}

fn writeTestQualificationPackageManifest(
    dir: std.Io.Dir,
    policy_version: u32,
    policy_identity: []const u8,
    entries: []const QualificationPackageManifestEntry,
    profile: QualificationPackageProfileInput,
) !void {
    const profile_digest_hex = artifactKeyHex(testQualificationPackageProfileDigest(profile));
    const profiles = [_]QualificationPackageManifestProfile{.{
        .canonical_profile_sha256 = &profile_digest_hex,
        .model_scope_fingerprint = profile.model_scope_fingerprint,
        .entries = entries,
    }};
    const manifest_bytes = try std.json.Stringify.valueAlloc(
        std.testing.allocator,
        QualificationPackageManifest{
            .schema = qualification_package_schema,
            .codegen_abi_version = codegen_abi_version,
            .qualification_policy_version = policy_version,
            .qualification_policy_identity = policy_identity,
            .device_registry_id = profile.device_registry_id,
            .compiler_fingerprint = profile.compiler_fingerprint,
            .profiles = &profiles,
        },
        .{ .whitespace = .indent_2 },
    );
    defer std.testing.allocator.free(manifest_bytes);
    try dir.writeFile(std.testing.io, .{
        .sub_path = qualification_package_manifest_name,
        .data = manifest_bytes,
    });
}

test "kernel JIT package replays weighted backfill coverage and full profile structure" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var accepted_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer accepted_tmp.cleanup();
    var rejected_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer rejected_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const keys = [_]ArtifactKey{
        testArtifactKey("weighted-rank-1"),
        testArtifactKey("weighted-rank-2"),
        testArtifactKey("weighted-rank-3"),
        testArtifactKey("weighted-rank-5"),
    };
    for (keys) |key| try storeTestQualification(&source, key);
    const entries = [_]workload_profile_policy.ProfileEntry{
        testWeightedPackageEntry(50, 500),
        testWeightedPackageEntry(20, 501),
        testWeightedPackageEntry(9, 502),
        testWeightedPackageEntry(8, 503),
        testWeightedPackageEntry(8, 504),
        testWeightedPackageEntry(1, 505),
    };
    var key_hex: [keys.len][cache_key_hex_bytes]u8 = undefined;
    for (keys, &key_hex) |key, *hex| hex.* = artifactKeyHex(key);
    const accepted_claims = [_]QualificationPackageProfileKeyClaim{
        testWeightedPackageClaim(entries[0], &key_hex[0]),
        testWeightedPackageClaim(entries[1], &key_hex[1]),
        testWeightedPackageClaim(entries[2], &key_hex[2]),
        testWeightedPackageClaim(entries[4], &key_hex[3]),
    };
    var accepted_report = workload_profile_policy.ProfileReport{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = test_package_compiler_fingerprint,
        .model_fingerprint = test_package_model_fingerprint,
        .entries = &entries,
        .qualified_kernels = &accepted_claims,
        .tuning_selected_signatures = 5,
        .tuning_qualified_signatures = 4,
        .tuning_failed_signatures = 1,
        .tuning_coverage_complete = true,
    };
    const accepted_json = try std.json.Stringify.valueAlloc(
        std.testing.allocator,
        accepted_report,
        .{ .whitespace = .indent_2 },
    );
    defer std.testing.allocator.free(accepted_json);
    const accepted_input = QualificationPackageProfileInput{
        .canonical_profile = accepted_json,
        .device_registry_id = accepted_report.device_registry_id,
        .compiler_fingerprint = accepted_report.compiler_fingerprint,
        .model_scope_fingerprint = accepted_report.model_fingerprint,
        .keys = &keys,
    };
    const exported = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        accepted_tmp.dir,
        &.{accepted_input},
    );
    try std.testing.expectEqual(@as(usize, 4), exported.entries);
    const verified = try verifyQualificationPackage(std.testing.allocator, std.testing.io, accepted_tmp.dir);
    try std.testing.expectEqual(exported.entries, verified.entries);

    const rejected_claims = accepted_claims[0..3];
    var rejected_report = accepted_report;
    rejected_report.qualified_kernels = rejected_claims;
    rejected_report.tuning_qualified_signatures = 3;
    rejected_report.tuning_failed_signatures = 2;
    rejected_report.tuning_coverage_complete = false;
    const rejected_json = try std.json.Stringify.valueAlloc(
        std.testing.allocator,
        rejected_report,
        .{ .whitespace = .indent_2 },
    );
    defer std.testing.allocator.free(rejected_json);
    var rejected_input = accepted_input;
    rejected_input.canonical_profile = rejected_json;
    rejected_input.keys = keys[0..3];
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            rejected_tmp.dir,
            &.{rejected_input},
        ),
    );

    rejected_report.tuning_coverage_complete = true;
    const forged_json = try std.json.Stringify.valueAlloc(std.testing.allocator, rejected_report, .{});
    defer std.testing.allocator.free(forged_json);
    rejected_input.canonical_profile = forged_json;
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        validateQualificationPackageProfileBinding(
            std.testing.allocator,
            forged_json,
            rejected_input.device_registry_id,
            rejected_input.compiler_fingerprint,
            rejected_input.model_scope_fingerprint,
            rejected_input.keys,
        ),
    );

    accepted_report.schema = "antfly.workload-profile.v1";
    const legacy_json = try std.json.Stringify.valueAlloc(std.testing.allocator, accepted_report, .{});
    defer std.testing.allocator.free(legacy_json);
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        validateQualificationPackageProfileBinding(
            std.testing.allocator,
            legacy_json,
            accepted_input.device_registry_id,
            accepted_input.compiler_fingerprint,
            accepted_input.model_scope_fingerprint,
            accepted_input.keys,
        ),
    );

    accepted_report.schema = workload_profile_schema;
    var malformed_claims = accepted_claims;
    malformed_claims[0].threads_per_threadgroup = 0;
    accepted_report.qualified_kernels = &malformed_claims;
    const malformed_json = try std.json.Stringify.valueAlloc(std.testing.allocator, accepted_report, .{});
    defer std.testing.allocator.free(malformed_json);
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        validateQualificationPackageProfileBinding(
            std.testing.allocator,
            malformed_json,
            accepted_input.device_registry_id,
            accepted_input.compiler_fingerprint,
            accepted_input.model_scope_fingerprint,
            accepted_input.keys,
        ),
    );
}

test "kernel JIT qualification package round trips finalized exact cache entries" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const first = testArtifactKey("package-first");
    const second = testArtifactKey("package-second");
    try storeTestQualification(&source, first);
    try storeTestQualification(&source, second);
    var package_profile = try testQualificationPackageProfile(&.{ second, first });
    defer package_profile.deinit();

    const exported = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );
    try std.testing.expectEqual(@as(usize, 2), exported.entries);
    try std.testing.expectEqual(@as(usize, 1), exported.profiles);

    const first_entry_name = try cacheEntryName(std.testing.allocator, .qualification, first);
    defer std.testing.allocator.free(first_entry_name);
    const profile_name = try qualificationProfileName(
        std.testing.allocator,
        testQualificationPackageProfileDigest(package_profile.input),
    );
    defer std.testing.allocator.free(profile_name);
    const embedded_profile = try package_tmp.dir.readFileAlloc(
        std.testing.io,
        profile_name,
        std.testing.allocator,
        .limited(maximum_qualification_package_profile_bytes),
    );
    defer std.testing.allocator.free(embedded_profile);
    try std.testing.expectEqualStrings(package_profile.input.canonical_profile, embedded_profile);
    const manifest_bytes = try package_tmp.dir.readFileAlloc(
        std.testing.io,
        qualification_package_manifest_name,
        std.testing.allocator,
        .limited(maximum_qualification_package_manifest_bytes),
    );
    defer std.testing.allocator.free(manifest_bytes);
    const parsed_manifest = try std.json.parseFromSlice(
        QualificationPackageManifest,
        std.testing.allocator,
        manifest_bytes,
        .{},
    );
    defer parsed_manifest.deinit();
    try std.testing.expectEqualStrings(qualification_package_schema, parsed_manifest.value.schema);
    try std.testing.expectEqual(@as(u64, 42), parsed_manifest.value.device_registry_id);
    try std.testing.expectEqualStrings(test_package_compiler_fingerprint, parsed_manifest.value.compiler_fingerprint);
    try std.testing.expectEqual(@as(usize, 1), parsed_manifest.value.profiles.len);
    try std.testing.expectEqualStrings(
        test_package_model_fingerprint,
        parsed_manifest.value.profiles[0].model_scope_fingerprint,
    );
    try std.testing.expectEqual(@as(usize, 2), parsed_manifest.value.profiles[0].entries.len);
    {
        var first_entry = try package_tmp.dir.openFile(std.testing.io, first_entry_name, .{});
        defer first_entry.close(std.testing.io);
        const old = std.Io.Timestamp.now(std.testing.io, .real).subDuration(.{ .nanoseconds = std.time.ns_per_hour });
        try first_entry.setTimestamps(std.testing.io, .{
            .access_timestamp = .{ .new = old },
            .modify_timestamp = .{ .new = old },
        });
        var profile_file = try package_tmp.dir.openFile(std.testing.io, profile_name, .{});
        defer profile_file.close(std.testing.io);
        try profile_file.setTimestamps(std.testing.io, .{
            .access_timestamp = .{ .new = old },
            .modify_timestamp = .{ .new = old },
        });
    }
    const before_verify = try package_tmp.dir.statFile(std.testing.io, first_entry_name, .{});
    const profile_before_verify = try package_tmp.dir.statFile(std.testing.io, profile_name, .{});
    const verified = try verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir);
    try std.testing.expectEqual(exported.entries, verified.entries);
    try std.testing.expectEqual(exported.profiles, verified.profiles);
    const after_verify = try package_tmp.dir.statFile(std.testing.io, first_entry_name, .{});
    const profile_after_verify = try package_tmp.dir.statFile(std.testing.io, profile_name, .{});
    try std.testing.expectEqual(before_verify.inode, after_verify.inode);
    try std.testing.expectEqual(before_verify.mtime.nanoseconds, after_verify.mtime.nanoseconds);
    try std.testing.expectEqual(profile_before_verify.inode, profile_after_verify.inode);
    try std.testing.expectEqual(profile_before_verify.mtime.nanoseconds, profile_after_verify.mtime.nanoseconds);

    const imported = try importQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        package_tmp.dir,
        &destination,
    );
    try std.testing.expectEqual(exported.entries, imported.entries);
    try std.testing.expectEqual(exported.profiles, imported.profiles);
    const first_payload = (try destination.load(.qualification, first)) orelse
        return error.MissingKernelJitQualification;
    defer std.testing.allocator.free(first_payload);
    const second_payload = (try destination.load(.qualification, second)) orelse
        return error.MissingKernelJitQualification;
    defer std.testing.allocator.free(second_payload);
    try std.testing.expect((try QualificationRecord.decode(first_payload)).eligible());
    try std.testing.expect((try QualificationRecord.decode(second_payload)).eligible());

    const executable_name = try cacheEntryName(std.testing.allocator, .cuda_ptx, testArtifactKey("package-ptx"));
    defer std.testing.allocator.free(executable_name);
    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = executable_name,
        .data = "not allowed in a qualification package",
    });
    try std.testing.expectError(
        error.InvalidKernelJitPackageContents,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
}

test "kernel JIT qualification package rejects corrupt entries before import" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const key = testArtifactKey("package-corrupt");
    try storeTestQualification(&source, key);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );
    const entry_name = try cacheEntryName(std.testing.allocator, .qualification, key);
    defer std.testing.allocator.free(entry_name);
    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = entry_name,
        .data = "corrupt",
    });

    const before = try package_tmp.dir.statFile(std.testing.io, entry_name, .{});
    for (0..2) |_| {
        try std.testing.expectError(
            error.InvalidKernelJitPackageEntry,
            verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
        );
    }
    const after = try package_tmp.dir.statFile(std.testing.io, entry_name, .{});
    try std.testing.expectEqual(before.inode, after.inode);
    try std.testing.expectEqual(before.size, after.size);
    try std.testing.expectEqual(before.mtime.nanoseconds, after.mtime.nanoseconds);

    try std.testing.expectError(
        error.InvalidKernelJitPackageEntry,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
}

test "kernel JIT qualification package rejects profile record mismatch before export writes" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const key = testArtifactKey("package-export-claim-mismatch");
    var mismatched = testEligibleQualificationRecord();
    mismatched.candidate_index += 1;
    try storeTestQualificationRecord(&source, key, mismatched);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();

    try std.testing.expectError(
        error.InvalidKernelJitQualification,
        exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            package_tmp.dir,
            &.{package_profile.input},
        ),
    );
    var iterator = package_tmp.dir.iterate();
    try std.testing.expectEqual(@as(?std.Io.Dir.Entry, null), try iterator.next(std.testing.io));
}

test "kernel JIT qualification package rejects candidate and evidence mismatch before import" {
    var candidate_mismatch = testEligibleQualificationRecord();
    candidate_mismatch.candidate_index += 1;
    var evidence_mismatch = testEligibleQualificationRecord();
    evidence_mismatch.measured_speedup += 0.01;
    evidence_mismatch.minimum_repeat_speedup += 0.01;
    const records = [_]QualificationRecord{ candidate_mismatch, evidence_mismatch };
    const labels = [_][]const u8{ "candidate", "evidence" };

    for (records, labels) |mismatched, label| {
        var source_tmp = std.testing.tmpDir(.{ .iterate = true });
        defer source_tmp.cleanup();
        var package_tmp = std.testing.tmpDir(.{ .iterate = true });
        defer package_tmp.cleanup();
        var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
        defer destination_tmp.cleanup();

        var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
        defer source.deinit();
        var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
        defer destination.deinit();
        const key = testArtifactKey(label);
        try storeTestQualification(&source, key);
        var package_profile = try testQualificationPackageProfile(&.{key});
        defer package_profile.deinit();
        _ = try exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            package_tmp.dir,
            &.{package_profile.input},
        );

        var package_cache = try ArtifactCache.initDir(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            qualification_package_cache_budget,
        );
        try storeTestQualificationRecord(&package_cache, key, mismatched);
        package_cache.deinit();

        try std.testing.expectError(
            error.InvalidKernelJitPackageEntry,
            verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
        );
        try std.testing.expectError(
            error.InvalidKernelJitPackageEntry,
            importQualificationPackage(
                std.testing.allocator,
                std.testing.io,
                package_tmp.dir,
                &destination,
            ),
        );
        try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
    }
}

test "kernel JIT qualification package rejects profile substitution before import" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const key = testArtifactKey("package-profile-substitution");
    try storeTestQualification(&source, key);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );

    const profile_name = try qualificationProfileName(
        std.testing.allocator,
        testQualificationPackageProfileDigest(package_profile.input),
    );
    defer std.testing.allocator.free(profile_name);
    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = profile_name,
        .data = "substituted-profile",
    });
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
}

test "kernel JIT qualification package rejects self-consistent malformed profile" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const key = testArtifactKey("package-malformed-profile");
    try storeTestQualification(&source, key);
    var valid = try testQualificationPackageProfile(&.{key});
    defer valid.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{valid.input},
    );

    const valid_name = try qualificationProfileName(
        std.testing.allocator,
        testQualificationPackageProfileDigest(valid.input),
    );
    defer std.testing.allocator.free(valid_name);
    try package_tmp.dir.deleteFile(std.testing.io, valid_name);
    var malformed = valid.input;
    malformed.canonical_profile = "not-json";
    const malformed_name = try qualificationProfileName(
        std.testing.allocator,
        testQualificationPackageProfileDigest(malformed),
    );
    defer std.testing.allocator.free(malformed_name);
    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = malformed_name,
        .data = malformed.canonical_profile,
    });
    const key_hex = artifactKeyHex(key);
    const entries = [_]QualificationPackageManifestEntry{.{ .key = &key_hex }};
    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version,
        qualification_policy_identity,
        &entries,
        malformed,
    );

    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
}

test "kernel JIT qualification package rejects manifest profile provenance mismatch" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const key = testArtifactKey("package-profile-provenance-mismatch");
    try storeTestQualification(&source, key);
    var profile = try testQualificationPackageProfile(&.{key});
    defer profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{profile.input},
    );

    var mismatched_manifest_profile = profile.input;
    mismatched_manifest_profile.model_scope_fingerprint =
        "sha256:3333333333333333333333333333333333333333333333333333333333333333";
    const key_hex = artifactKeyHex(key);
    const entries = [_]QualificationPackageManifestEntry{.{ .key = &key_hex }};
    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version,
        qualification_policy_identity,
        &entries,
        mismatched_manifest_profile,
    );

    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
}

test "kernel JIT qualification package binds shared keys to canonical profiles" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const key = testArtifactKey("package-shared-profile-key");
    try storeTestQualification(&source, key);
    var first = try testQualificationPackageProfile(&.{key});
    defer first.deinit();
    var second = try testQualificationPackageProfileForModel(
        &.{key},
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    defer second.deinit();

    const exported = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{ first.input, second.input },
    );
    try std.testing.expectEqual(@as(usize, 1), exported.entries);
    try std.testing.expectEqual(@as(usize, 2), exported.profiles);
    const verified = try verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir);
    try std.testing.expectEqual(exported.entries, verified.entries);
    try std.testing.expectEqual(exported.profiles, verified.profiles);
}

test "kernel JIT qualification package rejects unbound or mixed provenance" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const key = testArtifactKey("package-mixed-provenance");
    try storeTestQualification(&source, key);

    var bound = try testQualificationPackageProfile(&.{key});
    defer bound.deinit();
    var empty = try testQualificationPackageProfile(&.{});
    defer empty.deinit();
    try std.testing.expectError(
        error.InvalidKernelJitPackageEntryCount,
        exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            package_tmp.dir,
            &.{empty.input},
        ),
    );

    var mixed_device = bound.input;
    mixed_device.device_registry_id += 1;
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            package_tmp.dir,
            &.{ bound.input, mixed_device },
        ),
    );

    var mixed_compiler = bound.input;
    mixed_compiler.compiler_fingerprint = "sha256:4444444444444444444444444444444444444444444444444444444444444444";
    try std.testing.expectError(
        error.InvalidKernelJitPackageProfile,
        exportQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            &source,
            package_tmp.dir,
            &.{ bound.input, mixed_compiler },
        ),
    );
}

test "kernel JIT qualification package rejects stale policy before import" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var destination_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer destination_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    var destination = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, destination_tmp.dir, 1024 * 1024);
    defer destination.deinit();
    const key = testArtifactKey("package-stale");
    try storeTestQualification(&source, key);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );

    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = qualification_package_manifest_name,
        .data = "{\"schema\":\"antfly.kernel_jit.package.v1\"}",
    });
    try std.testing.expectError(
        error.StaleKernelJitPackage,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));

    const key_hex = artifactKeyHex(key);
    const entries = [_]QualificationPackageManifestEntry{.{ .key = &key_hex }};
    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version,
        "stale-policy-identity",
        &entries,
        package_profile.input,
    );

    try std.testing.expectError(
        error.StaleKernelJitPackage,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));

    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version + 1,
        qualification_policy_identity,
        &entries,
        package_profile.input,
    );

    try std.testing.expectError(
        error.StaleKernelJitPackage,
        importQualificationPackage(
            std.testing.allocator,
            std.testing.io,
            package_tmp.dir,
            &destination,
        ),
    );
    try std.testing.expectEqual(@as(?[]u8, null), try destination.load(.qualification, key));
}

test "kernel JIT qualification package rejects duplicate and unqualified manifest entries" {
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();

    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const key = testArtifactKey("package-invalid-entry");
    try storeTestQualification(&source, key);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );

    const key_hex = artifactKeyHex(key);
    const duplicate_entries = [_]QualificationPackageManifestEntry{
        .{ .key = &key_hex },
        .{ .key = &key_hex },
    };
    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version,
        qualification_policy_identity,
        &duplicate_entries,
        package_profile.input,
    );
    try std.testing.expectError(
        error.InvalidKernelJitPackageManifest,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );

    const valid_entries = [_]QualificationPackageManifestEntry{.{ .key = &key_hex }};
    try writeTestQualificationPackageManifest(
        package_tmp.dir,
        qualification_policy_version,
        qualification_policy_identity,
        &valid_entries,
        package_profile.input,
    );
    var unqualified = testEligibleQualificationRecord();
    unqualified.minimum_repeat_speedup = minimum_speedup - 0.01;
    const payload = try unqualified.encode();
    const encoded_entry = try encodeCacheEntry(std.testing.allocator, .qualification, key, &payload);
    defer std.testing.allocator.free(encoded_entry);
    const entry_name = try cacheEntryName(std.testing.allocator, .qualification, key);
    defer std.testing.allocator.free(entry_name);
    try package_tmp.dir.writeFile(std.testing.io, .{
        .sub_path = entry_name,
        .data = encoded_entry,
    });
    try std.testing.expectError(
        error.UnqualifiedKernelJitPackageEntry,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
}

test "kernel JIT qualification package rejects untrusted and unfinalized directories" {
    var empty_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer empty_tmp.cleanup();
    try std.testing.expectError(
        error.UnfinalizedKernelJitPackage,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, empty_tmp.dir),
    );

    if (comptime !std.Io.File.Permissions.has_executable_bit or
        (builtin.os.tag != .linux and builtin.os.tag != .macos)) return;
    var source_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer source_tmp.cleanup();
    var package_tmp = std.testing.tmpDir(.{ .iterate = true });
    defer package_tmp.cleanup();
    var source = try ArtifactCache.initDir(std.testing.allocator, std.testing.io, source_tmp.dir, 1024 * 1024);
    defer source.deinit();
    const key = testArtifactKey("package-untrusted");
    try storeTestQualification(&source, key);
    var package_profile = try testQualificationPackageProfile(&.{key});
    defer package_profile.deinit();
    _ = try exportQualificationPackage(
        std.testing.allocator,
        std.testing.io,
        &source,
        package_tmp.dir,
        &.{package_profile.input},
    );
    try package_tmp.dir.setPermissions(std.testing.io, .fromMode(0o777));
    defer package_tmp.dir.setPermissions(std.testing.io, .fromMode(0o700)) catch {};
    try std.testing.expectError(
        error.UntrustedKernelJitCacheDirectory,
        verifyQualificationPackage(std.testing.allocator, std.testing.io, package_tmp.dir),
    );
}
