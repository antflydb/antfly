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

const std = @import("std");
const builtin = @import("builtin");
const compat = @import("io/compat.zig");
const kernel_jit = @import("graph/kernel_jit.zig");
const workload_profile_policy = @import("workload_profile_policy.zig");

pub const schema_name = workload_profile_policy.schema_name;
pub const bundle_schema_name = "antfly.workload-profile-bundle.v1";
pub const cli_flag = "--kernel-jit-profile-out";
pub const qualified_profile_cli_flag = "--kernel-jit-qualified-profile";
pub const maximum_profile_entries = workload_profile_policy.maximum_profile_entries;
pub const maximum_tuning_signatures = workload_profile_policy.maximum_tuning_signatures;
const maximum_bundle_bytes: usize = 64 * 1024;
const maximum_profile_bytes: usize = 16 * 1024 * 1024;
var atomic_temp_sequence: std.atomic.Value(u64) = .init(0);

pub const BundleComponent = enum(u8) {
    primary,
    vision,
    audio,
    text_projection,
    visual_projection,
    audio_projection,
};

pub const BundleReport = struct {
    component: BundleComponent,
    report: ProfileReport,
};

const ProfileBundleManifest = struct {
    schema: []const u8,
    primary: []const u8,
    vision: ?[]const u8 = null,
    audio: ?[]const u8 = null,
    text_projection: ?[]const u8 = null,
    visual_projection: ?[]const u8 = null,
    audio_projection: ?[]const u8 = null,
};

pub const LoadedProfileBundle = struct {
    allocator: std.mem.Allocator,
    paths: [@typeInfo(BundleComponent).@"enum".fields.len]?[]u8 = @splat(null),
    has_qualified_kernels: [@typeInfo(BundleComponent).@"enum".fields.len]bool = @splat(false),

    pub fn deinit(self: *LoadedProfileBundle) void {
        for (self.paths) |maybe_path| if (maybe_path) |owned| self.allocator.free(owned);
        self.* = undefined;
    }

    pub fn profilePaths(self: *const LoadedProfileBundle) [@typeInfo(BundleComponent).@"enum".fields.len]?[]const u8 {
        var result: [@typeInfo(BundleComponent).@"enum".fields.len]?[]const u8 = @splat(null);
        for (self.paths, 0..) |maybe_path, index| result[index] = maybe_path;
        return result;
    }

    pub fn kernelJitBundle(self: *const LoadedProfileBundle) kernel_jit.QualifiedProfileBundle {
        return .{
            .primary = self.path(.primary).?,
            .vision = self.path(.vision),
            .audio = self.path(.audio),
            .text_projection = self.path(.text_projection),
            .visual_projection = self.path(.visual_projection),
            .audio_projection = self.path(.audio_projection),
        };
    }

    pub fn kernelJitBundleForMode(
        self: *const LoadedProfileBundle,
        mode: kernel_jit.Mode,
    ) kernel_jit.QualifiedProfileBundle {
        var bundle = self.kernelJitBundle();
        if (mode.failClosed()) return bundle;
        if (!self.hasQualifiedKernels(.vision)) bundle.vision = null;
        if (!self.hasQualifiedKernels(.audio)) bundle.audio = null;
        if (!self.hasQualifiedKernels(.text_projection)) bundle.text_projection = null;
        if (!self.hasQualifiedKernels(.visual_projection)) bundle.visual_projection = null;
        if (!self.hasQualifiedKernels(.audio_projection)) bundle.audio_projection = null;
        return bundle;
    }

    pub fn hasQualifiedKernels(self: *const LoadedProfileBundle, component: BundleComponent) bool {
        return self.has_qualified_kernels[@intFromEnum(component)];
    }

    fn path(self: *const LoadedProfileBundle, component: BundleComponent) ?[]const u8 {
        return self.paths[@intFromEnum(component)];
    }
};

/// Shared with qualification-package verification so canonical export and
/// offline package import replay exactly the same weighted tuning policy.
pub const ProfileEntry = workload_profile_policy.ProfileEntry;
pub const QualifiedKernel = workload_profile_policy.QualifiedKernel;
pub const ProfileReport = workload_profile_policy.ProfileReport;

/// Owned, canonical profile material used to build a self-contained
/// qualification package. The package layer deliberately accepts only this
/// profile-derived provenance rather than an unbound list of artifact keys.
pub const CanonicalQualifiedProfile = struct {
    allocator: std.mem.Allocator,
    json: []u8,
    device_registry_id: u64,
    compiler_fingerprint: []u8,
    model_fingerprint: []u8,
    keys: []kernel_jit.ArtifactKey,

    pub fn deinit(self: *CanonicalQualifiedProfile) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.model_fingerprint);
        self.allocator.free(self.compiler_fingerprint);
        self.allocator.free(self.json);
        self.* = undefined;
    }

    pub fn packageInput(self: *const CanonicalQualifiedProfile) kernel_jit.QualificationPackageProfileInput {
        return .{
            .canonical_profile = self.json,
            .device_registry_id = self.device_registry_id,
            .compiler_fingerprint = self.compiler_fingerprint,
            .model_scope_fingerprint = self.model_fingerprint,
            .keys = self.keys,
        };
    }
};

pub fn validateOutputPath(path: []const u8) !void {
    if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) {
        return error.InvalidKernelJitProfileOut;
    }
}

pub fn validateReport(report: ProfileReport) !void {
    try validateFingerprint(report.compiler_fingerprint);
    try validateFingerprint(report.model_fingerprint);
    try workload_profile_policy.validateStructure(
        report,
        kernel_jit.minimum_speedup,
        kernel_jit.maximum_candidates,
    );
    try workload_profile_policy.validateWeightedCoverage(report);
}

/// Produces stable JSON by sorting a temporary entry copy by its full workload
/// signature. Duplicate signatures are rejected rather than order-dependent.
pub fn renderAlloc(allocator: std.mem.Allocator, report: ProfileReport) ![]u8 {
    try validateReport(report);

    const entries = try allocator.dupe(ProfileEntry, report.entries);
    defer allocator.free(entries);
    std.mem.sort(ProfileEntry, entries, {}, lessThan);
    if (entries.len > 1) {
        for (entries[1..], entries[0 .. entries.len - 1]) |entry, previous| {
            if (sameSignature(previous, entry)) return error.DuplicateProfileEntry;
        }
    }

    const qualified = try allocator.dupe(QualifiedKernel, report.qualified_kernels);
    defer allocator.free(qualified);
    std.mem.sort(QualifiedKernel, qualified, {}, qualifiedLessThan);
    if (qualified.len > 1) {
        for (qualified[1..], qualified[0 .. qualified.len - 1]) |kernel, previous| {
            if (std.mem.eql(u8, kernel.artifact_key, previous.artifact_key)) {
                return error.DuplicateQualifiedKernel;
            }
        }
    }

    var canonical = report;
    canonical.entries = entries;
    canonical.qualified_kernels = qualified;
    return std.json.Stringify.valueAlloc(allocator, canonical, .{ .whitespace = .indent_2 });
}

/// Loads a profile, validates the complete report, and re-renders it through
/// the canonical serializer before any package digest is computed. Empty
/// qualification sets are permitted only for members of a multi-model bundle;
/// the package exporter still requires at least one qualified key overall.
pub fn loadCanonicalQualifiedProfile(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    allow_empty: bool,
) !CanonicalQualifiedProfile {
    try validateOutputPath(path);
    const bytes = try compat.cwd().readFileAlloc(
        io,
        path,
        allocator,
        .limited(maximum_profile_bytes),
    );
    defer allocator.free(bytes);
    var parsed = std.json.parseFromSlice(ProfileReport, allocator, bytes, .{}) catch
        return error.InvalidKernelJitProfile;
    defer parsed.deinit();
    validateReport(parsed.value) catch return error.InvalidKernelJitProfile;
    workload_profile_policy.validateQualifiedCoverage(parsed.value) catch
        return error.InvalidKernelJitProfile;
    if (!parsed.value.complete or (!allow_empty and parsed.value.qualified_kernels.len == 0)) {
        return error.MissingKernelJitQualification;
    }
    const empty_bundle_member = allow_empty and
        parsed.value.tuning_selected_signatures == 0 and
        parsed.value.tuning_qualified_signatures == 0 and
        parsed.value.tuning_failed_signatures == 0 and
        parsed.value.qualified_kernels.len == 0;
    if (!parsed.value.tuning_coverage_complete and !empty_bundle_member) {
        return error.IncompleteKernelJitTuningCoverage;
    }

    const json = try renderAlloc(allocator, parsed.value);
    errdefer allocator.free(json);
    const compiler_fingerprint = try allocator.dupe(u8, parsed.value.compiler_fingerprint);
    errdefer allocator.free(compiler_fingerprint);
    const model_fingerprint = try allocator.dupe(u8, parsed.value.model_fingerprint);
    errdefer allocator.free(model_fingerprint);
    const keys = try allocator.alloc(kernel_jit.ArtifactKey, parsed.value.qualified_kernels.len);
    errdefer allocator.free(keys);
    for (parsed.value.qualified_kernels, keys) |qualified, *key| {
        key.* = kernel_jit.parseArtifactKeyHex(qualified.artifact_key) catch
            return error.InvalidKernelJitProfile;
    }
    return .{
        .allocator = allocator,
        .json = json,
        .device_registry_id = parsed.value.device_registry_id,
        .compiler_fingerprint = compiler_fingerprint,
        .model_fingerprint = model_fingerprint,
        .keys = keys,
    };
}

pub fn writeFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    report: ProfileReport,
) !void {
    try validateOutputPath(path);
    const json = try renderAlloc(allocator, report);
    defer allocator.free(json);
    try writeFileAtomic(allocator, io, path, json);
}

/// Writes one report per materialized model and a small manifest that binds
/// each model component to its own profile. Member references are basenames so
/// the manifest and profiles remain relocatable as one directory.
pub fn writeBundle(
    allocator: std.mem.Allocator,
    io: std.Io,
    bundle_path: []const u8,
    reports: []const BundleReport,
) !void {
    try validateOutputPath(bundle_path);
    var names: [@typeInfo(BundleComponent).@"enum".fields.len]?[]u8 = @splat(null);
    defer for (names) |name| if (name) |owned| allocator.free(owned);

    const bundle_dir = std.fs.path.dirname(bundle_path) orelse ".";
    for (reports) |item| {
        const index = @intFromEnum(item.component);
        if (names[index] != null) return error.DuplicateKernelJitProfileBundleComponent;
        const profile_json = try renderAlloc(allocator, item.report);
        defer allocator.free(profile_json);
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(profile_json, &digest, .{});
        const digest_hex = std.fmt.bytesToHex(digest, .lower);
        const member_name = try std.fmt.allocPrint(
            allocator,
            "jit-profile-{s}-{s}.json",
            .{ @tagName(item.component), &digest_hex },
        );
        names[index] = member_name;
        const member_path = try std.fs.path.join(allocator, &.{ bundle_dir, member_name });
        defer allocator.free(member_path);
        try writeFileAtomic(allocator, io, member_path, profile_json);
    }
    const primary = names[@intFromEnum(BundleComponent.primary)] orelse
        return error.MissingKernelJitProfileBundlePrimary;
    const manifest = ProfileBundleManifest{
        .schema = bundle_schema_name,
        .primary = primary,
        .vision = names[@intFromEnum(BundleComponent.vision)],
        .audio = names[@intFromEnum(BundleComponent.audio)],
        .text_projection = names[@intFromEnum(BundleComponent.text_projection)],
        .visual_projection = names[@intFromEnum(BundleComponent.visual_projection)],
        .audio_projection = names[@intFromEnum(BundleComponent.audio_projection)],
    };
    const json = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(json);
    // Publish the manifest last: every visible bundle therefore references a
    // fully replaced set of durable component profiles.
    try writeFileAtomic(allocator, io, bundle_path, json);
}

fn writeFileAtomic(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const name = std.fs.path.basename(path);
    if (name.len == 0 or std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) {
        return error.InvalidKernelJitProfileOut;
    }
    // .iterate forces a real O_RDONLY directory fd; the Zig 0.16 Threaded io
    // otherwise opens O_PATH on Linux, and fsync on an O_PATH fd aborts with
    // EBADF inside syncDirectory.
    var dir = try compat.cwd().openDir(io, parent_path, .{ .iterate = true });
    defer dir.close(io);
    const tmp_name = try std.fmt.allocPrint(
        allocator,
        ".{s}.tmp-{x}-{x}-{x}",
        .{
            name,
            @intFromPtr(&atomic_temp_sequence),
            std.Thread.getCurrentId(),
            atomic_temp_sequence.fetchAdd(1, .monotonic),
        },
    );
    defer allocator.free(tmp_name);
    var file = try dir.createFile(io, tmp_name, .{ .exclusive = true, .truncate = false });
    var open = true;
    defer if (open) file.close(io);
    errdefer dir.deleteFile(io, tmp_name) catch {};
    try file.writeStreamingAll(io, data);
    try file.sync(io);
    file.close(io);
    open = false;
    try std.Io.Dir.rename(dir, tmp_name, dir, name, io);
    try syncDirectory(dir, io);
}

fn syncDirectory(dir: std.Io.Dir, io: std.Io) !void {
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

pub fn loadQualifiedProfileBundle(
    allocator: std.mem.Allocator,
    io: std.Io,
    bundle_path: []const u8,
) !LoadedProfileBundle {
    try validateOutputPath(bundle_path);
    const bytes = try compat.cwd().readFileAlloc(
        io,
        bundle_path,
        allocator,
        .limited(maximum_bundle_bytes),
    );
    defer allocator.free(bytes);
    var parsed = std.json.parseFromSlice(ProfileBundleManifest, allocator, bytes, .{}) catch
        return error.InvalidKernelJitProfileBundle;
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.schema, bundle_schema_name)) {
        return error.InvalidKernelJitProfileBundle;
    }

    const refs = [_]?[]const u8{
        parsed.value.primary,
        parsed.value.vision,
        parsed.value.audio,
        parsed.value.text_projection,
        parsed.value.visual_projection,
        parsed.value.audio_projection,
    };
    var loaded = LoadedProfileBundle{ .allocator = allocator };
    errdefer loaded.deinit();
    const bundle_dir = std.fs.path.dirname(bundle_path) orelse ".";
    for (refs, 0..) |maybe_ref, index| {
        const member_ref = maybe_ref orelse continue;
        try validateBundleMemberReference(member_ref);
        for (refs[0..index]) |previous| {
            if (previous != null and std.mem.eql(u8, previous.?, member_ref)) {
                return error.DuplicateKernelJitProfileBundleMember;
            }
        }
        const member_path = try std.fs.path.join(allocator, &.{ bundle_dir, member_ref });
        errdefer allocator.free(member_path);
        loaded.has_qualified_kernels[index] = try validateQualifiedProfileFile(allocator, io, member_path);
        loaded.paths[index] = member_path;
    }
    return loaded;
}

/// A single qualified-profile path remains valid for single-model callers.
/// ModelManager uses this discriminator to opt into bundle ownership only
/// when the configured JSON advertises the bundle schema.
pub fn loadQualifiedProfileBundleIfPresent(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !?LoadedProfileBundle {
    const bytes = try compat.cwd().readFileAlloc(
        io,
        path,
        allocator,
        .limited(maximum_profile_bytes),
    );
    defer allocator.free(bytes);
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const schema = parsed.value.object.get("schema") orelse return null;
    if (schema != .string or !std.mem.eql(u8, schema.string, bundle_schema_name)) return null;
    return try loadQualifiedProfileBundle(allocator, io, path);
}

fn validateBundleMemberReference(path: []const u8) !void {
    try validateOutputPath(path);
    if (std.fs.path.isAbsolute(path) or
        !std.mem.eql(u8, std.fs.path.basename(path), path) or
        std.mem.eql(u8, path, ".") or
        std.mem.eql(u8, path, ".."))
    {
        return error.InvalidKernelJitProfileBundleMember;
    }
}

fn validateQualifiedProfileFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !bool {
    const bytes = try compat.cwd().readFileAlloc(
        io,
        path,
        allocator,
        .limited(maximum_profile_bytes),
    );
    defer allocator.free(bytes);
    var parsed = std.json.parseFromSlice(ProfileReport, allocator, bytes, .{}) catch
        return error.InvalidKernelJitQualifiedProfile;
    defer parsed.deinit();
    validateReport(parsed.value) catch return error.InvalidKernelJitQualifiedProfile;
    if (!parsed.value.complete) return error.IncompleteKernelJitQualifiedProfile;
    return parsed.value.qualified_kernels.len != 0;
}

pub fn logWriteSummary(path: []const u8, report: ProfileReport) void {
    std.log.info(
        "kernel JIT profile written path={s} entries={d} attempted={d} qualified={d} rejected={d} coverage_complete={}",
        .{
            path,
            report.entries.len,
            report.tuning_selected_signatures,
            report.tuning_qualified_signatures,
            report.tuning_failed_signatures,
            report.tuning_coverage_complete,
        },
    );
}

fn validateFingerprint(value: []const u8) !void {
    if (value.len == 0 or
        std.mem.indexOfScalar(u8, value, 0) != null or
        std.mem.indexOfAny(u8, value, "/\\") != null)
    {
        return error.InvalidProfileFingerprint;
    }
}

fn lessThan(_: void, lhs: ProfileEntry, rhs: ProfileEntry) bool {
    const regime = std.mem.order(u8, lhs.regime, rhs.regime);
    if (regime != .eq) return regime == .lt;
    const quant_format = std.mem.order(u8, lhs.quant_format, rhs.quant_format);
    if (quant_format != .eq) return quant_format == .lt;
    const dispatch = std.mem.order(u8, lhs.dispatch, rhs.dispatch);
    if (dispatch != .eq) return dispatch == .lt;
    const implementation = std.mem.order(u8, lhs.implementation, rhs.implementation);
    if (implementation != .eq) return implementation == .lt;
    if (lhs.rows != rhs.rows) return lhs.rows < rhs.rows;
    if (lhs.in_dim != rhs.in_dim) return lhs.in_dim < rhs.in_dim;
    if (lhs.out_dim != rhs.out_dim) return lhs.out_dim < rhs.out_dim;
    const input_encoding = std.mem.order(u8, lhs.input_encoding, rhs.input_encoding);
    if (input_encoding != .eq) return input_encoding == .lt;
    const output_encoding = std.mem.order(u8, lhs.output_encoding, rhs.output_encoding);
    if (output_encoding != .eq) return output_encoding == .lt;
    const epilogue = std.mem.order(u8, lhs.epilogue, rhs.epilogue);
    if (epilogue != .eq) return epilogue == .lt;
    const fusion = std.mem.order(u8, lhs.fusion, rhs.fusion);
    if (fusion != .eq) return fusion == .lt;
    return std.mem.lessThan(u8, lhs.layout, rhs.layout);
}

fn sameSignature(lhs: ProfileEntry, rhs: ProfileEntry) bool {
    return std.mem.eql(u8, lhs.regime, rhs.regime) and
        std.mem.eql(u8, lhs.quant_format, rhs.quant_format) and
        std.mem.eql(u8, lhs.dispatch, rhs.dispatch) and
        std.mem.eql(u8, lhs.implementation, rhs.implementation) and
        lhs.rows == rhs.rows and
        lhs.in_dim == rhs.in_dim and
        lhs.out_dim == rhs.out_dim and
        std.mem.eql(u8, lhs.input_encoding, rhs.input_encoding) and
        std.mem.eql(u8, lhs.output_encoding, rhs.output_encoding) and
        std.mem.eql(u8, lhs.epilogue, rhs.epilogue) and
        std.mem.eql(u8, lhs.fusion, rhs.fusion) and
        std.mem.eql(u8, lhs.layout, rhs.layout);
}

fn sameQualifiedSignature(lhs: QualifiedKernel, rhs: QualifiedKernel) bool {
    return std.mem.eql(u8, lhs.regime, rhs.regime) and
        std.mem.eql(u8, lhs.quant_format, rhs.quant_format) and
        std.mem.eql(u8, lhs.dispatch, rhs.dispatch) and
        std.mem.eql(u8, lhs.implementation, rhs.implementation) and
        lhs.rows == rhs.rows and
        lhs.in_dim == rhs.in_dim and
        lhs.out_dim == rhs.out_dim and
        std.mem.eql(u8, lhs.input_encoding, rhs.input_encoding) and
        std.mem.eql(u8, lhs.output_encoding, rhs.output_encoding) and
        std.mem.eql(u8, lhs.epilogue, rhs.epilogue) and
        std.mem.eql(u8, lhs.fusion, rhs.fusion) and
        std.mem.eql(u8, lhs.layout, rhs.layout);
}

fn qualifiedLessThan(_: void, lhs: QualifiedKernel, rhs: QualifiedKernel) bool {
    const regime = std.mem.order(u8, lhs.regime, rhs.regime);
    if (regime != .eq) return regime == .lt;
    const format = std.mem.order(u8, lhs.quant_format, rhs.quant_format);
    if (format != .eq) return format == .lt;
    const dispatch = std.mem.order(u8, lhs.dispatch, rhs.dispatch);
    if (dispatch != .eq) return dispatch == .lt;
    if (lhs.rows != rhs.rows) return lhs.rows < rhs.rows;
    if (lhs.in_dim != rhs.in_dim) return lhs.in_dim < rhs.in_dim;
    if (lhs.out_dim != rhs.out_dim) return lhs.out_dim < rhs.out_dim;
    return std.mem.lessThan(u8, lhs.artifact_key, rhs.artifact_key);
}

fn testEntry(regime: []const u8, rows: u32) ProfileEntry {
    return .{
        .regime = regime,
        .quant_format = "q4_k",
        .dispatch = "quant_matmul",
        .implementation = "generated",
        .rows = rows,
        .in_dim = 256,
        .out_dim = 512,
        .input_encoding = "f32",
        .output_encoding = "f32",
        .epilogue = "bias_gelu",
        .fusion = "none",
        .layout = "row_major",
        .call_count = 3,
        .logical_bytes = 4096,
        .macs = 393216,
    };
}

fn testQualifiedKernel(artifact_key: []const u8, regime: []const u8, rows: u32) QualifiedKernel {
    return .{
        .artifact_key = artifact_key,
        .regime = regime,
        .quant_format = "q4_k",
        .dispatch = "scalar",
        .implementation = "bundled",
        .rows = rows,
        .in_dim = 256,
        .out_dim = 512,
        .input_encoding = "f32",
        .output_encoding = "f32",
        .epilogue = "none",
        .fusion = "none",
        .layout = "row_major",
        .candidate_index = 1,
        .threads_per_threadgroup = 128,
        .rows_per_threadgroup = 1,
        .cols_per_threadgroup = 8,
        .reduction = "simdgroup",
        .measured_speedup = 1.08,
        .minimum_repeat_speedup = 1.03,
    };
}

fn testTuningEntry(regime: []const u8, rows: u32) ProfileEntry {
    return .{
        .regime = regime,
        .quant_format = "q4_k",
        .dispatch = "scalar",
        .implementation = "bundled",
        .rows = rows,
        .in_dim = 256,
        .out_dim = 512,
        .input_encoding = "f32",
        .output_encoding = "f32",
        .epilogue = "none",
        .fusion = "none",
        .layout = "row_major",
        .call_count = 3,
        .logical_bytes = 4096,
        .macs = 393216,
        .estimated_gpu_nanos = 100,
        .calibrated = true,
    };
}

test "workload profile JSON is canonical and complete" {
    const allocator = std.testing.allocator;
    const entries = [_]ProfileEntry{
        testEntry("decode", 1),
        testEntry("prefill", 256),
    };
    const reverse_entries = [_]ProfileEntry{
        entries[1],
        entries[0],
    };
    const report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .whole_frame_gpu_nanos = 8_000_000,
        .profiled_frame_count = 2,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .requested_tokens = 256,
        .effective_tokens = 256,
        .batch = 8,
        .mtp_k = 4,
        .entries = &entries,
    };
    var reversed = report;
    reversed.entries = &reverse_entries;

    const json = try renderAlloc(allocator, report);
    defer allocator.free(json);
    const reversed_json = try renderAlloc(allocator, reversed);
    defer allocator.free(reversed_json);
    try std.testing.expectEqualStrings(json, reversed_json);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(schema_name, parsed.value.object.get("schema").?.string);
    try std.testing.expect(parsed.value.object.get("complete").?.bool);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("entries").?.array.items.len);
    try std.testing.expectEqualStrings("decode", parsed.value.object.get("entries").?.array.items[0].object.get("regime").?.string);
    try std.testing.expectEqual(@as(i64, 8_000_000), parsed.value.object.get("whole_frame_gpu_nanos").?.integer);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("profiled_frame_count").?.integer);
    try std.testing.expect(parsed.value.object.get("entries").?.array.items[0].object.get("estimated_gpu_nanos").? == .null);
}

test "workload profile JSON rejects incomplete metadata claims" {
    const entry = testEntry("embedding", 8);
    const report: ProfileReport = .{
        .complete = true,
        .dropped_dispatches = 1,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .entries = &.{entry},
    };
    try std.testing.expectError(error.InconsistentProfileCompleteness, renderAlloc(std.testing.allocator, report));
}

test "workload profile JSON supports an empty census" {
    const json = try renderAlloc(std.testing.allocator, .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .entries = &.{},
    });
    defer std.testing.allocator.free(json);

    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("entries").?.array.items.len);
}

test "workload profile output rejects empty and path-derived identifiers" {
    try std.testing.expectError(error.InvalidKernelJitProfileOut, validateOutputPath(""));

    const entry = testEntry("embedding", 8);
    const report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "/tmp/model.gguf",
        .entries = &.{entry},
    };
    try std.testing.expectError(error.InvalidProfileFingerprint, renderAlloc(std.testing.allocator, report));
}

test "workload profile JSON canonicalizes qualified kernels" {
    const entries = [_]ProfileEntry{
        testTuningEntry("decode", 2),
        testTuningEntry("prefill", 4),
    };
    const first_key = "0000000000000000000000000000000000000000000000000000000000000001";
    const second_key = "0000000000000000000000000000000000000000000000000000000000000002";
    var tiled = testQualifiedKernel(second_key, "prefill", 4);
    tiled.rows_per_threadgroup = 2;
    const qualified = [_]QualifiedKernel{
        tiled,
        testQualifiedKernel(first_key, "decode", 2),
    };
    const reverse_qualified = [_]QualifiedKernel{
        qualified[1],
        qualified[0],
    };
    const report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 2,
        .tuning_qualified_signatures = 2,
        .tuning_coverage_complete = true,
    };
    var reversed = report;
    reversed.qualified_kernels = &reverse_qualified;

    const json = try renderAlloc(std.testing.allocator, report);
    defer std.testing.allocator.free(json);
    const reversed_json = try renderAlloc(std.testing.allocator, reversed);
    defer std.testing.allocator.free(reversed_json);
    try std.testing.expectEqualStrings(json, reversed_json);

    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    const parsed_qualified = parsed.value.object.get("qualified_kernels").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), parsed_qualified.len);
    try std.testing.expectEqualStrings(first_key, parsed_qualified[0].object.get("artifact_key").?.string);
    try std.testing.expectEqual(@as(i64, 1), parsed_qualified[0].object.get("rows_per_threadgroup").?.integer);
    try std.testing.expectEqual(@as(i64, 2), parsed_qualified[1].object.get("rows_per_threadgroup").?.integer);

    const typed = try std.json.parseFromSlice(ProfileReport, std.testing.allocator, json, .{});
    defer typed.deinit();
    try std.testing.expectEqual(@as(u8, 2), typed.value.qualified_kernels[1].rows_per_threadgroup);
}

test "qualification package profile loader binds canonical bytes metadata and keys" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first_key = "0000000000000000000000000000000000000000000000000000000000000001";
    const second_key = "0000000000000000000000000000000000000000000000000000000000000002";
    const entries = [_]ProfileEntry{
        testTuningEntry("prefill", 4),
        testTuningEntry("decode", 2),
    };
    const qualified = [_]QualifiedKernel{
        testQualifiedKernel(second_key, "prefill", 4),
        testQualifiedKernel(first_key, "decode", 2),
    };
    const report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        .model_fingerprint = "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        .entries = &entries,
        .qualified_kernels = &qualified,
        .tuning_selected_signatures = 2,
        .tuning_qualified_signatures = 2,
        .tuning_coverage_complete = true,
    };
    const noncanonical = try std.json.Stringify.valueAlloc(std.testing.allocator, report, .{});
    defer std.testing.allocator.free(noncanonical);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "profile.json", .data = noncanonical });
    const profile_path = try std.fs.path.join(std.testing.allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "profile.json",
    });
    defer std.testing.allocator.free(profile_path);

    var loaded = try loadCanonicalQualifiedProfile(
        std.testing.allocator,
        std.testing.io,
        profile_path,
        false,
    );
    defer loaded.deinit();
    const canonical = try renderAlloc(std.testing.allocator, report);
    defer std.testing.allocator.free(canonical);
    try std.testing.expectEqualStrings(canonical, loaded.json);
    try std.testing.expectEqual(@as(u64, 42), loaded.device_registry_id);
    try std.testing.expectEqualStrings(report.compiler_fingerprint, loaded.compiler_fingerprint);
    try std.testing.expectEqualStrings(report.model_fingerprint, loaded.model_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), loaded.keys.len);
    try std.testing.expectEqualSlices(u8, &(try kernel_jit.parseArtifactKeyHex(second_key)), &loaded.keys[0]);
    try std.testing.expectEqualSlices(u8, &(try kernel_jit.parseArtifactKeyHex(first_key)), &loaded.keys[1]);
    const input = loaded.packageInput();
    try std.testing.expectEqualStrings(loaded.json, input.canonical_profile);
    try std.testing.expectEqualSlices(kernel_jit.ArtifactKey, loaded.keys, input.keys);

    var partial_first = testTuningEntry("decode", 2);
    partial_first.estimated_gpu_nanos = 70;
    var partial_second = partial_first;
    partial_second.out_dim = 513;
    partial_second.estimated_gpu_nanos = 30;
    const partial_entries = [_]ProfileEntry{ partial_first, partial_second };
    const partial_qualified = testQualifiedKernel(first_key, "decode", 2);
    const partial = ProfileReport{
        .complete = true,
        .device_registry_id = report.device_registry_id,
        .compiler_fingerprint = report.compiler_fingerprint,
        .model_fingerprint = report.model_fingerprint,
        .entries = &partial_entries,
        .qualified_kernels = &.{partial_qualified},
        .tuning_selected_signatures = 2,
        .tuning_qualified_signatures = 1,
        .tuning_failed_signatures = 1,
        .tuning_coverage_complete = false,
    };
    const partial_json = try renderAlloc(std.testing.allocator, partial);
    defer std.testing.allocator.free(partial_json);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "profile.json", .data = partial_json });
    try std.testing.expectError(
        error.IncompleteKernelJitTuningCoverage,
        loadCanonicalQualifiedProfile(
            std.testing.allocator,
            std.testing.io,
            profile_path,
            false,
        ),
    );
}

test "workload profile JSON defaults legacy qualified kernel tile rows" {
    const legacy_json =
        \\{
        \\  "artifact_key": "0000000000000000000000000000000000000000000000000000000000000001",
        \\  "regime": "decode",
        \\  "quant_format": "q4_k",
        \\  "dispatch": "scalar",
        \\  "implementation": "bundled",
        \\  "rows": 2,
        \\  "in_dim": 256,
        \\  "out_dim": 512,
        \\  "input_encoding": "f32",
        \\  "output_encoding": "f32",
        \\  "epilogue": "none",
        \\  "fusion": "none",
        \\  "layout": "row_major",
        \\  "candidate_index": 1,
        \\  "threads_per_threadgroup": 128,
        \\  "cols_per_threadgroup": 8,
        \\  "reduction": "simdgroup",
        \\  "measured_speedup": 1.08,
        \\  "minimum_repeat_speedup": 1.03
        \\}
    ;

    const parsed = try std.json.parseFromSlice(QualifiedKernel, std.testing.allocator, legacy_json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 1), parsed.value.rows_per_threadgroup);
}

test "workload profile JSON rejects invalid qualified kernel claims" {
    const entry = testTuningEntry("decode", 2);
    const key = "0000000000000000000000000000000000000000000000000000000000000001";
    var qualified = testQualifiedKernel(key, "decode", 2);
    var report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .entries = &.{entry},
        .qualified_kernels = &.{qualified},
        .tuning_selected_signatures = 1,
        .tuning_qualified_signatures = 1,
        .tuning_coverage_complete = true,
    };

    qualified.minimum_repeat_speedup = kernel_jit.minimum_speedup - 0.001;
    report.qualified_kernels = &.{qualified};
    try std.testing.expectError(error.InvalidQualifiedKernel, renderAlloc(std.testing.allocator, report));

    qualified.minimum_repeat_speedup = kernel_jit.minimum_speedup;
    qualified.rows_per_threadgroup = 0;
    report.qualified_kernels = &.{qualified};
    try std.testing.expectError(error.InvalidQualifiedKernel, renderAlloc(std.testing.allocator, report));

    qualified.rows_per_threadgroup = 3;
    report.qualified_kernels = &.{qualified};
    try std.testing.expectError(error.InvalidQualifiedKernel, renderAlloc(std.testing.allocator, report));

    qualified.rows_per_threadgroup = 2;
    report.qualified_kernels = &.{ qualified, qualified };
    report.tuning_selected_signatures = 2;
    report.tuning_qualified_signatures = 2;
    try std.testing.expectError(error.DuplicateQualifiedKernel, renderAlloc(std.testing.allocator, report));

    report.qualified_kernels = &.{qualified};
    report.tuning_selected_signatures = 2;
    report.tuning_qualified_signatures = 1;
    try std.testing.expectError(error.InvalidProfileTuningSummary, renderAlloc(std.testing.allocator, report));

    var partial_first = entry;
    partial_first.estimated_gpu_nanos = 70;
    var partial_second = partial_first;
    partial_second.out_dim = 513;
    partial_second.estimated_gpu_nanos = 30;
    const partial_entries = [_]ProfileEntry{ partial_first, partial_second };
    report.entries = &partial_entries;
    report.tuning_failed_signatures = 1;
    report.tuning_coverage_complete = false;
    const partially_qualified_json = try renderAlloc(std.testing.allocator, report);
    std.testing.allocator.free(partially_qualified_json);

    report.tuning_selected_signatures = maximum_tuning_signatures + 1;
    report.tuning_qualified_signatures = 1;
    report.tuning_failed_signatures = maximum_tuning_signatures;
    try std.testing.expectError(error.InvalidProfileTuningSummary, renderAlloc(std.testing.allocator, report));
}

test "parsed qualification claims reject duplicate signatures and noncanonical keys" {
    const entry = testEntry("decode", 2);
    var report: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:model",
        .entries = &.{ entry, entry },
        .tuning_selected_signatures = 0,
        .tuning_qualified_signatures = 0,
    };
    try std.testing.expectError(error.DuplicateProfileEntry, validateReport(report));

    const first_key = "0000000000000000000000000000000000000000000000000000000000000001";
    const second_key = "0000000000000000000000000000000000000000000000000000000000000002";
    const first = testQualifiedKernel(first_key, "decode", 2);
    var second = first;
    second.artifact_key = second_key;
    report.entries = &.{entry};
    report.qualified_kernels = &.{ first, second };
    report.tuning_selected_signatures = 2;
    report.tuning_qualified_signatures = 2;
    try std.testing.expectError(error.DuplicateQualifiedKernel, validateReport(report));

    var noncanonical = first;
    noncanonical.artifact_key = "A000000000000000000000000000000000000000000000000000000000000001";
    report.qualified_kernels = &.{noncanonical};
    report.tuning_selected_signatures = 1;
    report.tuning_qualified_signatures = 1;
    try std.testing.expectError(error.InvalidQualifiedKernel, validateReport(report));
}

test "component profile bundle is relocatable and accepts complete zero-winner members" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const bundle_path = try std.fs.path.join(std.testing.allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "clipclap.bundle.json",
    });
    defer std.testing.allocator.free(bundle_path);

    const entry = testTuningEntry("encoder", 2);
    const key = "0000000000000000000000000000000000000000000000000000000000000001";
    const qualified = testQualifiedKernel(key, "encoder", 2);
    var primary: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:primary",
        .entries = &.{entry},
        .qualified_kernels = &.{qualified},
        .tuning_selected_signatures = 1,
        .tuning_qualified_signatures = 1,
        .tuning_coverage_complete = true,
    };
    const audio: ProfileReport = .{
        .complete = true,
        .device_registry_id = 42,
        .compiler_fingerprint = "sha256:compiler",
        .model_fingerprint = "sha256:audio",
        .entries = &.{testEntry("encoder", 2)},
    };
    try writeBundle(std.testing.allocator, std.testing.io, bundle_path, &.{
        .{ .component = .primary, .report = primary },
        .{ .component = .audio, .report = audio },
    });

    var first = try loadQualifiedProfileBundle(std.testing.allocator, std.testing.io, bundle_path);
    defer first.deinit();
    const first_paths = first.kernelJitBundle();
    try std.testing.expect(first_paths.audio != null);
    try std.testing.expect(!std.mem.eql(u8, first_paths.primary, first_paths.audio.?));
    try std.testing.expect(first.hasQualifiedKernels(.primary));
    try std.testing.expect(!first.hasQualifiedKernels(.audio));
    try std.testing.expect(first.kernelJitBundleForMode(.on).audio == null);
    try std.testing.expect(first.kernelJitBundleForMode(.required).audio != null);

    const old_primary_path = try std.testing.allocator.dupe(u8, first_paths.primary);
    defer std.testing.allocator.free(old_primary_path);
    primary.model_fingerprint = "sha256:primary-v2";
    try writeBundle(std.testing.allocator, std.testing.io, bundle_path, &.{
        .{ .component = .primary, .report = primary },
        .{ .component = .audio, .report = audio },
    });
    var second = try loadQualifiedProfileBundle(std.testing.allocator, std.testing.io, bundle_path);
    defer second.deinit();
    try std.testing.expect(!std.mem.eql(u8, old_primary_path, second.kernelJitBundle().primary));
    const old_primary = try compat.cwd().readFileAlloc(
        std.testing.io,
        old_primary_path,
        std.testing.allocator,
        .limited(maximum_profile_bytes),
    );
    defer std.testing.allocator.free(old_primary);
    try std.testing.expect(old_primary.len > 0);
}
