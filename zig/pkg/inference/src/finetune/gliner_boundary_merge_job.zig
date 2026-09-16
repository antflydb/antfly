// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One bounded, immutable-source adapter materialization. The caller supplies
//! the process admission owner. The CLI additionally owns a disposable worker
//! with a hard teardown deadline; a first signal cancels private staging.
const std = @import("std");
const platform = @import("antfly_platform");
const merge = @import("gliner_boundary_merge.zig");
const adapter = @import("gliner_boundary_adapter.zig");
const source_mod = @import("gliner_boundary_training_source.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const files = @import("../runtime/file_snapshot.zig");
const memory = @import("../runtime/tier/memory.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
pub const max_config_bytes = 64 * 1024;
/// Fixed pre-admission setup reservation, independent of job-controlled limits.
/// Includes the parser owner and all allocations retained by its snapshot.
pub const config_setup_bytes = 8 * mib;

pub const Memory = struct {
    combined_bytes: usize = 3 * 1024 * mib,
    job_bytes: usize = 4 * mib,
    /// Exact adapter file bytes are reserved twice during import, plus this
    /// allowance for parsed headers, tensor names, shapes and arena growth.
    adapter_auxiliary_bytes: usize = 32 * mib,
    max_adapter_owner_bytes: usize = 512 * mib,
};
pub const Config = struct {
    version: u32,
    source_dir: []const u8,
    adapter_dir: []const u8,
    output_dir: []const u8,
    expected_source: bundle.Identity,
    expected_adapter: merge.AdapterFiles,
    schema_sha256: [32]u8,
    source_limits: source_mod.Limits = .{},
    merge_limits: merge.Limits = .{},
    memory: Memory = .{},
    timeout_seconds: u32 = 30 * 60,
    disk_headroom_bytes: u64 = 256 * mib,
};
pub const ConfigSnapshot = struct {
    owner: *ConfigOwner,
    parsed: std.json.Parsed(Config),
    digest: bundle.Digest,

    pub fn allocator(self: *const ConfigSnapshot) Allocator {
        return self.owner.scratch.allocator();
    }
    pub fn setupUsage(self: *const ConfigSnapshot) SetupUsage {
        return .{ .reserved_bytes = self.owner.reserved_bytes, .live_bytes = @sizeOf(ConfigOwner) + self.owner.scratch.budget.live, .peak_bytes = @sizeOf(ConfigOwner) + self.owner.scratch.budget.peak };
    }
    pub fn mapAllocationError(self: *const ConfigSnapshot, err: anyerror) anyerror {
        return self.owner.mapError(err);
    }
    pub fn deinit(self: *ConfigSnapshot) void {
        self.parsed.deinit();
        self.owner.deinit();
        self.* = undefined;
    }
};
pub const SetupUsage = struct { reserved_bytes: usize, live_bytes: usize, peak_bytes: usize };
const ConfigOwner = struct {
    backing: Allocator,
    scratch: merge.Scratch,
    reserved_bytes: usize,

    fn create(a: Allocator, reserved: usize) !*ConfigOwner {
        if (reserved <= @sizeOf(ConfigOwner) or reserved > config_setup_bytes) return error.BoundaryMergeJobConfigLimitExceeded;
        const self = try a.create(ConfigOwner);
        self.* = .{ .backing = a, .scratch = merge.Scratch.init(a, reserved - @sizeOf(ConfigOwner)), .reserved_bytes = reserved };
        return self;
    }
    fn mapError(self: *const ConfigOwner, err: anyerror) anyerror {
        return switch (self.scratch.mapError(err)) {
            error.BoundaryMergeMemoryLimitExceeded => error.BoundaryMergeJobConfigLimitExceeded,
            else => |mapped| mapped,
        };
    }
    fn deinit(self: *ConfigOwner) void {
        const a = self.backing;
        self.scratch.deinit();
        a.destroy(self);
    }
};
pub const Result = struct {
    version: u32 = 1,
    status: enum { complete } = .complete,
    configuration: bundle.Digest,
    setup: SetupUsage,
    model: merge.Result,
    admitted_host_bytes: usize,
    source_reserved_bytes: usize,
    adapter_reserved_bytes: usize,
    peak_adapter_bytes: usize,
    peak_job_bytes: usize,
};

/// Digest and parser consume the same owned, bounded descriptor snapshot.
/// CLI parent/worker compare this exact digest before either source is read.
pub fn loadConfigSnapshot(a: Allocator, io: std.Io, path: []const u8) !ConfigSnapshot {
    const owner = try ConfigOwner.create(a, config_setup_bytes);
    errdefer owner.deinit();
    const bounded = owner.scratch.allocator();
    const bytes = files.read(bounded, io, std.Io.Dir.cwd(), path, max_config_bytes, null) catch |err| return owner.mapError(err);
    defer bounded.free(bytes);
    return .{ .owner = owner, .parsed = parse(bounded, bytes) catch |err| return owner.mapError(err), .digest = bundle.Digest.of(bytes) };
}
/// Bounded in-memory equivalent for library callers. Both execution entry
/// routes retain this owner rather than accepting an unbound parsed Config.
pub fn parseSnapshot(a: Allocator, bytes: []const u8) !ConfigSnapshot {
    return parseSnapshotBounded(a, bytes, config_setup_bytes);
}
fn parseSnapshotBounded(a: Allocator, bytes: []const u8, reserved: usize) !ConfigSnapshot {
    if (bytes.len == 0 or bytes.len > max_config_bytes) return error.BoundaryMergeJobConfigLimitExceeded;
    const owner = try ConfigOwner.create(a, reserved);
    errdefer owner.deinit();
    const bounded = owner.scratch.allocator();
    const owned_bytes = bounded.dupe(u8, bytes) catch |err| return owner.mapError(err);
    defer bounded.free(owned_bytes);
    return .{ .owner = owner, .parsed = parse(bounded, owned_bytes) catch |err| return owner.mapError(err), .digest = bundle.Digest.of(owned_bytes) };
}
/// Internal parser: callers supply their bounded owner's allocator. Public
/// execution uses parseSnapshot/loadConfigSnapshot so its lifetime is charged.
pub fn parse(a: Allocator, bytes: []const u8) !std.json.Parsed(Config) {
    if (bytes.len == 0 or bytes.len > max_config_bytes) return error.BoundaryMergeJobConfigLimitExceeded;
    try merge.jsonDepth(bytes, 16);
    // Type-aware checking prevents quoted integers, floating-point resource
    // values and lossy conversions from entering the numeric wire contract.
    const raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer raw.deinit();
    try strictTypes(Config, raw.value);
    const parsed = try std.json.parseFromSlice(Config, a, bytes, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}
fn strictTypes(comptime T: type, value: std.json.Value) !void {
    switch (@typeInfo(T)) {
        .int => if (value != .integer or value.integer < 0) return error.InvalidBoundaryMergeJob,
        .float => if (value != .integer and value != .float) return error.InvalidBoundaryMergeJob,
        .@"enum" => if (value != .string) return error.InvalidBoundaryMergeJob,
        .optional => |info| if (value != .null) try strictTypes(info.child, value),
        .@"struct" => {
            if (value != .object) return error.InvalidBoundaryMergeJob;
            inline for (std.meta.fields(T)) |field| if (value.object.get(field.name)) |child| try strictTypes(field.type, child);
        },
        .array => |info| {
            if (value == .string and info.child == u8) return;
            if (value != .array or value.array.items.len != info.len) return error.InvalidBoundaryMergeJob;
            for (value.array.items) |child| try strictTypes(info.child, child);
        },
        .pointer => |info| {
            if (info.size != .slice or info.child != u8 or value != .string) return error.InvalidBoundaryMergeJob;
        },
        else => @compileError("Unsupported merge job wire type"),
    }
}

pub fn validate(config: Config) !void {
    if (config.version != 1 or config.timeout_seconds == 0 or config.timeout_seconds > 24 * 60 * 60 or
        config.disk_headroom_bytes > 16 * 1024 * mib or config.memory.job_bytes < mib or config.memory.job_bytes > 64 * mib or
        config.memory.combined_bytes == 0 or config.memory.combined_bytes > 16 * 1024 * mib or
        config.memory.adapter_auxiliary_bytes == 0 or config.memory.adapter_auxiliary_bytes > 512 * mib or
        config.memory.max_adapter_owner_bytes == 0 or config.memory.max_adapter_owner_bytes > 4 * 1024 * mib)
        return error.InvalidBoundaryMergeJob;
    try merge.validateLimits(config.merge_limits);
    const limits = config.source_limits;
    if (limits.max_source_bytes == 0 or limits.max_source_bytes > 4 * 1024 * mib or limits.max_auxiliary_bytes == 0 or limits.max_auxiliary_bytes > 1024 * mib or
        limits.max_weight_bytes == 0 or limits.max_weight_bytes > 2 * 1024 * mib or limits.max_tokenizer_bytes == 0 or limits.max_tokenizer_bytes > 64 * mib or
        limits.max_config_bytes == 0 or limits.max_config_bytes > mib or limits.max_header_bytes == 0 or limits.max_header_bytes > 64 * mib or
        limits.max_json_depth == 0 or limits.max_json_depth > 128) return error.InvalidBoundaryMergeJob;
    for ([_][]const u8{ config.source_dir, config.adapter_dir, config.output_dir }) |path| if (!pathValid(path)) return error.InvalidBoundaryMergeJobPath;
    if (std.mem.eql(u8, config.output_dir, config.source_dir) or std.mem.eql(u8, config.output_dir, config.adapter_dir)) return error.InvalidBoundaryMergeJobPath;
    if (config.expected_source.precision != .fp32) return error.UnsupportedBoundaryAdapterBasePrecision;
    try digestValid(config.expected_source.weight, limits.max_weight_bytes);
    for (config.expected_source.sidecars, 0..) |pin, index| try digestValid(pin, if (index == 2) limits.max_tokenizer_bytes else limits.max_config_bytes);
    try digestValid(config.expected_adapter.config, config.merge_limits.adapter.max_config_bytes);
    try digestValid(config.expected_adapter.weights, config.merge_limits.adapter.max_tensor_bytes);
    if (config.expected_adapter.receipt) |pin| try digestValid(pin, config.merge_limits.adapter.max_receipt_bytes);
    _ = try adapterReservation(config);
}
fn pathValid(path: []const u8) bool {
    if (!std.fs.path.isAbsolute(path) or path.len > std.fs.max_path_bytes or path.len < 2 or path[path.len - 1] == '/') return false;
    for (path) |byte| if (byte < 32 or byte == 127) return false;
    var parts = std.mem.splitScalar(u8, path[1..], '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}
fn digestValid(pin: bundle.Digest, maximum: u64) !void {
    if (pin.size_bytes == 0 or pin.size_bytes > maximum) return error.BoundaryMergeIdentityMismatch;
    for (pin.sha256) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.BoundaryMergeIdentityMismatch;
}
pub fn adapterReservation(config: Config) !usize {
    var raw = try add(config.expected_adapter.config.size_bytes, config.expected_adapter.weights.size_bytes);
    if (config.expected_adapter.receipt) |pin| raw = try add(raw, pin.size_bytes);
    const reserved = try add(try mul(raw, 2), config.memory.adapter_auxiliary_bytes);
    if (reserved > config.memory.max_adapter_owner_bytes) return error.BoundaryMergeMemoryLimitExceeded;
    return std.math.cast(usize, reserved) orelse error.BoundaryMergeMemoryLimitExceeded;
}
pub fn admissionBytes(config: Config, source_reserved: usize) !usize {
    try validate(config);
    const total = try add(config_setup_bytes, try add(try add(source_reserved, try adapterReservation(config)), try add(config.memory.job_bytes, config.merge_limits.max_scratch_bytes)));
    if (total > config.memory.combined_bytes) return error.BoundaryMergeMemoryLimitExceeded;
    return std.math.cast(usize, total) orelse error.BoundaryMergeMemoryLimitExceeded;
}

/// The snapshot and any argument allocations made through its allocator must
/// remain alive until return. Its fixed setup reservation is included in the
/// acquired process lease and its measured usage is reported separately.
pub fn execute(a: Allocator, io: std.Io, snapshot: *const ConfigSnapshot, admission: *memory.AdmissionController, outer_control: ?Control) !Result {
    const config = snapshot.parsed.value;
    const configuration = snapshot.digest;
    try validate(config);
    if (snapshot.owner.reserved_bytes > config_setup_bytes) return error.BoundaryMergeJobConfigLimitExceeded;
    try digestValid(configuration, max_config_bytes);
    var bounded_control = outer_control orelse Control{};
    bounded_control.io = io;
    const deadline = try std.math.add(u64, platform.time.monotonicNs(), try std.math.mul(u64, config.timeout_seconds, std.time.ns_per_s));
    bounded_control.deadline_ns = @min(deadline, bounded_control.deadline_ns orelse deadline);
    const control: ?Control = bounded_control;
    try check(control);
    try merge.validateDestination(io, config.output_dir);
    const output_parent = std.fs.path.dirname(config.output_dir) orelse return error.InvalidBoundaryMergeJobPath;
    var prospective_output = try add(config.expected_source.weight.size_bytes, try add(config.merge_limits.max_header_bytes, config.merge_limits.max_receipt_bytes));
    prospective_output = try add(prospective_output, 8 + 64 * 1024);
    for (config.expected_source.sidecars) |pin| prospective_output = try add(prospective_output, pin.size_bytes);
    if (prospective_output > config.merge_limits.max_output_bytes) return error.BoundaryMergeOutputLimitExceeded;
    try requireDisk(try platform.filesystem.capacity(output_parent), prospective_output, config.disk_headroom_bytes);
    const source_reserved = try source_mod.reservation(io, config.source_dir, config.source_limits, control);
    const adapter_reserved = try adapterReservation(config);
    const combined = try admissionBytes(config, source_reserved);
    var lease = try admission.tryAcquire(.cpu, .{ .host_limit_bytes = config.memory.combined_bytes, .backend_limit_bytes = 0, .combined_limit_bytes = config.memory.combined_bytes }, .{ .host_scratch_bytes = combined }, true);
    defer lease.release();
    var job_scratch = merge.Scratch.init(a, config.memory.job_bytes);
    defer job_scratch.deinit();
    var adapter_scratch = merge.Scratch.init(a, adapter_reserved);
    defer adapter_scratch.deinit();
    var result = executeOwned(a, io, config, configuration, source_reserved, combined, control, &job_scratch, &adapter_scratch) catch |err| {
        // These disjoint owners retain separate terminal allocation evidence.
        // Successful earlier phases cannot relabel backing OOM in merge/source.
        if (err == error.OutOfMemory or err == error.WriteFailed) {
            if (adapter_scratch.last_failure != null) return adapter_scratch.mapError(err);
            if (job_scratch.last_failure != null) return job_scratch.mapError(err);
        }
        return err;
    };
    result.peak_adapter_bytes = adapter_scratch.budget.peak;
    result.peak_job_bytes = job_scratch.budget.peak;
    result.setup = snapshot.setupUsage();
    return result;
}

fn executeOwned(a: Allocator, io: std.Io, config: Config, configuration: bundle.Digest, source_reserved: usize, combined: usize, control: ?Control, job_scratch: *merge.Scratch, adapter_scratch: *merge.Scratch) !Result {
    // Snapshot adapter files before loading the original model. All file
    // descriptors are opened together; optional absence is checked explicitly.
    var opened = try OpenedAdapter.open(io, config.adapter_dir, config.expected_adapter, control);
    defer opened.deinit(io);
    var loaded = try opened.load(adapter_scratch.allocator(), io, config, control);
    defer loaded.deinit();
    // No adapter source path will be consulted after this point.
    adapter_scratch.resetFailure();
    var limits = config.source_limits;
    limits.max_source_bytes = source_reserved;
    const source = try source_mod.Source.open(a, io, config.source_dir, .{ .limits = limits, .expected_identity = config.expected_source }, control);
    defer source.deinit();
    const provenance = merge.Provenance{ .configuration = configuration, .adapter_files = config.expected_adapter, .schema_sha256 = config.schema_sha256 };
    const estimate = try merge.estimate(a, source, &loaded, provenance, config.merge_limits, control);
    const parent = std.fs.path.dirname(config.output_dir) orelse return error.InvalidBoundaryMergeJobPath;
    try requireDisk(try platform.filesystem.capacity(parent), estimate.output_bytes_upper_bound, config.disk_headroom_bytes);
    // Charge small job metadata separately; merge owns its own reclaiming
    // scratch budget and all temporary source storage stays with Source.
    const output = try job_scratch.allocator().dupe(u8, config.output_dir);
    defer job_scratch.allocator().free(output);
    job_scratch.resetFailure();
    const result = try merge.materialize(a, io, source, &loaded, output, provenance, config.merge_limits, control);
    return .{ .configuration = configuration, .setup = undefined, .model = result, .admitted_host_bytes = combined, .source_reserved_bytes = source_reserved, .adapter_reserved_bytes = adapter_scratch.budget.limit, .peak_adapter_bytes = 0, .peak_job_bytes = 0 };
}

const OpenedAdapter = struct {
    descriptors: [3]?std.Io.File = .{null} ** 3,
    pins: [3]?bundle.Digest,

    fn open(io: std.Io, path: []const u8, pins: merge.AdapterFiles, control: ?Control) !OpenedAdapter {
        const directory = try std.Io.Dir.cwd().openDir(io, path, .{});
        defer directory.close(io);
        var self = OpenedAdapter{ .pins = .{ pins.config, pins.weights, pins.receipt } };
        errdefer self.deinit(io);
        for ([_][]const u8{ adapter.config_name, adapter.tensor_name, adapter.receipt_name }, 0..) |name, index| {
            try check(control);
            const file = files.openRegular(io, directory, name, control) catch |err| switch (err) {
                error.FileNotFound => if (self.pins[index] == null) continue else return error.BoundaryMergeIdentityMismatch,
                else => return err,
            };
            self.descriptors[index] = file;
            const expected = self.pins[index] orelse return error.BoundaryMergeIdentityMismatch;
            if ((try file.stat(io)).size != expected.size_bytes) return error.BoundaryMergeIdentityMismatch;
        }
        return self;
    }
    fn load(self: *OpenedAdapter, a: Allocator, io: std.Io, config: Config, control: ?Control) !adapter.Loaded {
        var bytes: [3]?[]u8 = .{null} ** 3;
        defer for (bytes) |raw| if (raw) |value| a.free(value);
        for (self.descriptors, self.pins, &bytes) |file, pin, *raw| if (file) |actual| {
            raw.* = try files.readOpened(a, io, actual, @intCast(pin.?.size_bytes), control);
            try merge.verifyBytes(raw.*.?, pin.?, control);
        };
        var frozen: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&frozen, &config.expected_source.weight.sha256);
        return adapter.importBytes(a, bytes[0].?, bytes[1].?, bytes[2], .{ .source = config.expected_source, .schema_sha256 = config.schema_sha256, .frozen_weight_sha256 = frozen }, config.merge_limits.adapter, control);
    }
    fn deinit(self: *OpenedAdapter, io: std.Io) void {
        for (self.descriptors) |file| if (file) |actual| actual.close(io);
        self.* = undefined;
    }
};
fn requireDisk(capacity: anytype, additional: u64, headroom: u64) !void {
    if (try add(additional, headroom) > capacity.available_bytes) return error.BoundaryMergeDiskLimitExceeded;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn add(left: u64, right: u64) !u64 {
    return std.math.add(u64, left, right) catch error.BoundaryMergeMemoryLimitExceeded;
}
fn mul(left: u64, right: u64) !u64 {
    return std.math.mul(u64, left, right) catch error.BoundaryMergeMemoryLimitExceeded;
}

fn testConfig() Config {
    return .{
        .version = 1,
        .source_dir = "/tmp/immutable-source",
        .adapter_dir = "/tmp/adapter",
        .output_dir = "/tmp/new-merged-model",
        .expected_source = .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of("source snapshot"), .sidecars = .{bundle.Digest.of("{}")} ** 4 },
        .expected_adapter = .{ .config = bundle.Digest.of("config"), .weights = bundle.Digest.of("weights"), .receipt = null },
        .schema_sha256 = @splat(9),
    };
}
fn parseAllocationFailures(a: Allocator, bytes: []const u8) !void {
    var parsed = try parse(a, bytes);
    defer parsed.deinit();
}

test "boundary merge job strict versioned config and exact snapshot ownership" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const json = try std.json.Stringify.valueAlloc(a, testConfig(), .{});
    defer a.free(json);
    var parsed = try parse(a, json);
    defer parsed.deinit();
    try std.testing.expectEqual(testConfig().expected_source, parsed.value.expected_source);
    try std.testing.checkAllAllocationFailures(a, parseAllocationFailures, .{json});
    const invalid_numbers = [_][]const u8{ "-1", "1.5", "\"2\"", "true", "18446744073709551616" };
    for (invalid_numbers) |number| {
        const invalid = try std.fmt.allocPrint(a, "{{\"version\":{s}{s}", .{ number, json["{\"version\":1".len..] });
        defer a.free(invalid);
        if (parse(a, invalid)) |bad| {
            bad.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    const duplicate = try std.fmt.allocPrint(a, "{{\"version\":1,{s}", .{json[1..]});
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, parse(a, duplicate));
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const directory = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const path = try std.fs.path.join(a, &.{ directory, "merge.json" });
    defer a.free(path);
    try temporary.dir.writeFile(io, .{ .sub_path = "merge.json", .data = json });
    var snapshot = try loadConfigSnapshot(a, io, path);
    defer snapshot.deinit();
    try std.testing.expectEqual(bundle.Digest.of(json), snapshot.digest);
    var changed = testConfig();
    changed.schema_sha256[0] ^= 1;
    const changed_json = try std.json.Stringify.valueAlloc(a, changed, .{});
    defer a.free(changed_json);
    try temporary.dir.writeFile(io, .{ .sub_path = "merge.json", .data = changed_json });
    try std.testing.expectEqual(testConfig().schema_sha256, snapshot.parsed.value.schema_sha256);
    var latest = try loadConfigSnapshot(a, io, path);
    defer latest.deinit();
    try std.testing.expect(!std.meta.eql(snapshot.digest, latest.digest));
}

test "boundary merge job admission charges source adapter import peak scratch and disk" {
    var config = testConfig();
    const adapter_bytes = try adapterReservation(config);
    const source_bytes = 512 * mib;
    try std.testing.expectEqual(config_setup_bytes + source_bytes + adapter_bytes + config.memory.job_bytes + config.merge_limits.max_scratch_bytes, try admissionBytes(config, source_bytes));
    config.memory.combined_bytes = source_bytes;
    try std.testing.expectError(error.BoundaryMergeMemoryLimitExceeded, admissionBytes(config, source_bytes));
    config = testConfig();
    try std.testing.expectError(error.BoundaryMergeMemoryLimitExceeded, admissionBytes(config, std.math.maxInt(usize)));
    config.memory.max_adapter_owner_bytes = adapter_bytes - 1;
    try std.testing.expectError(error.BoundaryMergeMemoryLimitExceeded, validate(config));
    config = testConfig();
    config.merge_limits.adapter.max_rank = 1025;
    try std.testing.expectError(error.InvalidBoundaryMergeLimits, validate(config));
    config = testConfig();
    config.source_limits.max_weight_bytes = 2 * 1024 * mib + 1;
    try std.testing.expectError(error.InvalidBoundaryMergeJob, validate(config));
    config = testConfig();
    config.output_dir = "/tmp/source/../merged";
    try std.testing.expectError(error.InvalidBoundaryMergeJobPath, validate(config));
    try requireDisk(.{ .available_bytes = @as(u64, 30) }, 20, 10);
    try std.testing.expectError(error.BoundaryMergeDiskLimitExceeded, requireDisk(.{ .available_bytes = @as(u64, 29) }, 20, 10));
    try std.testing.expectError(error.BoundaryMergeMemoryLimitExceeded, requireDisk(.{ .available_bytes = std.math.maxInt(u64) }, std.math.maxInt(u64), 1));
}

test "boundary merge job adapter descriptors reject receipt presence and changed snapshots" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(path);
    try temporary.dir.writeFile(io, .{ .sub_path = adapter.config_name, .data = "config" });
    try temporary.dir.writeFile(io, .{ .sub_path = adapter.tensor_name, .data = "weights" });
    const config = testConfig();
    var opened = try OpenedAdapter.open(io, path, config.expected_adapter, null);
    defer opened.deinit(io);
    try temporary.dir.writeFile(io, .{ .sub_path = adapter.receipt_name, .data = "receipt" });
    try std.testing.expectError(error.BoundaryMergeIdentityMismatch, OpenedAdapter.open(io, path, config.expected_adapter, null));
    // Same-size mutation is rejected by the exact consumed digest, before
    // adapter parsing or any source model allocation can occur.
    try temporary.dir.writeFile(io, .{ .sub_path = adapter.config_name, .data = "Config" });
    try std.testing.expectError(error.BoundaryMergeIdentityMismatch, opened.load(a, io, config, null));
    // A renamed replacement does not alter descriptors already held by this
    // import, and still cannot evade the original snapshot hash.
    try temporary.dir.deleteFile(io, adapter.config_name);
    try temporary.dir.writeFile(io, .{ .sub_path = adapter.config_name, .data = "config" });
    try std.testing.expectError(error.BoundaryMergeIdentityMismatch, opened.load(a, io, config, null));
    var pinned_receipt = config.expected_adapter;
    pinned_receipt.receipt = bundle.Digest.of("receipt");
    var present = try OpenedAdapter.open(io, path, pinned_receipt, null);
    present.deinit(io);
    try temporary.dir.deleteFile(io, adapter.receipt_name);
    try std.testing.expectError(error.BoundaryMergeIdentityMismatch, OpenedAdapter.open(io, path, pinned_receipt, null));
}

fn snapshotAllocationFailures(a: Allocator, bytes: []const u8) !void {
    var snapshot = try parseSnapshot(a, bytes);
    defer snapshot.deinit();
    const argument = try snapshot.allocator().dupe(u8, "/tmp/original invocation with spaces");
    defer snapshot.allocator().free(argument);
    try std.testing.expectEqual(bundle.Digest.of(bytes), snapshot.digest);
    const usage = snapshot.setupUsage();
    try std.testing.expect(usage.live_bytes > 0 and usage.live_bytes <= usage.peak_bytes and usage.peak_bytes <= usage.reserved_bytes);
}

test "boundary merge job bounded setup owner recovers declared and backing OOM without changing snapshot" {
    const a = std.testing.allocator;
    const bytes = try std.json.Stringify.valueAlloc(a, testConfig(), .{});
    defer a.free(bytes);
    try std.testing.checkAllAllocationFailures(a, snapshotAllocationFailures, .{bytes});
    try std.testing.expectError(error.BoundaryMergeJobConfigLimitExceeded, parseSnapshotBounded(a, bytes, @sizeOf(ConfigOwner) + 128));
    var snapshot = try parseSnapshot(a, bytes);
    defer snapshot.deinit();
    const digest = snapshot.digest;
    const before = snapshot.setupUsage();
    const full_limit = snapshot.owner.scratch.budget.limit;
    snapshot.owner.scratch.budget.limit = snapshot.owner.scratch.budget.live;
    try std.testing.expectError(error.OutOfMemory, snapshot.allocator().alloc(u8, 1));
    try std.testing.expectEqual(error.BoundaryMergeJobConfigLimitExceeded, snapshot.mapAllocationError(error.OutOfMemory));
    try std.testing.expectEqual(before.live_bytes, snapshot.setupUsage().live_bytes);
    snapshot.owner.scratch.budget.limit = full_limit;
    snapshot.owner.scratch.resetFailure();
    const original_backing = snapshot.owner.scratch.budget.backing;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    snapshot.owner.scratch.budget.backing = failing.allocator();
    defer snapshot.owner.scratch.budget.backing = original_backing;
    try std.testing.expectError(error.OutOfMemory, snapshot.allocator().alloc(u8, 1));
    try std.testing.expectEqual(error.OutOfMemory, snapshot.mapAllocationError(error.OutOfMemory));
    snapshot.owner.scratch.budget.backing = original_backing;
    snapshot.owner.scratch.resetFailure();
    const recovered = try snapshot.allocator().dupe(u8, "argv copy after denial");
    defer snapshot.allocator().free(recovered);
    try std.testing.expectEqual(digest, snapshot.digest);
    try std.testing.expectEqual(testConfig().schema_sha256, snapshot.parsed.value.schema_sha256);
    try std.testing.expectEqual(config_setup_bytes, snapshot.setupUsage().reserved_bytes);
}
