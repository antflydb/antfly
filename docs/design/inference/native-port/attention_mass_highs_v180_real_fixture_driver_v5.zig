//! Isolated real-HiGHS v1.8.0 differential driver for the exported 1128x288
//! restricted mass LP. This file is deliberately outside canonical runtime
//! sources. It is not a fitter and grants no execution or publication authority.
//!
//! The parent must preflight a complete pinned static-library receipt. The
//! driver repeats the receipt, fixture, ABI, option, lifecycle and witness
//! checks. It retains the rejected 1e-9 vectors and performs exactly one fresh
//! 1e-10 solve only when the frozen original-coordinate witness rejects.
const std = @import("std");
const mass_lp = @import("attention_mass_lp");
const cg = @import("attention_mass_cg");
const highs = @import("attention_mass_highs");

const fixture_schema = "antfly.attention-matching.mass-highs-restricted-fixture.v1";
const library_schema = "antfly.attention-matching.highs-v180-static-library.v2";
const result_schema = "antfly.attention-matching.mass-highs-v180-real-restricted-differential.v5";
const executable_schema = "antfly.attention-matching.highs-v180-real-driver-executable.v1";
const lp_sha256 = "ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de";
const controller_sha256 = "abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa";
const adapter_sha256 = "f9962c9ec47d6399c15eeda30c674fee0993ead3fbcf6f4527bc8037c75e0dc8";
const rows: usize = 1128;
const columns: usize = 288;
const constraints: usize = rows * 2;
const variables: usize = columns + 1;
const max_json_bytes = 4 * 1024 * 1024;
const max_fixture_bytes = rows * columns * @sizeOf(f64) + 4096;

extern fn Highs_create() callconv(.c) ?*anyopaque;
extern fn Highs_destroy(*anyopaque) callconv(.c) void;
extern fn Highs_versionMajor() callconv(.c) c_int;
extern fn Highs_versionMinor() callconv(.c) c_int;
extern fn Highs_versionPatch() callconv(.c) c_int;
extern fn Highs_githash() callconv(.c) [*:0]const u8;
extern fn Highs_getSizeofHighsInt(*const anyopaque) callconv(.c) c_int;
extern fn Highs_getInfinity(*const anyopaque) callconv(.c) f64;
extern fn Highs_setBoolOptionValue(*anyopaque, [*:0]const u8, c_int) callconv(.c) c_int;
extern fn Highs_setIntOptionValue(*anyopaque, [*:0]const u8, c_int) callconv(.c) c_int;
extern fn Highs_setDoubleOptionValue(*anyopaque, [*:0]const u8, f64) callconv(.c) c_int;
extern fn Highs_setStringOptionValue(*anyopaque, [*:0]const u8, [*:0]const u8) callconv(.c) c_int;
extern fn Highs_setCallback(*anyopaque, highs.Callback, ?*anyopaque) callconv(.c) c_int;
extern fn Highs_startCallback(*anyopaque, c_int) callconv(.c) c_int;
extern fn Highs_passLp(
    *anyopaque,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    f64,
    [*]const f64,
    [*]const f64,
    [*]const f64,
    [*]const f64,
    [*]const f64,
    [*]const c_int,
    [*]const c_int,
    [*]const f64,
) callconv(.c) c_int;
extern fn Highs_run(*anyopaque) callconv(.c) c_int;
extern fn Highs_getModelStatus(*const anyopaque) callconv(.c) c_int;
extern fn Highs_getSolution(*const anyopaque, [*]f64, [*]f64, [*]f64, [*]f64) callconv(.c) c_int;
extern fn Highs_getObjectiveValue(*const anyopaque) callconv(.c) f64;
extern fn Highs_getIntInfoValue(*const anyopaque, [*:0]const u8, *c_int) callconv(.c) c_int;

const Edge = struct { path: []const u8, sha256: []const u8, bytes: u64 };
const ArrayEdge = struct {
    name: []const u8,
    path: []const u8,
    sha256: []const u8,
    bytes: u64,
    dtype: []const u8,
    shape: []const usize,
    order: []const u8,
};
const FixtureProblem = struct {
    builder: []const u8,
    source_rows: usize,
    weight_columns: usize,
    variables: usize,
    constraint_rows: usize,
    variable_order: []const u8,
    constraint_order: []const u8,
    bounds: std.json.Value,
    objective: []const u8,
    raw_row_dual_mapping: []const u8,
};
const FixtureOracle = struct {
    vector_evidence: []const u8,
    objective: f64,
    iterations: usize,
    witness_all_checks_passed: bool,
    primal_max_violation: f64,
    expected_controller_action: ?[]const u8 = null,
};
const FixtureManifest = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    source_edges: std.json.Value,
    raw_arrays: []const ArrayEdge,
    problem: FixtureProblem,
    oracles: struct {
        original_1e_9: FixtureOracle,
        retry_1e_10: FixtureOracle,
    },
    limits: std.json.Value,
};

const LibraryReceipt = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    source: struct {
        tag: []const u8,
        commit: []const u8,
        runtime_githash: []const u8,
        archive: Edge,
        tree_sha256: []const u8,
        license: Edge,
        highs_c_api_header: Edge,
        callback_header: Edge,
        generated_hconfig: Edge,
    },
    build: struct {
        target: []const u8,
        architecture: []const u8,
        compiler: []const u8,
        compiler_sha256: []const u8,
        cmake: []const u8,
        cmake_sha256: []const u8,
        flags: []const []const u8,
        built_static: bool,
        highsint64: bool,
        highs_no_default_threads: bool,
    },
    artifacts: struct {
        libhighs: Edge,
        symbol_inventory: Edge,
        link_inventory: Edge,
    },
};

const ExecutableReceipt = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    target: []const u8,
    optimize: []const u8,
    strip: bool,
    zig_executable: Edge,
    link_argv: []const []const u8,
    sources: struct {
        driver: Edge,
        lp: Edge,
        controller: Edge,
        adapter: Edge,
    },
    library_receipt: Edge,
    libhighs: Edge,
    binary: Edge,
};

const Lifecycle = struct {
    creates_attempted: usize = 0,
    creates_succeeded: usize = 0,
    destroys: usize = 0,
    passes: usize = 0,
    runs: usize = 0,
    solutions: usize = 0,
    option_sets: usize = 0,
    callbacks_registered: usize = 0,
    callbacks_started: usize = 0,
};
var lifecycle = Lifecycle{};

fn realCreate() callconv(.c) ?*anyopaque {
    lifecycle.creates_attempted += 1;
    const result = Highs_create();
    if (result != null) lifecycle.creates_succeeded += 1;
    return result;
}
fn realDestroy(handle: *anyopaque) callconv(.c) void {
    lifecycle.destroys += 1;
    Highs_destroy(handle);
}
fn realBool(h: *anyopaque, n: [*:0]const u8, v: c_int) callconv(.c) c_int {
    lifecycle.option_sets += 1;
    return Highs_setBoolOptionValue(h, n, v);
}
fn realInt(h: *anyopaque, n: [*:0]const u8, v: c_int) callconv(.c) c_int {
    lifecycle.option_sets += 1;
    return Highs_setIntOptionValue(h, n, v);
}
fn realDouble(h: *anyopaque, n: [*:0]const u8, v: f64) callconv(.c) c_int {
    lifecycle.option_sets += 1;
    return Highs_setDoubleOptionValue(h, n, v);
}
fn realString(h: *anyopaque, n: [*:0]const u8, v: [*:0]const u8) callconv(.c) c_int {
    lifecycle.option_sets += 1;
    return Highs_setStringOptionValue(h, n, v);
}
fn realCallback(h: *anyopaque, cb: highs.Callback, p: ?*anyopaque) callconv(.c) c_int {
    lifecycle.callbacks_registered += 1;
    return Highs_setCallback(h, cb, p);
}
fn realStartCallback(h: *anyopaque, kind: c_int) callconv(.c) c_int {
    lifecycle.callbacks_started += 1;
    return Highs_startCallback(h, kind);
}
fn realPass(
    h: *anyopaque,
    nc: c_int,
    nr: c_int,
    nnz: c_int,
    f: c_int,
    s: c_int,
    off: f64,
    cost: [*]const f64,
    cl: [*]const f64,
    cu: [*]const f64,
    rl: [*]const f64,
    ru: [*]const f64,
    start: [*]const c_int,
    index: [*]const c_int,
    value: [*]const f64,
) callconv(.c) c_int {
    lifecycle.passes += 1;
    return Highs_passLp(h, nc, nr, nnz, f, s, off, cost, cl, cu, rl, ru, start, index, value);
}
fn realRun(h: *anyopaque) callconv(.c) c_int {
    lifecycle.runs += 1;
    return Highs_run(h);
}
fn realSolution(h: *const anyopaque, cv: [*]f64, cd: [*]f64, rv: [*]f64, rd: [*]f64) callconv(.c) c_int {
    lifecycle.solutions += 1;
    return Highs_getSolution(h, cv, cd, rv, rd);
}

const api = highs.Api{
    .create = realCreate,
    .destroy = realDestroy,
    .version_major = Highs_versionMajor,
    .version_minor = Highs_versionMinor,
    .version_patch = Highs_versionPatch,
    .sizeof_highs_int = Highs_getSizeofHighsInt,
    .get_infinity = Highs_getInfinity,
    .set_bool_option = realBool,
    .set_int_option = realInt,
    .set_double_option = realDouble,
    .set_string_option = realString,
    .set_callback = realCallback,
    .start_callback = realStartCallback,
    .pass_lp = realPass,
    .run = realRun,
    .model_status = Highs_getModelStatus,
    .get_solution = realSolution,
    .get_objective = Highs_getObjectiveValue,
    .get_int_info = Highs_getIntInfoValue,
};

const Clock = struct {
    io: std.Io,
    fn now(self: *Clock) u64 {
        const value = std.Io.Clock.awake.now(self.io).nanoseconds;
        if (value <= 0) return 0;
        if (value > std.math.maxInt(u64)) return std.math.maxInt(u64);
        return @intCast(value);
    }
    fn nowCallback(raw: ?*anyopaque) u64 {
        const self: *Clock = @ptrCast(@alignCast(raw.?));
        return self.now();
    }
    fn check(_: ?*anyopaque) anyerror!void {}
    fn control(self: *Clock) cg.Control {
        return .{ .context = self, .now_ns_fn = nowCallback, .check_fn = check };
    }
};

const LoadedAttempt = struct {
    solve: cg.RestrictedSolve,
    alpha: []f64,
    beta: []f64,
    audit: ?mass_lp.PrimaryAudit,
    witness_rejected: bool,
    elapsed_ns: u64,
    fn deinit(self: *LoadedAttempt, allocator: std.mem.Allocator) void {
        self.solve.deinit();
        allocator.free(self.alpha);
        allocator.free(self.beta);
        self.* = undefined;
    }
};

fn runAttempt(
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    matrix: []const f64,
    tolerance: f64,
    deadline_ns: u64,
    clock: *Clock,
) !LoadedAttempt {
    var adapter = highs.Adapter{ .api = &api };
    const start = clock.now();
    var solve = try highs.solveRestricted(&adapter, allocator, problem, deadline_ns, clock.control(), tolerance);
    errdefer solve.deinit();
    if (solve.status != .optimal or solve.row_duals.len != constraints or solve.weights.len != columns)
        return error.NonOptimalRestrictedSolve;
    const alpha = try allocator.alloc(f64, rows);
    errdefer allocator.free(alpha);
    const beta = try allocator.alloc(f64, rows);
    errdefer allocator.free(beta);
    for (0..rows) |i| {
        alpha[i] = -solve.row_duals[i];
        beta[i] = -solve.row_duals[rows + i];
    }
    const witness = mass_lp.PrimaryWitness{
        .weights = solve.weights,
        .t = solve.t,
        .objective = solve.objective,
        .alpha = alpha,
        .beta = beta,
    };
    const audited = mass_lp.auditPrimaryWitness(matrix, rows, columns, witness);
    const audit = audited catch |err| switch (err) {
        error.WitnessRejected => null,
        else => return err,
    };
    return .{
        .solve = solve,
        .alpha = alpha,
        .beta = beta,
        .audit = audit,
        .witness_rejected = audit == null,
        .elapsed_ns = clock.now() - start,
    };
}

fn validDigest(value: []const u8) bool {
    if (value.len != 64) return false;
    var digest: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&digest, value) catch return false;
    return true;
}
fn validRelative(value: []const u8) bool {
    if (value.len == 0 or value.len > 1024 or value[0] == '/') return false;
    var parts = std.mem.splitScalar(u8, value, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}
fn requireEdge(edge: Edge) !void {
    if (edge.path.len == 0 or edge.path[0] != '/' or !validDigest(edge.sha256) or edge.bytes == 0)
        return error.InvalidProvenance;
}
fn validateLibrary(receipt: LibraryReceipt) !void {
    if (!std.mem.eql(u8, receipt.schema, library_schema) or !receipt.completed or
        !receipt.diagnostic_only or receipt.execution_authority or receipt.publication_authority or
        !std.mem.eql(u8, receipt.source.tag, highs.pinned_highs_tag) or
        !std.mem.eql(u8, receipt.source.commit, highs.pinned_highs_commit) or
        !validDigest(receipt.source.tree_sha256) or receipt.source.tree_sha256.len == 0 or
        receipt.source.runtime_githash.len == 0 or receipt.source.runtime_githash.len > 64 or
        !receipt.build.built_static or receipt.build.highsint64 or receipt.build.highs_no_default_threads or
        receipt.build.flags.len == 0 or
        receipt.build.target.len == 0 or receipt.build.architecture.len == 0 or
        receipt.build.compiler.len == 0 or !validDigest(receipt.build.compiler_sha256) or
        receipt.build.cmake.len == 0 or !validDigest(receipt.build.cmake_sha256))
        return error.InvalidProvenance;
    try requireEdge(receipt.source.archive);
    try requireEdge(receipt.source.license);
    try requireEdge(receipt.source.highs_c_api_header);
    try requireEdge(receipt.source.callback_header);
    try requireEdge(receipt.source.generated_hconfig);
    try requireEdge(receipt.artifacts.libhighs);
    try requireEdge(receipt.artifacts.symbol_inventory);
    try requireEdge(receipt.artifacts.link_inventory);
}

fn validateExecutable(receipt: ExecutableReceipt, argv0: []const u8, library_path: []const u8, library_sha: []const u8, lib: LibraryReceipt) !void {
    if (!std.mem.eql(u8, receipt.schema, executable_schema) or !receipt.completed or
        !receipt.diagnostic_only or receipt.execution_authority or receipt.publication_authority or
        receipt.target.len == 0 or !std.mem.eql(u8, receipt.optimize, "ReleaseSafe") or !receipt.strip or
        receipt.link_argv.len == 0 or !std.mem.eql(u8, receipt.binary.path, argv0) or
        !std.mem.eql(u8, receipt.library_receipt.path, library_path) or
        !std.mem.eql(u8, receipt.library_receipt.sha256, library_sha) or
        !std.mem.eql(u8, receipt.libhighs.path, lib.artifacts.libhighs.path) or
        !std.mem.eql(u8, receipt.libhighs.sha256, lib.artifacts.libhighs.sha256) or
        receipt.libhighs.bytes != lib.artifacts.libhighs.bytes or
        !std.mem.eql(u8, receipt.sources.lp.sha256, lp_sha256) or
        !std.mem.eql(u8, receipt.sources.controller.sha256, controller_sha256) or
        !std.mem.eql(u8, receipt.sources.adapter.sha256, adapter_sha256))
        return error.InvalidExecutableProvenance;
    try requireEdge(receipt.zig_executable);
    try requireEdge(receipt.sources.driver);
    try requireEdge(receipt.sources.lp);
    try requireEdge(receipt.sources.controller);
    try requireEdge(receipt.sources.adapter);
    try requireEdge(receipt.library_receipt);
    try requireEdge(receipt.libhighs);
    try requireEdge(receipt.binary);
}

fn exactArray(manifest: FixtureManifest, name: []const u8, shape: []const usize) !ArrayEdge {
    var found: ?ArrayEdge = null;
    for (manifest.raw_arrays) |edge| {
        if (!std.mem.eql(u8, edge.name, name)) continue;
        if (found != null) return error.InvalidFixture;
        found = edge;
    }
    const edge = found orelse return error.InvalidFixture;
    if (!validRelative(edge.path) or !validDigest(edge.sha256) or
        !std.mem.eql(u8, edge.dtype, "float64-le") or !std.mem.eql(u8, edge.order, "C") or
        !std.mem.eql(usize, edge.shape, shape))
        return error.InvalidFixture;
    var count: usize = 1;
    for (shape) |dim| count = try std.math.mul(usize, count, dim);
    if (edge.bytes != try std.math.mul(u64, @intCast(count), @sizeOf(f64))) return error.InvalidFixture;
    return edge;
}

fn digestBytes(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn digestText(bytes: []const u8) [64]u8 {
    return std.fmt.bytesToHex(digestBytes(bytes), .lower);
}
fn readJsonExpected(
    comptime T: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    expected_sha256: []const u8,
) !std.json.Parsed(T) {
    if (!validDigest(expected_sha256)) return error.InvalidExpectedDigest;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_json_bytes));
    defer allocator.free(bytes);
    if (!std.mem.eql(u8, digestText(bytes)[0..], expected_sha256)) return error.ManifestChanged;
    return std.json.parseFromSlice(T, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
        .duplicate_field_behavior = .@"error",
    });
}
fn verifyAbsoluteEdge(allocator: std.mem.Allocator, io: std.Io, edge: Edge) !void {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, edge.path, allocator, .limited(@intCast(edge.bytes + 1)));
    defer allocator.free(bytes);
    if (bytes.len != edge.bytes or !std.mem.eql(u8, &digestText(bytes), edge.sha256)) return error.ProvenanceChanged;
}
fn loadF64(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    edge: ArrayEdge,
) ![]f64 {
    const joined = try std.fs.path.join(allocator, &.{ owner, edge.path });
    defer allocator.free(joined);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, joined, allocator, .limited(max_fixture_bytes));
    defer allocator.free(raw);
    if (raw.len != edge.bytes or !std.mem.eql(u8, &digestText(raw), edge.sha256) or raw.len % 8 != 0)
        return error.FixtureChanged;
    if (@import("builtin").cpu.arch.endian() != .little) return error.UnsupportedEndian;
    const result = try allocator.alloc(f64, raw.len / 8);
    @memcpy(std.mem.sliceAsBytes(result), raw);
    for (result) |value| if (!std.math.isFinite(value)) return error.NonFiniteInput;
    return result;
}

fn verifyFixtureEdge(allocator: std.mem.Allocator, io: std.Io, owner: []const u8, edge: ArrayEdge) !void {
    const joined = try std.fs.path.join(allocator, &.{ owner, edge.path });
    defer allocator.free(joined);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, joined, allocator, .limited(max_fixture_bytes));
    defer allocator.free(raw);
    if (raw.len != edge.bytes or !std.mem.eql(u8, digestText(raw)[0..], edge.sha256))
        return error.FixtureChanged;
}

fn verifyAllFixtureEdges(allocator: std.mem.Allocator, io: std.Io, owner: []const u8, manifest: FixtureManifest) !void {
    if (manifest.raw_arrays.len != 6) return error.InvalidFixture;
    for (manifest.raw_arrays) |edge| try verifyFixtureEdge(allocator, io, owner, edge);
}

fn verifyLibraryClosure(allocator: std.mem.Allocator, io: std.Io, receipt: LibraryReceipt) !void {
    try verifyAbsoluteEdge(allocator, io, receipt.source.archive);
    try verifyAbsoluteEdge(allocator, io, receipt.source.license);
    try verifyAbsoluteEdge(allocator, io, receipt.source.highs_c_api_header);
    try verifyAbsoluteEdge(allocator, io, receipt.source.callback_header);
    try verifyAbsoluteEdge(allocator, io, receipt.source.generated_hconfig);
    try verifyAbsoluteEdge(allocator, io, receipt.artifacts.libhighs);
    try verifyAbsoluteEdge(allocator, io, receipt.artifacts.symbol_inventory);
    try verifyAbsoluteEdge(allocator, io, receipt.artifacts.link_inventory);
}

const VectorReceipt = struct { path: []const u8, sha256: [64]u8, bytes: usize, count: usize };
fn writeVector(io: std.Io, dir: std.Io.Dir, name: []const u8, values: []const f64) !VectorReceipt {
    const raw = std.mem.sliceAsBytes(values);
    const file = try dir.createFile(io, name, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, raw);
    try file.sync(io);
    return .{ .path = name, .sha256 = digestText(raw), .bytes = raw.len, .count = values.len };
}

const AttemptReceipt = struct {
    tolerance: f64,
    status: []const u8,
    witness_passed: bool,
    elapsed_ns: u64,
    objective: f64,
    t: f64,
    iterations: usize,
    audit: ?mass_lp.PrimaryAudit,
    weights: VectorReceipt,
    raw_row_duals: VectorReceipt,
    alpha: VectorReceipt,
    beta: VectorReceipt,
};
fn persistAttempt(io: std.Io, dir: std.Io.Dir, prefix: []const u8, tolerance: f64, attempt: *LoadedAttempt) !AttemptReceipt {
    const original = std.mem.eql(u8, prefix, "original-1e-9");
    if (!original and !std.mem.eql(u8, prefix, "retry-1e-10")) return error.InvalidAttemptName;
    const weights_name = if (original) "original-1e-9-weights.f64le" else "retry-1e-10-weights.f64le";
    const weights = try writeVector(io, dir, weights_name, attempt.solve.weights);
    const dual_name = if (original) "original-1e-9-raw-row-duals.f64le" else "retry-1e-10-raw-row-duals.f64le";
    const duals = try writeVector(io, dir, dual_name, attempt.solve.row_duals);
    const alpha_name = if (original) "original-1e-9-alpha.f64le" else "retry-1e-10-alpha.f64le";
    const alpha = try writeVector(io, dir, alpha_name, attempt.alpha);
    const beta_name = if (original) "original-1e-9-beta.f64le" else "retry-1e-10-beta.f64le";
    const beta = try writeVector(io, dir, beta_name, attempt.beta);
    return .{
        .tolerance = tolerance,
        .status = @tagName(attempt.solve.status),
        .witness_passed = !attempt.witness_rejected,
        .elapsed_ns = attempt.elapsed_ns,
        .objective = attempt.solve.objective,
        .t = attempt.solve.t,
        .iterations = attempt.solve.iterations,
        .audit = attempt.audit,
        .weights = weights,
        .raw_row_duals = duals,
        .alpha = alpha,
        .beta = beta,
    };
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(allocator);
    defer allocator.free(args);
    if (args.len != 17 or !std.mem.eql(u8, args[1], "--fixture-manifest") or
        !std.mem.eql(u8, args[3], "--fixture-sha256") or
        !std.mem.eql(u8, args[5], "--library-receipt") or
        !std.mem.eql(u8, args[7], "--library-receipt-sha256") or
        !std.mem.eql(u8, args[9], "--executable-receipt") or
        !std.mem.eql(u8, args[11], "--executable-receipt-sha256") or
        !std.mem.eql(u8, args[13], "--output-dir") or
        !std.mem.eql(u8, args[15], "--deadline-ms"))
        return error.InvalidArguments;
    const deadline_ms = try std.fmt.parseUnsigned(u64, args[16], 10);
    if (deadline_ms == 0 or deadline_ms > 600_000) return error.InvalidDeadline;

    // Fail closed on unresolved or incomplete library identity before reading
    // the fixture or creating output.
    var library = try readJsonExpected(LibraryReceipt, allocator, io, args[6], args[8]);
    defer library.deinit();
    try validateLibrary(library.value);
    try verifyLibraryClosure(allocator, io, library.value);
    var executable = try readJsonExpected(ExecutableReceipt, allocator, io, args[10], args[12]);
    defer executable.deinit();
    try validateExecutable(executable.value, args[0], args[6], args[8], library.value);
    try verifyAbsoluteEdge(allocator, io, executable.value.zig_executable);
    try verifyAbsoluteEdge(allocator, io, executable.value.sources.driver);
    try verifyAbsoluteEdge(allocator, io, executable.value.sources.lp);
    try verifyAbsoluteEdge(allocator, io, executable.value.sources.controller);
    try verifyAbsoluteEdge(allocator, io, executable.value.sources.adapter);
    try verifyAbsoluteEdge(allocator, io, executable.value.library_receipt);
    try verifyAbsoluteEdge(allocator, io, executable.value.libhighs);
    try verifyAbsoluteEdge(allocator, io, executable.value.binary);

    if (Highs_versionMajor() != 1 or Highs_versionMinor() != 8 or Highs_versionPatch() != 0 or
        !std.mem.eql(u8, std.mem.span(Highs_githash()), library.value.source.runtime_githash))
        return error.LinkedHighsIdentityMismatch;

    var fixture = try readJsonExpected(FixtureManifest, allocator, io, args[2], args[4]);
    defer fixture.deinit();
    const fm = fixture.value;
    if (!std.mem.eql(u8, fm.schema, fixture_schema) or !fm.completed or !fm.diagnostic_only or
        fm.execution_authority or fm.publication_authority or
        fm.problem.source_rows != rows or fm.problem.weight_columns != columns or
        fm.problem.variables != variables or fm.problem.constraint_rows != constraints or
        !std.mem.eql(u8, fm.problem.builder, "attention_mass_lp_v4.buildPrimaryProblem") or
        !std.mem.eql(u8, fm.problem.variable_order, "w[0:288],t") or
        !std.mem.eql(u8, fm.problem.constraint_order, "upper[0:1128],lower[0:1128]") or
        !std.mem.eql(u8, fm.problem.objective, "minimize t") or
        !std.mem.eql(u8, fm.problem.raw_row_dual_mapping, "alpha=-row_dual[0:1128],beta=-row_dual[1128:2256]"))
        return error.InvalidFixture;
    const matrix_edge = try exactArray(fm, "restricted_matrix_f64.npy", &.{ rows, columns });
    const expected_weights_edge = try exactArray(fm, "raw_weights_f64.npy", &.{columns});
    const expected_alpha_edge = try exactArray(fm, "alpha_f64.npy", &.{rows});
    const expected_beta_edge = try exactArray(fm, "beta_f64.npy", &.{rows});
    const owner = std.fs.path.dirname(args[2]) orelse ".";
    try verifyAllFixtureEdges(allocator, io, owner, fm);
    const matrix = try loadF64(allocator, io, owner, matrix_edge);
    defer allocator.free(matrix);
    var problem = try mass_lp.buildPrimaryProblem(allocator, matrix, rows, columns);
    defer problem.deinit();

    var clock = Clock{ .io = io };
    const start_ns = clock.now();
    const duration_ns = try std.math.mul(u64, deadline_ms, std.time.ns_per_ms);
    const deadline_ns = try std.math.add(u64, start_ns, duration_ns);
    lifecycle = .{};
    var first = try runAttempt(allocator, &problem, matrix, cg.original_feasibility_tolerance, deadline_ns, &clock);
    defer first.deinit(allocator);
    // The exported regression is specifically a witness-rejected first solve.
    // A first-attempt pass or any non-witness failure is a differential result,
    // not permission to skip or broaden the retry policy.
    if (!first.witness_rejected) return error.ExpectedOriginalWitnessRejectionNotObserved;
    var retry = try runAttempt(allocator, &problem, matrix, cg.retry_feasibility_tolerance, deadline_ns, &clock);
    defer retry.deinit(allocator);
    if (retry.witness_rejected or retry.audit == null) return error.RetryWitnessRejected;
    if (lifecycle.creates_attempted != 2 or lifecycle.creates_succeeded != 2 or
        lifecycle.destroys != 2 or lifecycle.passes != 2 or lifecycle.runs != 2 or
        lifecycle.solutions != 2 or lifecycle.callbacks_registered != 2 or lifecycle.callbacks_started != 2)
        return error.InvalidHighsLifecycle;
    if (clock.now() >= deadline_ns) return error.Timeout;

    // Terminally repeat every consumed receipt and edge check after both
    // solves, before creating any output that could be mistaken for a result.
    var terminal_library = try readJsonExpected(LibraryReceipt, allocator, io, args[6], args[8]);
    defer terminal_library.deinit();
    try validateLibrary(terminal_library.value);
    try verifyLibraryClosure(allocator, io, terminal_library.value);
    var terminal_executable = try readJsonExpected(ExecutableReceipt, allocator, io, args[10], args[12]);
    defer terminal_executable.deinit();
    try validateExecutable(terminal_executable.value, args[0], args[6], args[8], terminal_library.value);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.zig_executable);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.sources.driver);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.sources.lp);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.sources.controller);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.sources.adapter);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.library_receipt);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.libhighs);
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.binary);
    var terminal_fixture = try readJsonExpected(FixtureManifest, allocator, io, args[2], args[4]);
    defer terminal_fixture.deinit();
    try verifyAllFixtureEdges(allocator, io, owner, terminal_fixture.value);

    try std.Io.Dir.cwd().createDir(io, args[14], .default_dir);
    var output = try std.Io.Dir.cwd().openDir(io, args[14], .{ .follow_symlinks = false });
    defer output.close(io);
    const first_receipt = try persistAttempt(io, output, "original-1e-9", cg.original_feasibility_tolerance, &first);
    const retry_receipt = try persistAttempt(io, output, "retry-1e-10", cg.retry_feasibility_tolerance, &retry);
    const fixture_manifest_bytes = try std.Io.Dir.cwd().readFileAlloc(io, args[2], allocator, .limited(max_json_bytes));
    defer allocator.free(fixture_manifest_bytes);
    const receipt = .{
        .schema = result_schema,
        .completed = true,
        .diagnostic_only = true,
        .execution_authority = false,
        .publication_authority = false,
        .fixture_manifest = .{ .path = args[2], .sha256 = digestText(fixture_manifest_bytes) },
        .library_receipt = .{ .path = args[6], .sha256 = args[8], .tag = library.value.source.tag, .commit = library.value.source.commit, .libhighs_sha256 = library.value.artifacts.libhighs.sha256 },
        .executable_receipt = .{ .path = args[10], .sha256 = args[12], .binary = executable.value.binary },
        .linked_runtime = .{ .version = "1.8.0", .githash = std.mem.span(Highs_githash()), .full_source_commit = library.value.source.commit, .highs_int_bytes = highs.required_highs_int_bytes },
        .options = .{
            .solver = "simplex",
            .presolve = "on",
            .simplex_strategy = 1,
            .output_flag = false,
            .log_to_console = false,
            .original_primal_dual_feasibility_tolerance = cg.original_feasibility_tolerance,
            .retry_primal_dual_feasibility_tolerance = cg.retry_feasibility_tolerance,
            .absolute_deadline_ms = deadline_ms,
            .highs_no_default_threads = false,
        },
        .expected = fm.oracles,
        .observed = .{ .original_1e_9 = first_receipt, .retry_1e_10 = retry_receipt },
        .expected_vs_observed = .{
            .original_weights_sha256_equal = std.mem.eql(u8, expected_weights_edge.sha256, first_receipt.weights.sha256[0..]),
            .original_alpha_sha256_equal = std.mem.eql(u8, expected_alpha_edge.sha256, first_receipt.alpha.sha256[0..]),
            .original_beta_sha256_equal = std.mem.eql(u8, expected_beta_edge.sha256, first_receipt.beta.sha256[0..]),
            .retry_vector_oracle_available = false,
            .differences_are_reported_not_promoted = true,
        },
        .lifecycle = lifecycle,
        .scope = .{
            .original_coordinate_rows = rows,
            .meaning = "all 1128 exported restricted rows; not the unexported full constraint-generation matrix",
            .persistent_model_or_basis_reuse = false,
            .solver_internal_timing_available = false,
            .timing = "wall-clock adapter attempt elapsed_ns only",
        },
    };
    const json = try std.json.Stringify.valueAlloc(allocator, receipt, .{ .whitespace = .indent_2 });
    defer allocator.free(json);
    const result_file = try output.createFile(io, "result.json", .{ .exclusive = true });
    defer result_file.close(io);
    try result_file.writeStreamingAll(io, json);
    try result_file.writeStreamingAll(io, "\n");
    try result_file.sync(io);
}
