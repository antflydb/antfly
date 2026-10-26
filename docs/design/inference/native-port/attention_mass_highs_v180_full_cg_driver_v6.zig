//! Isolated full constraint-generation differential driver for one exported
//! 3000x288 original-seed mass matrix. Outside canonical runtime sources.
//! It composes frozen CG V5 with HiGHS Adapter V4; it is not a fitter and
//! grants no execution, serving, or publication authority.
const std = @import("std");
const mass_lp = @import("attention_mass_lp");
const cg = @import("attention_mass_cg");
const highs = @import("attention_mass_highs");

const fixture_schema = "antfly.attention-matching.mass-full-cg-fixture.v2";
const oracle_schema = "antfly.attention-matching.mass-python-full-cg-oracle.v2";
const library_schema = "antfly.attention-matching.highs-v180-static-library.v2";
const result_schema = "antfly.attention-matching.mass-highs-v180-full-cg-differential.v6";
const executable_schema = "antfly.attention-matching.highs-v180-real-driver-executable.v1";
const lp_sha256 = "ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de";
const controller_sha256 = "abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa";
const adapter_sha256 = "f9962c9ec47d6399c15eeda30c674fee0993ead3fbcf6f4527bc8037c75e0dc8";
const rows: usize = 3000;
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
    max_abs: f64,
};
const FixtureManifest = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    fit_invoked: bool,
    held_tensors_loaded: bool,
    cell: struct { owner_layer: usize, block_start: usize, retained_rows: usize },
    plan: Edge,
    source: std.json.Value,
    expected_final_build: std.json.Value,
    origins: std.json.Value,
    input: struct { warm_rows: ?[]const usize, original_seed_only: bool, query_rows: usize, exact_rows: usize, dimension: usize, scale: f64, strata_count: usize },
    matrix: ArrayEdge,
    limits: std.json.Value,
};
const OracleAttempt = struct {
    sequence: usize,
    rows: usize,
    columns: usize,
    restricted_matrix_sha256: []const u8,
    feasibility_tolerance: f64,
    status: []const u8,
    error_type: ?[]const u8 = null,
    error_message: ?[]const u8 = null,
    iterations: ?usize = null,
    objective: ?f64 = null,
    raw_weights_sha256: ?[]const u8 = null,
    alpha_sha256: ?[]const u8 = null,
    beta_sha256: ?[]const u8 = null,
};
const OracleRound = struct {
    round: usize,
    selected_before: []const usize,
    selected_before_sha256: []const u8,
    tolerance: f64,
    worst_full_violation: f64,
    minimum_threshold_margin: f64,
    addition_cutoff_margin: ?f64,
    fitted_sha256: []const u8,
    upper_sha256: []const u8,
    lower_sha256: []const u8,
    added_rows: []const usize,
    iterations: usize,
    objective: f64,
    retried_witness: bool,
    priced_prefix: std.json.Value,
};
const OracleReceipt = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    fixture: struct { path: []const u8, sha256: []const u8 },
    origins: std.json.Value,
    deadline_seconds: f64,
    elapsed_seconds: f64,
    warm_rows: ?[]const usize,
    attempts: []const OracleAttempt,
    rounds: []const OracleRound,
    terminal: struct {
        objective: f64,
        iterations: usize,
        selected_row_indices: []const usize,
        full_witness: std.json.Value,
        stored_f32_log_bias_mass: std.json.Value,
        array_hashes: struct {
            raw_weights: []const u8,
            bias: []const u8,
            stored_weights: []const u8,
            mass: []const u8,
            alpha: []const u8,
            beta: []const u8,
        },
        arrays: Edge,
    },
    retry_telemetry: []const Edge,
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

const AttemptTrace = struct {
    restricted_matrix_sha256: [64]u8,
    rows: usize,
    columns: usize,
    feasibility_tolerance: f64,
    status: cg.SolverStatus,
    iterations: usize,
    objective: f64,
    raw_weights_sha256: [64]u8,
    alpha_sha256: [64]u8,
    beta_sha256: [64]u8,
    finite_nonnegative_weights: bool,
};
const TraceSolver = struct {
    adapter: highs.Adapter,
    attempts: [cg.max_rounds * 2]AttemptTrace = undefined,
    count: usize = 0,
};
fn tracedSolve(
    raw: ?*anyopaque,
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    absolute_deadline_ns: u64,
    control: cg.Control,
    tolerance: f64,
) anyerror!cg.RestrictedSolve {
    if (raw == null) return error.InvalidAdapter;
    const self: *TraceSolver = @ptrCast(@alignCast(raw.?));
    if (self.count >= self.attempts.len) return error.TooManySolverAttempts;
    const count = try std.math.mul(usize, problem.source_rows, problem.weight_columns);
    const restricted = try allocator.alloc(f64, count);
    defer allocator.free(restricted);
    for (0..problem.source_rows) |row| {
        const source = problem.a_ub[row * problem.variables ..][0..problem.weight_columns];
        @memcpy(restricted[row * problem.weight_columns ..][0..problem.weight_columns], source);
    }
    const digest = digestText(std.mem.sliceAsBytes(restricted));
    var result = try highs.solveRestricted(&self.adapter, allocator, problem, absolute_deadline_ns, control, tolerance);
    errdefer result.deinit();
    if (result.row_duals.len != 2 * problem.source_rows) return error.InvalidSolverResult;
    const alpha = try allocator.alloc(f64, problem.source_rows);
    defer allocator.free(alpha);
    const beta = try allocator.alloc(f64, problem.source_rows);
    defer allocator.free(beta);
    for (0..problem.source_rows) |row| {
        alpha[row] = -result.row_duals[row];
        beta[row] = -result.row_duals[problem.source_rows + row];
    }
    var finite_nonnegative_weights = true;
    for (result.weights) |weight| {
        if (!std.math.isFinite(weight) or weight < 0) {
            finite_nonnegative_weights = false;
            break;
        }
    }
    self.attempts[self.count] = .{
        .restricted_matrix_sha256 = digest,
        .rows = problem.source_rows,
        .columns = problem.weight_columns,
        .feasibility_tolerance = tolerance,
        .status = result.status,
        .iterations = result.iterations,
        .objective = result.objective,
        .raw_weights_sha256 = digestText(std.mem.sliceAsBytes(result.weights)),
        .alpha_sha256 = digestText(std.mem.sliceAsBytes(alpha)),
        .beta_sha256 = digestText(std.mem.sliceAsBytes(beta)),
        .finite_nonnegative_weights = finite_nonnegative_weights,
    };
    self.count += 1;
    return result;
}
const VectorReceiptF32 = struct { path: []const u8, sha256: [64]u8, bytes: usize, count: usize };
fn writeVectorF32(io: std.Io, dir: std.Io.Dir, name: []const u8, values: []const f32) !VectorReceiptF32 {
    const raw = std.mem.sliceAsBytes(values);
    const file = try dir.createFile(io, name, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, raw);
    try file.sync(io);
    return .{ .path = name, .sha256 = digestText(raw), .bytes = raw.len, .count = values.len };
}
fn exactAttempt(observed: AttemptTrace, expected: OracleAttempt) bool {
    if (observed.rows != expected.rows or observed.columns != expected.columns or
        observed.feasibility_tolerance != expected.feasibility_tolerance or
        !std.mem.eql(u8, &observed.restricted_matrix_sha256, expected.restricted_matrix_sha256) or
        !std.mem.eql(u8, @tagName(observed.status), expected.status) or expected.error_type != null or
        expected.error_message != null or !observed.finite_nonnegative_weights) return false;
    const iterations = expected.iterations orelse return false;
    if (observed.iterations != iterations) return false;
    const objective = expected.objective orelse return false;
    if (observed.objective != objective) return false;
    const raw_weights_sha256 = expected.raw_weights_sha256 orelse return false;
    if (!std.mem.eql(u8, &observed.raw_weights_sha256, raw_weights_sha256)) return false;
    const alpha_sha256 = expected.alpha_sha256 orelse return false;
    if (!std.mem.eql(u8, &observed.alpha_sha256, alpha_sha256)) return false;
    const beta_sha256 = expected.beta_sha256 orelse return false;
    return std.mem.eql(u8, &observed.beta_sha256, beta_sha256);
}
fn exactTrajectory(trace: *const TraceSolver, oracle: OracleReceipt) bool {
    if (trace.count != oracle.attempts.len) return false;
    for (trace.attempts[0..trace.count], oracle.attempts) |observed, expected| {
        if (!exactAttempt(observed, expected)) return false;
    }
    return true;
}
fn exactRounds(diagnostics: cg.Diagnostics, oracle: OracleReceipt) bool {
    if (diagnostics.round_count != oracle.rounds.len) return false;
    for (diagnostics.rounds[0..diagnostics.round_count], oracle.rounds) |observed, expected| {
        if (observed.round != expected.round or observed.subset_rows != expected.selected_before.len or
            observed.iterations != expected.iterations or observed.objective != expected.objective or
            observed.worst_full_violation != expected.worst_full_violation or
            observed.added_rows != expected.added_rows.len or
            observed.retried_witness != expected.retried_witness)
            return false;
    }
    return true;
}

const RoundReductionBound = struct {
    round: usize,
    native_worst: f64,
    python_worst: f64,
    absolute_difference: f64,
    derived_absolute_bound: f64,
    minimum_threshold_margin: f64,
    addition_cutoff_margin: ?f64,
    reduction_difference_within_bound: bool,
    threshold_decision_stable: bool,
    cutoff_decision_stable: bool,
};
const RoundReductionComparison = struct {
    items: [cg.max_rounds]RoundReductionBound = undefined,
    count: usize = 0,
    all_required_equal: bool = false,
};
fn compareRoundReduction(observed: cg.Round, expected: OracleRound) ?RoundReductionBound {
    if (observed.round != expected.round or observed.subset_rows != expected.selected_before.len or
        observed.iterations != expected.iterations or observed.objective != expected.objective or
        observed.added_rows != expected.added_rows.len or
        observed.retried_witness != expected.retried_witness) return null;
    const unit_roundoff = 0x1p-53;
    const operations = 2 * @as(f64, @floatFromInt(columns));
    const gamma = (operations * unit_roundoff) / (1 - operations * unit_roundoff);
    const cross_reduction_factor = (2 * gamma) / (1 - gamma) + 4 * unit_roundoff;
    const difference = @abs(observed.worst_full_violation - expected.worst_full_violation);
    const scale = 1 + @abs(expected.objective) + @max(@abs(observed.worst_full_violation), @abs(expected.worst_full_violation));
    const bound = cross_reduction_factor * scale;
    const threshold_stable = std.math.isFinite(expected.minimum_threshold_margin) and expected.minimum_threshold_margin > bound;
    const cutoff_stable = if (expected.addition_cutoff_margin) |margin| std.math.isFinite(margin) and margin > 2 * bound else true;
    const result = RoundReductionBound{
        .round = observed.round,
        .native_worst = observed.worst_full_violation,
        .python_worst = expected.worst_full_violation,
        .absolute_difference = difference,
        .derived_absolute_bound = bound,
        .minimum_threshold_margin = expected.minimum_threshold_margin,
        .addition_cutoff_margin = expected.addition_cutoff_margin,
        .reduction_difference_within_bound = std.math.isFinite(difference) and difference <= bound,
        .threshold_decision_stable = threshold_stable,
        .cutoff_decision_stable = cutoff_stable,
    };
    if (!result.reduction_difference_within_bound or !threshold_stable or !cutoff_stable) return null;
    return result;
}
fn boundedRoundReduction(diagnostics: cg.Diagnostics, oracle: OracleReceipt, finite_nonnegative_terms: bool) RoundReductionComparison {
    var comparison = RoundReductionComparison{};
    if (!finite_nonnegative_terms or diagnostics.round_count != oracle.rounds.len) return comparison;
    for (diagnostics.rounds[0..diagnostics.round_count], oracle.rounds) |observed, expected| {
        comparison.items[comparison.count] = compareRoundReduction(observed, expected) orelse return comparison;
        comparison.count += 1;
    }
    comparison.all_required_equal = comparison.count == oracle.rounds.len;
    return comparison;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(allocator);
    defer allocator.free(args);
    if (args.len != 21 or !std.mem.eql(u8, args[1], "--fixture-manifest") or
        !std.mem.eql(u8, args[3], "--fixture-sha256") or
        !std.mem.eql(u8, args[5], "--python-oracle") or
        !std.mem.eql(u8, args[7], "--python-oracle-sha256") or
        !std.mem.eql(u8, args[9], "--library-receipt") or
        !std.mem.eql(u8, args[11], "--library-receipt-sha256") or
        !std.mem.eql(u8, args[13], "--executable-receipt") or
        !std.mem.eql(u8, args[15], "--executable-receipt-sha256") or
        !std.mem.eql(u8, args[17], "--output-dir") or
        !std.mem.eql(u8, args[19], "--deadline-ms")) return error.InvalidArguments;
    const deadline_ms = try std.fmt.parseUnsigned(u64, args[20], 10);
    if (deadline_ms == 0 or deadline_ms > 560_000) return error.InvalidDeadline;

    var library = try readJsonExpected(LibraryReceipt, allocator, io, args[10], args[12]);
    defer library.deinit();
    try validateLibrary(library.value);
    try verifyLibraryClosure(allocator, io, library.value);
    var executable = try readJsonExpected(ExecutableReceipt, allocator, io, args[14], args[16]);
    defer executable.deinit();
    try validateExecutable(executable.value, args[0], args[10], args[12], library.value);
    try verifyAbsoluteEdge(allocator, io, executable.value.binary);
    if (Highs_versionMajor() != 1 or Highs_versionMinor() != 8 or Highs_versionPatch() != 0 or
        !std.mem.eql(u8, std.mem.span(Highs_githash()), library.value.source.runtime_githash))
        return error.LinkedHighsIdentityMismatch;

    var fixture = try readJsonExpected(FixtureManifest, allocator, io, args[2], args[4]);
    defer fixture.deinit();
    const fm = fixture.value;
    if (!std.mem.eql(u8, fm.schema, fixture_schema) or !fm.completed or !fm.diagnostic_only or
        fm.execution_authority or fm.publication_authority or fm.fit_invoked or fm.held_tensors_loaded or
        fm.cell.owner_layer != 14 or fm.cell.block_start != 2048 or fm.cell.retained_rows != columns or
        fm.input.warm_rows != null or !fm.input.original_seed_only or
        fm.input.query_rows != rows or fm.input.exact_rows != 512 or fm.input.dimension != 512 or
        fm.matrix.shape.len != 2 or fm.matrix.shape[0] != rows or fm.matrix.shape[1] != columns or
        !std.mem.eql(u8, fm.matrix.name, "full_matrix_f64") or
        !std.mem.eql(u8, fm.matrix.dtype, "float64-le") or !std.mem.eql(u8, fm.matrix.order, "C") or
        !std.math.isFinite(fm.matrix.max_abs) or fm.matrix.max_abs < 0)
        return error.InvalidFixture;
    try requireEdge(fm.plan);
    const owner = std.fs.path.dirname(args[2]) orelse ".";
    try verifyFixtureEdge(allocator, io, owner, fm.matrix);
    const matrix = try loadF64(allocator, io, owner, fm.matrix);
    defer allocator.free(matrix);
    var observed_matrix_max_abs: f64 = 0;
    var finite_nonnegative_matrix = true;
    for (matrix) |value| {
        if (!std.math.isFinite(value) or value < 0) finite_nonnegative_matrix = false;
        observed_matrix_max_abs = @max(observed_matrix_max_abs, @abs(value));
    }
    if (!finite_nonnegative_matrix or observed_matrix_max_abs != fm.matrix.max_abs) return error.InvalidFixture;

    var oracle = try readJsonExpected(OracleReceipt, allocator, io, args[6], args[8]);
    defer oracle.deinit();
    if (!std.mem.eql(u8, oracle.value.schema, oracle_schema) or !oracle.value.completed or
        !oracle.value.diagnostic_only or oracle.value.execution_authority or oracle.value.publication_authority or
        !std.mem.eql(u8, oracle.value.fixture.path, args[2]) or
        !std.mem.eql(u8, oracle.value.fixture.sha256, args[4]) or oracle.value.warm_rows != null)
        return error.InvalidOracle;

    var clock = Clock{ .io = io };
    const start_ns = clock.now();
    const deadline_ns = try std.math.add(u64, start_ns, try std.math.mul(u64, deadline_ms, std.time.ns_per_ms));
    lifecycle = .{};
    var traced = TraceSolver{ .adapter = .{ .api = &api } };
    var result = try cg.solvePrimaryConstraintGeneration(
        allocator,
        matrix,
        rows,
        columns,
        null,
        deadline_ns,
        clock.control(),
        .{ .context = &traced, .solve_restricted_fn = tracedSolve },
    );
    defer result.deinit();
    if (clock.now() >= deadline_ns) return error.Timeout;
    const accepted = switch (result) {
        .accepted => |*value| value,
        .terminal => return error.NativeCgTerminal,
    };
    const trajectory_equal = exactTrajectory(&traced, oracle.value);
    const rounds_equal = exactRounds(accepted.diagnostics, oracle.value);
    var finite_nonnegative_attempt_weights = true;
    for (traced.attempts[0..traced.count]) |attempt| finite_nonnegative_attempt_weights = finite_nonnegative_attempt_weights and attempt.finite_nonnegative_weights;
    const bounded_rounds = boundedRoundReduction(accepted.diagnostics, oracle.value, finite_nonnegative_matrix and finite_nonnegative_attempt_weights);
    const selected_equal = std.mem.eql(usize, accepted.selected_row_indices, oracle.value.terminal.selected_row_indices);
    const bias_digest = digestText(std.mem.sliceAsBytes(accepted.bias));
    const bias_equal = std.mem.eql(u8, &bias_digest, oracle.value.terminal.array_hashes.bias);
    const full_witness = try mass_lp.auditPrimaryWitness(matrix, rows, columns, .{
        .weights = accepted.raw_weights,
        .t = accepted.objective,
        .objective = accepted.objective,
        .alpha = accepted.alpha,
        .beta = accepted.beta,
    });
    var stored_mass_max: f64 = 0;
    for (accepted.mass) |value| stored_mass_max = @max(stored_mass_max, @abs(value - 1));

    // Rehash all inputs after the solve and before creating output.
    var terminal_fixture = try readJsonExpected(FixtureManifest, allocator, io, args[2], args[4]);
    defer terminal_fixture.deinit();
    try verifyFixtureEdge(allocator, io, owner, terminal_fixture.value.matrix);
    var terminal_oracle = try readJsonExpected(OracleReceipt, allocator, io, args[6], args[8]);
    defer terminal_oracle.deinit();
    var terminal_library = try readJsonExpected(LibraryReceipt, allocator, io, args[10], args[12]);
    defer terminal_library.deinit();
    try verifyLibraryClosure(allocator, io, terminal_library.value);
    var terminal_executable = try readJsonExpected(ExecutableReceipt, allocator, io, args[14], args[16]);
    defer terminal_executable.deinit();
    try verifyAbsoluteEdge(allocator, io, terminal_executable.value.binary);

    try std.Io.Dir.cwd().createDir(io, args[18], .default_dir);
    var output = try std.Io.Dir.cwd().openDir(io, args[18], .{ .follow_symlinks = false });
    defer output.close(io);
    const raw_weights = try writeVector(io, output, "raw-weights.f64le", accepted.raw_weights);
    const bias = try writeVectorF32(io, output, "bias.f32le", accepted.bias);
    const stored_weights = try writeVector(io, output, "stored-weights.f64le", accepted.stored_weights);
    const mass = try writeVector(io, output, "mass.f64le", accepted.mass);
    const alpha = try writeVector(io, output, "alpha.f64le", accepted.alpha);
    const beta = try writeVector(io, output, "beta.f64le", accepted.beta);
    const receipt = .{
        .schema = result_schema,
        .completed = true,
        .diagnostic_only = true,
        .execution_authority = false,
        .publication_authority = false,
        .fixture = .{ .path = args[2], .sha256 = args[4] },
        .python_oracle = .{ .path = args[6], .sha256 = args[8] },
        .library_receipt = .{ .path = args[10], .sha256 = args[12] },
        .executable_receipt = .{ .path = args[14], .sha256 = args[16] },
        .options = .{ .deadline_ms = deadline_ms, .warm_rows = @as(?[]const usize, null), .original_tolerance = cg.original_feasibility_tolerance, .retry_tolerance = cg.retry_feasibility_tolerance, .highs_no_default_threads = false },
        .observed = .{ .attempts = traced.attempts[0..traced.count], .objective = accepted.objective, .diagnostics = accepted.diagnostics, .selected_row_indices = accepted.selected_row_indices, .full_original_coordinate_witness = full_witness, .stored_training_mass_max = stored_mass_max, .raw_weights = raw_weights, .bias = bias, .stored_weights = stored_weights, .mass = mass, .alpha = alpha, .beta = beta },
        .comparison = .{ .trajectory_equal = trajectory_equal, .rounds_exact_binary64_equal = rounds_equal, .rounds_within_derived_bound = bounded_rounds.all_required_equal, .round_reduction_evidence = bounded_rounds.items[0..bounded_rounds.count], .selected_rows_equal = selected_equal, .bias_bytes_equal = bias_equal, .all_required_equal = trajectory_equal and bounded_rounds.all_required_equal and selected_equal and bias_equal },
        .lifecycle = lifecycle,
        .scope = .{ .full_rows = rows, .columns = columns, .full_cg_only = true, .fitter = false, .held_data = false, .persistent_model_or_basis_reuse = false, .row_selection_identity_basis = "exact next restricted-matrix digest for every transition plus exact final selected-row indices; duplicate source rows do not weaken final row identity" },
    };
    const json = try std.json.Stringify.valueAlloc(allocator, receipt, .{ .whitespace = .indent_2 });
    defer allocator.free(json);
    const result_file = try output.createFile(io, "result.json", .{ .exclusive = true });
    defer result_file.close(io);
    try result_file.writeStreamingAll(io, json);
    try result_file.writeStreamingAll(io, "\n");
    try result_file.sync(io);
    if (!trajectory_equal or !bounded_rounds.all_required_equal or !selected_equal or !bias_equal) return error.FullCgParityMismatch;
}

const test_hash_0 = "0000000000000000000000000000000000000000000000000000000000000000";
const test_hash_1 = "1111111111111111111111111111111111111111111111111111111111111111";
const test_hash_2 = "2222222222222222222222222222222222222222222222222222222222222222";
const test_hash_3 = "3333333333333333333333333333333333333333333333333333333333333333";

fn testAttempt() struct { observed: AttemptTrace, expected: OracleAttempt } {
    const observed = AttemptTrace{
        .restricted_matrix_sha256 = @splat('0'),
        .rows = 2,
        .columns = 1,
        .feasibility_tolerance = 1e-9,
        .status = .optimal,
        .iterations = 3,
        .objective = 0.25,
        .raw_weights_sha256 = @splat('1'),
        .alpha_sha256 = @splat('2'),
        .beta_sha256 = @splat('3'),
        .finite_nonnegative_weights = true,
    };
    return .{ .observed = observed, .expected = .{
        .sequence = 0,
        .rows = 2,
        .columns = 1,
        .restricted_matrix_sha256 = test_hash_0,
        .feasibility_tolerance = 1e-9,
        .status = "optimal",
        .iterations = 3,
        .objective = 0.25,
        .raw_weights_sha256 = test_hash_1,
        .alpha_sha256 = test_hash_2,
        .beta_sha256 = test_hash_3,
    } };
}

test "context full CG exact attempt comparator rejects missing or divergent evidence" {
    var pair = testAttempt();
    try std.testing.expect(exactAttempt(pair.observed, pair.expected));
    pair.expected.iterations = null;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
    pair = testAttempt();
    pair.expected.objective = null;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
    pair = testAttempt();
    pair.expected.raw_weights_sha256 = null;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
    pair = testAttempt();
    pair.expected.rows += 1;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
    pair = testAttempt();
    // Retry identity is represented by the exact tolerance and attempt sequence.
    pair.expected.feasibility_tolerance = 1e-10;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
    pair = testAttempt();
    pair.observed.finite_nonnegative_weights = false;
    try std.testing.expect(!exactAttempt(pair.observed, pair.expected));
}

fn testRound() struct { observed: cg.Round, expected: OracleRound } {
    return .{
        .observed = .{ .round = 0, .subset_rows = 2, .iterations = 3, .cumulative_iterations = 3, .objective = 0.25, .worst_full_violation = 0.5, .added_rows = 1, .retried_witness = false },
        .expected = .{ .round = 0, .selected_before = &.{ 0, 2 }, .selected_before_sha256 = "0", .tolerance = 1e-9, .worst_full_violation = 0.5, .minimum_threshold_margin = 1e-6, .addition_cutoff_margin = 1e-6, .fitted_sha256 = "1", .upper_sha256 = "2", .lower_sha256 = "3", .added_rows = &.{1}, .iterations = 3, .objective = 0.25, .retried_witness = false, .priced_prefix = .null },
    };
}

test "context full CG derived reduction bound rejects ambiguous or divergent rounds" {
    var pair = testRound();
    const accepted = compareRoundReduction(pair.observed, pair.expected) orelse return error.TestUnexpectedResult;
    try std.testing.expect(accepted.derived_absolute_bound > 0);
    pair.expected.minimum_threshold_margin = accepted.derived_absolute_bound;
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
    pair = testRound();
    const bound = (compareRoundReduction(pair.observed, pair.expected) orelse return error.TestUnexpectedResult).derived_absolute_bound;
    pair.expected.addition_cutoff_margin = 2 * bound;
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
    pair = testRound();
    pair.observed.worst_full_violation += 1e-6;
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
    pair = testRound();
    pair.observed.retried_witness = true;
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
    pair = testRound();
    pair.expected.minimum_threshold_margin = -1;
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
    pair = testRound();
    pair.expected.worst_full_violation = std.math.nan(f64);
    try std.testing.expect(compareRoundReduction(pair.observed, pair.expected) == null);
}
