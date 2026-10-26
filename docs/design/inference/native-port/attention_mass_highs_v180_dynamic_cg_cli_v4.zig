//! Portable, diagnostic-only native mass constraint-generation worker.
//! This lives outside canonical runtime sources and preserves CG V5 and the
//! HiGHS v1.8.0 Adapter V4 policy. It is not a full fitter or release path.
const std = @import("std");
const mass_lp = @import("attention_mass_lp");
const cg = @import("attention_mass_cg");
const highs = @import("attention_mass_highs");

const input_schema = "antfly.attention-matching.mass-dynamic-cg-input.v1";
const result_schema = "antfly.attention-matching.mass-dynamic-cg-result.v1";
const library_schema = "antfly.attention-matching.highs-v180-static-library.v2";
const executable_schema = "antfly.attention-matching.highs-v180-real-driver-executable.v1";
const lp_sha256 = "ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de";
const controller_sha256 = "abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa";
const adapter_sha256 = "f9962c9ec47d6399c15eeda30c674fee0993ead3fbcf6f4527bc8037c75e0dc8";
const max_json_bytes = 4 * 1024 * 1024;
const max_matrix_bytes = 3000 * 352 * @sizeOf(f64);
const max_provenance_edge_bytes: u64 = 512 * 1024 * 1024;

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
const ArrayEdge = struct { path: []const u8, sha256: []const u8, bytes: u64, dtype: []const u8, order: []const u8, shape: []const usize };
const IndexEdge = struct { path: []const u8, sha256: []const u8, bytes: u64, dtype: []const u8, order: []const u8, count: usize };
const InputManifest = struct {
    schema: []const u8,
    completed: bool,
    diagnostic_only: bool,
    execution_authority: bool,
    publication_authority: bool,
    fit_invoked: bool,
    held_tensors_loaded: bool,
    query_rows: usize,
    retained_rows: usize,
    matrix: ArrayEdge,
    warm_rows: ?IndexEdge,
    source: Edge,
    request_sha256: []const u8,
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
fn digestBytes(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn digestText(bytes: []const u8) [64]u8 {
    return std.fmt.bytesToHex(digestBytes(bytes), .lower);
}
fn requestDigest(m: InputManifest) [64]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly.attention-matching.mass-dynamic-cg-request.v1\x00");
    var word: [8]u8 = undefined;
    std.mem.writeInt(u64, &word, @intCast(m.query_rows), .little);
    hasher.update(&word);
    std.mem.writeInt(u64, &word, @intCast(m.retained_rows), .little);
    hasher.update(&word);
    hasher.update(m.matrix.sha256);
    if (m.warm_rows) |warm| {
        hasher.update("\x01");
        hasher.update(warm.sha256);
    } else {
        hasher.update("\x00");
    }
    hasher.update(m.source.sha256);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}
fn requireAbsoluteEdge(edge: Edge) !void {
    if (edge.path.len == 0 or edge.path[0] != '/' or !validDigest(edge.sha256) or edge.bytes == 0 or edge.bytes > max_provenance_edge_bytes) return error.InvalidProvenance;
}
fn verifyAbsoluteEdge(_: std.mem.Allocator, io: std.Io, edge: Edge) !void {
    try requireAbsoluteEdge(edge);
    const stat = try std.Io.Dir.cwd().statFile(io, edge.path, .{ .follow_symlinks = false });
    if (stat.kind != .file or stat.size != edge.bytes) return error.ProvenanceChanged;
    const file = try std.Io.Dir.cwd().openFile(io, edge.path, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false });
    defer file.close(io);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        const count = file.readStreaming(io, &.{buffer[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (count == 0) break;
        total = std.math.add(u64, total, @intCast(count)) catch return error.ProvenanceChanged;
        if (total > edge.bytes) return error.ProvenanceChanged;
        hasher.update(buffer[0..count]);
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    const text = std.fmt.bytesToHex(digest, .lower);
    if (total != edge.bytes or !std.mem.eql(u8, &text, edge.sha256)) return error.ProvenanceChanged;
}
fn readJsonExpected(comptime T: type, allocator: std.mem.Allocator, io: std.Io, path: []const u8, expected: []const u8) !std.json.Parsed(T) {
    if (!validDigest(expected)) return error.InvalidExpectedDigest;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_json_bytes));
    defer allocator.free(bytes);
    if (!std.mem.eql(u8, &digestText(bytes), expected)) return error.ManifestChanged;
    return std.json.parseFromSlice(T, allocator, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
}
fn validateLibrary(r: LibraryReceipt) !void {
    if (!std.mem.eql(u8, r.schema, library_schema) or !r.completed or !r.diagnostic_only or r.execution_authority or r.publication_authority or
        !std.mem.eql(u8, r.source.tag, highs.pinned_highs_tag) or !std.mem.eql(u8, r.source.commit, highs.pinned_highs_commit) or
        !validDigest(r.source.tree_sha256) or r.source.runtime_githash.len == 0 or r.source.runtime_githash.len > 64 or !r.build.built_static or
        r.build.highsint64 or r.build.highs_no_default_threads or r.build.flags.len == 0) return error.InvalidProvenance;
    try requireAbsoluteEdge(r.source.archive);
    try requireAbsoluteEdge(r.source.license);
    try requireAbsoluteEdge(r.source.highs_c_api_header);
    try requireAbsoluteEdge(r.source.callback_header);
    try requireAbsoluteEdge(r.source.generated_hconfig);
    try requireAbsoluteEdge(r.artifacts.libhighs);
    try requireAbsoluteEdge(r.artifacts.symbol_inventory);
    try requireAbsoluteEdge(r.artifacts.link_inventory);
}
fn verifyLibraryClosure(a: std.mem.Allocator, io: std.Io, r: LibraryReceipt) !void {
    try verifyAbsoluteEdge(a, io, r.source.archive);
    try verifyAbsoluteEdge(a, io, r.source.license);
    try verifyAbsoluteEdge(a, io, r.source.highs_c_api_header);
    try verifyAbsoluteEdge(a, io, r.source.callback_header);
    try verifyAbsoluteEdge(a, io, r.source.generated_hconfig);
    try verifyAbsoluteEdge(a, io, r.artifacts.libhighs);
    try verifyAbsoluteEdge(a, io, r.artifacts.symbol_inventory);
    try verifyAbsoluteEdge(a, io, r.artifacts.link_inventory);
}
fn validateExecutable(r: ExecutableReceipt, argv0: []const u8, library_path: []const u8, library_sha: []const u8, lib: LibraryReceipt) !void {
    if (!std.mem.eql(u8, r.schema, executable_schema) or !r.completed or !r.diagnostic_only or r.execution_authority or r.publication_authority or
        !std.mem.eql(u8, r.target, "aarch64-macos-native") or !std.mem.eql(u8, r.optimize, "ReleaseSafe") or !r.strip or
        !validLinkArgv(r.link_argv, r.zig_executable.path, r.sources.driver.path, r.sources.lp.path, r.sources.controller.path, r.sources.adapter.path, r.libhighs.path, r.binary.path) or
        !std.mem.eql(u8, r.binary.path, argv0) or
        !std.mem.eql(u8, r.library_receipt.path, library_path) or !std.mem.eql(u8, r.library_receipt.sha256, library_sha) or
        !std.mem.eql(u8, r.libhighs.path, lib.artifacts.libhighs.path) or !std.mem.eql(u8, r.libhighs.sha256, lib.artifacts.libhighs.sha256) or
        r.libhighs.bytes != lib.artifacts.libhighs.bytes or !std.mem.eql(u8, r.sources.lp.sha256, lp_sha256) or
        !std.mem.eql(u8, r.sources.controller.sha256, controller_sha256) or !std.mem.eql(u8, r.sources.adapter.sha256, adapter_sha256)) return error.InvalidExecutableProvenance;
    try requireAbsoluteEdge(r.zig_executable);
    try requireAbsoluteEdge(r.sources.driver);
    try requireAbsoluteEdge(r.sources.lp);
    try requireAbsoluteEdge(r.sources.controller);
    try requireAbsoluteEdge(r.sources.adapter);
    try requireAbsoluteEdge(r.library_receipt);
    try requireAbsoluteEdge(r.libhighs);
    try requireAbsoluteEdge(r.binary);
}
fn argumentWithPath(value: []const u8, prefix: []const u8, path: []const u8) bool {
    return value.len == prefix.len + path.len and std.mem.startsWith(u8, value, prefix) and std.mem.eql(u8, value[prefix.len..], path);
}
fn validLinkArgv(argv: []const []const u8, zig: []const u8, driver: []const u8, lp: []const u8, controller: []const u8, adapter: []const u8, libhighs: []const u8, binary: []const u8) bool {
    if (argv.len != 30) return false;
    const exact = [_]?[]const u8{
        zig,             "build-exe", "-OReleaseSafe", "-fstrip",     "--dep",             "attention_mass_lp",  "--dep",         "attention_mass_cg", "--dep",             "attention_mass_highs", null,
        "-OReleaseSafe", null,        "-OReleaseSafe", "--dep",       "attention_mass_lp", null,                 "-OReleaseSafe", "--dep",             "attention_mass_lp", "--dep",                "attention_mass_cg",
        null,            libhighs,    "-lc++",         "--cache-dir", null,                "--global-cache-dir", null,            null,
    };
    for (exact, argv) |expected, actual| if (expected) |value| {
        if (!std.mem.eql(u8, value, actual)) return false;
    };
    if (!argumentWithPath(argv[10], "-Mroot=", driver) or !argumentWithPath(argv[12], "-Mattention_mass_lp=", lp) or
        !argumentWithPath(argv[16], "-Mattention_mass_cg=", controller) or !argumentWithPath(argv[22], "-Mattention_mass_highs=", adapter) or
        !argumentWithPath(argv[29], "-femit-bin=", binary)) return false;
    if (argv[26].len == 0 or argv[26][0] != '/' or argv[28].len == 0 or argv[28][0] != '/') return false;
    return true;
}
fn verifyExecutableClosure(a: std.mem.Allocator, io: std.Io, r: ExecutableReceipt) !void {
    try verifyAbsoluteEdge(a, io, r.zig_executable);
    try verifyAbsoluteEdge(a, io, r.sources.driver);
    try verifyAbsoluteEdge(a, io, r.sources.lp);
    try verifyAbsoluteEdge(a, io, r.sources.controller);
    try verifyAbsoluteEdge(a, io, r.sources.adapter);
    try verifyAbsoluteEdge(a, io, r.library_receipt);
    try verifyAbsoluteEdge(a, io, r.libhighs);
    try verifyAbsoluteEdge(a, io, r.binary);
}
fn supportedProfile(rows: usize, columns: usize) bool {
    return (rows == 912 and (columns == 288 or columns == 352)) or (rows == 3000 and columns == 288);
}
fn validateManifest(m: InputManifest) !void {
    if (!std.mem.eql(u8, m.schema, input_schema) or !m.completed or !m.diagnostic_only or m.execution_authority or m.publication_authority or
        m.fit_invoked or m.held_tensors_loaded or !supportedProfile(m.query_rows, m.retained_rows) or !validDigest(m.request_sha256) or
        !validRelative(m.matrix.path) or !validDigest(m.matrix.sha256) or !std.mem.eql(u8, m.matrix.dtype, "float64-le") or
        !std.mem.eql(u8, m.matrix.order, "C") or m.matrix.shape.len != 2 or m.matrix.shape[0] != m.query_rows or m.matrix.shape[1] != m.retained_rows or
        m.matrix.bytes != m.query_rows * m.retained_rows * @sizeOf(f64) or m.matrix.bytes > max_matrix_bytes) return error.InvalidInputManifest;
    try requireAbsoluteEdge(m.source);
    if (m.warm_rows) |w| {
        if (!validRelative(w.path) or !validDigest(w.sha256) or !std.mem.eql(u8, w.dtype, "uint64-le") or !std.mem.eql(u8, w.order, "C") or
            w.count > m.query_rows or w.bytes != w.count * @sizeOf(u64)) return error.InvalidInputManifest;
    }
    if (!std.mem.eql(u8, &requestDigest(m), m.request_sha256)) return error.InvalidRequestDigest;
}
fn ownerPath(a: std.mem.Allocator, manifest_path: []const u8, relative: []const u8) ![]u8 {
    return std.fs.path.join(a, &.{ std.fs.path.dirname(manifest_path) orelse ".", relative });
}
fn readBoundFile(a: std.mem.Allocator, io: std.Io, path: []const u8, expected_sha: []const u8, expected_bytes: u64, limit: usize) ![]u8 {
    if (!validDigest(expected_sha) or expected_bytes > limit) return error.InvalidInputManifest;
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(limit + 1));
    errdefer a.free(raw);
    if (raw.len != expected_bytes or !std.mem.eql(u8, &digestText(raw), expected_sha)) return error.InputChanged;
    return raw;
}
fn loadMatrix(a: std.mem.Allocator, io: std.Io, manifest_path: []const u8, m: InputManifest) ![]f64 {
    const path = try ownerPath(a, manifest_path, m.matrix.path);
    defer a.free(path);
    const raw = try readBoundFile(a, io, path, m.matrix.sha256, m.matrix.bytes, max_matrix_bytes);
    defer a.free(raw);
    if (@import("builtin").cpu.arch.endian() != .little) return error.UnsupportedEndian;
    const matrix = try a.alloc(f64, raw.len / 8);
    errdefer a.free(matrix);
    @memcpy(std.mem.sliceAsBytes(matrix), raw);
    for (matrix) |v| if (!std.math.isFinite(v) or v < 0) return error.InvalidMassMatrix;
    return matrix;
}
fn loadWarm(a: std.mem.Allocator, io: std.Io, manifest_path: []const u8, m: InputManifest) !?[]usize {
    const w = m.warm_rows orelse return null;
    const path = try ownerPath(a, manifest_path, w.path);
    defer a.free(path);
    const raw = try readBoundFile(a, io, path, w.sha256, w.bytes, max_matrix_bytes);
    defer a.free(raw);
    return decodeWarm(a, raw, w.count, m.query_rows);
}
fn decodeWarm(a: std.mem.Allocator, raw: []const u8, count: usize, query_rows: usize) ![]usize {
    if (raw.len != count * @sizeOf(u64)) return error.InvalidWarmRows;
    const result = try a.alloc(usize, count);
    errdefer a.free(result);
    for (0..count) |i| {
        const v = std.mem.readInt(u64, raw[i * 8 ..][0..8], .little);
        if (v >= query_rows) return error.InvalidWarmRows;
        result[i] = @intCast(v);
        for (result[0..i]) |prior| if (prior == result[i]) return error.InvalidWarmRows;
    }
    return result;
}
const VectorReceipt = struct { path: []const u8, sha256: [64]u8, bytes: usize, count: usize, dtype: []const u8 };
fn writeF64(io: std.Io, dir: std.Io.Dir, name: []const u8, v: []const f64) !VectorReceipt {
    const raw = std.mem.sliceAsBytes(v);
    const f = try dir.createFile(io, name, .{ .exclusive = true });
    defer f.close(io);
    try f.writeStreamingAll(io, raw);
    try f.sync(io);
    return .{ .path = name, .sha256 = digestText(raw), .bytes = raw.len, .count = v.len, .dtype = "float64-le" };
}
fn writeF32(io: std.Io, dir: std.Io.Dir, name: []const u8, v: []const f32) !VectorReceipt {
    const raw = std.mem.sliceAsBytes(v);
    const f = try dir.createFile(io, name, .{ .exclusive = true });
    defer f.close(io);
    try f.writeStreamingAll(io, raw);
    try f.sync(io);
    return .{ .path = name, .sha256 = digestText(raw), .bytes = raw.len, .count = v.len, .dtype = "float32-le" };
}
fn writeU64(io: std.Io, dir: std.Io.Dir, name: []const u8, v: []const usize, a: std.mem.Allocator) !VectorReceipt {
    const raw = try a.alloc(u8, v.len * 8);
    defer a.free(raw);
    for (v, 0..) |x, i| std.mem.writeInt(u64, raw[i * 8 ..][0..8], @intCast(x), .little);
    const f = try dir.createFile(io, name, .{ .exclusive = true });
    defer f.close(io);
    try f.writeStreamingAll(io, raw);
    try f.sync(io);
    return .{ .path = name, .sha256 = digestText(raw), .bytes = raw.len, .count = v.len, .dtype = "uint64-le" };
}
const AttemptTrace = struct { restricted_matrix_sha256: [64]u8, rows: usize, columns: usize, feasibility_tolerance: f64, status: cg.SolverStatus, iterations: usize, objective: f64, raw_weights_sha256: [64]u8, alpha_sha256: [64]u8, beta_sha256: [64]u8, finite_nonnegative_weights: bool };
const TraceSolver = struct { adapter: highs.Adapter, attempts: [cg.max_rounds * 2]AttemptTrace = undefined, count: usize = 0 };
fn tracedSolve(raw: ?*anyopaque, a: std.mem.Allocator, p: *const mass_lp.Problem, deadline: u64, control: cg.Control, tolerance: f64) anyerror!cg.RestrictedSolve {
    if (raw == null) return error.InvalidAdapter;
    const self: *TraceSolver = @ptrCast(@alignCast(raw.?));
    if (self.count >= self.attempts.len) return error.TooManySolverAttempts;
    const count = try std.math.mul(usize, p.source_rows, p.weight_columns);
    const restricted = try a.alloc(f64, count);
    defer a.free(restricted);
    for (0..p.source_rows) |row| @memcpy(restricted[row * p.weight_columns ..][0..p.weight_columns], p.a_ub[row * p.variables ..][0..p.weight_columns]);
    var result = try highs.solveRestricted(&self.adapter, a, p, deadline, control, tolerance);
    errdefer result.deinit();
    if (result.row_duals.len != 2 * p.source_rows) return error.InvalidSolverResult;
    const alpha = try a.alloc(f64, p.source_rows);
    defer a.free(alpha);
    const beta = try a.alloc(f64, p.source_rows);
    defer a.free(beta);
    for (0..p.source_rows) |row| {
        alpha[row] = -result.row_duals[row];
        beta[row] = -result.row_duals[p.source_rows + row];
    }
    var nonnegative = true;
    for (result.weights) |w| if (!std.math.isFinite(w) or w < 0) {
        nonnegative = false;
        break;
    };
    self.attempts[self.count] = .{ .restricted_matrix_sha256 = digestText(std.mem.sliceAsBytes(restricted)), .rows = p.source_rows, .columns = p.weight_columns, .feasibility_tolerance = tolerance, .status = result.status, .iterations = result.iterations, .objective = result.objective, .raw_weights_sha256 = digestText(std.mem.sliceAsBytes(result.weights)), .alpha_sha256 = digestText(std.mem.sliceAsBytes(alpha)), .beta_sha256 = digestText(std.mem.sliceAsBytes(beta)), .finite_nonnegative_weights = nonnegative };
    self.count += 1;
    return result;
}
/// Solver-neutral library boundary. The caller owns `matrix` and `warm_rows`;
/// the returned union owns only the allocations documented by CG V5.
pub fn solveDynamic(a: std.mem.Allocator, matrix: []const f64, query_rows: usize, retained_rows: usize, warm_rows: ?[]const usize, deadline_ns: u64, control: cg.Control, solver: cg.Solver) !cg.Result {
    if (!supportedProfile(query_rows, retained_rows)) return error.UnsupportedProfile;
    const expected = std.math.mul(usize, query_rows, retained_rows) catch return error.InvalidShape;
    if (matrix.len != expected) return error.InvalidShape;
    return cg.solvePrimaryConstraintGeneration(a, matrix, query_rows, retained_rows, warm_rows, deadline_ns, control, solver);
}
fn terminalRecheck(a: std.mem.Allocator, io: std.Io, argv0: []const u8, manifest_path: []const u8, manifest_sha: []const u8, library_path: []const u8, library_sha: []const u8, executable_path: []const u8, executable_sha: []const u8) !void {
    var m = try readJsonExpected(InputManifest, a, io, manifest_path, manifest_sha);
    defer m.deinit();
    try validateManifest(m.value);
    const matrix = try loadMatrix(a, io, manifest_path, m.value);
    a.free(matrix);
    if (try loadWarm(a, io, manifest_path, m.value)) |warm| a.free(warm);
    try verifyAbsoluteEdge(a, io, m.value.source);
    var l = try readJsonExpected(LibraryReceipt, a, io, library_path, library_sha);
    defer l.deinit();
    try validateLibrary(l.value);
    try verifyLibraryClosure(a, io, l.value);
    var e = try readJsonExpected(ExecutableReceipt, a, io, executable_path, executable_sha);
    defer e.deinit();
    try validateExecutable(e.value, argv0, library_path, library_sha, l.value);
    try verifyExecutableClosure(a, io, e.value);
}
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    if (args.len != 17 or !std.mem.eql(u8, args[1], "--input-manifest") or !std.mem.eql(u8, args[3], "--input-sha256") or !std.mem.eql(u8, args[5], "--library-receipt") or !std.mem.eql(u8, args[7], "--library-sha256") or !std.mem.eql(u8, args[9], "--executable-receipt") or !std.mem.eql(u8, args[11], "--executable-sha256") or !std.mem.eql(u8, args[13], "--output-dir") or !std.mem.eql(u8, args[15], "--deadline-ms")) return error.InvalidArguments;
    const deadline_ms = try std.fmt.parseUnsigned(u64, args[16], 10);
    if (deadline_ms == 0 or deadline_ms > 560000) return error.InvalidDeadline;
    var lib = try readJsonExpected(LibraryReceipt, a, io, args[6], args[8]);
    defer lib.deinit();
    try validateLibrary(lib.value);
    try verifyLibraryClosure(a, io, lib.value);
    var exe = try readJsonExpected(ExecutableReceipt, a, io, args[10], args[12]);
    defer exe.deinit();
    try validateExecutable(exe.value, args[0], args[6], args[8], lib.value);
    try verifyExecutableClosure(a, io, exe.value);
    if (Highs_versionMajor() != 1 or Highs_versionMinor() != 8 or Highs_versionPatch() != 0 or !std.mem.eql(u8, std.mem.span(Highs_githash()), lib.value.source.runtime_githash)) return error.LinkedHighsIdentityMismatch;
    var parsed = try readJsonExpected(InputManifest, a, io, args[2], args[4]);
    defer parsed.deinit();
    try validateManifest(parsed.value);
    try verifyAbsoluteEdge(a, io, parsed.value.source);
    const matrix = try loadMatrix(a, io, args[2], parsed.value);
    defer a.free(matrix);
    const warm = try loadWarm(a, io, args[2], parsed.value);
    defer if (warm) |v| a.free(v);
    var clock = Clock{ .io = io };
    const start = clock.now();
    const deadline = try std.math.add(u64, start, try std.math.mul(u64, deadline_ms, std.time.ns_per_ms));
    lifecycle = .{};
    var traced = TraceSolver{ .adapter = .{ .api = &api } };
    var result = try solveDynamic(a, matrix, parsed.value.query_rows, parsed.value.retained_rows, warm, deadline, clock.control(), .{ .context = &traced, .solve_restricted_fn = tracedSolve });
    defer result.deinit();
    try terminalRecheck(a, io, args[0], args[2], args[4], args[6], args[8], args[10], args[12]);
    if (clock.now() >= deadline) return error.Timeout;
    try std.Io.Dir.cwd().createDir(io, args[14], .default_dir);
    var out = try std.Io.Dir.cwd().openDir(io, args[14], .{ .follow_symlinks = false });
    defer out.close(io);
    const input_edge = Edge{ .path = args[2], .sha256 = args[4], .bytes = (try std.Io.Dir.cwd().statFile(io, args[2], .{})).size };
    const lib_edge = Edge{ .path = args[6], .sha256 = args[8], .bytes = (try std.Io.Dir.cwd().statFile(io, args[6], .{})).size };
    const exe_edge = Edge{ .path = args[10], .sha256 = args[12], .bytes = (try std.Io.Dir.cwd().statFile(io, args[10], .{})).size };
    switch (result) {
        .terminal => |t| {
            const receipt = .{ .schema = result_schema, .completed = true, .diagnostic_only = true, .execution_authority = false, .publication_authority = false, .status = "terminal", .input = input_edge, .library_receipt = lib_edge, .executable_receipt = exe_edge, .request_sha256 = parsed.value.request_sha256, .geometry = .{ .query_rows = parsed.value.query_rows, .retained_rows = parsed.value.retained_rows, .warm_rows = if (warm) |v| v.len else 0 }, .terminal_reason = @tagName(t.reason), .diagnostics = .{ .rounds = t.diagnostics.rounds[0..t.diagnostics.round_count], .round_count = t.diagnostics.round_count, .total_iterations = t.diagnostics.total_iterations, .full_rows = t.diagnostics.full_rows, .selected_rows = t.diagnostics.selected_rows, .warm_hint_rows = t.diagnostics.warm_hint_rows, .initial_selected_rows = t.diagnostics.initial_selected_rows }, .attempts = traced.attempts[0..traced.count], .lifecycle = lifecycle, .rollback_required = true, .arrays = @as(?u8, null) };
            try writeReceipt(a, io, out, receipt);
        },
        .accepted => |*v| {
            const raw = try writeF64(io, out, "raw-weights.f64le", v.raw_weights);
            const bias = try writeF32(io, out, "bias.f32le", v.bias);
            const stored = try writeF64(io, out, "stored-weights.f64le", v.stored_weights);
            const mass = try writeF64(io, out, "mass.f64le", v.mass);
            const alpha = try writeF64(io, out, "alpha.f64le", v.alpha);
            const beta = try writeF64(io, out, "beta.f64le", v.beta);
            const selected = try writeU64(io, out, "selected-rows.u64le", v.selected_row_indices, a);
            const witness = try mass_lp.auditPrimaryWitness(matrix, parsed.value.query_rows, parsed.value.retained_rows, .{ .weights = v.raw_weights, .t = v.objective, .objective = v.objective, .alpha = v.alpha, .beta = v.beta });
            const receipt = .{ .schema = result_schema, .completed = true, .diagnostic_only = true, .execution_authority = false, .publication_authority = false, .status = "accepted", .input = input_edge, .library_receipt = lib_edge, .executable_receipt = exe_edge, .request_sha256 = parsed.value.request_sha256, .geometry = .{ .query_rows = parsed.value.query_rows, .retained_rows = parsed.value.retained_rows, .warm_rows = if (warm) |x| x.len else 0 }, .objective = v.objective, .diagnostics = .{ .rounds = v.diagnostics.rounds[0..v.diagnostics.round_count], .round_count = v.diagnostics.round_count, .total_iterations = v.diagnostics.total_iterations, .full_rows = v.diagnostics.full_rows, .selected_rows = v.diagnostics.selected_rows, .warm_hint_rows = v.diagnostics.warm_hint_rows, .initial_selected_rows = v.diagnostics.initial_selected_rows }, .attempts = traced.attempts[0..traced.count], .full_original_coordinate_witness = witness, .lifecycle = lifecycle, .rollback_required = false, .arrays = .{ .raw_weights = raw, .bias = bias, .stored_weights = stored, .mass = mass, .alpha = alpha, .beta = beta, .selected_rows = selected } };
            try writeReceipt(a, io, out, receipt);
        },
    }
}
fn writeReceipt(a: std.mem.Allocator, io: std.Io, out: std.Io.Dir, receipt: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(a, receipt, .{ .whitespace = .indent_2 });
    defer a.free(json);
    const f = try out.createFile(io, "result.json", .{ .exclusive = true });
    defer f.close(io);
    try f.writeStreamingAll(io, json);
    try f.writeStreamingAll(io, "\n");
    try f.sync(io);
}

test "context dynamic mass CG profiles and manifest edges are strict" {
    try std.testing.expect(supportedProfile(912, 352));
    try std.testing.expect(supportedProfile(912, 288));
    try std.testing.expect(supportedProfile(3000, 288));
    try std.testing.expect(!supportedProfile(3000, 352));
    try std.testing.expect(!validRelative("../matrix"));
    try std.testing.expect(validRelative("matrix.f64le"));
    try std.testing.expectError(error.InvalidProvenance, requireAbsoluteEdge(.{
        .path = "/does-not-need-to-exist",
        .sha256 = "0000000000000000000000000000000000000000000000000000000000000000",
        .bytes = max_provenance_edge_bytes + 1,
    }));
}
test "context dynamic mass CG executable argv is exact and rejects rewiring" {
    var argv = [_][]const u8{
        "/zig",           "build-exe",         "-OReleaseSafe",                   "-fstrip",       "--dep", "attention_mass_lp", "--dep",                   "attention_mass_cg",  "--dep",         "attention_mass_highs",
        "-Mroot=/driver", "-OReleaseSafe",     "-Mattention_mass_lp=/lp",         "-OReleaseSafe", "--dep", "attention_mass_lp", "-Mattention_mass_cg=/cg", "-OReleaseSafe",      "--dep",         "attention_mass_lp",
        "--dep",          "attention_mass_cg", "-Mattention_mass_highs=/adapter", "/libhighs.a",   "-lc++", "--cache-dir",       "/cache",                  "--global-cache-dir", "/global-cache", "-femit-bin=/binary",
    };
    try std.testing.expect(validLinkArgv(&argv, "/zig", "/driver", "/lp", "/cg", "/adapter", "/libhighs.a", "/binary"));
    argv[23] = "/other-libhighs.a";
    try std.testing.expect(!validLinkArgv(&argv, "/zig", "/driver", "/lp", "/cg", "/adapter", "/libhighs.a", "/binary"));
    argv[23] = "/libhighs.a";
    argv[16] = "-Mattention_mass_cg=/other-cg";
    try std.testing.expect(!validLinkArgv(&argv, "/zig", "/driver", "/lp", "/cg", "/adapter", "/libhighs.a", "/binary"));
    argv[16] = "-Mattention_mass_cg=/cg";
    argv[26] = "relative-cache";
    try std.testing.expect(!validLinkArgv(&argv, "/zig", "/driver", "/lp", "/cg", "/adapter", "/libhighs.a", "/binary"));
}
test "context dynamic mass CG provenance edge hashes with bounded storage and rejects mutations" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const payload = "streaming provenance edge";
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "edge.bin", .data = payload });
    const relative = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "edge.bin" });
    defer std.testing.allocator.free(relative);
    const absolute_sentinel = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, relative, std.testing.allocator);
    defer std.testing.allocator.free(absolute_sentinel);
    const absolute = absolute_sentinel[0..absolute_sentinel.len];
    const payload_digest = digestText(payload);
    const good = Edge{ .path = absolute, .sha256 = &payload_digest, .bytes = payload.len };
    try verifyAbsoluteEdge(std.testing.allocator, std.testing.io, good);
    var wrong_size = good;
    wrong_size.bytes += 1;
    try std.testing.expectError(error.ProvenanceChanged, verifyAbsoluteEdge(std.testing.allocator, std.testing.io, wrong_size));
    var wrong_digest = good;
    wrong_digest.sha256 = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(error.ProvenanceChanged, verifyAbsoluteEdge(std.testing.allocator, std.testing.io, wrong_digest));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "edge.bin", .data = "short" });
    try std.testing.expectError(error.ProvenanceChanged, verifyAbsoluteEdge(std.testing.allocator, std.testing.io, good));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "empty.bin", .data = "" });
    const empty_relative = try std.fs.path.join(std.testing.allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "empty.bin" });
    defer std.testing.allocator.free(empty_relative);
    const empty_sentinel = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, empty_relative, std.testing.allocator);
    defer std.testing.allocator.free(empty_sentinel);
    try std.testing.expectError(error.InvalidProvenance, verifyAbsoluteEdge(std.testing.allocator, std.testing.io, .{
        .path = empty_sentinel[0..empty_sentinel.len],
        .sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        .bytes = 0,
    }));
}
test "context dynamic mass CG request digest binds geometry and edges" {
    const shape = [_]usize{ 912, 288 };
    var manifest = InputManifest{
        .schema = input_schema,
        .completed = true,
        .diagnostic_only = true,
        .execution_authority = false,
        .publication_authority = false,
        .fit_invoked = false,
        .held_tensors_loaded = false,
        .query_rows = 912,
        .retained_rows = 288,
        .matrix = .{ .path = "matrix.f64le", .sha256 = "0000000000000000000000000000000000000000000000000000000000000000", .bytes = 912 * 288 * 8, .dtype = "float64-le", .order = "C", .shape = &shape },
        .warm_rows = null,
        .source = .{ .path = "/source.json", .sha256 = "1111111111111111111111111111111111111111111111111111111111111111", .bytes = 1 },
        .request_sha256 = undefined,
    };
    const digest = requestDigest(manifest);
    manifest.request_sha256 = &digest;
    try validateManifest(manifest);
    manifest.query_rows = 3000;
    try std.testing.expectError(error.InvalidInputManifest, validateManifest(manifest));
    manifest.query_rows = 912;
    manifest.request_sha256 = "2222222222222222222222222222222222222222222222222222222222222222";
    try std.testing.expectError(error.InvalidRequestDigest, validateManifest(manifest));
}
test "context dynamic mass CG warm rows preserve order and reject duplicates" {
    var encoded: [24]u8 = undefined;
    std.mem.writeInt(u64, encoded[0..8], 8, .little);
    std.mem.writeInt(u64, encoded[8..16], 1, .little);
    std.mem.writeInt(u64, encoded[16..24], 7, .little);
    const decoded = try decodeWarm(std.testing.allocator, &encoded, 3, 912);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualSlices(usize, &.{ 8, 1, 7 }, decoded);
    std.mem.writeInt(u64, encoded[16..24], 8, .little);
    try std.testing.expectError(error.InvalidWarmRows, decodeWarm(std.testing.allocator, &encoded, 3, 912));
    std.mem.writeInt(u64, encoded[16..24], 912, .little);
    try std.testing.expectError(error.InvalidWarmRows, decodeWarm(std.testing.allocator, &encoded, 3, 912));
    try std.testing.expectError(error.InvalidWarmRows, decodeWarm(std.testing.allocator, encoded[0..16], 3, 912));
}

const TestClock = struct {
    now: u64,
    checks: usize = 0,
    cancel_at: ?usize = null,
    fn nowFn(raw: ?*anyopaque) u64 {
        const self: *TestClock = @ptrCast(@alignCast(raw.?));
        return self.now;
    }
    fn checkFn(raw: ?*anyopaque) anyerror!void {
        const self: *TestClock = @ptrCast(@alignCast(raw.?));
        self.checks += 1;
        if (self.cancel_at) |at| if (self.checks >= at) return error.Cancelled;
    }
};
fn unreachableSolver(_: ?*anyopaque, _: std.mem.Allocator, _: *const mass_lp.Problem, _: u64, _: cg.Control, _: f64) anyerror!cg.RestrictedSolve {
    return error.UnexpectedSolverCall;
}
test "context dynamic mass CG library boundary rejects shape and honors initial deadline" {
    const matrix = try std.testing.allocator.alloc(f64, 912 * 288);
    defer std.testing.allocator.free(matrix);
    @memset(matrix, 0);
    var clock = TestClock{ .now = 100 };
    const control = cg.Control{ .context = &clock, .now_ns_fn = TestClock.nowFn, .check_fn = TestClock.checkFn };
    try std.testing.expectError(error.InvalidShape, solveDynamic(std.testing.allocator, matrix[0 .. matrix.len - 1], 912, 288, null, 100, control, .{ .solve_restricted_fn = unreachableSolver }));
    var result = try solveDynamic(std.testing.allocator, matrix, 912, 288, null, 100, control, .{ .solve_restricted_fn = unreachableSolver });
    defer result.deinit();
    try std.testing.expectEqual(cg.TerminalReason.deadline_exhausted, result.terminal.reason);
    try std.testing.expectEqual(@as(usize, 1), clock.checks);
}

const UniformSolver = struct {
    calls: usize = 0,
    fn solve(raw: ?*anyopaque, a: std.mem.Allocator, problem: *const mass_lp.Problem, _: u64, control: cg.Control, tolerance: f64) anyerror!cg.RestrictedSolve {
        const self: *UniformSolver = @ptrCast(@alignCast(raw.?));
        try control.check();
        if (tolerance != cg.original_feasibility_tolerance) return error.UnexpectedTolerance;
        self.calls += 1;
        const weights = try a.alloc(f64, problem.weight_columns);
        errdefer a.free(weights);
        @memset(weights, 1 / @as(f64, @floatFromInt(problem.weight_columns)));
        const duals = try a.alloc(f64, 2 * problem.source_rows);
        @memset(duals, 0);
        return .{ .allocator = a, .status = .optimal, .weights = weights, .t = 0, .objective = 0, .row_duals = duals, .iterations = 3 };
    }
};
test "context dynamic mass CG library accepts supported profile with ordered warm hints and owned result" {
    const matrix = try std.testing.allocator.alloc(f64, 912 * 288);
    defer std.testing.allocator.free(matrix);
    @memset(matrix, 1);
    const warm = [_]usize{ 8, 1, 7 };
    var clock = TestClock{ .now = 1 };
    var fake = UniformSolver{};
    var result = try solveDynamic(
        std.testing.allocator,
        matrix,
        912,
        288,
        &warm,
        1000,
        .{ .context = &clock, .now_ns_fn = TestClock.nowFn, .check_fn = TestClock.checkFn },
        .{ .context = &fake, .solve_restricted_fn = UniformSolver.solve },
    );
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    switch (result) {
        .terminal => return error.UnexpectedTerminal,
        .accepted => |accepted| {
            try std.testing.expectEqual(@as(usize, 288), accepted.raw_weights.len);
            try std.testing.expectEqual(@as(usize, 912), accepted.alpha.len);
            try std.testing.expectEqual(@as(usize, 912), accepted.beta.len);
            try std.testing.expectEqual(@as(usize, 3), accepted.diagnostics.warm_hint_rows);
            try std.testing.expectEqual(@as(f32, @floatCast(@log(1 / @as(f64, 288)))), accepted.bias[0]);
        },
    }
}
test "context dynamic mass CG library propagates cancellation before solver ownership" {
    const matrix = try std.testing.allocator.alloc(f64, 912 * 352);
    defer std.testing.allocator.free(matrix);
    @memset(matrix, 1);
    var clock = TestClock{ .now = 1, .cancel_at = 1 };
    var fake = UniformSolver{};
    try std.testing.expectError(error.Cancelled, solveDynamic(
        std.testing.allocator,
        matrix,
        912,
        352,
        null,
        1000,
        .{ .context = &clock, .now_ns_fn = TestClock.nowFn, .check_fn = TestClock.checkFn },
        .{ .context = &fake, .solve_restricted_fn = UniformSolver.solve },
    ));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}
