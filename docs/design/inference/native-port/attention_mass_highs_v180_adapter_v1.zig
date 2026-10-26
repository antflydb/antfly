//! Unapplied HiGHS v1.8.0 C-ABI adapter draft for the solver-neutral mass-CG
//! controller. This file is outside canonical runtime sources. It links no
//! solver by itself: tests inject an exact-shape C API table.
//!
//! Frozen oracle contract: HiGHS 1.8.0, dual simplex, presolve on, raw row
//! duals, first tolerance 1e-9 and controller-owned witness retry at 1e-10.
//! The adapter never negates row duals; attention_mass_constraint_generation
//! performs alpha=-upper_dual and beta=-lower_dual in original coordinates.
const std = @import("std");
const mass_lp = @import("attention_mass_lp");
const cg = @import("attention_mass_cg");

pub const pinned_highs_tag = "v1.8.0";
pub const pinned_highs_commit = "fcfb5341462f8a7db5ef5038613413f97d6cac3d";
pub const required_highs_int_bytes: c_int = 4;

const status_error: c_int = -1;
const status_ok: c_int = 0;
const status_warning: c_int = 1;
const model_optimal: c_int = 7;
const model_infeasible: c_int = 8;
const model_unbounded_or_infeasible: c_int = 9;
const model_unbounded: c_int = 10;
const model_time_limit: c_int = 13;
const model_iteration_limit: c_int = 14;
const model_interrupt: c_int = 17;
const matrix_rowwise: c_int = 2;
const sense_minimize: c_int = 1;
const callback_simplex_interrupt: c_int = 1;
const simplex_strategy_dual_serial: c_int = 1;
const assembly_checkpoint_values: usize = 4096;

pub const CallbackDataOut = extern struct {
    log_type: c_int,
    running_time: f64,
    simplex_iteration_count: c_int,
    ipm_iteration_count: c_int,
    pdlp_iteration_count: c_int,
    objective_function_value: f64,
    mip_node_count: i64,
    mip_total_lp_iterations: i64,
    mip_primal_bound: f64,
    mip_dual_bound: f64,
    mip_gap: f64,
    mip_solution: ?[*]f64,
    cutpool_num_col: c_int,
    cutpool_num_cut: c_int,
    cutpool_num_nz: c_int,
    cutpool_start: ?[*]c_int,
    cutpool_index: ?[*]c_int,
    cutpool_value: ?[*]f64,
    cutpool_lower: ?[*]f64,
    cutpool_upper: ?[*]f64,
};

pub const CallbackDataIn = extern struct {
    user_interrupt: c_int,
};

pub const Callback = *const fn (
    c_int,
    [*:0]const u8,
    *const CallbackDataOut,
    *CallbackDataIn,
    ?*anyopaque,
) callconv(.c) void;

/// Exact v1.8.0 C signatures used by this adapter. A production binding may
/// initialize this table with extern functions; tests provide fakes and need no
/// HiGHS library.
pub const Api = struct {
    create: *const fn () callconv(.c) ?*anyopaque,
    destroy: *const fn (*anyopaque) callconv(.c) void,
    version_major: *const fn () callconv(.c) c_int,
    version_minor: *const fn () callconv(.c) c_int,
    version_patch: *const fn () callconv(.c) c_int,
    sizeof_highs_int: *const fn (*const anyopaque) callconv(.c) c_int,
    get_infinity: *const fn (*const anyopaque) callconv(.c) f64,
    set_bool_option: *const fn (*anyopaque, [*:0]const u8, c_int) callconv(.c) c_int,
    set_int_option: *const fn (*anyopaque, [*:0]const u8, c_int) callconv(.c) c_int,
    set_double_option: *const fn (*anyopaque, [*:0]const u8, f64) callconv(.c) c_int,
    set_string_option: *const fn (*anyopaque, [*:0]const u8, [*:0]const u8) callconv(.c) c_int,
    set_callback: *const fn (*anyopaque, Callback, ?*anyopaque) callconv(.c) c_int,
    start_callback: *const fn (*anyopaque, c_int) callconv(.c) c_int,
    pass_lp: *const fn (
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
    ) callconv(.c) c_int,
    run: *const fn (*anyopaque) callconv(.c) c_int,
    model_status: *const fn (*const anyopaque) callconv(.c) c_int,
    get_solution: *const fn (
        *const anyopaque,
        [*]f64,
        [*]f64,
        [*]f64,
        [*]f64,
    ) callconv(.c) c_int,
    get_objective: *const fn (*const anyopaque) callconv(.c) f64,
    get_int_info: *const fn (*const anyopaque, [*:0]const u8, *c_int) callconv(.c) c_int,
};

pub const Adapter = struct {
    api: *const Api,

    pub fn solver(self: *Adapter) cg.Solver {
        return .{ .context = self, .solve_restricted_fn = solveRestricted };
    }
};

const CallbackState = struct {
    control: cg.Control,
    absolute_deadline_ns: u64,
    failure: ?anyerror = null,
    interrupted: bool = false,
};

const Csr = struct {
    allocator: std.mem.Allocator,
    start: []c_int,
    index: []c_int,
    value: []f64,
    col_cost: []f64,
    col_lower: []f64,
    col_upper: []f64,
    row_lower: []f64,
    row_upper: []f64,

    fn deinit(self: *Csr) void {
        self.allocator.free(self.start);
        self.allocator.free(self.index);
        self.allocator.free(self.value);
        self.allocator.free(self.col_cost);
        self.allocator.free(self.col_lower);
        self.allocator.free(self.col_upper);
        self.allocator.free(self.row_lower);
        self.allocator.free(self.row_upper);
        self.* = undefined;
    }
};

fn checkedCInt(value: usize) !c_int {
    return std.math.cast(c_int, value) orelse error.HighsIntOverflow;
}

fn requireOk(code: c_int) !void {
    if (code != status_ok) return error.HighsApiFailure;
}

fn finiteProblem(problem: *const mass_lp.Problem) !void {
    if (problem.constraint_rows == 0 or problem.variables == 0 or problem.weight_columns + 1 != problem.variables)
        return error.InvalidProblem;
    const coefficient_count = std.math.mul(usize, problem.constraint_rows, problem.variables) catch
        return error.HighsIntOverflow;
    if (problem.objective.len != problem.variables or problem.bounds.len != problem.variables or
        problem.b_ub.len != problem.constraint_rows or problem.a_ub.len != coefficient_count)
        return error.InvalidProblem;
    _ = try checkedCInt(problem.constraint_rows);
    _ = try checkedCInt(problem.variables);
    _ = try checkedCInt(coefficient_count);
    for (problem.objective) |value| if (!std.math.isFinite(value)) return error.NonFiniteProblem;
    for (problem.b_ub) |value| if (!std.math.isFinite(value)) return error.NonFiniteProblem;
    for (problem.bounds) |bound| {
        if (!std.math.isFinite(bound.lower)) return error.NonFiniteProblem;
        if (!std.math.isFinite(bound.upper) and !std.math.isPositiveInf(bound.upper)) return error.NonFiniteProblem;
        if (bound.lower > bound.upper) return error.InvalidProblem;
    }
}

fn callback(
    callback_type: c_int,
    _: [*:0]const u8,
    _: *const CallbackDataOut,
    input: *CallbackDataIn,
    raw: ?*anyopaque,
) callconv(.c) void {
    if (callback_type != callback_simplex_interrupt or raw == null) return;
    const state: *CallbackState = @ptrCast(@alignCast(raw.?));
    if (state.failure != null or state.interrupted) {
        input.user_interrupt = 1;
        return;
    }
    state.control.check() catch |err| {
        state.failure = err;
        input.user_interrupt = 1;
        return;
    };
    if (state.control.nowNs() >= state.absolute_deadline_ns) {
        state.failure = error.DeadlineExceeded;
        input.user_interrupt = 1;
    }
}

fn buildCsr(
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    highs_infinity: f64,
    control: cg.Control,
    absolute_deadline_ns: u64,
) !Csr {
    try control.check();
    if (control.nowNs() >= absolute_deadline_ns) return error.DeadlineExceeded;
    try finiteProblem(problem);
    if (!std.math.isFinite(highs_infinity) or highs_infinity <= 0) return error.InvalidHighsInfinity;

    const nnz = std.math.mul(usize, problem.constraint_rows, problem.variables) catch
        return error.HighsIntOverflow;
    var result = Csr{
        .allocator = allocator,
        .start = try allocator.alloc(c_int, problem.constraint_rows),
        .index = undefined,
        .value = undefined,
        .col_cost = undefined,
        .col_lower = undefined,
        .col_upper = undefined,
        .row_lower = undefined,
        .row_upper = undefined,
    };
    errdefer allocator.free(result.start);
    result.index = try allocator.alloc(c_int, nnz);
    errdefer allocator.free(result.index);
    result.value = try allocator.alloc(f64, nnz);
    errdefer allocator.free(result.value);
    result.col_cost = try allocator.dupe(f64, problem.objective);
    errdefer allocator.free(result.col_cost);
    result.col_lower = try allocator.alloc(f64, problem.variables);
    errdefer allocator.free(result.col_lower);
    result.col_upper = try allocator.alloc(f64, problem.variables);
    errdefer allocator.free(result.col_upper);
    result.row_lower = try allocator.alloc(f64, problem.constraint_rows);
    errdefer allocator.free(result.row_lower);
    result.row_upper = try allocator.dupe(f64, problem.b_ub);
    errdefer allocator.free(result.row_upper);

    for (0..problem.constraint_rows) |row| {
        if (row % 64 == 0) {
            try control.check();
            if (control.nowNs() >= absolute_deadline_ns) return error.DeadlineExceeded;
        }
        result.start[row] = try checkedCInt(try std.math.mul(usize, row, problem.variables));
        for (0..problem.variables) |column| {
            const offset = row * problem.variables + column;
            if (offset % assembly_checkpoint_values == 0) try control.check();
            const value = problem.a_ub[offset];
            if (!std.math.isFinite(value)) return error.NonFiniteProblem;
            result.index[offset] = try checkedCInt(column);
            result.value[offset] = value;
        }
    }
    for (problem.bounds, 0..) |bound, column| {
        result.col_lower[column] = bound.lower;
        result.col_upper[column] = if (std.math.isPositiveInf(bound.upper)) highs_infinity else bound.upper;
    }
    @memset(result.row_lower, -highs_infinity);
    try control.check();
    if (control.nowNs() >= absolute_deadline_ns) return error.DeadlineExceeded;
    return result;
}

fn remainingSeconds(control: cg.Control, absolute_deadline_ns: u64) !f64 {
    try control.check();
    const now = control.nowNs();
    if (now >= absolute_deadline_ns) return error.DeadlineExceeded;
    const remaining_ns = absolute_deadline_ns - now;
    const seconds = @as(f64, @floatFromInt(remaining_ns)) / @as(f64, std.time.ns_per_s);
    if (!std.math.isFinite(seconds) or seconds <= 0) return error.DeadlineExceeded;
    return seconds;
}

fn emptyResult(allocator: std.mem.Allocator, status: cg.SolverStatus) !cg.RestrictedSolve {
    const weights = try allocator.alloc(f64, 0);
    errdefer allocator.free(weights);
    const duals = try allocator.alloc(f64, 0);
    return .{
        .allocator = allocator,
        .status = status,
        .weights = weights,
        .t = 0,
        .objective = 0,
        .row_duals = duals,
        .iterations = 0,
    };
}

pub fn solveRestricted(
    raw: ?*anyopaque,
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    absolute_deadline_ns: u64,
    control: cg.Control,
    feasibility_tolerance: f64,
) anyerror!cg.RestrictedSolve {
    try control.check();
    if (control.nowNs() >= absolute_deadline_ns) return error.DeadlineExceeded;
    if (raw == null) return error.InvalidAdapter;
    if (!std.math.isFinite(feasibility_tolerance) or
        (feasibility_tolerance != cg.original_feasibility_tolerance and
            feasibility_tolerance != cg.retry_feasibility_tolerance))
        return error.InvalidTolerance;
    const adapter: *Adapter = @ptrCast(@alignCast(raw.?));
    const api = adapter.api;
    if (api.version_major() != 1 or api.version_minor() != 8 or api.version_patch() != 0)
        return error.InvalidHighsVersion;

    const handle = api.create() orelse return error.HighsCreateFailed;
    defer api.destroy(handle);
    if (api.sizeof_highs_int(handle) != required_highs_int_bytes) return error.InvalidHighsIntWidth;
    const infinity = api.get_infinity(handle);
    var csr = try buildCsr(allocator, problem, infinity, control, absolute_deadline_ns);
    defer csr.deinit();

    try requireOk(api.set_string_option(handle, "solver", "simplex"));
    try requireOk(api.set_string_option(handle, "presolve", "on"));
    try requireOk(api.set_int_option(handle, "simplex_strategy", simplex_strategy_dual_serial));
    try requireOk(api.set_int_option(handle, "highs_debug_level", 0));
    try requireOk(api.set_bool_option(handle, "output_flag", 0));
    try requireOk(api.set_bool_option(handle, "log_to_console", 0));
    try requireOk(api.set_double_option(handle, "primal_feasibility_tolerance", feasibility_tolerance));
    try requireOk(api.set_double_option(handle, "dual_feasibility_tolerance", feasibility_tolerance));

    var callback_state = CallbackState{ .control = control, .absolute_deadline_ns = absolute_deadline_ns };
    try requireOk(api.set_callback(handle, callback, &callback_state));
    try requireOk(api.start_callback(handle, callback_simplex_interrupt));

    const num_col = try checkedCInt(problem.variables);
    const num_row = try checkedCInt(problem.constraint_rows);
    const num_nz = try checkedCInt(problem.a_ub.len);
    try requireOk(api.pass_lp(
        handle,
        num_col,
        num_row,
        num_nz,
        matrix_rowwise,
        sense_minimize,
        0,
        csr.col_cost.ptr,
        csr.col_lower.ptr,
        csr.col_upper.ptr,
        csr.row_lower.ptr,
        csr.row_upper.ptr,
        csr.start.ptr,
        csr.index.ptr,
        csr.value.ptr,
    ));
    try control.check();
    const seconds = try remainingSeconds(control, absolute_deadline_ns);
    try requireOk(api.set_double_option(handle, "time_limit", seconds));
    const run_status = api.run(handle);
    if (callback_state.failure) |failure| return failure;
    if (run_status != status_ok and run_status != status_warning) return error.HighsRunFailed;

    const status = api.model_status(handle);
    switch (status) {
        model_time_limit => return emptyResult(allocator, .time_limit),
        model_interrupt => return emptyResult(allocator, .interrupted),
        model_infeasible => return emptyResult(allocator, .infeasible),
        model_unbounded, model_unbounded_or_infeasible => return emptyResult(allocator, .unbounded),
        model_iteration_limit => return emptyResult(allocator, .iteration_limit),
        model_optimal => {},
        else => return error.UnsupportedHighsModelStatus,
    }
    try control.check();
    if (control.nowNs() >= absolute_deadline_ns) return error.DeadlineExceeded;

    const col_value = try allocator.alloc(f64, problem.variables);
    errdefer allocator.free(col_value);
    const col_dual = try allocator.alloc(f64, problem.variables);
    defer allocator.free(col_dual);
    const row_value = try allocator.alloc(f64, problem.constraint_rows);
    defer allocator.free(row_value);
    const row_dual = try allocator.alloc(f64, problem.constraint_rows);
    errdefer allocator.free(row_dual);
    try requireOk(api.get_solution(handle, col_value.ptr, col_dual.ptr, row_value.ptr, row_dual.ptr));
    const objective = api.get_objective(handle);
    var iterations: c_int = 0;
    try requireOk(api.get_int_info(handle, "simplex_iteration_count", &iterations));
    if (iterations < 0 or !std.math.isFinite(objective)) return error.InvalidHighsSolution;
    for (col_value) |value| if (!std.math.isFinite(value)) return error.InvalidHighsSolution;
    for (row_dual) |value| if (!std.math.isFinite(value)) return error.InvalidHighsSolution;
    const t = col_value[problem.weight_columns];
    if (!std.math.isFinite(t)) return error.InvalidHighsSolution;
    const weights = try allocator.dupe(f64, col_value[0..problem.weight_columns]);
    allocator.free(col_value);
    return .{
        .allocator = allocator,
        .status = .optimal,
        .weights = weights,
        .t = t,
        .objective = objective,
        .row_duals = row_dual,
        .iterations = @intCast(iterations),
    };
}

const Fake = struct {
    var state: State = .{};

    const State = struct {
        destroyed: usize = 0,
        callback_fn: ?Callback = null,
        callback_data: ?*anyopaque = null,
        callback_started: bool = false,
        run_status: c_int = status_ok,
        model: c_int = model_optimal,
        fail_pass: bool = false,
        invoke_callback: bool = false,
        option_count: usize = 0,
        saw_time_limit: bool = false,
        saw_tolerance: f64 = 0,
        num_col: c_int = 0,
        num_row: c_int = 0,
        num_nz: c_int = 0,
        first_start: c_int = -1,
        second_start: c_int = -1,
        first_index: c_int = -1,
        first_value: f64 = 0,
    };

    fn reset() void {
        state = .{};
    }
    fn create() callconv(.c) ?*anyopaque {
        return &state;
    }
    fn destroy(_: *anyopaque) callconv(.c) void {
        state.destroyed += 1;
    }
    fn versionMajor() callconv(.c) c_int {
        return 1;
    }
    fn versionMinor() callconv(.c) c_int {
        return 8;
    }
    fn versionPatch() callconv(.c) c_int {
        return 0;
    }
    fn intSize(_: *const anyopaque) callconv(.c) c_int {
        return 4;
    }
    fn infinity(_: *const anyopaque) callconv(.c) f64 {
        return 1e30;
    }
    fn boolOption(_: *anyopaque, _: [*:0]const u8, _: c_int) callconv(.c) c_int {
        state.option_count += 1;
        return status_ok;
    }
    fn intOption(_: *anyopaque, _: [*:0]const u8, _: c_int) callconv(.c) c_int {
        state.option_count += 1;
        return status_ok;
    }
    fn doubleOption(_: *anyopaque, name: [*:0]const u8, value: f64) callconv(.c) c_int {
        state.option_count += 1;
        const key = std.mem.span(name);
        if (std.mem.eql(u8, key, "time_limit")) state.saw_time_limit = value > 0;
        if (std.mem.eql(u8, key, "primal_feasibility_tolerance")) state.saw_tolerance = value;
        return status_ok;
    }
    fn stringOption(_: *anyopaque, _: [*:0]const u8, _: [*:0]const u8) callconv(.c) c_int {
        state.option_count += 1;
        return status_ok;
    }
    fn setCallback(_: *anyopaque, cb: Callback, data: ?*anyopaque) callconv(.c) c_int {
        state.callback_fn = cb;
        state.callback_data = data;
        return status_ok;
    }
    fn startCallback(_: *anyopaque, kind: c_int) callconv(.c) c_int {
        state.callback_started = kind == callback_simplex_interrupt;
        return status_ok;
    }
    fn passLp(
        _: *anyopaque,
        num_col: c_int,
        num_row: c_int,
        num_nz: c_int,
        format: c_int,
        sense: c_int,
        _: f64,
        _: [*]const f64,
        _: [*]const f64,
        _: [*]const f64,
        _: [*]const f64,
        _: [*]const f64,
        start: [*]const c_int,
        index: [*]const c_int,
        value: [*]const f64,
    ) callconv(.c) c_int {
        if (state.fail_pass or format != matrix_rowwise or sense != sense_minimize) return status_error;
        state.num_col = num_col;
        state.num_row = num_row;
        state.num_nz = num_nz;
        state.first_start = start[0];
        state.second_start = if (num_row > 1) start[1] else -1;
        state.first_index = index[0];
        state.first_value = value[0];
        return status_ok;
    }
    fn run(_: *anyopaque) callconv(.c) c_int {
        if (state.invoke_callback) {
            var output: CallbackDataOut = std.mem.zeroes(CallbackDataOut);
            var input = CallbackDataIn{ .user_interrupt = 0 };
            state.callback_fn.?(callback_simplex_interrupt, "fixture", &output, &input, state.callback_data);
            if (input.user_interrupt != 0) state.model = model_interrupt;
        }
        return state.run_status;
    }
    fn modelStatus(_: *const anyopaque) callconv(.c) c_int {
        return state.model;
    }
    fn solution(
        _: *const anyopaque,
        col_value: [*]f64,
        col_dual: [*]f64,
        row_value: [*]f64,
        row_dual: [*]f64,
    ) callconv(.c) c_int {
        for (0..@as(usize, @intCast(state.num_col))) |i| {
            col_value[i] = if (i + 1 == @as(usize, @intCast(state.num_col))) 0.25 else 1;
            col_dual[i] = 0;
        }
        for (0..@as(usize, @intCast(state.num_row))) |i| {
            row_value[i] = 1;
            row_dual[i] = -@as(f64, @floatFromInt(i + 1)) / 10;
        }
        return status_ok;
    }
    fn objective(_: *const anyopaque) callconv(.c) f64 {
        return 0.25;
    }
    fn intInfo(_: *const anyopaque, _: [*:0]const u8, value: *c_int) callconv(.c) c_int {
        value.* = 17;
        return status_ok;
    }

    const api = Api{
        .create = create,
        .destroy = destroy,
        .version_major = versionMajor,
        .version_minor = versionMinor,
        .version_patch = versionPatch,
        .sizeof_highs_int = intSize,
        .get_infinity = infinity,
        .set_bool_option = boolOption,
        .set_int_option = intOption,
        .set_double_option = doubleOption,
        .set_string_option = stringOption,
        .set_callback = setCallback,
        .start_callback = startCallback,
        .pass_lp = passLp,
        .run = run,
        .model_status = modelStatus,
        .get_solution = solution,
        .get_objective = objective,
        .get_int_info = intInfo,
    };
};

const SweepArgs = struct {
    problem: *const mass_lp.Problem,
    adapter: *Adapter,
    control: cg.Control,
};

const Clock = struct {
    now: u64 = 1,
    fail: ?anyerror = null,

    fn nowNs(raw: ?*anyopaque) u64 {
        const self: *Clock = @ptrCast(@alignCast(raw.?));
        return self.now;
    }
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *Clock = @ptrCast(@alignCast(raw.?));
        if (self.fail) |failure| return failure;
    }
    fn control(self: *Clock) cg.Control {
        return .{ .context = self, .now_ns_fn = nowNs, .check_fn = check };
    }
};

fn tinyProblem(allocator: std.mem.Allocator) !mass_lp.Problem {
    const matrix = [_]f64{ 1, 2 };
    return mass_lp.buildPrimaryProblem(allocator, &matrix, 1, 2);
}

test "injected v1.8 ABI preserves rowwise coefficients options and raw dual signs" {
    Fake.reset();
    var problem = try tinyProblem(std.testing.allocator);
    defer problem.deinit();
    var clock = Clock{};
    var adapter = Adapter{ .api = &Fake.api };
    var solved = try solveRestricted(&adapter, std.testing.allocator, &problem, std.time.ns_per_s, clock.control(), 1e-9);
    defer solved.deinit();
    try std.testing.expectEqual(cg.SolverStatus.optimal, solved.status);
    try std.testing.expectEqualSlices(f64, &.{ 1, 1 }, solved.weights);
    try std.testing.expectEqual(@as(f64, 0.25), solved.t);
    try std.testing.expectEqualSlices(f64, &.{ -0.1, -0.2 }, solved.row_duals);
    try std.testing.expectEqual(@as(usize, 17), solved.iterations);
    try std.testing.expectEqual(@as(c_int, 3), Fake.state.num_col);
    try std.testing.expectEqual(@as(c_int, 2), Fake.state.num_row);
    try std.testing.expectEqual(@as(c_int, 6), Fake.state.num_nz);
    try std.testing.expectEqual(@as(c_int, 0), Fake.state.first_start);
    try std.testing.expectEqual(@as(c_int, 3), Fake.state.second_start);
    try std.testing.expectEqual(@as(c_int, 0), Fake.state.first_index);
    try std.testing.expectEqual(@as(f64, 1), Fake.state.first_value);
    try std.testing.expect(Fake.state.callback_started and Fake.state.saw_time_limit);
    try std.testing.expectEqual(@as(f64, 1e-9), Fake.state.saw_tolerance);
    try std.testing.expectEqual(@as(usize, 1), Fake.state.destroyed);
}

test "terminal model status returns owned empty slices and destroys instance" {
    Fake.reset();
    Fake.state.model = model_time_limit;
    var problem = try tinyProblem(std.testing.allocator);
    defer problem.deinit();
    var clock = Clock{};
    var adapter = Adapter{ .api = &Fake.api };
    var solved = try solveRestricted(&adapter, std.testing.allocator, &problem, std.time.ns_per_s, clock.control(), 1e-10);
    defer solved.deinit();
    try std.testing.expectEqual(cg.SolverStatus.time_limit, solved.status);
    try std.testing.expectEqual(@as(usize, 0), solved.weights.len);
    try std.testing.expectEqual(@as(usize, 0), solved.row_duals.len);
    try std.testing.expectEqual(@as(usize, 1), Fake.state.destroyed);
}

test "simplex callback returns exact control error after cleanup" {
    Fake.reset();
    Fake.state.invoke_callback = true;
    var problem = try tinyProblem(std.testing.allocator);
    defer problem.deinit();
    var clock = Clock{ .fail = error.Cancelled };
    var adapter = Adapter{ .api = &Fake.api };
    try std.testing.expectError(error.Cancelled, solveRestricted(
        &adapter,
        std.testing.allocator,
        &problem,
        std.time.ns_per_s,
        clock.control(),
        1e-9,
    ));
    try std.testing.expectEqual(@as(usize, 1), Fake.state.destroyed);
}

test "pass failure destroys instance and allocation sweep reaches every adapter allocation" {
    Fake.reset();
    Fake.state.fail_pass = true;
    var problem = try tinyProblem(std.testing.allocator);
    defer problem.deinit();
    var clock = Clock{};
    var adapter = Adapter{ .api = &Fake.api };
    try std.testing.expectError(error.HighsApiFailure, solveRestricted(
        &adapter,
        std.testing.allocator,
        &problem,
        std.time.ns_per_s,
        clock.control(),
        1e-9,
    ));
    try std.testing.expectEqual(@as(usize, 1), Fake.state.destroyed);

    Fake.reset();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator, pair: SweepArgs) !void {
            var solved = try solveRestricted(pair.adapter, allocator, pair.problem, std.time.ns_per_s, pair.control, 1e-9);
            solved.deinit();
        }
    }.run, .{SweepArgs{ .problem = &problem, .adapter = &adapter, .control = clock.control() }});
}
