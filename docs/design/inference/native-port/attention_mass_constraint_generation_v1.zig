//! Unapplied, solver-neutral draft of the frozen attention-mass constraint
//! generation controller. This file deliberately lives outside canonical
//! runtime sources. It does not select, link, or authorize an LP solver.
//!
//! The `attention_mass_lp` named module must be the separately reviewed V4
//! domain implementation. A future build/review must pin that module's bytes.
const std = @import("std");
const mass_lp = @import("attention_mass_lp");

pub const seed_rows: usize = 512;
pub const add_rows: usize = 256;
pub const max_rounds: usize = 12;
pub const original_feasibility_tolerance: f64 = 1e-9;
pub const retry_feasibility_tolerance: f64 = 1e-10;
const scan_checkpoint_rows: usize = 64;

pub const TerminalReason = enum {
    deadline_exhausted,
    solver_time_limit,
    solver_interrupted,
    solver_infeasible,
    solver_unbounded,
    solver_iteration_limit,
    full_witness_rejected,
    stored_conversion_rejected,
    maximum_rounds_exhausted,
};

pub const Control = struct {
    context: ?*anyopaque = null,
    now_ns_fn: *const fn (?*anyopaque) u64,
    check_fn: *const fn (?*anyopaque) anyerror!void,

    pub fn nowNs(self: Control) u64 {
        return self.now_ns_fn(self.context);
    }

    pub fn check(self: Control) !void {
        try self.check_fn(self.context);
    }
};

pub const SolverStatus = enum {
    optimal,
    time_limit,
    interrupted,
    infeasible,
    unbounded,
    iteration_limit,
};

/// The callback returns raw HiGHS-style row duals in the exact restricted LP
/// order: all upper rows followed by all lower rows. The controller performs
/// `alpha=-upper_dual`, `beta=-lower_dual` and independently audits them.
pub const RestrictedSolve = struct {
    allocator: std.mem.Allocator,
    status: SolverStatus,
    weights: []f64,
    t: f64,
    objective: f64,
    row_duals: []f64,
    iterations: usize,

    pub fn deinit(self: *RestrictedSolve) void {
        self.allocator.free(self.weights);
        self.allocator.free(self.row_duals);
        self.* = undefined;
    }
};

pub const SolveRestrictedFn = *const fn (
    context: ?*anyopaque,
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    absolute_deadline_ns: u64,
    control: Control,
    feasibility_tolerance: f64,
) anyerror!RestrictedSolve;

pub const Solver = struct {
    context: ?*anyopaque = null,
    solve_restricted_fn: SolveRestrictedFn,

    pub fn solve(
        self: Solver,
        allocator: std.mem.Allocator,
        problem: *const mass_lp.Problem,
        absolute_deadline_ns: u64,
        control: Control,
        feasibility_tolerance: f64,
    ) !RestrictedSolve {
        return self.solve_restricted_fn(
            self.context,
            allocator,
            problem,
            absolute_deadline_ns,
            control,
            feasibility_tolerance,
        );
    }
};

pub const Round = struct {
    round: usize,
    subset_rows: usize,
    iterations: usize,
    cumulative_iterations: usize,
    objective: f64,
    worst_full_violation: f64,
    added_rows: usize,
    retried_witness: bool,
};

pub const Diagnostics = struct {
    rounds: [max_rounds]Round = undefined,
    round_count: usize = 0,
    total_iterations: usize = 0,
    full_rows: usize,
    selected_rows: usize,
    warm_hint_rows: usize,
    initial_selected_rows: usize,
};

pub const Accepted = struct {
    allocator: std.mem.Allocator,
    raw_weights: []f64,
    bias: []f32,
    stored_weights: []f64,
    mass: []f64,
    alpha: []f64,
    beta: []f64,
    selected_row_indices: []usize,
    objective: f64,
    diagnostics: Diagnostics,

    pub fn deinit(self: *Accepted) void {
        self.allocator.free(self.raw_weights);
        self.allocator.free(self.bias);
        self.allocator.free(self.stored_weights);
        self.allocator.free(self.mass);
        self.allocator.free(self.alpha);
        self.allocator.free(self.beta);
        self.allocator.free(self.selected_row_indices);
        self.* = undefined;
    }
};

pub const Terminal = struct {
    reason: TerminalReason,
    diagnostics: Diagnostics,
};

pub const Result = union(enum) {
    accepted: Accepted,
    terminal: Terminal,

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            .accepted => |*accepted| accepted.deinit(),
            .terminal => {},
        }
        self.* = undefined;
    }
};

const Violation = struct {
    amount: f64,
    row: usize,
    sign: enum { upper, lower },
};

fn checkedCount(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch return error.InvalidShape;
}

fn checkpoint(control: Control, deadline_ns: u64, last_now_ns: *u64) !void {
    try control.check();
    const now = control.nowNs();
    if (now < last_now_ns.*) return error.ClockRegression;
    last_now_ns.* = now;
    if (now >= deadline_ns) return error.Timeout;
}

fn lessViolation(_: void, left: Violation, right: Violation) bool {
    if (left.amount != right.amount) return left.amount > right.amount;
    if (left.row != right.row) return left.row < right.row;
    return @intFromEnum(left.sign) < @intFromEnum(right.sign);
}

fn seedSelection(selected: []bool) !void {
    const count = @min(seed_rows, selected.len);
    for (0..count) |index| {
        const numerator = try checkedCount(index, selected.len);
        selected[numerator / count] = true;
    }
}

fn validateWarm(selected: []bool, warm: ?[]const usize) !usize {
    const hints = warm orelse return 0;
    if (hints.len > selected.len) return error.InvalidWarmRows;
    for (hints, 0..) |row, index| {
        if (row >= selected.len) return error.InvalidWarmRows;
        for (hints[0..index]) |prior| if (prior == row) return error.InvalidWarmRows;
        selected[row] = true;
    }
    return hints.len;
}

fn writeSelected(selected: []const bool, output: []usize) !usize {
    var count: usize = 0;
    for (selected, 0..) |is_selected, row| {
        if (!is_selected) continue;
        if (count >= output.len) return error.InvalidShape;
        output[count] = row;
        count += 1;
    }
    return count;
}

fn gatherRows(
    full: []const f64,
    columns: usize,
    indices: []const usize,
    output: []f64,
) !void {
    if (output.len != try checkedCount(indices.len, columns)) return error.InvalidShape;
    for (indices, 0..) |source_row, destination_row| {
        const source_start = try checkedCount(source_row, columns);
        const destination_start = try checkedCount(destination_row, columns);
        @memcpy(
            output[destination_start .. destination_start + columns],
            full[source_start .. source_start + columns],
        );
    }
}

fn restrictedWitness(
    restricted_matrix: []const f64,
    restricted_rows: usize,
    columns: usize,
    solved: *const RestrictedSolve,
    allocator: std.mem.Allocator,
) !void {
    if (solved.status != .optimal or solved.weights.len != columns or
        solved.row_duals.len != try checkedCount(2, restricted_rows) or
        !std.math.isFinite(solved.t) or !std.math.isFinite(solved.objective))
        return error.InvalidSolverResult;
    const alpha = try allocator.alloc(f64, restricted_rows);
    defer allocator.free(alpha);
    const beta = try allocator.alloc(f64, restricted_rows);
    defer allocator.free(beta);
    for (0..restricted_rows) |row| {
        const upper = solved.row_duals[row];
        const lower = solved.row_duals[restricted_rows + row];
        if (!std.math.isFinite(upper) or !std.math.isFinite(lower))
            return error.InvalidSolverResult;
        alpha[row] = -upper;
        beta[row] = -lower;
    }
    _ = try mass_lp.auditPrimaryWitness(
        restricted_matrix,
        restricted_rows,
        columns,
        .{
            .weights = solved.weights,
            .t = solved.t,
            .objective = solved.objective,
            .alpha = alpha,
            .beta = beta,
        },
    );
}

fn terminalFromStatus(status: SolverStatus, diagnostics: Diagnostics) !Result {
    return switch (status) {
        .optimal => error.InvalidSolverResult,
        .time_limit => .{ .terminal = .{ .reason = .solver_time_limit, .diagnostics = diagnostics } },
        .interrupted => .{ .terminal = .{ .reason = .solver_interrupted, .diagnostics = diagnostics } },
        .infeasible => .{ .terminal = .{ .reason = .solver_infeasible, .diagnostics = diagnostics } },
        .unbounded => .{ .terminal = .{ .reason = .solver_unbounded, .diagnostics = diagnostics } },
        .iteration_limit => .{ .terminal = .{ .reason = .solver_iteration_limit, .diagnostics = diagnostics } },
    };
}

fn solveAndAuditRestricted(
    allocator: std.mem.Allocator,
    problem: *const mass_lp.Problem,
    restricted_matrix: []const f64,
    restricted_rows: usize,
    columns: usize,
    deadline_ns: u64,
    control: Control,
    solver: Solver,
    last_now_ns: *u64,
    retried: *bool,
) !RestrictedSolve {
    try checkpoint(control, deadline_ns, last_now_ns);
    var solved = try solver.solve(
        allocator,
        problem,
        deadline_ns,
        control,
        original_feasibility_tolerance,
    );
    var solved_owned = true;
    errdefer if (solved_owned) solved.deinit();
    try checkpoint(control, deadline_ns, last_now_ns);
    if (solved.status != .optimal) {
        solved_owned = false;
        return solved;
    }
    restrictedWitness(restricted_matrix, restricted_rows, columns, &solved, allocator) catch |err| {
        if (err != error.WitnessRejected) return err;
        solved.deinit();
        solved_owned = false;
        try checkpoint(control, deadline_ns, last_now_ns);
        retried.* = true;
        solved = try solver.solve(
            allocator,
            problem,
            deadline_ns,
            control,
            retry_feasibility_tolerance,
        );
        solved_owned = true;
        try checkpoint(control, deadline_ns, last_now_ns);
        if (solved.status != .optimal) {
            solved_owned = false;
            return solved;
        }
        try restrictedWitness(restricted_matrix, restricted_rows, columns, &solved, allocator);
    };
    solved_owned = false;
    return solved;
}

fn scanViolations(
    allocator: std.mem.Allocator,
    matrix: []const f64,
    rows: usize,
    columns: usize,
    weights: []const f64,
    t: f64,
    tolerance: f64,
    selected: []bool,
    deadline_ns: u64,
    control: Control,
    last_now_ns: *u64,
) !struct { added: usize, worst: f64 } {
    if (weights.len != columns or selected.len != rows or !std.math.isFinite(t))
        return error.InvalidSolverResult;
    const capacity = try checkedCount(2, rows);
    const candidates = try allocator.alloc(Violation, capacity);
    defer allocator.free(candidates);
    var count: usize = 0;
    var worst: f64 = 0;
    for (0..rows) |row| {
        if (row % scan_checkpoint_rows == 0)
            try checkpoint(control, deadline_ns, last_now_ns);
        var fitted: f64 = 0;
        for (0..columns) |column| {
            const product = matrix[row * columns + column] * weights[column];
            if (!std.math.isFinite(product)) return error.NonFiniteResult;
            fitted += product;
            if (!std.math.isFinite(fitted)) return error.NonFiniteResult;
        }
        const upper = fitted - 1 - t;
        const lower = 1 - fitted - t;
        if (!std.math.isFinite(upper) or !std.math.isFinite(lower))
            return error.NonFiniteResult;
        worst = @max(worst, @max(upper, lower));
        if (selected[row]) continue;
        if (upper > tolerance) {
            candidates[count] = .{ .amount = upper, .row = row, .sign = .upper };
            count += 1;
        }
        if (lower > tolerance) {
            candidates[count] = .{ .amount = lower, .row = row, .sign = .lower };
            count += 1;
        }
    }
    std.mem.sort(Violation, candidates[0..count], {}, lessViolation);
    var added: usize = 0;
    for (candidates[0..count]) |candidate| {
        if (selected[candidate.row]) continue;
        selected[candidate.row] = true;
        added += 1;
        if (added == add_rows) break;
    }
    return .{ .added = added, .worst = @max(0, worst) };
}

fn finiteMatrix(matrix: []const f64, rows: usize, columns: usize) !void {
    if (rows == 0 or columns == 0 or matrix.len != try checkedCount(rows, columns))
        return error.InvalidShape;
    for (matrix) |value| if (!std.math.isFinite(value)) return error.NonFiniteInput;
}

fn fullWitness(
    matrix: []const f64,
    rows: usize,
    columns: usize,
    indices: []const usize,
    solved: *const RestrictedSolve,
    alpha: []f64,
    beta: []f64,
) !void {
    if (alpha.len != rows or beta.len != rows or solved.row_duals.len != try checkedCount(2, indices.len))
        return error.InvalidSolverResult;
    @memset(alpha, 0);
    @memset(beta, 0);
    for (indices, 0..) |full_row, restricted_row| {
        alpha[full_row] = -solved.row_duals[restricted_row];
        beta[full_row] = -solved.row_duals[indices.len + restricted_row];
    }
    _ = mass_lp.auditPrimaryWitness(
        matrix,
        rows,
        columns,
        .{
            .weights = solved.weights,
            .t = solved.t,
            .objective = solved.objective,
            .alpha = alpha,
            .beta = beta,
        },
    ) catch |err| switch (err) {
        error.WitnessRejected => return error.FullWitnessRejected,
        else => return err,
    };
}

fn materializeStored(
    matrix: []const f64,
    rows: usize,
    columns: usize,
    raw: []const f64,
    bias: []f32,
    stored: []f64,
    mass: []f64,
    deadline_ns: u64,
    control: Control,
    last_now_ns: *u64,
) !void {
    if (raw.len != columns or bias.len != columns or stored.len != columns or mass.len != rows)
        return error.InvalidShape;
    for (raw, 0..) |weight, column| {
        if (!std.math.isFinite(weight) or weight <= 0) return error.StoredConversionRejected;
        const stored_bias: f32 = @floatCast(@log(weight));
        const stored_weight = @exp(@as(f64, stored_bias));
        if (!std.math.isFinite(stored_bias) or !std.math.isFinite(stored_weight) or stored_weight <= 0)
            return error.StoredConversionRejected;
        bias[column] = stored_bias;
        stored[column] = stored_weight;
    }
    for (0..rows) |row| {
        if (row % scan_checkpoint_rows == 0)
            try checkpoint(control, deadline_ns, last_now_ns);
        var value: f64 = 0;
        for (0..columns) |column| {
            const product = matrix[row * columns + column] * stored[column];
            if (!std.math.isFinite(product)) return error.StoredConversionRejected;
            value += product;
            if (!std.math.isFinite(value)) return error.StoredConversionRejected;
        }
        if (value <= 0) return error.StoredConversionRejected;
        mass[row] = value;
    }
}

pub fn solvePrimaryConstraintGeneration(
    allocator: std.mem.Allocator,
    matrix: []const f64,
    rows: usize,
    columns: usize,
    warm_rows: ?[]const usize,
    absolute_deadline_ns: u64,
    control: Control,
    solver: Solver,
) !Result {
    try finiteMatrix(matrix, rows, columns);
    var last_now_ns = control.nowNs();
    if (last_now_ns >= absolute_deadline_ns) return .{ .terminal = .{
        .reason = .deadline_exhausted,
        .diagnostics = .{ .full_rows = rows, .selected_rows = 0, .warm_hint_rows = 0, .initial_selected_rows = 0 },
    } };
    try checkpoint(control, absolute_deadline_ns, &last_now_ns);

    const selected = try allocator.alloc(bool, rows);
    defer allocator.free(selected);
    @memset(selected, false);
    try seedSelection(selected);
    const warm_count = try validateWarm(selected, warm_rows);
    const indices_storage = try allocator.alloc(usize, rows);
    defer allocator.free(indices_storage);
    var selected_count = try writeSelected(selected, indices_storage);
    var diagnostics = Diagnostics{
        .full_rows = rows,
        .selected_rows = selected_count,
        .warm_hint_rows = warm_count,
        .initial_selected_rows = selected_count,
    };
    var restricted_storage = try allocator.alloc(f64, try checkedCount(rows, columns));
    defer allocator.free(restricted_storage);

    for (0..max_rounds) |round_index| {
        try checkpoint(control, absolute_deadline_ns, &last_now_ns);
        const indices = indices_storage[0..selected_count];
        const restricted = restricted_storage[0 .. try checkedCount(selected_count, columns)];
        try gatherRows(matrix, columns, indices, restricted);
        var problem = try mass_lp.buildPrimaryProblem(allocator, restricted, selected_count, columns);
        var retried = false;
        var solved = solveAndAuditRestricted(
            allocator,
            &problem,
            restricted,
            selected_count,
            columns,
            absolute_deadline_ns,
            control,
            solver,
            &last_now_ns,
            &retried,
        ) catch |err| {
            problem.deinit();
            if (err == error.Timeout) return .{ .terminal = .{ .reason = .deadline_exhausted, .diagnostics = diagnostics } };
            return err;
        };
        problem.deinit();
        defer solved.deinit();
        if (solved.status != .optimal)
            return terminalFromStatus(solved.status, diagnostics);

        diagnostics.total_iterations = std.math.add(usize, diagnostics.total_iterations, solved.iterations) catch
            return error.IntegerOverflow;
        const scanned = scanViolations(
            allocator,
            matrix,
            rows,
            columns,
            solved.weights,
            solved.t,
            mass_lp.witness_rel_tol * @max(1, maxAbs(matrix)),
            selected,
            absolute_deadline_ns,
            control,
            &last_now_ns,
        ) catch |err| {
            if (err == error.Timeout) return .{ .terminal = .{ .reason = .deadline_exhausted, .diagnostics = diagnostics } };
            return err;
        };
        diagnostics.rounds[round_index] = .{
            .round = round_index,
            .subset_rows = selected_count,
            .iterations = solved.iterations,
            .cumulative_iterations = diagnostics.total_iterations,
            .objective = solved.objective,
            .worst_full_violation = scanned.worst,
            .added_rows = scanned.added,
            .retried_witness = retried,
        };
        diagnostics.round_count = round_index + 1;
        if (scanned.added != 0) {
            selected_count = try writeSelected(selected, indices_storage);
            diagnostics.selected_rows = selected_count;
            continue;
        }

        const alpha = try allocator.alloc(f64, rows);
        errdefer allocator.free(alpha);
        const beta = try allocator.alloc(f64, rows);
        errdefer allocator.free(beta);
        fullWitness(matrix, rows, columns, indices, &solved, alpha, beta) catch |err| switch (err) {
            error.FullWitnessRejected => {
                allocator.free(alpha);
                allocator.free(beta);
                return .{ .terminal = .{ .reason = .full_witness_rejected, .diagnostics = diagnostics } };
            },
            else => return err,
        };
        try checkpoint(control, absolute_deadline_ns, &last_now_ns);
        const raw = try allocator.dupe(f64, solved.weights);
        errdefer allocator.free(raw);
        const bias = try allocator.alloc(f32, columns);
        errdefer allocator.free(bias);
        const stored = try allocator.alloc(f64, columns);
        errdefer allocator.free(stored);
        const mass = try allocator.alloc(f64, rows);
        errdefer allocator.free(mass);
        materializeStored(matrix, rows, columns, raw, bias, stored, mass, absolute_deadline_ns, control, &last_now_ns) catch |err| switch (err) {
            error.Timeout, error.StoredConversionRejected => {
                allocator.free(alpha);
                allocator.free(beta);
                allocator.free(raw);
                allocator.free(bias);
                allocator.free(stored);
                allocator.free(mass);
                return .{ .terminal = .{
                    .reason = if (err == error.Timeout) .deadline_exhausted else .stored_conversion_rejected,
                    .diagnostics = diagnostics,
                } };
            },
            else => return err,
        };
        const accepted_indices = try allocator.dupe(usize, indices);
        errdefer allocator.free(accepted_indices);
        try checkpoint(control, absolute_deadline_ns, &last_now_ns);
        return .{ .accepted = .{
            .allocator = allocator,
            .raw_weights = raw,
            .bias = bias,
            .stored_weights = stored,
            .mass = mass,
            .alpha = alpha,
            .beta = beta,
            .selected_row_indices = accepted_indices,
            .objective = solved.objective,
            .diagnostics = diagnostics,
        } };
    }
    return .{ .terminal = .{ .reason = .maximum_rounds_exhausted, .diagnostics = diagnostics } };
}

fn maxAbs(values: []const f64) f64 {
    var result: f64 = 0;
    for (values) |value| result = @max(result, @abs(value));
    return result;
}

const TestClock = struct {
    now_ns: u64 = 1,
    checks: usize = 0,
    cancel_at: ?usize = null,

    fn now(raw: ?*anyopaque) u64 {
        const self: *TestClock = @ptrCast(@alignCast(raw.?));
        return self.now_ns;
    }

    fn check(raw: ?*anyopaque) !void {
        const self: *TestClock = @ptrCast(@alignCast(raw.?));
        self.checks += 1;
        if (self.cancel_at) |at| {
            if (self.checks >= at) return error.Cancelled;
        }
    }

    fn control(self: *TestClock) Control {
        return .{ .context = self, .now_ns_fn = now, .check_fn = check };
    }
};

const FakeSolver = struct {
    calls: usize = 0,
    tolerances: [4]f64 = undefined,
    reject_first_witness: bool = false,
    terminal_status: ?SolverStatus = null,

    fn solve(
        raw: ?*anyopaque,
        allocator: std.mem.Allocator,
        problem: *const mass_lp.Problem,
        _: u64,
        control: Control,
        tolerance: f64,
    ) !RestrictedSolve {
        const self: *FakeSolver = @ptrCast(@alignCast(raw.?));
        try control.check();
        self.tolerances[self.calls] = tolerance;
        self.calls += 1;
        if (self.terminal_status) |status| return .{
            .allocator = allocator,
            .status = status,
            .weights = try allocator.alloc(f64, 0),
            .t = 0,
            .objective = 0,
            .row_duals = try allocator.alloc(f64, 0),
            .iterations = 0,
        };
        const rows = problem.source_rows;
        const columns = problem.weight_columns;
        const weights = try allocator.alloc(f64, columns);
        errdefer allocator.free(weights);
        @memset(weights, 1);
        if (self.reject_first_witness and self.calls == 1) weights[0] = -1;
        const duals = try allocator.alloc(f64, try checkedCount(2, rows));
        @memset(duals, 0);
        return .{
            .allocator = allocator,
            .status = .optimal,
            .weights = weights,
            .t = 0,
            .objective = 0,
            .row_duals = duals,
            .iterations = 1,
        };
    }

    fn interface(self: *FakeSolver) Solver {
        return .{ .context = self, .solve_restricted_fn = solve };
    }
};

test "seed formula and violation comparator preserve frozen order" {
    var selected = [_]bool{false} ** 600;
    try seedSelection(&selected);
    var count: usize = 0;
    for (selected) |value| {
        if (value) count += 1;
    }
    try std.testing.expectEqual(seed_rows, count);
    try std.testing.expect(selected[0]);
    try std.testing.expect(selected[5]);
    try std.testing.expect(!selected[6]);
    try std.testing.expect(selected[7]);
    try std.testing.expect(selected[598]);
    try std.testing.expect(!selected[599]);

    var violations = [_]Violation{
        .{ .amount = 2, .row = 8, .sign = .lower },
        .{ .amount = 3, .row = 9, .sign = .upper },
        .{ .amount = 2, .row = 7, .sign = .lower },
        .{ .amount = 2, .row = 7, .sign = .upper },
    };
    std.mem.sort(Violation, &violations, {}, lessViolation);
    try std.testing.expectEqual(@as(usize, 9), violations[0].row);
    try std.testing.expectEqual(@as(usize, 7), violations[1].row);
    try std.testing.expectEqual(.upper, violations[1].sign);
    try std.testing.expectEqual(.lower, violations[2].sign);
    try std.testing.expectEqual(@as(usize, 8), violations[3].row);
}

test "accepted fake solve owns and releases exact result arrays" {
    var clock = TestClock{};
    var fake = FakeSolver{};
    const matrix = [_]f64{ 1, 1, 1, 1 };
    var result = try solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        4,
        1,
        null,
        1000,
        clock.control(),
        fake.interface(),
    );
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    switch (result) {
        .accepted => |accepted| {
            try std.testing.expectEqualSlices(f64, &.{1}, accepted.raw_weights);
            try std.testing.expectEqualSlices(f32, &.{0}, accepted.bias);
            try std.testing.expectEqualSlices(f64, &.{ 1, 1, 1, 1 }, accepted.mass);
            try std.testing.expectEqual(@as(usize, 4), accepted.selected_row_indices.len);
        },
        .terminal => return error.UnexpectedTerminal,
    }
}

test "restricted witness rejection retries once at tighter tolerance" {
    var clock = TestClock{};
    var fake = FakeSolver{ .reject_first_witness = true };
    const matrix = [_]f64{ 1, 1 };
    var result = try solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        2,
        1,
        null,
        1000,
        clock.control(),
        fake.interface(),
    );
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(original_feasibility_tolerance, fake.tolerances[0]);
    try std.testing.expectEqual(retry_feasibility_tolerance, fake.tolerances[1]);
    switch (result) {
        .accepted => |accepted| try std.testing.expect(accepted.diagnostics.rounds[0].retried_witness),
        .terminal => return error.UnexpectedTerminal,
    }
}

test "solver terminal status returns no candidate allocations" {
    var clock = TestClock{};
    var fake = FakeSolver{ .terminal_status = .time_limit };
    const matrix = [_]f64{1};
    var result = try solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        1,
        1,
        null,
        1000,
        clock.control(),
        fake.interface(),
    );
    defer result.deinit();
    switch (result) {
        .terminal => |terminal| try std.testing.expectEqual(TerminalReason.solver_time_limit, terminal.reason),
        .accepted => return error.UnexpectedAccepted,
    }
}

test "cancellation propagates and allocator owns no leaked candidate" {
    // The fake solver allocates its result at check 4. Cancellation at check 5
    // exercises cleanup after callback return rather than only pre-solve exit.
    var clock = TestClock{ .cancel_at = 5 };
    var fake = FakeSolver{};
    const matrix = [_]f64{ 1, 1, 1, 1 };
    try std.testing.expectError(error.Cancelled, solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        4,
        1,
        null,
        1000,
        clock.control(),
        fake.interface(),
    ));
}

test "deadline crossing after solver result frees callback ownership" {
    const AdvancingClock = struct {
        now_ns: u64 = 0,
        fn now(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.now_ns += 1;
            return self.now_ns;
        }
        fn check(_: ?*anyopaque) !void {}
    };
    var clock = AdvancingClock{};
    var fake = FakeSolver{};
    const control = Control{ .context = &clock, .now_ns_fn = AdvancingClock.now, .check_fn = AdvancingClock.check };
    const matrix = [_]f64{ 1, 1 };
    var result = try solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        2,
        1,
        null,
        5,
        control,
        fake.interface(),
    );
    defer result.deinit();
    switch (result) {
        .terminal => |terminal| try std.testing.expectEqual(TerminalReason.deadline_exhausted, terminal.reason),
        .accepted => return error.UnexpectedAccepted,
    }
}

test "warm rows reject duplicates and clock regression fails closed" {
    var clock = TestClock{};
    var fake = FakeSolver{};
    const matrix = [_]f64{ 1, 1 };
    try std.testing.expectError(error.InvalidWarmRows, solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        2,
        1,
        &.{ 1, 1 },
        1000,
        clock.control(),
        fake.interface(),
    ));
    const Regressing = struct {
        calls: usize = 0,
        fn now(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return if (self.calls == 1) 10 else 9;
        }
        fn check(_: ?*anyopaque) !void {}
    };
    var regressing = Regressing{};
    const control = Control{ .context = &regressing, .now_ns_fn = Regressing.now, .check_fn = Regressing.check };
    try std.testing.expectError(error.ClockRegression, solvePrimaryConstraintGeneration(
        std.testing.allocator,
        &matrix,
        2,
        1,
        null,
        1000,
        control,
        fake.interface(),
    ));
}

test "violation selection is stable and adds each original row once" {
    var clock = TestClock{};
    var last: u64 = 1;
    const matrix = [_]f64{ 1, 3, 3, -1 };
    var selected = [_]bool{ true, false, false, false };
    const scan = try scanViolations(
        std.testing.allocator,
        &matrix,
        4,
        1,
        &.{1},
        0,
        0.5,
        &selected,
        1000,
        clock.control(),
        &last,
    );
    try std.testing.expectEqual(@as(usize, 3), scan.added);
    try std.testing.expect(selected[1] and selected[2] and selected[3]);
    try std.testing.expectEqual(@as(f64, 2), scan.worst);
}
