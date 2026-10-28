// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const Staging = @import("../metadata/restore_staging.zig");

/// Complete an owned authority snapshot after releasing metadata locks. The
/// caller owns response cleanup even when validation or publication rejects it.
pub fn finishAuthority(allocator: std.mem.Allocator, response: *Staging.AuthorityResponse, job: Staging.Job, input: Staging.AuthorityRequest, request: Context) !void {
    try request.ensureExecutionActive();
    if (input.include_plan) response.job_json = try encode(allocator, job, request);
    try response.validate(input);
    try request.ensureExecutionActive();
}

/// Metadata cancellation may inspect this same mutex. Poll only while the
/// caller does not own it; result publication is checked after provider unlock.
/// Retain existing CPU ownership until the complete service-holder graph allows
/// a safe transfer. A yielding OS thread does not lend its runnable grant.
pub fn lock(mutex: *std.atomic.Mutex, request: Context) !void {
    while (true) {
        try request.ensureExecutionActive();
        if (mutex.tryLock()) return;
        @import("antfly_platform").time.yieldNow();
    }
}

test "controlled metadata read mutex polls original lifetime outside owned critical section" {
    const runtime = @import("antfly_runtime_abi");
    const Scope = struct {
        mutex: *std.atomic.Mutex,
        foreign_held: bool = false,
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        release_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.foreign_held) {
                if (!self.mutex.tryLock()) return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
                self.mutex.unlock();
            }
            if (self.checks == self.fail_at) return runtime.error_abi.statusFromError(self.cause);
            if (self.foreign_held and self.checks == self.release_at) {
                self.foreign_held = false;
                self.mutex.unlock();
            }
            return .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var mutex: std.atomic.Mutex = .unlocked;
    var scope: Scope = .{ .mutex = &mutex };
    var team: runtime.runnable_abi.Team = .{ .borrow = .{ .context = &scope, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Scope.checkpoint, .park = Scope.phase, .enter = Scope.phase, .check_lifetime = Scope.lifetime }, .maximum_slots = 1, .allocator = std.testing.allocator };
    const request: Context = .{ .execution_team = &team };
    try lock(&mutex, request);
    try std.testing.expectEqual(@as(usize, 1), scope.checks);
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock();
    try std.testing.expect(mutex.tryLock());
    scope = .{ .mutex = &mutex, .foreign_held = true, .release_at = 3 };
    try lock(&mutex, request);
    try std.testing.expectEqual(@as(usize, 3), scope.checks);
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock();
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        try std.testing.expect(mutex.tryLock());
        scope = .{ .mutex = &mutex, .foreign_held = true, .fail_at = 3, .cause = cause };
        try std.testing.expectError(cause, lock(&mutex, request));
        try std.testing.expectEqual(@as(usize, 3), scope.checks);
        try std.testing.expect(!mutex.tryLock());
        mutex.unlock();
    }
    try lock(&mutex, .{});
    mutex.unlock();
    try std.testing.expectError(error.DeadlineExceeded, lock(&mutex, .{ .deadline_ns = 0 }));
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

/// Encode only after metadata locks and transactions have released ownership.
pub fn encode(allocator: std.mem.Allocator, value: anytype, request: Context) ![]u8 {
    const Adapter = struct {
        request: Context,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.request.ensureExecutionActive();
        }
    };
    var adapter: Adapter = .{ .request = request };
    return @import("antfly_local_sources").sql_cooperative_json_output.valueAlloc(allocator, value, .{}, .{ .ptr = &adapter, .check = Adapter.check });
}

test "controlled metadata read mutex owned job encoding preserves bytes and unlocked callbacks" {
    const staging = @import("../metadata/restore_staging.zig");
    const runtime = @import("antfly_runtime_abi");
    const data: [8193]u8 = @splat('x');
    const job: staging.Job = .{ .plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{}, .source_location = &data }, .plan_digest = @splat(3) };
    const expected = try std.json.Stringify.valueAlloc(std.testing.allocator, job, .{});
    defer std.testing.allocator.free(expected);
    const Scope = struct {
        mutex: *std.atomic.Mutex,
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.mutex.tryLock()) return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
            self.mutex.unlock();
            return if (self.checks == self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var mutex: std.atomic.Mutex = .unlocked;
    var scope: Scope = .{ .mutex = &mutex };
    var team: runtime.runnable_abi.Team = .{ .borrow = .{ .context = &scope, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Scope.checkpoint, .park = Scope.phase, .enter = Scope.phase, .check_lifetime = Scope.lifetime }, .maximum_slots = 1, .allocator = std.testing.allocator };
    const request: Context = .{ .execution_team = &team };
    const actual = try encode(std.testing.allocator, job, request);
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
    const checks = scope.checks;
    try std.testing.expect(checks > 10);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            scope = .{ .mutex = &mutex, .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, encode(std.testing.allocator, job, request));
        }
    }
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator, value: staging.Job) !void {
            const encoded = try encode(allocator, value, .{});
            defer allocator.free(encoded);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{job});

    const AuthorityFixture = struct {
        fn response() staging.AuthorityResponse {
            return .{
                .node_id = 7,
                .plan_id = @splat(1),
                .metadata_group_id = 9,
                .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
                .metadata_epoch = 42,
                .progress = .{ .revision = 11, .state = .activating },
                .receipt = @splat(4),
            };
        }
        fn run(allocator: std.mem.Allocator, value: staging.Job, input: staging.AuthorityRequest, context: Context) !void {
            var result = response();
            defer result.deinit(allocator);
            try finishAuthority(allocator, &result, value, input, context);
        }
    };
    const input: staging.AuthorityRequest = .{ .node_id = 7, .plan_id = @splat(1), .owner_group = 13, .include_plan = true, .receipt = .{ .state = .activating, .owner_group = 13 } };
    scope = .{ .mutex = &mutex };
    var response = AuthorityFixture.response();
    defer response.deinit(std.testing.allocator);
    try finishAuthority(std.testing.allocator, &response, job, input, request);
    try std.testing.expectEqualSlices(u8, expected, response.job_json.?);
    var legacy = AuthorityFixture.response();
    legacy.job_json = expected;
    const legacy_bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, legacy, .{});
    defer std.testing.allocator.free(legacy_bytes);
    const response_bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, response, .{});
    defer std.testing.allocator.free(response_bytes);
    try std.testing.expectEqualSlices(u8, legacy_bytes, response_bytes);
    const authority_checks = scope.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..authority_checks + 1) |fail_at| {
            scope = .{ .mutex = &mutex, .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, AuthorityFixture.run(std.testing.allocator, job, input, request));
        }
    }
    scope = .{ .mutex = &mutex };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, AuthorityFixture.run, .{ job, input, request });
    // Fixed-size authority replies retain the final lifetime check without
    // creating JSON storage; invalid publication retires any encoded plan.
    var without_plan = input;
    without_plan.include_plan = false;
    scope = .{ .mutex = &mutex };
    var fixed = AuthorityFixture.response();
    defer fixed.deinit(std.testing.allocator);
    try finishAuthority(std.testing.allocator, &fixed, job, without_plan, request);
    try std.testing.expect(fixed.job_json == null);
    try std.testing.expectEqual(@as(usize, 2), scope.checks);
    scope = .{ .mutex = &mutex, .cause = error.SchedulerClosed, .fail_at = 2 };
    try std.testing.expectError(error.SchedulerClosed, AuthorityFixture.run(std.testing.allocator, job, without_plan, request));
    scope = .{ .mutex = &mutex };
    var invalid = AuthorityFixture.response();
    defer invalid.deinit(std.testing.allocator);
    invalid.progress = null;
    try std.testing.expectError(error.InvalidRestoreStaging, finishAuthority(std.testing.allocator, &invalid, job, input, request));
}
