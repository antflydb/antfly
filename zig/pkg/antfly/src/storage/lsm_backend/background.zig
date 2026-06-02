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
const background_runtime = @import("../background_runtime.zig");

pub const JobClass = background_runtime.Job.Class;
pub const JobRunFn = *const fn (ptr: *anyopaque) anyerror!void;
pub const JobDeinitFn = *const fn (ptr: *anyopaque) void;

pub const Executor = struct {
    jobs: ?background_runtime.DurableJobLane = null,
    owner_id: u64,
    detached: bool = false,

    pub fn init(runtime: *background_runtime.BackendRuntime, owner_id: u64) Executor {
        return .{
            .jobs = runtime.durable_jobs,
            .owner_id = owner_id,
            .detached = runtime.backend != .manual,
        };
    }

    pub fn initLane(jobs: background_runtime.DurableJobLane, owner_id: u64) Executor {
        return .{
            .jobs = jobs,
            .owner_id = owner_id,
            .detached = true,
        };
    }

    pub fn initInline(owner_id: u64) Executor {
        return .{ .owner_id = owner_id };
    }

    pub fn canRunDetached(self: Executor) bool {
        return self.detached;
    }

    pub fn submit(
        self: Executor,
        class: JobClass,
        ptr: *anyopaque,
        run: JobRunFn,
        deinit: JobDeinitFn,
    ) !void {
        const job: background_runtime.Job = .{
            .owner_id = self.owner_id,
            .class = class,
            .ptr = ptr,
            .run = run,
            .deinit = deinit,
        };
        if (self.jobs) |jobs| {
            try jobs.submit(job);
            return;
        }
        defer job.deinit(job.ptr);
        try job.run(job.ptr);
    }

    pub fn drain(self: Executor) void {
        if (self.jobs) |jobs| jobs.closeOwner(self.owner_id);
    }

    pub fn poll(self: Executor, max_jobs: usize) !usize {
        return if (self.jobs) |jobs| try jobs.poll(max_jobs) else 0;
    }
};

test "lsm background executor submits jobs with backend owner id" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    var ctx = Ctx{};
    const executor = Executor.init(handle.ptr(), 42);
    try executor.submit(.commit_durable, &ctx, Fns.run, Fns.deinit);

    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "lsm background executor drains by backend owner id" {
    const FakeLane = struct {
        drained_owner: ?u64 = null,
        polled_max_jobs: ?usize = null,

        fn lane(self: *@This()) background_runtime.DurableJobLane {
            return .{
                .ptr = self,
                .vtable = &vtable,
            };
        }

        fn submit(_: *anyopaque, _: background_runtime.Job) !void {}

        fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.drained_owner = owner_id;
        }

        fn poll(ptr: *anyopaque, max_jobs: usize) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.polled_max_jobs = max_jobs;
            return 0;
        }

        const vtable = background_runtime.DurableJobLane.VTable{
            .submit = submit,
            .drain_owner = drainOwner,
            .close_owner = drainOwner,
            .poll = poll,
        };
    };

    var lane = FakeLane{};
    const executor = Executor.initLane(lane.lane(), 99);

    executor.drain();
    try std.testing.expectEqual(@as(?u64, 99), lane.drained_owner);
    try std.testing.expectEqual(@as(usize, 0), try executor.poll(8));
    try std.testing.expectEqual(@as(?usize, 8), lane.polled_max_jobs);
}
