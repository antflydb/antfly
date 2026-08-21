// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Typed storage-maintenance operations shared by HTTP and in-process callers.

const std = @import("std");
const operation = @import("operation.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const maintenance = @import("../storage/maintenance.zig");

pub const Error = operation.ApiError || error{
    MaintenanceBusy,
    IdempotencyConflict,
    InvalidIdempotencyKey,
    MaintenanceHistoryFull,
    MaintenanceJobIdExhausted,
    OperationUnsupported,
};

pub const StartInput = struct {
    operation: maintenance.Operation,
    idempotency_key: ?[]const u8 = null,
};

pub const JobInput = struct {
    job_id: u64,
};

pub const Operations = struct {
    coordinator: ?*maintenance.Coordinator,

    pub fn init(coordinator: ?*maintenance.Coordinator) Operations {
        return .{ .coordinator = coordinator };
    }

    pub fn start(
        self: Operations,
        request: operation.RequestContext,
        input: StartInput,
    ) Error!maintenance.Coordinator.Snapshot {
        try request.ensureActive();
        const coordinator = self.coordinator orelse return error.Unsupported;
        const capabilities = coordinator.status().maintenance;
        const supported = switch (input.operation) {
            .check => capabilities.check,
            .compact => capabilities.compact,
            .vacuum => capabilities.vacuum,
        };
        if (!supported) return error.OperationUnsupported;
        return coordinator.start(input.operation, input.idempotency_key) catch |err| switch (err) {
            error.MaintenanceBusy => error.MaintenanceBusy,
            error.IdempotencyConflict => error.IdempotencyConflict,
            error.InvalidIdempotencyKey => error.InvalidIdempotencyKey,
            error.MaintenanceHistoryFull => error.MaintenanceHistoryFull,
            error.MaintenanceJobIdExhausted => error.MaintenanceJobIdExhausted,
            error.OutOfMemory => error.Internal,
            else => {
                std.log.err("storage maintenance submission failed err={s}", .{@errorName(err)});
                return error.Unavailable;
            },
        };
    }

    pub fn get(
        self: Operations,
        request: operation.RequestContext,
        input: JobInput,
    ) Error!maintenance.Coordinator.Snapshot {
        try request.ensureActive();
        const coordinator = self.coordinator orelse return error.Unsupported;
        return coordinator.get(input.job_id) orelse error.NotFound;
    }

    pub fn cancel(
        self: Operations,
        request: operation.RequestContext,
        input: JobInput,
    ) Error!maintenance.Coordinator.Snapshot {
        try request.ensureActive();
        const coordinator = self.coordinator orelse return error.Unsupported;
        return coordinator.cancel(input.job_id) orelse error.NotFound;
    }
};

test "unsupported maintenance operations fail before transport adaptation" {
    const operations = Operations.init(null);
    try std.testing.expectError(
        error.Unsupported,
        operations.start(.{}, .{ .operation = .check }),
    );
    try std.testing.expectError(error.Unsupported, operations.get(.{}, .{ .job_id = 1 }));
    try std.testing.expectError(error.Unsupported, operations.cancel(.{}, .{ .job_id = 1 }));
}

test "storage maintenance typed operations run without an HTTP request" {
    const FakeMaintenance = struct {
        fn source(self: *@This()) maintenance.Source {
            return .{ .ptr = self, .vtable = &.{ .status = status, .run = run } };
        }

        fn status(_: *anyopaque) maintenance.Status {
            return .{
                .engine = "test",
                .maintenance = .{ .check = true, .online = true },
            };
        }

        fn run(_: *anyopaque, selected: maintenance.Operation, cancel_token: *const maintenance.CancelToken) anyerror!maintenance.Result {
            try std.testing.expectEqual(maintenance.Operation.check, selected);
            try cancel_token.check();
            return .{ .valid = true };
        }
    };

    var source = FakeMaintenance{};
    var runtime = try background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var coordinator = try maintenance.Coordinator.init(std.testing.allocator, source.source(), runtime.ptr());
    defer coordinator.deinit();
    const operations = Operations.init(&coordinator);

    const started = try operations.start(.{ .request_id = "direct-operation-test" }, .{
        .operation = .check,
        .idempotency_key = "direct-check",
    });
    const replayed = try operations.start(.{}, .{
        .operation = .check,
        .idempotency_key = "direct-check",
    });
    try std.testing.expectEqual(started.job_id, replayed.job_id);
    try std.testing.expectEqual(started.job_id, (try operations.get(.{}, .{ .job_id = started.job_id })).job_id);
    try std.testing.expectError(error.NotFound, operations.get(.{}, .{ .job_id = 0 }));

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        operations.get(.{
            .cancellation = operation.CancellationToken.fromAtomic(&cancelled),
        }, .{ .job_id = started.job_id }),
    );
}
