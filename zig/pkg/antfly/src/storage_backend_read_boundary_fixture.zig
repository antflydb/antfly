// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Separately compiled callback provider. Do not import this into the test root:
//! the regression requires independent Zig error tables and provider dispatch.
const std = @import("std");
const erased = @import("storage/backend_erased.zig");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");

const Read = struct {
    pub fn abort(_: *@This()) void {}
    pub fn get(_: *@This(), key: []const u8) ![]const u8 {
        if (std.mem.eql(u8, key, "present")) return "value";
        return error.NotFound;
    }
    pub fn openCursor(_: *@This()) !Cursor {
        return .{};
    }
    pub fn forkBorrowedRead(_: *@This()) !Read {
        return .{};
    }
    pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !Scope {
        return .{};
    }
};
const Scope = struct {
    pub fn close(_: *@This()) void {}
    pub fn get(_: *@This(), _: []const u8) ![]const u8 {
        return error.NotFound;
    }
};
const Cursor = struct {
    pub fn close(_: *@This()) void {}
    pub fn first(_: *@This()) !erased.Entry {
        return error.NotFound;
    }
    pub fn last(_: *@This()) !erased.Entry {
        return error.NotFound;
    }
    pub fn next(_: *@This()) !erased.Entry {
        return error.InvalidArgument;
    }
    pub fn prev(_: *@This()) !erased.Entry {
        return error.InvalidArgument;
    }
    pub fn seekAtOrAfter(_: *@This(), _: []const u8) !erased.Entry {
        return error.NotFound;
    }
    pub fn seekAtOrBefore(_: *@This(), _: []const u8) !erased.Entry {
        return error.NotFound;
    }
};

export fn antfly_test_backend_read(contract: *const native.TypeContract, out: *anyopaque) callconv(.c) errors.Status {
    if (!contract.matches(native.TypeContract.of(erased.ReadTxn))) return errors.statusFromError(error.UnsupportedVersion);
    const result: *erased.ReadTxn = @ptrCast(@alignCast(out));
    result.* = erased.readTxnFrom(std.heap.c_allocator, Read{}) catch |err| return errors.statusFromError(err);
    return errors.Status.ok;
}

export fn antfly_test_backend_probe(contract: *const native.TypeContract, out: *anyopaque) callconv(.c) errors.Status {
    if (!contract.matches(native.TypeContract.of(erased.ProbeTxn))) return errors.statusFromError(error.UnsupportedVersion);
    const result: *erased.ProbeTxn = @ptrCast(@alignCast(out));
    result.* = erased.probeTxnFrom(std.heap.c_allocator, Read{}) catch |err| return errors.statusFromError(err);
    return errors.Status.ok;
}

export fn antfly_test_backend_not_found_ordinal() callconv(.c) u32 {
    return @intFromError(error.NotFound);
}
