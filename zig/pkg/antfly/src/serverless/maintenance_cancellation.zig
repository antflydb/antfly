// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// Borrowed cooperative cancellation for one synchronous maintenance pass.
/// The atomic flag covers graceful shutdown before Future cancellation is
/// armed; `checkCancel` also observes cancellation delivered by std.Io.
pub const Token = struct {
    io: std.Io,
    requested: ?*const std.atomic.Value(bool) = null,
    checkpoint_ptr: ?*anyopaque = null,
    checkpoint_fn: ?*const fn (*anyopaque) anyerror!void = null,

    pub fn check(self: Token) !void {
        if (self.requested) |requested| {
            if (requested.load(.acquire)) return error.Canceled;
        }
        try self.io.checkCancel();
        if (self.checkpoint_ptr) |ptr| {
            if (self.checkpoint_fn) |checkpoint_fn| try checkpoint_fn(ptr);
        }
    }

    pub fn withCheckpoint(
        self: Token,
        ptr: *anyopaque,
        checkpoint_fn: *const fn (*anyopaque) anyerror!void,
    ) Token {
        var result = self;
        result.checkpoint_ptr = ptr;
        result.checkpoint_fn = checkpoint_fn;
        return result;
    }
};

pub fn check(token: ?Token) !void {
    if (token) |value| try value.check();
}
