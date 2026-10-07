// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared caller/child phase machinery. Inject the production backend control
//! module so both sides use their owning OS worker's existing checkpoint TLS.
pub fn For(comptime controls: type, comptime rpc: type) type {
    return struct {
        pub const Parent = struct {
            wait: ?controls.ResourceWait = null,
            pub fn transition(self: *@This(), phase: rpc.Phase) !void {
                switch (phase) {
                    .resource_wait => {
                        if (self.wait != null) return error.InvalidWorkerPhase;
                        const wait = try controls.ResourceWait.init();
                        if (wait.checkpoint == null) return error.UnsupportedWorkerPhase;
                        self.wait = wait;
                    },
                    .runnable => {
                        if (self.wait == null) return error.InvalidWorkerPhase;
                        try self.finish();
                    },
                }
            }
            pub fn finish(self: *@This()) !void {
                var wait = self.wait orelse return;
                self.wait = null;
                try wait.finish();
            }
        };
        pub const Child = struct {
            request: *rpc.Request,
            failure: ?anyerror = null,
            pub fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.failure) |err| return err;
                if (self.request.cancelled.load(.acquire)) return error.Cancelled;
            }
            fn transition(self: *@This(), phase: rpc.Phase) !void {
                self.request.transition(phase) catch |err| {
                    self.failure = err;
                    return err;
                };
            }
            pub fn park(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.transition(.resource_wait);
            }
            pub fn reenter(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.transition(.runnable);
            }
        };
    };
}
