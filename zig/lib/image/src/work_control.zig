//! Cancellation for synchronous image kernels. A worker installs its borrowed
//! control for the duration of one job; nested scopes restore the previous
//! control. Never keep a scope installed across an async suspension.
pub const Control = struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) anyerror!void = null,

    pub fn check(self: Control) !void {
        if (self.check_fn) |callback| try callback(self.context);
    }
};

threadlocal var active: Control = .{};

pub const Scope = struct {
    previous: Control,

    pub fn enter(control: Control) Scope {
        const previous = active;
        active = control;
        return .{ .previous = previous };
    }

    pub fn deinit(self: Scope) void {
        active = self.previous;
    }
};

pub fn check() !void {
    try active.check();
}
