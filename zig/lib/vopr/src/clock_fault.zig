// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Reusable deterministic node-clock fault surface.
//!
//! A domain owns no host resources. It advances a `VoprIo` clock pair, keeps
//! realtime rate and node-pause state replayable, and separates clock passage
//! from timer delivery. Fault changes consume an explicit budget so campaigns
//! cannot accidentally create an unbounded overlapping-fault state space.

const std = @import("std");
const vopr_io = @import("vopr_io.zig");

pub const normal_rate_ppm: i64 = 1_000_000;

pub const Config = struct {
    fault_budget: u32 = 4,
    minimum_rate_ppm: i64 = 1,
    maximum_rate_ppm: i64 = 4_000_000,
};

pub const Domain = struct {
    sim: *vopr_io.VoprIo,
    config: Config,
    rate_ppm: i64 = normal_rate_ppm,
    paused: bool = false,
    timer_delivery_pending: bool = false,
    faults_used: u32 = 0,

    pub fn init(sim: *vopr_io.VoprIo, config: Config) !Domain {
        if (config.fault_budget == 0) return error.InvalidClockFaultBudget;
        if (config.minimum_rate_ppm <= 0 or config.maximum_rate_ppm < config.minimum_rate_ppm)
            return error.InvalidClockRateBounds;
        return .{ .sim = sim, .config = config };
    }

    pub fn jumpRealtime(self: *Domain, delta_ns: i64) !void {
        try self.consumeFault();
        try self.sim.advanceClocks(0, delta_ns, !self.paused);
        self.timer_delivery_pending = self.timer_delivery_pending or self.paused;
    }

    pub fn setRate(self: *Domain, rate_ppm: i64) !void {
        if (rate_ppm < self.config.minimum_rate_ppm or rate_ppm > self.config.maximum_rate_ppm)
            return error.ClockRateOutOfRange;
        if (rate_ppm != self.rate_ppm) try self.consumeFault();
        self.rate_ppm = rate_ppm;
    }

    pub fn setPaused(self: *Domain, paused: bool) !void {
        if (paused != self.paused) try self.consumeFault();
        self.paused = paused;
    }

    /// Advance the node's oscillator. Realtime follows the selected frequency;
    /// monotonic time remains nondecreasing. A paused node accumulates expired
    /// timers but cannot deliver them until explicitly resumed and delivered.
    pub fn advance(self: *Domain, monotonic_delta_ns: u64) !void {
        const scaled = try scale(monotonic_delta_ns, self.rate_ppm);
        try self.sim.advanceClocks(monotonic_delta_ns, scaled, !self.paused);
        self.timer_delivery_pending = self.timer_delivery_pending or self.paused;
    }

    pub fn deliverTimers(self: *Domain) !void {
        if (self.paused) return error.ClockDomainPaused;
        try self.sim.deliverDueTimers();
        self.timer_delivery_pending = false;
    }

    pub fn stabilize(self: *Domain) !void {
        self.rate_ppm = normal_rate_ppm;
        self.paused = false;
        try self.deliverTimers();
    }

    pub fn remainingFaultBudget(self: *const Domain) u32 {
        return self.config.fault_budget - self.faults_used;
    }

    fn consumeFault(self: *Domain) !void {
        if (self.faults_used == self.config.fault_budget) return error.ClockFaultBudgetExhausted;
        self.faults_used += 1;
    }
};

fn scale(delta_ns: u64, rate_ppm: i64) !i64 {
    const product = try std.math.mul(i128, @as(i128, delta_ns), @as(i128, rate_ppm));
    const value = @divTrunc(product, normal_rate_ppm);
    if (value > std.math.maxInt(i64)) return error.VoprIoClockOverflow;
    return @intCast(value);
}

test "clock domain separates faults oscillator passage and timer delivery" {
    var sim = try vopr_io.VoprIo.init(.{ .required = .of(&.{ .clock_read, .sleep }) });
    defer sim.deinit();
    var domain = try Domain.init(&sim, .{ .fault_budget = 4 });
    try domain.setRate(2_000_000);
    try domain.advance(10);
    try std.testing.expectEqual(@as(i96, 10), std.Io.Clock.awake.now(sim.io()).toNanoseconds());
    try std.testing.expectEqual(@as(i96, 20), std.Io.Clock.real.now(sim.io()).toNanoseconds());
    try domain.setPaused(true);
    try domain.advance(10);
    try std.testing.expect(domain.timer_delivery_pending);
    try std.testing.expectError(error.ClockDomainPaused, domain.deliverTimers());
    try domain.setPaused(false);
    try domain.deliverTimers();
    try std.testing.expect(!domain.timer_delivery_pending);
    try domain.jumpRealtime(-5);
    try std.testing.expectEqual(@as(u32, 0), domain.remainingFaultBudget());
}

test "clock domain enforces fault and frequency budgets" {
    var sim = try vopr_io.VoprIo.init(.{ .required = .of(&.{.clock_read}) });
    defer sim.deinit();
    var domain = try Domain.init(&sim, .{ .fault_budget = 1 });
    try std.testing.expectError(error.ClockRateOutOfRange, domain.setRate(0));
    try domain.jumpRealtime(1);
    try std.testing.expectError(error.ClockFaultBudgetExhausted, domain.setPaused(true));
}
