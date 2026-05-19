// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Shared early-stopping state machine used across trainers.
//
// The policy is "stop when the tracked metric has failed to improve for
// `patience` consecutive evaluations beyond a `min_delta` threshold". This
// matches the inline logic in `gliner2.zig` and is generic over whether the
// metric is minimized (loss, perplexity) or maximized (F1, accuracy, MRR).

const std = @import("std");

pub const Direction = enum { minimize, maximize };

pub const EarlyStoppingConfig = struct {
    /// Direction the metric should move in to count as an improvement.
    direction: Direction = .minimize,
    /// Number of consecutive non-improving evaluations before stopping.
    patience: u32 = 3,
    /// Minimum improvement required to reset the patience counter. Strict
    /// inequality is used: a new best must differ from the current best by
    /// at least `min_delta` in the improvement direction.
    min_delta: f32 = 0.0,
};

pub const EarlyStoppingState = struct {
    config: EarlyStoppingConfig,
    best: f32,
    bad_count: u32 = 0,
    /// Total evaluations seen so far.
    steps: u64 = 0,
    /// True once `bad_count >= patience`.
    stopped: bool = false,

    pub fn init(config: EarlyStoppingConfig) EarlyStoppingState {
        return .{
            .config = config,
            .best = switch (config.direction) {
                .minimize => std.math.inf(f32),
                .maximize => -std.math.inf(f32),
            },
        };
    }

    /// Observe a new metric value. Returns true the moment `stopped`
    /// transitions from false to true (edge-triggered).
    pub fn observe(self: *EarlyStoppingState, metric: f32) bool {
        self.steps += 1;
        const improved = switch (self.config.direction) {
            .minimize => metric < self.best - self.config.min_delta,
            .maximize => metric > self.best + self.config.min_delta,
        };
        if (improved) {
            self.best = metric;
            self.bad_count = 0;
            return false;
        }
        self.bad_count += 1;
        if (!self.stopped and self.bad_count >= self.config.patience) {
            self.stopped = true;
            return true;
        }
        return false;
    }

    pub fn reset(self: *EarlyStoppingState) void {
        self.bad_count = 0;
        self.stopped = false;
        self.best = switch (self.config.direction) {
            .minimize => std.math.inf(f32),
            .maximize => -std.math.inf(f32),
        };
        self.steps = 0;
    }
};

test "early stopping minimize patience" {
    var es = EarlyStoppingState.init(.{ .direction = .minimize, .patience = 2, .min_delta = 0.0 });
    try std.testing.expect(!es.observe(1.0)); // new best
    try std.testing.expect(!es.observe(0.9)); // new best
    try std.testing.expect(!es.observe(0.95)); // bad 1
    try std.testing.expect(es.observe(0.95)); // bad 2 -> stop edge
    try std.testing.expect(!es.observe(0.95)); // already stopped, no new edge
    try std.testing.expectEqual(@as(f32, 0.9), es.best);
}

test "early stopping maximize min_delta" {
    var es = EarlyStoppingState.init(.{ .direction = .maximize, .patience = 2, .min_delta = 0.01 });
    try std.testing.expect(!es.observe(0.50));
    try std.testing.expect(!es.observe(0.505)); // within min_delta, counts as bad
    try std.testing.expect(!es.observe(0.52)); // genuine improvement resets
    try std.testing.expect(!es.observe(0.515)); // bad 1
    try std.testing.expect(es.observe(0.51)); // bad 2 -> stop edge
}

test "early stopping reset" {
    var es = EarlyStoppingState.init(.{ .direction = .minimize, .patience = 1 });
    _ = es.observe(1.0);
    _ = es.observe(2.0); // stop edge
    try std.testing.expect(es.stopped);
    es.reset();
    try std.testing.expect(!es.stopped);
    try std.testing.expectEqual(std.math.inf(f32), es.best);
}
