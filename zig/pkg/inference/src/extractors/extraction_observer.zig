// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional scalar-only instrumentation. No callback allocates, returns an
//! error, changes decoder options, or receives request content or identities.
//! The caller owns context through synchronous execution and its teardown.
const std = @import("std");

pub const Stage = enum {
    admission,
    parsing,
    preflight,
    model,
    tokenizing,
    execution,
    windowing,
    merging,
    serializing,
    teardown,

    pub fn fromFailure(name: []const u8) ?Stage {
        if (std.mem.eql(u8, name, "request") or std.mem.eql(u8, name, "input") or std.mem.eql(u8, name, "options") or std.mem.eql(u8, name, "schema")) return .parsing;
        // Metal executes encoder and learned heads together. Keep the same
        // timing boundary for native and Metal instead of mislabeling heads.
        if (std.mem.eql(u8, name, "encoder") or std.mem.eql(u8, name, "decoding")) return .execution;
        return std.meta.stringToEnum(Stage, name);
    }
};
pub const Solver = enum { classification, joint, records };
pub const Status = enum { optimal, feasible };
pub const Diagnostic = struct { status: Status, exhausted: bool, visited_nodes: usize };
pub const Sample = struct {
    prompt_tokens: usize,
    output_values: usize,
    solvers: [3]?Diagnostic = .{ null, null, null },
};
pub const Event = union(enum) {
    phase: Stage,
    parsed: struct { items: usize, input_bytes: usize },
    sample_decoded: Sample,
    document_planned: usize,
    window_completed,
    host_peak: usize,
};
pub const Observer = struct {
    context: ?*anyopaque,
    emit_fn: *const fn (?*anyopaque, Event) void,

    pub fn emit(self: Observer, event: Event) void {
        self.emit_fn(self.context, event);
    }
};

pub fn emit(observer: ?Observer, event: Event) void {
    if (observer) |value| value.emit(event);
}

test "extraction observability event contract has no fallible or request-owned payloads" {
    const Capture = struct {
        calls: usize = 0,
        nodes: usize = 0,
        fn receive(raw: ?*anyopaque, event: Event) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (event == .sample_decoded) for (event.sample_decoded.solvers) |optional| {
                if (optional) |diagnostic| self.nodes += diagnostic.visited_nodes;
            };
        }
    };
    var capture = Capture{};
    const observer = Observer{ .context = &capture, .emit_fn = Capture.receive };
    emit(null, .window_completed);
    emit(observer, .{ .sample_decoded = .{ .prompt_tokens = 3, .output_values = 1, .solvers = .{ .{ .status = .feasible, .exhausted = true, .visited_nodes = 4 }, null, null } } });
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    try std.testing.expectEqual(@as(usize, 4), capture.nodes);
    try std.testing.expectEqual(Stage.execution, Stage.fromFailure("encoder").?);
    try std.testing.expectEqual(Stage.execution, Stage.fromFailure("decoding").?);
    try std.testing.expect(Stage.fromFailure("caller-supplied-secret") == null);
}
