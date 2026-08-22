// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Canonical semantic result of one selected simulation transition.
//!
//! Expected rejection and injected-error outcomes are product behavior, not
//! harness failures. Process, harness, replay, and property outcomes are
//! failure-bearing and are retained by campaigns with stable fingerprints.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const trace = @import("trace.zig");

pub const Class = enum {
    applied,
    rejected_valid_response,
    expected_injected_error,
    property_violation,
    target_reached,
    process_crash,
    process_panic,
    harness_error,
    replay_divergence,
};

pub const TransitionOutcome = struct {
    class: Class,
    /// Stable, low-cardinality semantic identity. Never put host paths,
    /// addresses, timestamps, or allocator-dependent text here.
    identity: []const u8,
    payload_digest: u64 = 0,
    property_id: ?ids.StableId = null,

    pub fn applied() TransitionOutcome {
        return named(.applied, "vopr.outcome.applied", 0);
    }

    pub fn rejected(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.rejected_valid_response, identity, payload_digest);
    }

    pub fn injectedError(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.expected_injected_error, identity, payload_digest);
    }

    pub fn targetReached(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.target_reached, identity, payload_digest);
    }

    pub fn propertyViolation(property_id: ids.StableId, identity: []const u8, payload_digest: u64) TransitionOutcome {
        var result = named(.property_violation, identity, payload_digest);
        result.property_id = property_id;
        return result;
    }

    pub fn processCrash(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.process_crash, identity, payload_digest);
    }

    pub fn processPanic(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.process_panic, identity, payload_digest);
    }

    pub fn harnessError(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.harness_error, identity, payload_digest);
    }

    pub fn replayDivergence(identity: []const u8, payload_digest: u64) TransitionOutcome {
        return named(.replay_divergence, identity, payload_digest);
    }

    pub fn validate(self: TransitionOutcome) !void {
        if (self.identity.len == 0) return error.EmptyTransitionOutcomeIdentity;
        if (self.class == .property_violation and self.property_id == null)
            return error.PropertyViolationMissingPropertyId;
        if (self.class != .property_violation and self.property_id != null)
            return error.UnexpectedTransitionOutcomePropertyId;
    }

    pub fn asEvent(self: TransitionOutcome) event.Event {
        return .{
            .id = ids.stable("event", eventName(self.class)),
            .name = eventName(self.class),
            .kind = switch (self.class) {
                .rejected_valid_response => .client_response,
                .expected_injected_error => .injected_error,
                else => .domain,
            },
            .payload_digest = ids.derive("outcome.payload", ids.stable("outcome", self.identity), self.payload_digest),
        };
    }

    pub fn failureClass(self: TransitionOutcome) ?trace.FailureClass {
        return switch (self.class) {
            .property_violation => .property,
            .process_crash => .process,
            .process_panic => .panic,
            .harness_error => .harness,
            .replay_divergence => .replay_divergence,
            .applied, .rejected_valid_response, .expected_injected_error, .target_reached => null,
        };
    }

    pub fn fingerprint(self: TransitionOutcome) ids.StableId {
        return ids.derive("failure.outcome", ids.stable("outcome", self.identity), @intFromEnum(self.class));
    }

    fn named(class: Class, identity: []const u8, payload_digest: u64) TransitionOutcome {
        return .{ .class = class, .identity = identity, .payload_digest = payload_digest };
    }
};

pub fn eventName(class: Class) []const u8 {
    return switch (class) {
        .applied => "vopr.outcome.applied",
        .rejected_valid_response => "vopr.outcome.rejected_valid_response",
        .expected_injected_error => "vopr.outcome.expected_injected_error",
        .property_violation => "vopr.outcome.property_violation",
        .target_reached => "vopr.outcome.target_reached",
        .process_crash => "vopr.outcome.process_crash",
        .process_panic => "vopr.outcome.process_panic",
        .harness_error => "vopr.outcome.harness_error",
        .replay_divergence => "vopr.outcome.replay_divergence",
    };
}

test "outcomes preserve expected errors without classifying them as failures" {
    const expected = TransitionOutcome.injectedError("storage.write.injected", 7);
    try expected.validate();
    try std.testing.expectEqual(@as(?trace.FailureClass, null), expected.failureClass());
    try std.testing.expectEqual(event.Kind.injected_error, expected.asEvent().kind);
}

test "failure-bearing outcomes have stable fingerprints" {
    const first = TransitionOutcome.processCrash("node.worker.crash", 10);
    const second = TransitionOutcome.processCrash("node.worker.crash", 99);
    try std.testing.expectEqual(first.fingerprint(), second.fingerprint());
    try std.testing.expectEqual(trace.FailureClass.process, first.failureClass().?);
}
