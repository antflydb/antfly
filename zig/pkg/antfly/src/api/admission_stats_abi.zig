// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Scalar-only admission snapshots shared by the API kernel and its host.
//! Keep vocabulary sizes explicit: changing this layout requires an API ABI
//! version bump. Conversion fails to compile if the controller grows new fields.
const std = @import("std");

pub const Stats = extern struct {
    capacity: usize,
    in_flight: usize,
    peak_in_flight: usize,
    rejected_total: u64,
    queued: usize,
    queued_bytes: usize,
    retained_bytes: usize,
    waited_total: u64,
    wait_ns_total: u64,
    wait_completed_total: u64,
    wait_buckets: [11]u64,
    expired_total: u64,
    cancelled_total: u64,
    draining: u8,
    max_queued_requests: usize,
    max_queued_bytes: usize,
    max_retained_bytes: usize,
    max_wait_ms: u32,
    policy_generation: u64,
    rejection_reasons: [9]u64,
    allocation_denials: [3]u64,

    pub fn fromNative(value: anytype) Stats {
        comptime validateNative(@TypeOf(value));
        var out: Stats = undefined;
        inline for (std.meta.fields(Stats)) |field| {
            @field(out, field.name) = if (comptime std.mem.eql(u8, field.name, "draining"))
                @intFromBool(value.draining)
            else
                @field(value, field.name);
        }
        return out;
    }

    pub fn toNative(self: Stats, comptime T: type) T {
        comptime validateNative(T);
        var out: T = undefined;
        inline for (std.meta.fields(Stats)) |field| {
            @field(out, field.name) = if (comptime std.mem.eql(u8, field.name, "draining"))
                self.draining != 0
            else
                @field(self, field.name);
        }
        return out;
    }

    fn validateNative(comptime T: type) void {
        if (std.meta.fields(T).len != std.meta.fields(Stats).len)
            @compileError("admission stats changed: update the wire snapshot and API ABI version");
        for (std.meta.fields(Stats)) |field| {
            const expected = if (std.mem.eql(u8, field.name, "draining")) bool else field.type;
            if (@FieldType(T, field.name) != expected)
                @compileError("admission stats field changed: " ++ field.name);
        }
    }
};

pub const HandlerStats = extern struct {
    query: Stats,
    write: Stats,
    inference: Stats,
    query_body: Stats,
};
