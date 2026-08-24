// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared durable record for serverless head publication and work ownership.
//!
//! Keeping the visible head and fencing token in one conditionally-written
//! object makes lease takeover and publication linearizable. A worker cannot
//! validate one object and then publish through another after ownership moved.

pub const format_version: u32 = 1;

pub const Record = struct {
    format_version: u32 = format_version,
    head_version: u64 = 0,
    owner_id: ?[]const u8 = null,
    fencing_token: u64 = 0,
    expires_at_unix_ns: u64 = 0,
    released: bool = true,

    pub fn fromLegacyHead(head_version: u64) Record {
        return .{ .head_version = head_version };
    }
};

pub const Fence = struct {
    owner_id: []const u8,
    fencing_token: u64,
};
