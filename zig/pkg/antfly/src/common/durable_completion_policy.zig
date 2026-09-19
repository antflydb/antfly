// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! New-work admission is distinct from the trusted runtime's authority to
//! restore and finish an existing durable obligation.
/// Implementation readiness only, never admission authority. Keep fresh
/// replicated activation closed until ordinary/control entry reservations and
/// maintenance capacity guarantees are complete. Existing obligations restore.
pub const replicated_activation_supported = false;

pub const Config = struct {
    enabled: bool = false,
};

/// Internal bootstrap/apply contract, never parsed from request or config JSON.
/// A local authority must come from a positively selected standalone native
/// runtime; Raft authority additionally requires the apply protocol's proof.
pub const Authority = enum(u8) {
    none = 0,
    standalone_local = 1,
    raft_apply = 2,
};

pub fn standaloneAuthority(native_directory: bool, ha_enabled: bool, durable_sync: bool) Authority {
    return if (native_directory and !ha_enabled and durable_sync) .standalone_local else .none;
}

test "workload admission durable completion bootstrap authority excludes unsupported owners" {
    const std = @import("std");
    try std.testing.expectEqual(Authority.standalone_local, standaloneAuthority(true, false, true));
    try std.testing.expectEqual(Authority.none, standaloneAuthority(false, false, true));
    try std.testing.expectEqual(Authority.none, standaloneAuthority(true, true, true));
    try std.testing.expectEqual(Authority.none, standaloneAuthority(true, false, false));
}
