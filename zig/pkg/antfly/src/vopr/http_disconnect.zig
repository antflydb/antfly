// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Adapter from VoprIo's virtual reset state to httpx's backend-neutral H1
//! hard-disconnect probe. The production HTTP runtime imports no VOPR types.

const std = @import("std");
const httpx = @import("httpx");
const vopr = @import("vopr");

pub const Probe = struct {
    vopr_io: *vopr.vopr_io.VoprIo,

    pub fn iface(self: *Probe) httpx.H1DisconnectProbe {
        return .{ .ptr = self, .is_hard_disconnected = isHardDisconnected };
    }

    fn isHardDisconnected(raw: ?*const anyopaque, handle: std.Io.net.Socket.Handle) bool {
        const self: *const Probe = @ptrCast(@alignCast(raw orelse return true));
        return self.vopr_io.socketPeerHardDisconnected(handle);
    }
};
