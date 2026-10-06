// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Adapter from VoprIo's ordered FIN/reset state to httpx's backend-neutral H1
//! disconnect probe. The production HTTP runtime imports no VOPR types.

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
        return self.vopr_io.socketPeerAbandonedConnection(handle);
    }
};
