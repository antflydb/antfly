// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
