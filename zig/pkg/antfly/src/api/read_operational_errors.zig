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

//! Shared read availability classification without importing execution owners.
/// Normalize known read-transport failures before they cross a compiled
/// boundary. Reads have no ambiguous mutation outcome; unavailable peers must
/// not become an unknown runtime defect or a successful empty/partial result.
pub fn normalizeDistributedReadTransportError(err: anyerror) anyerror {
    return switch (err) {
        // Local request/watchdog admission is also an availability outcome.
        // No read result exists; never turn saturation into a missing row or
        // retry it here outside the caller's request budget.
        error.ConcurrencyUnavailable,
        error.ResourceTemporarilyUnavailable,
        error.RemoteUnavailable,
        error.ConnectionFailed,
        error.AddressUnavailable,
        error.ConnectionReset,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.ConnectionClosed,
        error.ConnectionAborted,
        error.ConnectionTimeout,
        error.ConnectionTimedOut,
        error.BrokenPipe,
        error.NotConnected,
        error.NetworkUnreachable,
        error.NetworkDown,
        error.HostUnreachable,
        error.DnsResolutionFailed,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.RecvFailed,
        error.SendFailed,
        => error.StorageReadTemporarilyUnavailable,
        else => err,
    };
}

pub fn normalizeDistributedQueryOperationalError(err: anyerror) anyerror {
    return switch (normalizeDistributedReadTransportError(err)) {
        error.StorageReadTemporarilyUnavailable,
        // A topology retry that loses its race twice is an availability
        // outcome, not an internal server failure. The coordinator still
        // owns all intermediate results and the caller can safely retry the
        // complete request against a fresh plan.
        error.TopologyChanged,
        error.UnknownGroup,
        error.NotLeader,
        error.GroupLeaderUnavailable,
        error.LeaderUnavailable,
        error.ReadIndexTimeout,
        => error.DistributedQueryUnavailable,
        else => err,
    };
}
