// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

/// Every metadata voter must advertise at least this version before a forced
/// reallocation request can be admitted. Older reconcilers do not understand
/// the causal acknowledgement barrier and may clear the request prematurely.
pub const barrier_protocol_version: u16 = 1;

pub const ReallocationRequestRecord = struct {
    request_id: u128,
    requested_at_ms: u64,
};

pub fn generateRequestId(io: std.Io) !u128 {
    var request_id: u128 = 0;
    while (request_id == 0) try io.randomSecure(std.mem.asBytes(&request_id));
    return request_id;
}

pub fn isValid(record: ReallocationRequestRecord) bool {
    return record.request_id != 0;
}
