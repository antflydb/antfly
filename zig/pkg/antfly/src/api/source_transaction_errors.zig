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

//! Deterministic source validation outcomes, distinct from admission ambiguity.
const std = @import("std");
pub const Error = error{ InvalidSourceAdmission, SourcePayloadConflict, SourceCheckpointConflict, SourceTransactionIdentityConflict, SourceReceiptExpired, SourceAlreadyCommitted };
pub fn classify(err: anyerror) ?Error {
    return switch (err) {
        error.InvalidSourceAdmission => error.InvalidSourceAdmission,
        error.SourcePayloadConflict => error.SourcePayloadConflict,
        error.SourceCheckpointConflict => error.SourceCheckpointConflict,
        error.SourceTransactionIdentityConflict => error.SourceTransactionIdentityConflict,
        error.SourceReceiptExpired => error.SourceReceiptExpired,
        error.SourceAlreadyCommitted => error.SourceAlreadyCommitted,
        else => null,
    };
}
pub fn status(err: Error) u16 {
    return if (err == error.InvalidSourceAdmission) 400 else 409;
}
pub fn decode(body: []const u8) ?Error {
    const text = std.mem.trim(u8, body, " \t\r\n");
    inline for (@typeInfo(Error).error_set.error_names.?) |name| {
        if (std.mem.eql(u8, text, name)) return @field(Error, name);
    }
    return null;
}
