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

//! Private decision authority failures retain exact identity across owner RPCs.
pub const Error = error{
    InvalidExternalDecisionOwner,
    ExternalTransactionDecisionRequired,
    ExternalTransactionDecisionChanged,
    ExternalTransactionRoleRequired,
    ExternalTransactionCommitNotActivated,
    ExternalTransactionAdmissionExpired,
};

pub fn classify(err: anyerror) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |name| if (err == @field(Error, name)) return @field(Error, name);
    return null;
}

pub fn decode(bytes: []const u8) ?Error {
    inline for (@typeInfo(Error).error_set.error_names.?) |name| if (@import("std").mem.eql(u8, bytes, name)) return @field(Error, name);
    return null;
}
