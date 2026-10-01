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

//! Borrowed, normalized mutations executed by the local database. Wire version
//! selection and server provenance interpretation belong to ingress adapters.
const types = @import("types.zig");
pub const Batch = struct {
    request: types.BatchRequest,
    receipt: Receipt = .none,
    pub const Receipt = union(enum) {
        none,
        ordered: types.RaftAppliedEntryIdentity,
        native: @import("receipt_position.zig").Native,
        online_source: u64,
    };
};
