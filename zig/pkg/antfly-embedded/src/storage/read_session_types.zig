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

//! Canonical value namespace for source and client session backends. This
//! file must not import DB, physical reader/session bodies, or an engine.
pub const contract = @import("read_session_contract.zig");
pub const values = @import("read_session_values.zig");
pub const view = @import("relational_read_view.zig").View;
pub const types = @import("db/types.zig");
pub const range_protection = @import("range_protection.zig");
pub const reader_values = @import("read_reader_values.zig");

pub const query_lease = @import("query_read_lease_contract.zig");

pub const document_values = @import("read_document_values.zig");
