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

//! Source-only session implementation selection. Client graphs never import
//! this module or any of these private implementation files.
pub const Relational = @import("db/relational_read_session.zig").Implementation;
pub const Document = @import("db/document_rows.zig").Implementation;
pub const Statement = @import("db/read_statement_snapshot.zig").Implementation;
pub const RelationalRows = @import("db/relational_rows.zig");

pub const QueryReadLease = @import("db/query_read_lease.zig").For;

pub const DocumentRows = @import("db/document_read_rows.zig").For;
pub const DocumentPrimaryReadWindow = @import("db/document_primary_read_window.zig").Implementation;
