// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Metadata uses the same independently compiled candidate as native owners.
const native = @import("antfly_local_sources").storage_db_relational_index_candidate;
pub const max_schema_bytes = native.max_schema_bytes;
pub const Candidate = native.Candidate;
