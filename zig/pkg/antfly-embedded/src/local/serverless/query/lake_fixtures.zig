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

//! Independently produced Parquet fixtures shared by reader tests.
pub const pyarrow_empty_no_groups = @embedFile("testdata/pyarrow_empty_no_groups.parquet");
pub const pyarrow_plain_nullable_snappy = @embedFile("testdata/pyarrow_plain_nullable_snappy.parquet");
pub const pyarrow_dictionary_nullable_snappy = @embedFile("testdata/pyarrow_dictionary_nullable_snappy.parquet");
pub const pyarrow_empty_row_group = @embedFile("testdata/pyarrow_empty_row_group.parquet");
