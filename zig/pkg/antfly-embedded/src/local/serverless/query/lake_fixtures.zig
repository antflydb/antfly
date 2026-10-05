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

//! Independently produced Parquet fixtures shared by reader tests.
pub const pyarrow_empty_no_groups = @embedFile("testdata/pyarrow_empty_no_groups.parquet");
pub const pyarrow_plain_nullable_snappy = @embedFile("testdata/pyarrow_plain_nullable_snappy.parquet");
pub const pyarrow_dictionary_nullable_snappy = @embedFile("testdata/pyarrow_dictionary_nullable_snappy.parquet");
pub const pyarrow_empty_row_group = @embedFile("testdata/pyarrow_empty_row_group.parquet");
