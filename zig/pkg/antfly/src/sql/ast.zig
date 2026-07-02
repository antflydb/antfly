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

const db_mod = @import("../storage/db/mod.zig");

pub const SelectOutputKind = enum {
    field,
    json_extract,
    array_length,
    coalesce,
    field_alias,
    expression,
    scalar_subquery,
};

pub const SelectOutputRef = struct {
    kind: SelectOutputKind,
    index: usize,
};

pub const SelectSetOperation = enum {
    union_distinct,
    union_all,
    intersect,
    except,
};

pub const SqlExplainFormat = enum {
    text,
    json,
};

pub const SqlPatternQuantifier = enum {
    any,
    all,
};

pub const SqlRowClaimClause = struct {
    mode: db_mod.types.RowClaimMode,
    wait_policy: db_mod.types.RowClaimWaitPolicy,
};
