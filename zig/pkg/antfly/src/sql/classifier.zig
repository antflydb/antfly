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

pub const SqlStatementFamily = enum {
    select,
    insert,
    update,
    delete,
    truncate,
    merge,
    with,
    ddl,
};

pub const SqlReadStatementKind = enum {
    query,
    set_operation,
    recursive_cte,
    aggregate,
    join,
    lateral,
    window,
};

pub const SqlWriteStatementKind = enum {
    insert,
    insert_source,
    update,
    update_source,
    update_joined_source,
    delete,
    delete_source,
    delete_joined_source,
    truncate,
    merge,
};

pub const SqlPreparedStatementSubjectKind = enum {
    read,
    write,
    ddl,
};

pub const SqlPreparedStatementStatementKind = enum {
    read,
    insert,
    insert_source,
    update,
    delete,
    truncate,
    merge,
    ddl,
};
