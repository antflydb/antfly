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

pub const source = @import("source.zig");
pub const postgres = @import("postgres.zig");
pub const postgres_source = @import("postgres_source.zig");
pub const postgres_libpq = @import("postgres_libpq.zig");
pub const filter = @import("filter.zig");
pub const sql = @import("sql.zig");

pub const Source = source.Source;
pub const SourceKind = source.SourceKind;
pub const Column = source.Column;
pub const SortField = source.SortField;
pub const TableStatistics = source.TableStatistics;
pub const QueryParams = source.QueryParams;
pub const AggregationDef = source.AggregationDef;
pub const NamedAggregation = source.NamedAggregation;
pub const AggregateParams = source.AggregateParams;
pub const QueryResult = source.QueryResult;
pub const SnapshotReader = source.SnapshotReader;
pub const PreparedReplicationSnapshot = source.PreparedReplicationSnapshot;
pub const ReplicationOp = source.ReplicationOp;
pub const ReplicationChange = source.ReplicationChange;
pub const ReplicationPrepareResult = source.ReplicationPrepareResult;
pub const ReplicationCleanupParams = source.ReplicationCleanupParams;
pub const ReplicationPollParams = source.ReplicationPollParams;
pub const ReplicationPollResult = source.ReplicationPollResult;
pub const NamedValue = source.NamedValue;
pub const AggregateResult = source.AggregateResult;
pub const deinitJsonValue = source.deinitJsonValue;
pub const Config = source.Config;
pub const Factory = source.Factory;
pub const Registry = source.Registry;
pub const PostgresConfig = postgres.Config;
pub const PostgresNamedConfig = postgres.NamedConfig;
pub const PostgresSourceMap = postgres.SourceMap;
pub const PostgresQueryExecutor = postgres_source.QueryExecutor;
pub const PostgresLibpqExecutor = postgres_libpq.Executor;
pub const FilterTranslation = filter.Translation;
pub const postgresConfigFromPublicOpenApi = postgres.fromPublicOpenApi;
pub const postgresConfigFromMetadataOpenApi = postgres.fromMetadataOpenApi;
pub const postgresSourceMapFromPublicOpenApi = postgres.mapFromPublicOpenApi;
pub const postgresSourceMapFromMetadataOpenApi = postgres.mapFromMetadataOpenApi;
pub const registerPostgresExecutor = postgres_source.registerExecutor;
pub const registerDefaultPostgresExecutor = postgres_libpq.registerDefaultExecutor;
pub const Dialect = sql.Dialect;
pub const PlaceholderStyle = sql.PlaceholderStyle;
pub const ParameterValue = sql.ParameterValue;
pub const PreparedQuery = sql.PreparedQuery;
pub const SqlSourceConfig = sql.SqlSourceConfig;
pub const SelectStatementOptions = sql.SelectStatementOptions;
pub const postgresDialect = sql.postgresDialect;
pub const placeholderAlloc = sql.placeholderAlloc;
pub const buildSelectStatementAlloc = sql.buildSelectStatementAlloc;

test "foreign module compiles" {
    _ = source;
    _ = postgres;
    _ = postgres_source;
    _ = postgres_libpq;
    _ = filter;
    _ = sql;
    _ = Source;
    _ = SourceKind;
    _ = Column;
    _ = SortField;
    _ = TableStatistics;
    _ = QueryParams;
    _ = AggregationDef;
    _ = NamedAggregation;
    _ = AggregateParams;
    _ = QueryResult;
    _ = SnapshotReader;
    _ = PreparedReplicationSnapshot;
    _ = ReplicationOp;
    _ = ReplicationChange;
    _ = ReplicationPrepareResult;
    _ = ReplicationCleanupParams;
    _ = ReplicationPollParams;
    _ = ReplicationPollResult;
    _ = NamedValue;
    _ = AggregateResult;
    _ = deinitJsonValue;
    _ = Config;
    _ = Factory;
    _ = Registry;
    _ = PostgresConfig;
    _ = PostgresNamedConfig;
    _ = PostgresSourceMap;
    _ = PostgresQueryExecutor;
    _ = PostgresLibpqExecutor;
    _ = FilterTranslation;
    _ = postgresConfigFromPublicOpenApi;
    _ = postgresConfigFromMetadataOpenApi;
    _ = postgresSourceMapFromPublicOpenApi;
    _ = postgresSourceMapFromMetadataOpenApi;
    _ = registerPostgresExecutor;
    _ = registerDefaultPostgresExecutor;
    _ = Dialect;
    _ = PlaceholderStyle;
    _ = ParameterValue;
    _ = PreparedQuery;
    _ = SqlSourceConfig;
    _ = SelectStatementOptions;
    _ = postgresDialect;
    _ = placeholderAlloc;
    _ = buildSelectStatementAlloc;
}
