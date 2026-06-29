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

const sql_catalog_resources = @import("../../sql/catalog_resources.zig");

pub const default_database_name = sql_catalog_resources.default_database_name;
pub const default_namespace_name = sql_catalog_resources.default_namespace_name;
pub const TableTarget = sql_catalog_resources.TableTarget;
pub const NamespaceTarget = sql_catalog_resources.NamespaceTarget;
pub const SqlSessionSetting = sql_catalog_resources.SqlSessionSetting;
pub const SqlCatalogSession = sql_catalog_resources.SqlCatalogSession;

pub const databaseResourceNameAlloc = sql_catalog_resources.databaseResourceNameAlloc;
pub const namespaceResourceNameAlloc = sql_catalog_resources.namespaceResourceNameAlloc;
pub const tableResourceNameAlloc = sql_catalog_resources.tableResourceNameAlloc;
pub const tablespaceResourceNameAlloc = sql_catalog_resources.tablespaceResourceNameAlloc;
pub const defaultPublicTableResourceNameAlloc = sql_catalog_resources.defaultPublicTableResourceNameAlloc;
pub const namespaceTargetFromOptional = sql_catalog_resources.namespaceTargetFromOptional;
pub const tableTargetFromOptional = sql_catalog_resources.tableTargetFromOptional;
pub const isDefaultPublicNamespace = sql_catalog_resources.isDefaultPublicNamespace;
pub const namespaceIsDefaultPublic = sql_catalog_resources.namespaceIsDefaultPublic;
pub const tableIsDefaultPublic = sql_catalog_resources.tableIsDefaultPublic;
pub const storageTableNameForTargetAlloc = sql_catalog_resources.storageTableNameForTargetAlloc;
pub const tableIdStorageResourceNameAlloc = sql_catalog_resources.tableIdStorageResourceNameAlloc;
pub const storageTableNameForRecordAlloc = sql_catalog_resources.storageTableNameForRecordAlloc;
pub const sqlTableTargetFromObjectNameWithSession = sql_catalog_resources.sqlTableTargetFromObjectNameWithSession;
pub const sqlTableTargetFromObjectName = sql_catalog_resources.sqlTableTargetFromObjectName;
pub const tableResourceNameFromSqlObjectAlloc = sql_catalog_resources.tableResourceNameFromSqlObjectAlloc;
pub const tableResourceNameFromSqlObjectWithSessionAlloc = sql_catalog_resources.tableResourceNameFromSqlObjectWithSessionAlloc;
pub const tableResourceMatches = sql_catalog_resources.tableResourceMatches;
