// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! Compatibility facade for catalog resource types owned by metadata.

const catalog_resources = @import("../metadata/catalog/resources.zig");

pub const default_database_name = catalog_resources.default_database_name;
pub const default_namespace_name = catalog_resources.default_namespace_name;
pub const TableTarget = catalog_resources.TableTarget;
pub const NamespaceTarget = catalog_resources.NamespaceTarget;
pub const SqlSessionSetting = catalog_resources.SqlSessionSetting;
pub const SqlCatalogSession = catalog_resources.SqlCatalogSession;

pub const databaseResourceNameAlloc = catalog_resources.databaseResourceNameAlloc;
pub const namespaceResourceNameAlloc = catalog_resources.namespaceResourceNameAlloc;
pub const tableResourceNameAlloc = catalog_resources.tableResourceNameAlloc;
pub const tablespaceResourceNameAlloc = catalog_resources.tablespaceResourceNameAlloc;
pub const defaultPublicTableResourceNameAlloc = catalog_resources.defaultPublicTableResourceNameAlloc;
pub const namespaceTargetFromOptional = catalog_resources.namespaceTargetFromOptional;
pub const tableTargetFromOptional = catalog_resources.tableTargetFromOptional;
pub const isDefaultPublicNamespace = catalog_resources.isDefaultPublicNamespace;
pub const namespaceIsDefaultPublic = catalog_resources.namespaceIsDefaultPublic;
pub const tableIsDefaultPublic = catalog_resources.tableIsDefaultPublic;
pub const storageTableNameForTargetAlloc = catalog_resources.storageTableNameForTargetAlloc;
pub const tableIdStorageResourceNameAlloc = catalog_resources.tableIdStorageResourceNameAlloc;
pub const storageTableNameForRecordAlloc = catalog_resources.storageTableNameForRecordAlloc;
pub const sqlTableTargetFromObjectNameWithSession = catalog_resources.sqlTableTargetFromObjectNameWithSession;
pub const sqlTableTargetFromObjectName = catalog_resources.sqlTableTargetFromObjectName;
pub const tableResourceNameFromSqlObjectAlloc = catalog_resources.tableResourceNameFromSqlObjectAlloc;
pub const tableResourceNameFromSqlObjectWithSessionAlloc = catalog_resources.tableResourceNameFromSqlObjectWithSessionAlloc;
pub const tableResourceMatches = catalog_resources.tableResourceMatches;
