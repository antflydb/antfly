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

//! Portable lake reader aliases; no server query runtime.
pub const lake_iceberg_snapshot = @import("lake_iceberg_snapshot.zig");
pub const lake_object_reader = @import("lake_object_reader.zig");
pub const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
pub const lake_range_io = @import("lake_range_io.zig");
pub const lake_rows = @import("lake_rows.zig");
pub const lake_scan_plan = @import("lake_scan_plan.zig");
pub const lake_sidecar_selection = @import("lake_sidecar_selection.zig");

pub const LakeIcebergDeletePlan = lake_iceberg_snapshot.IcebergDeletePlan;
pub const LakeObjectStorageRangeReader = lake_object_reader.ObjectStorageRangeReader;
pub const LakeParquetObjectRangeCache = lake_parquet_rowgroup.ObjectRangeCache;
pub const LakeParquetObjectRangeReader = lake_parquet_rowgroup.ObjectRangeReader;
pub const LakeRangeCoalesceOptions = lake_range_io.CoalesceOptions;
pub const LakeRowsExpressionAggregateRequest = lake_rows.ExpressionAggregateRequest;
pub const LakeRowsExpressionAggregateResult = lake_rows.ExpressionAggregateResult;
pub const LakeRowsScanRequest = lake_rows.ScanRequest;
pub const LakeRowsScanResult = lake_rows.ScanResult;
pub const LakeRowsSidecarCandidateSet = lake_rows.SidecarCandidateSet;
pub const LakeSidecarDesired = lake_sidecar_selection.DesiredSidecar;
pub const LakeSidecarSelectionPolicy = lake_sidecar_selection.Policy;
pub const discoverLakeParquetSupportedI64ObjectRangeRowGroupsFromFootersAlloc = lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc;
pub const executeLakeParquetSupportedI64ObjectRangeExpressionAggregatesAlloc = lake_parquet_rowgroup.executeSupportedI64ObjectRangeExpressionAggregatesAlloc;
pub const pinLakeIcebergInventoryDataFileObjectVersionsAlloc = lake_iceberg_snapshot.pinInventoryDataFileObjectVersions;
pub const planProjectedLakeScanAlloc = lake_scan_plan.planProjectedScanAlloc;
pub const queryLakeParquetSupportedI64ObjectRangeRowsAlloc = lake_parquet_rowgroup.querySupportedI64ObjectRangeRowsAlloc;
pub const readLakeIcebergDeleteRowRefsAlloc = lake_iceberg_snapshot.readDeleteRowRefsAlloc;
pub const validateLakeBindingInventory = lake_scan_plan.validateBindingInventory;
