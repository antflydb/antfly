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

const std = @import("std");

const catalog_resources = @import("catalog_resources.zig");
const db_mod = @import("../storage/db/mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const relational_rows = @import("../api/relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const sql_adapter = @import("mod.zig");
const table_catalog = @import("../api/table_catalog.zig");
const transactions_mod = @import("../storage/transactions.zig");
const usermgr = @import("../usermgr/mod.zig");

// Runtime SQL adapter bridge for storage and API execution callers.
//
// sql/mod.zig owns pure SQL language and planning APIs. This bridge is
// intentionally allowed to depend on API and storage modules while
// storage-backed execution helpers keep their current owning modules.

pub const default_array_agg_max_items: u32 = db_mod.types.default_relational_rows_array_agg_max_items;
pub const DocumentAlgebraicAggregatePlan = sql_adapter.DocumentAlgebraicAggregatePlan;
pub const DocumentAggregateInput = sql_adapter.DocumentAggregateInput;
pub const DocumentAggregateGroupBy = sql_adapter.DocumentAggregateGroupBy;
pub const DocumentAggregateOp = sql_adapter.DocumentAggregateOp;
pub const BoundedDocumentScan = sql_adapter.BoundedDocumentScan;
pub const DocumentIndexQuery = sql_adapter.DocumentIndexQuery;
pub const DocumentOrderBy = sql_adapter.DocumentOrderBy;
pub const DocumentOrderDirection = sql_adapter.DocumentOrderDirection;
pub const DocumentProjection = sql_adapter.DocumentProjection;
pub const DocumentReadPlan = sql_adapter.DocumentReadPlan;
pub const DocumentUnnest = sql_adapter.DocumentUnnest;
pub const SqlValue = sql_adapter.SqlValue;
const AggregateFilter = sql_adapter.AggregateFilter;
pub const ExtensionFunctionBinding = sql_adapter.ExtensionFunctionBinding;
pub const RoutineExpressionBinding = sql_adapter.RoutineExpressionBinding;
pub const SqlFunctionBindings = sql_adapter.SqlFunctionBindings;
const SqlIntervalLiteral = sql_adapter.SqlIntervalLiteral;
const SelectOutputKind = sql_adapter.SelectOutputKind;
pub const SelectSetOperation = sql_adapter.SelectSetOperation;
const SqlPatternQuantifier = sql_adapter.SqlPatternQuantifier;
const SqlRowClaimClause = sql_adapter.SqlRowClaimClause;
const aggregateOpName = sql_adapter.aggregateOpName;
const aggregateOutputColumnExists = sql_adapter.aggregateOutputColumnExists;
const aggregateDescendingPercentileCount = sql_adapter.aggregateDescendingPercentileCount;
const aggregateFilterExpressionArrayCount = sql_adapter.aggregateFilterExpressionArrayCount;
const aggregateFilterExpressionCount = sql_adapter.aggregateFilterExpressionCount;
const aggregateFilterJsonAccessCount = sql_adapter.aggregateFilterJsonAccessCount;
const aggregateFilterStructuredAccessCount = sql_adapter.aggregateFilterStructuredAccessCount;
const aggregateInputExpressionCount = sql_adapter.aggregateInputExpressionCount;
const aggregateModeCount = sql_adapter.aggregateModeCount;
const aggregatePercentileArrayCount = sql_adapter.aggregatePercentileArrayCount;
const alterRelationalColumnDefaultAlloc = sql_adapter.alterRelationalColumnDefaultAlloc;
const alterRelationalColumnNullability = sql_adapter.alterRelationalColumnNullability;
const alterRelationalColumnTypeAlloc = sql_adapter.alterRelationalColumnTypeAlloc;
const applyAlterTablePlanToSchemaJsonValue = sql_adapter.applyAlterTablePlanToSchemaJsonValue;
const applyCommentMetadataPlanToSchemaJsonValue = sql_adapter.applyCommentMetadataPlanToSchemaJsonValue;
const applyCreateIndexPlanToSchemaJsonValue = sql_adapter.applyCreateIndexPlanToSchemaJsonValue;
const applyCreateUpdatePolicyPlanToSchemaJsonValue = sql_adapter.applyCreateUpdatePolicyPlanToSchemaJsonValue;
const applyDropIndexPlanToSchemaJsonValue = sql_adapter.applyDropIndexPlanToSchemaJsonValue;
const appendBoolFingerprintAlloc = sql_adapter.appendBoolFingerprintAlloc;
const appendNamedNonZeroUsizeFingerprintAlloc = sql_adapter.appendNamedNonZeroUsizeFingerprintAlloc;
const appendNonZeroU32FingerprintAlloc = sql_adapter.appendNonZeroU32FingerprintAlloc;
const appendNonZeroUsizeFingerprintAlloc = sql_adapter.appendNonZeroUsizeFingerprintAlloc;
const appendForeignKeyAlloc = sql_adapter.appendForeignKeyAlloc;
const ddlAppliedFingerprintAlloc = sql_adapter.ddlAppliedFingerprintAlloc;
const ddlFingerprintAlloc = sql_adapter.ddlFingerprintAlloc;
const appendRelationalCheckAlloc = sql_adapter.appendRelationalCheckAlloc;
const appendRelationalColumnAlloc = sql_adapter.appendRelationalColumnAlloc;
const appendStringFingerprintAlloc = sql_adapter.appendStringFingerprintAlloc;
const appendTrueBoolFingerprintAlloc = sql_adapter.appendTrueBoolFingerprintAlloc;
const appendUniqueConstraintAlloc = sql_adapter.appendUniqueConstraintAlloc;
const clearDdlForeignKeys = sql_adapter.clearDdlForeignKeys;
const clearDdlRelationalChecks = sql_adapter.clearDdlRelationalChecks;
const clearDdlRelationalColumns = sql_adapter.clearDdlRelationalColumns;
const clearDdlUniqueConstraints = sql_adapter.clearDdlUniqueConstraints;
const cloneExpressionAlloc = sql_adapter.cloneExpressionAlloc;
const cloneExpressionConditionAlloc = sql_adapter.cloneExpressionConditionAlloc;
const cloneExpressionConditionsConcatAlloc = sql_adapter.cloneExpressionConditionsConcatAlloc;
const cloneDdlDefaultValue = sql_adapter.cloneDdlDefaultValue;
const cloneDdlForeignKey = sql_adapter.cloneDdlForeignKey;
const cloneDdlForeignKeys = sql_adapter.cloneDdlForeignKeys;
const cloneDdlPeriod = sql_adapter.cloneDdlPeriod;
const cloneDdlPeriods = sql_adapter.cloneDdlPeriods;
const cloneDdlPrimaryKey = sql_adapter.cloneDdlPrimaryKey;
const cloneDdlPrimaryKeyMaybe = sql_adapter.cloneDdlPrimaryKeyMaybe;
const cloneDdlRelationalCheck = sql_adapter.cloneDdlRelationalCheck;
const cloneDdlRelationalChecks = sql_adapter.cloneDdlRelationalChecks;
const cloneDdlRelationalColumn = sql_adapter.cloneDdlRelationalColumn;
const cloneDdlRelationalColumns = sql_adapter.cloneDdlRelationalColumns;
const cloneDdlUniqueConstraint = sql_adapter.cloneDdlUniqueConstraint;
const cloneDdlUniqueConstraints = sql_adapter.cloneDdlUniqueConstraints;
const cloneDdlUniquePredicates = sql_adapter.cloneDdlUniquePredicates;
const cloneEmptyRuntimeSchemaAlloc = sql_adapter.cloneEmptyRuntimeSchemaAlloc;
const cloneRelationalRuntimeSchemaAlloc = sql_adapter.cloneRelationalRuntimeSchemaAlloc;
const cloneInPredicateAlloc = sql_adapter.cloneInPredicateAlloc;
const cloneInPredicatesAlloc = sql_adapter.cloneInPredicatesAlloc;
const cloneOrderByAlloc = sql_adapter.cloneOrderByAlloc;
const cloneQueryRelationalChecksAlloc = sql_adapter.cloneQueryRelationalChecksAlloc;
const cloneSelectOutputsAlloc = sql_adapter.cloneSelectOutputsAlloc;
const cloneStringSlice = sql_adapter.cloneStringSlice;
const cursorFetchDirectionFromSyntax = sql_adapter.cursorFetchDirectionFromSyntax;
const cursorScrollModeFromSyntax = sql_adapter.cursorScrollModeFromSyntax;
const createTablePlanFromTableCloneSourceAlloc = sql_adapter.createTablePlanFromTableCloneSourceAlloc;
const createIndexPlanGeneratedExpressionCount = sql_adapter.createIndexPlanGeneratedExpressionCount;
const expressionConditionReferencesField = sql_adapter.expressionConditionReferencesField;
const expressionGroupsFromInSetQueryAlloc = sql_adapter.expressionGroupsFromInSetQueryAlloc;
const expressionOrderCount = sql_adapter.expressionOrderCount;
const expressionReferencesField = sql_adapter.expressionReferencesField;
const fieldValueJsonFor = sql_adapter.fieldValueJsonFor;
const findCteByName = sql_adapter.findCteByName;
const freeAccessPredicateGroup = sql_adapter.freeAccessPredicateGroup;
const freeAccessPredicateGroups = sql_adapter.freeAccessPredicateGroups;
const freeAggregateSpecs = sql_adapter.freeAggregateSpecs;
const freeArrayLengthProjections = sql_adapter.freeArrayLengthProjections;
const freeArrayAny = sql_adapter.freeArrayAny;
const freeArrayContains = sql_adapter.freeArrayContains;
const freeArrayEq = sql_adapter.freeArrayEq;
const freeCoalesceProjection = sql_adapter.freeCoalesceProjection;
const freeCoalesceProjections = sql_adapter.freeCoalesceProjections;
const freeExpression = sql_adapter.freeExpression;
const freeExpressionArrayContains = sql_adapter.freeExpressionArrayContains;
const freeExpressionCaseBranch = sql_adapter.freeExpressionCaseBranch;
const freeExpressionCondition = sql_adapter.freeExpressionCondition;
const freeExpressionConditions = sql_adapter.freeExpressionConditions;
const freeExpressionPredicateGroup = sql_adapter.freeExpressionPredicateGroup;
const freeExpressionPredicateGroups = sql_adapter.freeExpressionPredicateGroups;
const freeExpressionProjection = sql_adapter.freeExpressionProjection;
const freeExpressionProjections = sql_adapter.freeExpressionProjections;
const freeExpressionSlice = sql_adapter.freeExpressionSlice;
const freeAlterTableOperation = sql_adapter.freeAlterTableOperation;
const freeDdlForeignKey = sql_adapter.freeDdlForeignKey;
const freeDdlForeignKeys = sql_adapter.freeDdlForeignKeys;
const freeDdlGeneratedValue = sql_adapter.freeDdlGeneratedValue;
const freeDdlPeriod = sql_adapter.freeDdlPeriod;
const freeDdlPeriods = sql_adapter.freeDdlPeriods;
const freeDdlPrimaryKey = sql_adapter.freeDdlPrimaryKey;
const freeDdlRelationalCheck = sql_adapter.freeDdlRelationalCheck;
const freeDdlRelationalChecks = sql_adapter.freeDdlRelationalChecks;
const freeDdlRelationalColumn = sql_adapter.freeDdlRelationalColumn;
const freeDdlUniqueConstraint = sql_adapter.freeDdlUniqueConstraint;
const freeDdlUniqueConstraints = sql_adapter.freeDdlUniqueConstraints;
const freeDdlUniquePredicates = sql_adapter.freeDdlUniquePredicates;
const freeFieldAliasProjections = sql_adapter.freeFieldAliasProjections;
const freeInPredicates = sql_adapter.freeInPredicates;
const freeJoinProjections = sql_adapter.freeJoinProjections;
const freeJsonExtract = sql_adapter.freeJsonExtract;
const freeJsonContains = sql_adapter.freeJsonContains;
const freeJsonPathEq = sql_adapter.freeJsonPathEq;
const freeJsonPathExists = sql_adapter.freeJsonPathExists;
const freeLateralSubquery = sql_adapter.freeLateralSubquery;
const freeOrderBy = sql_adapter.freeOrderBy;
const freePredicateGroup = sql_adapter.freePredicateGroup;
const freePredicateGroups = sql_adapter.freePredicateGroups;
const freeQualifiedField = sql_adapter.freeQualifiedField;
const freeQualifiedProjections = sql_adapter.freeQualifiedProjections;
const freeArrayTransformValues = sql_adapter.freeArrayTransformValues;
const freeConflictClause = sql_adapter.freeConflictClause;
const freeConflictTarget = sql_adapter.freeConflictTarget;
const freeStringSlice = sql_adapter.freeStringSlice;
const freeTableAlias = sql_adapter.freeTableAlias;
const freeTextPatterns = sql_adapter.freeTextPatterns;
const freeTokens = sql_adapter.freeTokens;
const freeWindowSpecs = sql_adapter.freeWindowSpecs;
const generatedColumnReferencesAny = sql_adapter.generatedColumnReferencesAny;
const identifierContainsQualifier = sql_adapter.identifierContainsQualifier;
const isCaseFoldExpressionOp = sql_adapter.isCaseFoldExpressionOp;
const stringSlicesContains = sql_adapter.stringSlicesContains;
const stringSlicesIntersect = sql_adapter.stringSlicesIntersect;
const sqlExpressionResultTypesCompatible = sql_adapter.sqlExpressionResultTypesCompatible;
const sqlExpressionTypeIsOrderKey = sql_adapter.sqlExpressionTypeIsOrderKey;
const sqlExpressionTypeIsOrderable = sql_adapter.sqlExpressionTypeIsOrderable;
const sqlExpressionTypeIsTextLike = sql_adapter.sqlExpressionTypeIsTextLike;
const sqlExpressionTypesComparable = sql_adapter.sqlExpressionTypesComparable;
const sqlExpressionContainsInterval = sql_adapter.sqlExpressionContainsInterval;
const sqlExpressionIsInterval = sql_adapter.sqlExpressionIsInterval;
const jsonSetTypedTransformPathAlloc = sql_adapter.jsonSetTypedTransformPathAlloc;
const joinSideForQualifier = sql_adapter.joinSideForQualifier;
const markColumnIndexedAlloc = sql_adapter.markColumnIndexedAlloc;
const markColumnsIndexedAlloc = sql_adapter.markColumnsIndexedAlloc;
const mergeSourceQueryIsDefault = sql_adapter.mergeSourceQueryIsDefault;
const dropIndexFromRuntimeSchemaAlloc = sql_adapter.dropIndexFromRuntimeSchemaAlloc;
const dropRelationalColumnAlloc = sql_adapter.dropRelationalColumnAlloc;
const dropRelationalConstraintAlloc = sql_adapter.dropRelationalConstraintAlloc;
const windowDefaultCount = sql_adapter.windowDefaultCount;
const windowFilterAccessCount = sql_adapter.windowFilterAccessCount;
const windowFilterExpressionCount = sql_adapter.windowFilterExpressionCount;
const windowFilterGroupCount = sql_adapter.windowFilterGroupCount;
const windowFilterPredicateCount = sql_adapter.windowFilterPredicateCount;
const windowFrameSignature = sql_adapter.windowFrameSignature;
const windowValueExpressionCount = sql_adapter.windowValueExpressionCount;
const advisoryLockActionFromSyntax = sql_adapter.advisoryLockActionFromSyntax;
pub const bulkSqlIoExecutionFingerprintAlloc = sql_adapter.bulkSqlIoExecutionFingerprintAlloc;
pub const bulkSqlIoExecutionPlanFromDdlPlan = sql_adapter.bulkSqlIoExecutionPlanFromDdlPlan;
pub const bulkSqlIoExportRowsCsvToStdoutAlloc = sql_adapter.bulkSqlIoExportRowsCsvToStdoutAlloc;
pub const bulkSqlIoImportRowsBatchFromStdinAlloc = sql_adapter.bulkSqlIoImportRowsBatchFromStdinAlloc;
const constraintCheckModeFromSyntax = sql_adapter.constraintCheckModeFromSyntax;
const defaultPrimaryKeyNameEquals = sql_adapter.defaultPrimaryKeyNameEquals;
const findUniqueConstraintByColumns = sql_adapter.findUniqueConstraintByColumns;
const findUniqueConstraintByExpression = sql_adapter.findUniqueConstraintByExpression;
const foreignKeyActionName = sql_adapter.foreignKeyActionName;
const foreignKeyMatchName = sql_adapter.foreignKeyMatchName;
const foreignKeyNameExists = sql_adapter.foreignKeyNameExists;
const foreignKeyTimingName = sql_adapter.foreignKeyTimingName;
const foreignKeyValidationStateName = sql_adapter.foreignKeyValidationStateName;
const preparedStatementStatementKindFromSyntax = sql_adapter.preparedStatementStatementKindFromSyntax;
const preparedStatementSubjectKindFromSyntax = sql_adapter.preparedStatementSubjectKindFromSyntax;
const primaryKeyNameEquals = sql_adapter.primaryKeyNameEquals;
const queryHasOnlySimpleIntersectExceptPredicateSurface = sql_adapter.queryHasOnlySimpleIntersectExceptPredicateSurface;
const queryHasOnlySimpleUnionPredicateSurface = sql_adapter.queryHasOnlySimpleUnionPredicateSurface;
const reindexMaintenanceTargetFromSyntax = sql_adapter.reindexMaintenanceTargetFromSyntax;
const sourceQueryUsesExtendedPredicates = sql_adapter.sourceQueryUsesExtendedPredicates;
const relationalCheckNameExists = sql_adapter.relationalCheckNameExists;
const relationalColumnForDdl = sql_adapter.relationalColumnForDdl;
const relationalColumnForField = sql_adapter.relationalColumnForField;
const relationalColumnHasIndexName = sql_adapter.relationalColumnHasIndexName;
const relationalColumnIndex = sql_adapter.relationalColumnIndex;
const relationalColumnIndexForIndexName = sql_adapter.relationalColumnIndexForIndexName;
const relationalConstraintNameExists = sql_adapter.relationalConstraintNameExists;
const relationalCheckOpToken = sql_adapter.relationalCheckOpToken;
const relationalCheckOpFromUniquePredicateToken = sql_adapter.relationalCheckOpFromUniquePredicateToken;
const relationalCheckValidationStateName = sql_adapter.relationalCheckValidationStateName;
const relationalGeneratedOpForUniqueExpressionOp = sql_adapter.relationalGeneratedOpForUniqueExpressionOp;
const relationalIndexNameExists = sql_adapter.relationalIndexNameExists;
const relationalIndexLifecycleName = sql_adapter.relationalIndexLifecycleName;
const relationalPeriodRangeTypeName = sql_adapter.relationalPeriodRangeTypeName;
const relationLifetimeKindName = sql_adapter.relationLifetimeKindName;
const renameRelationalColumnAlloc = sql_adapter.renameRelationalColumnAlloc;
const renameRelationalConstraintAlloc = sql_adapter.renameRelationalConstraintAlloc;
const rewriteExpressionConditionFieldsToSource = sql_adapter.rewriteExpressionConditionFieldsToSource;
const putJsonString = sql_adapter.putJsonString;
const schemaJsonExpressionConditionsAlloc = sql_adapter.schemaJsonExpressionConditionsAlloc;
const schemaJsonForeignKeysAlloc = sql_adapter.schemaJsonForeignKeysAlloc;
const schemaJsonPeriodsAlloc = sql_adapter.schemaJsonPeriodsAlloc;
const schemaJsonPrimaryKeyNameEquals = sql_adapter.schemaJsonPrimaryKeyNameEquals;
const schemaJsonRelationalChecksAlloc = sql_adapter.schemaJsonRelationalChecksAlloc;
const schemaJsonUniqueConstraintsAlloc = sql_adapter.schemaJsonUniqueConstraintsAlloc;
const schemaJsonUniquePredicateDefinitionAlloc = sql_adapter.schemaJsonUniquePredicateDefinitionAlloc;
const validateDdlAppliedSchemaJsonAlloc = sql_adapter.validateDdlAppliedSchemaJsonAlloc;
const schemaJsonFromTableClonePlanAlloc = sql_adapter.schemaJsonFromTableClonePlanAlloc;
const schemaJsonValueFromCreateTablePlanAlloc = sql_adapter.schemaJsonValueFromCreateTablePlanAlloc;
const setColumnOnUpdatePolicyAlloc = sql_adapter.setColumnOnUpdatePolicyAlloc;
const setSqlRowClaimClause = sql_adapter.setSqlRowClaimClause;
const jsonSetHasExpression = sql_adapter.jsonSetHasExpression;
const tableSchemaCatalogExists = sql_adapter.tableSchemaCatalogExists;
const tableLockModeFromSyntax = sql_adapter.tableLockModeFromSyntax;
const transactionAccessModeFromSyntax = sql_adapter.transactionAccessModeFromSyntax;
const transactionIsolationLevelFromSyntax = sql_adapter.transactionIsolationLevelFromSyntax;
const transactionModeStarterFromSyntax = sql_adapter.transactionModeStarterFromSyntax;
const simpleAccessSetQueryBranchCount = sql_adapter.simpleAccessSetQueryBranchCount;
const simpleScalarSetQueryBranchAt = sql_adapter.simpleScalarSetQueryBranchAt;
const simpleScalarSetQueryBranchCount = sql_adapter.simpleScalarSetQueryBranchCount;
const selectorCompareJsonScalars = sql_adapter.selectorCompareJsonScalars;
const selectorExpressionValueJsonAlloc = sql_adapter.selectorExpressionValueJsonAlloc;
const selectorJsonValuesEqual = sql_adapter.selectorJsonValuesEqual;
const sequenceOptionCount = sql_adapter.sequenceOptionCount;
const sqlRowClaimFingerprintName = sql_adapter.sqlRowClaimFingerprintName;
const sqlRowClaimForClause = sql_adapter.sqlRowClaimForClause;
const sqlScalarValueMatches = sql_adapter.sqlScalarValueMatches;
const tokenKindIsJsonExtractOperator = sql_adapter.tokenKindIsJsonExtractOperator;
const tokenKindIsJsonExtractPathOperator = sql_adapter.tokenKindIsJsonExtractPathOperator;
const tokenizeAlloc = sql_adapter.tokenizeAlloc;
const uniqueConstraintNameExists = sql_adapter.uniqueConstraintNameExists;
const uniqueConstraintReferencesAny = sql_adapter.uniqueConstraintReferencesAny;
const uniqueConstraintValidationStateString = sql_adapter.uniqueConstraintValidationStateString;
const uniqueExpressionsEqual = sql_adapter.uniqueExpressionsEqual;
const updateWillLookupExistingRow = sql_adapter.updateWillLookupExistingRow;
const validateCheckForColumns = sql_adapter.validateCheckForColumns;
const validateCommentMetadataPlanForRuntimeSchemaAlloc = sql_adapter.validateCommentMetadataPlanForRuntimeSchemaAlloc;
const validateCreateIndexIncludeColumns = sql_adapter.validateCreateIndexIncludeColumns;
const validateSqlUpdateTargetPaths = sql_adapter.validateSqlUpdateTargetPaths;
const validateAggregateGroupBy = sql_adapter.validateAggregateGroupBy;
const validateBulkSqlIoCsvOptions = sql_adapter.validateBulkSqlIoCsvOptions;
const validateBulkSqlIoPlanForSchema = sql_adapter.validateBulkSqlIoPlanForSchema;
const validateConstraintByName = sql_adapter.validateConstraintByName;
const validateDefaultValueForColumnAlloc = sql_adapter.validateDefaultValueForColumnAlloc;
const validateGeneratedColumnForColumns = sql_adapter.validateGeneratedColumnForColumns;
const validateForeignKeyForColumns = sql_adapter.validateForeignKeyForColumns;
const validateForeignKeyCatalog = sql_adapter.validateForeignKeyCatalog;
const validatePrimaryKeyColumns = sql_adapter.validatePrimaryKeyColumns;
const validatePrimaryKeyTemporalCatalog = sql_adapter.validatePrimaryKeyTemporalCatalog;
const validateRelationalCheckCatalog = sql_adapter.validateRelationalCheckCatalog;
const validateRelationalColumnCatalog = sql_adapter.validateRelationalColumnCatalog;
const validateRelationalPeriodCatalog = sql_adapter.validateRelationalPeriodCatalog;
const validateUniqueConstraintCatalog = sql_adapter.validateUniqueConstraintCatalog;
const validateUniqueConstraintForColumns = sql_adapter.validateUniqueConstraintForColumns;
const validateUniquePredicatesForColumns = sql_adapter.validateUniquePredicatesForColumns;
const validateUniquePredicateExpressionsForColumns = sql_adapter.validateUniquePredicateExpressionsForColumns;
const writeInPredicateAtomsJson = sql_adapter.writeInPredicateAtomsJson;
const writeJsonPathEqPredicateAtomsJson = sql_adapter.writeJsonPathEqPredicateAtomsJson;
const writeJsonPathExistsPredicateAtomsJson = sql_adapter.writeJsonPathExistsPredicateAtomsJson;
const writeRelationalCheckAtomJson = sql_adapter.writeRelationalCheckAtomJson;
const writeRelationalCheckAtomsJson = sql_adapter.writeRelationalCheckAtomsJson;
const writeRowExpressionConditionJson = sql_adapter.writeRowExpressionConditionJson;
const writeStructuredValuePredicateAtomsJson = sql_adapter.writeStructuredValuePredicateAtomsJson;
const writeTextPatternPredicateAtomsJson = sql_adapter.writeTextPatternPredicateAtomsJson;
const ScalarOrCheckBranch = sql_adapter.ScalarOrCheckBranch;
const conflictActionName = sql_adapter.conflictActionName;
const expressionAssignmentComputedCount = sql_adapter.expressionAssignmentComputedCount;
const rowExpressionBoundaryKeyword = sql_adapter.rowExpressionBoundaryKeyword;
const sqlAssignmentTailKeyword = sql_adapter.sqlAssignmentTailKeyword;
const sqlJoinedSourceAliasTerminator = sql_adapter.sqlJoinedSourceAliasTerminator;
const sqlKeywordIsArrayLengthFunction = sql_adapter.sqlKeywordIsArrayLengthFunction;
const sqlKeywordIsArrayPositionFunction = sql_adapter.sqlKeywordIsArrayPositionFunction;
const sqlKeywordIsArrayToStringFunction = sql_adapter.sqlKeywordIsArrayToStringFunction;
const sqlKeywordIsAsciiFunction = sql_adapter.sqlKeywordIsAsciiFunction;
const sqlKeywordIsBitLengthFunction = sql_adapter.sqlKeywordIsBitLengthFunction;
const sqlKeywordIsChrFunction = sql_adapter.sqlKeywordIsChrFunction;
const sqlKeywordIsInitcapFunction = sql_adapter.sqlKeywordIsInitcapFunction;
const sqlKeywordIsJsonArrayLengthFunction = sql_adapter.sqlKeywordIsJsonArrayLengthFunction;
const sqlKeywordIsJsonTypeofFunction = sql_adapter.sqlKeywordIsJsonTypeofFunction;
const sqlKeywordIsLengthFunction = sql_adapter.sqlKeywordIsLengthFunction;
const sqlKeywordIsOctetLengthFunction = sql_adapter.sqlKeywordIsOctetLengthFunction;
const stableSecondaryIndexGeneration = sql_adapter.stableSecondaryIndexGeneration;
const sqlKeywordIsTrimVariantFunction = sql_adapter.sqlKeywordIsTrimVariantFunction;
const sqlKeywordStartsScalarPredicate = sql_adapter.sqlKeywordStartsScalarPredicate;

pub const LoweredSelect = sql_adapter.LoweredSelect;
pub const LoweredQueryPlan = sql_adapter.LoweredQueryPlan;
pub const LoweredRecursiveCteJoinMemberPlan = sql_adapter.LoweredRecursiveCteJoinMemberPlan;
pub const LoweredRecursiveCteMemberPlan = sql_adapter.LoweredRecursiveCteMemberPlan;
pub const LoweredRecursiveCtePlan = sql_adapter.LoweredRecursiveCtePlan;
pub const LoweredSetOperationPlan = sql_adapter.LoweredSetOperationPlan;
pub const LoweredWindowPlan = sql_adapter.LoweredWindowPlan;

pub const LoweredInsert = sql_adapter.LoweredInsert;
pub const LoweredInsertSource = sql_adapter.LoweredInsertSource;
pub const LoweredRecursiveInsertSource = sql_adapter.LoweredRecursiveInsertSource;
pub const LoweredMutation = sql_adapter.LoweredMutation;
const ReturningProjection = sql_adapter.ReturningProjection;
pub const LoweredMutationSource = sql_adapter.LoweredMutationSource;
pub const LoweredJoinedMutationSource = sql_adapter.LoweredJoinedMutationSource;
pub const LoweredRecursiveJoinedMutationSource = sql_adapter.LoweredRecursiveJoinedMutationSource;
const ArrayTransformValue = sql_adapter.ArrayTransformValue;
const AggregateSelectOutputKind = sql_adapter.AggregateSelectOutputKind;
const ConflictAction = sql_adapter.ConflictAction;
const ConflictClause = sql_adapter.ConflictClause;
const ConflictTarget = sql_adapter.ConflictTarget;
const conflictActionToken = sql_adapter.conflictActionToken;
const JoinedMutationExpressionSide = sql_adapter.JoinedMutationExpressionSide;
const JsonSetParsedValue = sql_adapter.JsonSetParsedValue;
const QualifiedField = sql_adapter.QualifiedField;
const QualifiedProjection = sql_adapter.QualifiedProjection;
const SelectList = sql_adapter.SelectList;
const SelectItem = sql_adapter.SelectItem;
const SetOperationResultTail = sql_adapter.SetOperationResultTail;
const TableAlias = sql_adapter.TableAlias;
const UniqueConflictTarget = sql_adapter.UniqueConflictTarget;
const WindowSelectOutputKind = sql_adapter.WindowSelectOutputKind;
pub const MergeFieldMapping = sql_adapter.MergeFieldMapping;
pub const MergeExpressionAssignment = sql_adapter.MergeExpressionAssignment;

pub const MergePredicateSide = sql_adapter.MergePredicateSide;
pub const MergeArmPredicate = sql_adapter.MergeArmPredicate;
pub const MergeMatchedArm = sql_adapter.MergeMatchedArm;
pub const MergeNotMatchedArm = sql_adapter.MergeNotMatchedArm;
pub const MergeExecutionTargetRow = sql_adapter.MergeExecutionTargetRow;
pub const LoweredMergeMutationPlan = sql_adapter.LoweredMergeMutationPlan;
pub const LoweredRecursiveMergeMutation = sql_adapter.LoweredRecursiveMergeMutation;
pub const buildMergeMutationBatchAlloc = sql_adapter.buildMergeMutationBatchAlloc;
pub const LoweredWritePlan = sql_adapter.LoweredWritePlan;

pub const LowerWritePlanOptions = sql_adapter.LowerWritePlanOptions;

pub const LoweredAggregate = sql_adapter.LoweredAggregate;
pub const LoweredAggregatePlan = sql_adapter.LoweredAggregatePlan;
pub const LoweredJoin = sql_adapter.LoweredJoin;
pub const LoweredLateralPlan = sql_adapter.LoweredLateralPlan;
pub const LoweredReadPlan = sql_adapter.LoweredReadPlan;

pub const LoweredExplainPlan = sql_adapter.LoweredExplainPlan;
pub const LoweredExplainSubject = sql_adapter.LoweredExplainSubject;
pub const ExplainFormat = sql_adapter.ExplainFormat;

pub const LoweredRelationPopulationPlan = sql_adapter.LoweredRelationPopulationPlan;
pub const RelationPopulationMode = sql_adapter.RelationPopulationMode;

pub const AdapterNoopDdlReason = sql_adapter.AdapterNoopDdlReason;
pub const AdapterNoopDdlPlan = sql_adapter.AdapterNoopDdlPlan;
pub const SessionCatalogPlan = sql_adapter.SessionCatalogPlan;
pub const SetSearchPathPlan = sql_adapter.SetSearchPathPlan;
pub const EnumTypeCatalogPlan = sql_adapter.EnumTypeCatalogPlan;
pub const CreateEnumTypePlan = sql_adapter.CreateEnumTypePlan;
pub const AddEnumValuePlan = sql_adapter.AddEnumValuePlan;
pub const EnumValuePosition = sql_adapter.EnumValuePosition;
pub const DropEnumTypePlan = sql_adapter.DropEnumTypePlan;
pub const SchemaNamespaceCatalogPlan = sql_adapter.SchemaNamespaceCatalogPlan;
pub const CreateSchemaNamespacePlan = sql_adapter.CreateSchemaNamespacePlan;
pub const RenameSchemaNamespacePlan = sql_adapter.RenameSchemaNamespacePlan;
pub const DropSchemaNamespacePlan = sql_adapter.DropSchemaNamespacePlan;
pub const ExtensionCatalogPlan = sql_adapter.ExtensionCatalogPlan;
pub const CreateExtensionPlan = sql_adapter.CreateExtensionPlan;
pub const UpdateExtensionPlan = sql_adapter.UpdateExtensionPlan;
pub const DropExtensionPlan = sql_adapter.DropExtensionPlan;
pub const SequenceCatalogPlan = sql_adapter.SequenceCatalogPlan;
pub const CreateSequencePlan = sql_adapter.CreateSequencePlan;
pub const AlterSequencePlan = sql_adapter.AlterSequencePlan;
pub const DropSequencePlan = sql_adapter.DropSequencePlan;
pub const SequenceOptions = sql_adapter.SequenceOptions;
pub const SequenceOwnedBy = sql_adapter.SequenceOwnedBy;
pub const SequenceAlterOperation = sql_adapter.SequenceAlterOperation;
pub const FunctionCatalogPlan = sql_adapter.FunctionCatalogPlan;
pub const CreateRoutinePlan = sql_adapter.CreateRoutinePlan;
pub const DropRoutinePlan = sql_adapter.DropRoutinePlan;
pub const RoutineBodyKind = sql_adapter.RoutineBodyKind;
pub const RoutineBodyPlan = sql_adapter.RoutineBodyPlan;
pub const RoutineExecutionHook = sql_adapter.RoutineExecutionHook;
pub const RoutinePerformCall = sql_adapter.RoutinePerformCall;
pub const RoutineKind = sql_adapter.RoutineKind;
pub const RoutineNullInput = sql_adapter.RoutineNullInput;
pub const RoutineParallelSafety = sql_adapter.RoutineParallelSafety;
pub const RoutineSecurity = sql_adapter.RoutineSecurity;
pub const RoutineSetting = sql_adapter.RoutineSetting;
pub const RoutineVolatility = sql_adapter.RoutineVolatility;
pub const AuthorizationCatalogPlan = sql_adapter.AuthorizationCatalogPlan;
pub const CreateRolePlan = sql_adapter.CreateRolePlan;
pub const AlterRolePlan = sql_adapter.AlterRolePlan;
pub const DropRolePlan = sql_adapter.DropRolePlan;
pub const PrivilegeChangePlan = sql_adapter.PrivilegeChangePlan;
pub const PreparedStatementPlan = sql_adapter.PreparedStatementPlan;
pub const PrepareStatementPlan = sql_adapter.PrepareStatementPlan;
pub const PreparedStatementSubjectKind = sql_adapter.PreparedStatementSubjectKind;
pub const PreparedStatementStatementKind = sql_adapter.PreparedStatementStatementKind;
pub const ExecutePreparedStatementPlan = sql_adapter.ExecutePreparedStatementPlan;
pub const DeallocatePreparedStatementPlan = sql_adapter.DeallocatePreparedStatementPlan;
pub const PreparedTransactionPlan = sql_adapter.PreparedTransactionPlan;
pub const PreparedTransactionAction = sql_adapter.PreparedTransactionAction;
pub const PreparedTransactionRecoveryOperation = sql_adapter.PreparedTransactionRecoveryOperation;
pub const PreparedTransactionRecoveryIntent = sql_adapter.PreparedTransactionRecoveryIntent;
pub const PreparedTransactionCoordinatorResult = sql_adapter.PreparedTransactionCoordinatorResult;
pub const preparedTransactionRecoveryIntentFromPlan = sql_adapter.preparedTransactionRecoveryIntentFromPlan;
pub const preparedTransactionRecoveryFingerprintAlloc = sql_adapter.preparedTransactionRecoveryFingerprintAlloc;
pub const preparedTransactionTxnIdFromGid = sql_adapter.preparedTransactionTxnIdFromGid;
pub const executePreparedTransactionRecoveryPlan = sql_adapter.executePreparedTransactionRecoveryPlan;
pub const executePreparedTransactionRecoveryIntent = sql_adapter.executePreparedTransactionRecoveryIntent;
pub const CursorPortalPlan = sql_adapter.CursorPortalPlan;
pub const DeclareCursorPortalPlan = sql_adapter.DeclareCursorPortalPlan;
pub const CursorScrollMode = sql_adapter.CursorScrollMode;
pub const FetchCursorPortalPlan = sql_adapter.FetchCursorPortalPlan;
pub const CursorFetchDirection = sql_adapter.CursorFetchDirection;
pub const CloseCursorPortalPlan = sql_adapter.CloseCursorPortalPlan;
pub const SavepointTransactionPlan = sql_adapter.SavepointTransactionPlan;
pub const SavepointNamePlan = sql_adapter.SavepointNamePlan;
pub const CommentMetadataPlan = sql_adapter.CommentMetadataPlan;
pub const CommentMetadataTarget = sql_adapter.CommentMetadataTarget;
pub const TransactionControlPlan = sql_adapter.TransactionControlPlan;
pub const TableLockPlan = sql_adapter.TableLockPlan;
pub const TableLockMode = sql_adapter.TableLockMode;
pub const ConstraintModePlan = sql_adapter.ConstraintModePlan;
pub const ConstraintCheckMode = sql_adapter.ConstraintCheckMode;
pub const TransactionModePlan = sql_adapter.TransactionModePlan;
pub const TransactionModeStarter = sql_adapter.TransactionModeStarter;
pub const TransactionIsolationLevel = sql_adapter.TransactionIsolationLevel;
pub const TransactionAccessMode = sql_adapter.TransactionAccessMode;
pub const AdvisoryLockPlan = sql_adapter.AdvisoryLockPlan;
pub const AdvisoryLockAction = sql_adapter.AdvisoryLockAction;
pub const DatabaseCatalogPlan = sql_adapter.DatabaseCatalogPlan;
pub const CreateDatabasePlan = sql_adapter.CreateDatabasePlan;
pub const AlterDatabasePlan = sql_adapter.AlterDatabasePlan;
pub const DatabaseAlterOperation = sql_adapter.DatabaseAlterOperation;
pub const DatabaseSetParameterOperation = sql_adapter.DatabaseSetParameterOperation;
pub const DropDatabasePlan = sql_adapter.DropDatabasePlan;
pub const TablespaceCatalogPlan = sql_adapter.TablespaceCatalogPlan;
pub const CreateTablespacePlan = sql_adapter.CreateTablespacePlan;
pub const RenameTablespacePlan = sql_adapter.RenameTablespacePlan;
pub const DropTablespacePlan = sql_adapter.DropTablespacePlan;
pub const ProcedureCallPlan = sql_adapter.ProcedureCallPlan;
pub const NotificationChannelPlan = sql_adapter.NotificationChannelPlan;
pub const ListenNotificationPlan = sql_adapter.ListenNotificationPlan;
pub const NotifyNotificationPlan = sql_adapter.NotifyNotificationPlan;
pub const UnlistenNotificationPlan = sql_adapter.UnlistenNotificationPlan;
pub const LogicalReplicationPlan = sql_adapter.LogicalReplicationPlan;
pub const PublicationCatalogPlan = sql_adapter.PublicationCatalogPlan;
pub const CreatePublicationPlan = sql_adapter.CreatePublicationPlan;
pub const AlterPublicationPlan = sql_adapter.AlterPublicationPlan;
pub const PublicationAlterOperation = sql_adapter.PublicationAlterOperation;
pub const DropPublicationPlan = sql_adapter.DropPublicationPlan;
pub const SubscriptionCatalogPlan = sql_adapter.SubscriptionCatalogPlan;
pub const CreateSubscriptionPlan = sql_adapter.CreateSubscriptionPlan;
pub const AlterSubscriptionPlan = sql_adapter.AlterSubscriptionPlan;
pub const DropSubscriptionPlan = sql_adapter.DropSubscriptionPlan;
pub const TypeSystemCatalogPlan = sql_adapter.TypeSystemCatalogPlan;
pub const CollationCatalogPlan = sql_adapter.CollationCatalogPlan;
pub const CreateCollationPlan = sql_adapter.CreateCollationPlan;
pub const RenameCollationPlan = sql_adapter.RenameCollationPlan;
pub const DropCollationPlan = sql_adapter.DropCollationPlan;
pub const OperatorCatalogPlan = sql_adapter.OperatorCatalogPlan;
pub const CreateOperatorPlan = sql_adapter.CreateOperatorPlan;
pub const DropOperatorPlan = sql_adapter.DropOperatorPlan;
pub const AggregateCatalogPlan = sql_adapter.AggregateCatalogPlan;
pub const CreateAggregatePlan = sql_adapter.CreateAggregatePlan;
pub const DropAggregatePlan = sql_adapter.DropAggregatePlan;
pub const CastCatalogPlan = sql_adapter.CastCatalogPlan;
pub const CreateCastPlan = sql_adapter.CreateCastPlan;
pub const DropCastPlan = sql_adapter.DropCastPlan;
pub const MaintenanceJobPlan = sql_adapter.MaintenanceJobPlan;
pub const VacuumMaintenancePlan = sql_adapter.VacuumMaintenancePlan;
pub const AnalyzeMaintenancePlan = sql_adapter.AnalyzeMaintenancePlan;
pub const ReindexMaintenancePlan = sql_adapter.ReindexMaintenancePlan;
pub const ReindexMaintenanceTarget = sql_adapter.ReindexMaintenanceTarget;
pub const ClusterMaintenancePlan = sql_adapter.ClusterMaintenancePlan;
pub const BulkIoPlan = sql_adapter.BulkIoPlan;
pub const BulkIoDirection = sql_adapter.BulkIoDirection;
pub const BulkIoEndpointKind = sql_adapter.BulkIoEndpointKind;
pub const BulkIoOnErrorPolicy = sql_adapter.BulkIoOnErrorPolicy;
pub const BulkIoLogVerbosity = sql_adapter.BulkIoLogVerbosity;
pub const BulkSqlIoOperation = sql_adapter.BulkSqlIoOperation;
pub const BulkSqlIoNativeRoute = sql_adapter.BulkSqlIoNativeRoute;
pub const BulkSqlIoStream = sql_adapter.BulkSqlIoStream;
pub const BulkSqlIoCodec = sql_adapter.BulkSqlIoCodec;
pub const BulkSqlIoAuditAction = sql_adapter.BulkSqlIoAuditAction;
pub const BulkSqlIoExecutionPlan = sql_adapter.BulkSqlIoExecutionPlan;

pub const MaterializedViewCatalogPlan = sql_adapter.MaterializedViewCatalogPlan;
pub const CreateMaterializedViewPlan = sql_adapter.CreateMaterializedViewPlan;
pub const RefreshMaterializedViewPlan = sql_adapter.RefreshMaterializedViewPlan;
pub const DropMaterializedViewPlan = sql_adapter.DropMaterializedViewPlan;
pub const ViewCatalogPlan = sql_adapter.ViewCatalogPlan;
pub const CreateViewPlan = sql_adapter.CreateViewPlan;
pub const RenameViewPlan = sql_adapter.RenameViewPlan;
pub const DropViewPlan = sql_adapter.DropViewPlan;
pub const TableClonePlan = sql_adapter.TableClonePlan;
pub const TableCloneOptions = sql_adapter.TableCloneOptions;
pub const DropTablePlan = sql_adapter.DropTablePlan;
pub const DropIndexPlan = sql_adapter.DropIndexPlan;
pub const CreateUpdatePolicyPlan = sql_adapter.CreateUpdatePolicyPlan;
pub const RowSecurityCatalogPlan = sql_adapter.RowSecurityCatalogPlan;
pub const AlterRowSecurityPlan = sql_adapter.AlterRowSecurityPlan;
pub const CreateRowSecurityPolicyPlan = sql_adapter.CreateRowSecurityPolicyPlan;
pub const AlterRowSecurityPolicyPlan = sql_adapter.AlterRowSecurityPolicyPlan;
pub const DropRowSecurityPolicyPlan = sql_adapter.DropRowSecurityPolicyPlan;
pub const RowSecurityPolicyPredicate = sql_adapter.RowSecurityPolicyPredicate;
pub const RowSecurityCurrentSettingPredicate = sql_adapter.RowSecurityCurrentSettingPredicate;
pub const RowSecurityLiteralPredicate = sql_adapter.RowSecurityLiteralPredicate;
pub const RowSecurityConjunctionPredicate = sql_adapter.RowSecurityConjunctionPredicate;
pub const DomainCatalogPlan = sql_adapter.DomainCatalogPlan;
pub const CreateDomainPlan = sql_adapter.CreateDomainPlan;
pub const AlterDomainPlan = sql_adapter.AlterDomainPlan;
pub const DomainAlterOperation = sql_adapter.DomainAlterOperation;
pub const DropDomainPlan = sql_adapter.DropDomainPlan;
pub const IdentityAllocatorPlan = sql_adapter.IdentityAllocatorPlan;
pub const IdentityAllocatorKind = sql_adapter.IdentityAllocatorKind;
pub const IdentityAllocatorSpec = sql_adapter.IdentityAllocatorSpec;

pub const RelationLifetimePlan = sql_adapter.RelationLifetimePlan;
pub const RelationLifetimeKind = sql_adapter.RelationLifetimeKind;
pub const PrivilegeChangeAction = sql_adapter.PrivilegeChangeAction;

pub const TablePartitionCatalogPlan = sql_adapter.TablePartitionCatalogPlan;
pub const CreatePartitionedTablePlan = sql_adapter.CreatePartitionedTablePlan;
pub const CreateTablePartitionPlan = sql_adapter.CreateTablePartitionPlan;
pub const AttachTablePartitionPlan = sql_adapter.AttachTablePartitionPlan;
pub const DetachTablePartitionPlan = sql_adapter.DetachTablePartitionPlan;
pub const TablePartitionBounds = sql_adapter.TablePartitionBounds;
pub const TablePartitionMethod = sql_adapter.TablePartitionMethod;
pub const CreateTablePlan = sql_adapter.CreateTablePlan;
pub const AlterTablePlan = sql_adapter.AlterTablePlan;
pub const AlterTableOperation = sql_adapter.AlterTableOperation;
pub const AddColumnOperation = sql_adapter.AddColumnOperation;
pub const RenameColumnOperation = sql_adapter.RenameColumnOperation;
pub const RenameConstraintOperation = sql_adapter.RenameConstraintOperation;
pub const DropDependencyMode = sql_adapter.DropDependencyMode;
pub const DropColumnOperation = sql_adapter.DropColumnOperation;
pub const DropConstraintOperation = sql_adapter.DropConstraintOperation;
pub const DropUpdatePolicyOperation = sql_adapter.DropUpdatePolicyOperation;
pub const AlterColumnDefaultOperation = sql_adapter.AlterColumnDefaultOperation;
pub const AlterColumnNullabilityOperation = sql_adapter.AlterColumnNullabilityOperation;
pub const AlterColumnTypeOperation = sql_adapter.AlterColumnTypeOperation;
pub const AlterColumnRewriteExpression = sql_adapter.AlterColumnRewriteExpression;
pub const CreateIndexPlan = sql_adapter.CreateIndexPlan;
pub const DdlIndexMethod = sql_adapter.DdlIndexMethod;
pub const DdlIndexOpClass = sql_adapter.DdlIndexOpClass;
pub const AppliedDdlSchemaJson = sql_adapter.AppliedDdlSchemaJson;
pub const AppliedDdlWorkAction = sql_adapter.AppliedDdlWorkAction;
pub const AppliedDdlWorkSubject = sql_adapter.AppliedDdlWorkSubject;
pub const AppliedDdlWorkReason = sql_adapter.AppliedDdlWorkReason;
pub const AppliedDdlWorkItem = sql_adapter.AppliedDdlWorkItem;
pub const AppliedDdlRewriteExpression = sql_adapter.AppliedDdlRewriteExpression;
pub const AppliedDdlRowRewritePlan = sql_adapter.AppliedDdlRowRewritePlan;
pub const AppliedDdlRowRewriteRename = sql_adapter.AppliedDdlRowRewriteRename;
pub const runtimeSchemaFromCreateTablePlanAlloc = sql_adapter.runtimeSchemaFromCreateTablePlanAlloc;
pub const schemaJsonFromCreateTablePlanAlloc = sql_adapter.schemaJsonFromCreateTablePlanAlloc;
pub const applyLogicalDdlPlanToRuntimeSchemaAlloc = sql_adapter.applyLogicalDdlPlanToRuntimeSchemaAlloc;
pub const applyLogicalDdlPlanToSchemaJsonAlloc = sql_adapter.applyLogicalDdlPlanToSchemaJsonAlloc;
pub const applyTableDdlPlanToRuntimeSchemaAlloc = sql_adapter.applyTableDdlPlanToRuntimeSchemaAlloc;
pub const applyTableDdlPlanToSchemaJsonAlloc = sql_adapter.applyTableDdlPlanToSchemaJsonAlloc;
pub const appliedDdlTableWorkItemsForFlagsAlloc = sql_adapter.appliedDdlTableWorkItemsForFlagsAlloc;

pub const OwnedSqlCatalogSession = sql_adapter.OwnedSqlCatalogSession;
pub const parseSqlStatementTimeoutNs = sql_adapter.parseSqlStatementTimeoutNs;
pub const sqlStatementTimeoutNsFromSession = sql_adapter.sqlStatementTimeoutNsFromSession;
pub const sqlStatementTimeoutExpired = sql_adapter.sqlStatementTimeoutExpired;
pub const sqlSyncLevelFromSession = sql_adapter.sqlSyncLevelFromSession;
pub const enforceSqlStatementTimeoutAt = sql_adapter.enforceSqlStatementTimeoutAt;
pub const validateSqlRuntimeSettingValue = sql_adapter.validateSqlRuntimeSettingValue;
pub const validateSqlDatabaseSettingValue = sql_adapter.validateSqlDatabaseSettingValue;
pub const applyOwnedSessionCatalogPlanAlloc = sql_adapter.applyOwnedSessionCatalogPlanAlloc;
pub const applySessionCatalogPlanAlloc = sql_adapter.applySessionCatalogPlanAlloc;

fn generatedReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
    expected_kind: sql_adapter.generated_parser.GeneratedSqlReadKind,
) ?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| if (read.kind == expected_kind and read.set_operation_tokens == null and read.cte_tokens == null) read else null,
                else => null,
            };
        }
    }
    return null;
}

fn generatedQueryPlanReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
) ?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| if ((read.kind == .query or read.kind == .set_operation) and read.cte_tokens == null) read else null,
                else => null,
            };
        }
    }
    return null;
}

fn generatedCteReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
) ?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| if (read.kind == .cte and read.cte_tokens != null) read else null,
                else => null,
            };
        }
    }
    return null;
}

fn generatedReadAstOrUnsupported(
    parsed_sql: *const sql_adapter.ParsedSql,
    read_ast: ?*const sql_adapter.generated_parser.GeneratedSqlReadAst,
) !?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (read_ast) |ast| return ast;
    if (parsed_sql.generatedStatementKind() == .read) return error.UnsupportedSqlShape;
    return null;
}

fn requireGeneratedDmlWriteFamily(
    parsed_sql: *const sql_adapter.ParsedSql,
    allowed_kinds: []const sql_adapter.SqlWriteStatementKind,
    expected_recursive: ?bool,
) !void {
    if (parsed_sql.generatedStatementKind() != .dml) return;
    const published = parsed_sql.writeStatementIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (expected_recursive) |recursive| {
        if (published.recursive != recursive) return error.UnsupportedSqlShape;
    }
    for (allowed_kinds) |allowed| {
        if (published.kind == allowed) return;
    }
    return error.UnsupportedSqlShape;
}

pub fn lowerSelectAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerSelectParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

pub fn lowerSelectParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedQueryPlanReadAstForParsedSql(parsed_sql),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        params,
        parser.generated_read_ast,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.queryPlanParserHooks(&parser),
        Parser.ContextAccessors.simpleSelectSetTailHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsQueryPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };

    const table_name = lowered.table_name;
    const ctes = lowered.plan.ctes;
    const query = lowered.plan.query;
    lowered.table_name = "";
    lowered.plan.ctes = &.{};
    lowered.plan.query = .{};
    return .{
        .table_name = table_name,
        .ctes = ctes,
        .query = query,
    };
}

pub fn lowerQueryPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredQueryPlan {
    return try lowerQueryPlanWithExtensionFunctionsAlloc(alloc, sql, schema, params, &.{});
}

pub fn lowerQueryPlanWithExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredQueryPlan {
    return try lowerQueryPlanWithFunctionBindingsAlloc(alloc, sql, schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerQueryPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredQueryPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, params, function_bindings);
}

pub fn lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedQueryPlanReadAstForParsedSql(parsed_sql),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        params,
        parser.generated_read_ast,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.queryPlanParserHooks(&parser),
        Parser.ContextAccessors.simpleSelectSetTailHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsQueryPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

pub fn lowerReadPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, .{});
}

pub fn lowerReadPlanWithExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithFunctionBindingsAlloc(alloc, sql, schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, function_bindings);
}

pub fn lowerReadPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

pub fn lowerReadPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, &.{});
}

pub fn lowerReadPlanWithSchemasAndExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithSchemasAndFunctionBindingsAlloc(alloc, sql, schema, source_schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithSchemasAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, function_bindings);
}

pub fn lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerReadPlanWithOptionalSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    if (schema.storage_mode == .document) {
        if (source_schema != null) return error.DocumentSqlUnsupportedJoin;
        const document_capabilities = sql_adapter.documentCapabilitiesForRuntimeSchema(schema);
        return switch (parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape) {
            .aggregate => .{ .document_aggregate = try sql_adapter.lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, parsed_sql, schema, null, document_capabilities) },
            .query => .{ .document_query = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, parsed_sql, schema, document_capabilities) },
            .join, .lateral => error.DocumentSqlUnsupportedJoin,
            else => error.UnsupportedSqlShape,
        };
    }
    var context = sql_adapter.ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralPlanWithSchemasParsedSqlAlloc,
            .lower_window = lowerWindowPlanParsedSqlAlloc,
            .lower_aggregate_plan = lowerAggregatePlanParsedSqlAlloc,
            .lower_recursive_cte_plan = lowerRecursiveCtePlanParsedSqlAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlAlloc,
            .lower_query_plan = lowerQueryPlanWithFunctionBindingsParsedSqlAlloc,
            .lower_set_operation_optional_source_schema = lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveCtePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveCtePlanParsedSqlAlloc(alloc, &parsed_sql, schema, params, function_bindings);
}

fn lowerRecursiveCtePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveCtePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    return try sql_adapter.parseRecursiveCtePlanAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.recursiveCteParserHooks(&parser));
}

pub fn lowerSetOperationPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, .{});
}

pub fn lowerSetOperationPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, function_bindings);
}

pub fn lowerSetOperationPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, .{});
}

pub fn lowerSetOperationPlanWithSchemasAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, function_bindings);
}

fn lowerSetOperationPlanWithOptionalSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema) |joined_source_schema| {
        if (joined_source_schema.storage_mode != .relational or joined_source_schema.primary_key == null) return error.InvalidSqlCatalog;
    }
    const tokens = parsed_sql.items();
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        generatedReadAstForParsedSql(parsed_sql, .set_operation),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
        .function_bindings = function_bindings,
    };
    return try sql_adapter.parseSetOperationPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        source_schema orelse schema,
        source_schema != null,
        Parser.ContextAccessors.setOperationParserHooks(&parser),
    );
}

pub fn lowerReadPlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogAndExtensionFunctionsAlloc(alloc, sql, schema, params, catalog, &.{});
}

pub fn lowerReadPlanWithCatalogAndExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithCatalogAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, catalog_resources.SqlCatalogSession.default(), function_bindings);
}

pub fn lowerReadPlanWithCatalogSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, params, catalog, session, function_bindings);
}

pub fn lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, catalog, catalog_resources.SqlCatalogSession.default(), function_bindings);
}

pub fn lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsedWithSession(parsed_sql, catalog, session);
}

pub fn lowerReadPlanWithBoundStatementAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    bound: *sql_adapter.BoundSqlStatement,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerBoundParsed(parsed_sql, bound);
}

pub fn lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    logical: *sql_adapter.LogicalSqlPlan,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerLogicalParsed(parsed_sql, logical);
}

fn lowerDocumentReadPlanFromBindingParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    document: sql_adapter.DocumentBinding,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    _ = params;
    _ = function_bindings;
    return switch (parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape) {
        .aggregate => .{
            .document_aggregate = try sql_adapter.lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.virtual_schema,
                document.indexes_json,
                document.capabilities,
            ),
        },
        .query => .{
            .document_query = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.virtual_schema,
                document.capabilities,
            ),
        },
        .join, .lateral => error.DocumentSqlUnsupportedJoin,
        else => error.UnsupportedSqlShape,
    };
}

test "sql runtime rejects document joins with document diagnostic" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"document","enforce_types":false,"document_schemas":{"document":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT docs._id FROM docs JOIN docs AS other_docs ON docs._id = other_docs._id",
    );
    defer parsed_sql.deinit(alloc);

    try std.testing.expectError(
        error.DocumentSqlUnsupportedJoin,
        lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, null, &.{}, .{}),
    );
    try std.testing.expectError(
        error.DocumentSqlUnsupportedJoin,
        lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, schema, &.{}, .{}),
    );
}

fn corruptRuntimeTestGeneratedReadKind(
    parsed_sql: *sql_adapter.ParsedSql,
    kind: sql_adapter.generated_parser.GeneratedSqlReadKind,
) !void {
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .read => |read| {
                read.kind = kind;
                return;
            },
            else => return error.TestUnexpectedResult,
        };
    }
    return error.TestUnexpectedResult;
}

fn corruptRuntimeTestGeneratedDmlKind(
    parsed_sql: *sql_adapter.ParsedSql,
    kind: sql_adapter.generated_parser.GeneratedSqlDmlKind,
) !void {
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .dml => |dml| {
                dml.kind = kind;
                return;
            },
            else => return error.TestUnexpectedResult,
        };
    }
    return error.TestUnexpectedResult;
}

test "sql runtime specialized read lowerers fail closed on mismatched generated read AST family" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);

    var query_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records WHERE status = 'open'");
    defer query_sql.deinit(alloc);
    try corruptRuntimeTestGeneratedReadKind(&query_sql, .aggregate);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectParsedSqlAlloc(alloc, &query_sql, schema, &.{}));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, &query_sql, schema, &.{}, .{}));

    var aggregate_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT status, COUNT(*) AS total FROM usage_records GROUP BY status");
    defer aggregate_sql.deinit(alloc);
    try corruptRuntimeTestGeneratedReadKind(&aggregate_sql, .query);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateParsedSqlAlloc(alloc, &aggregate_sql, schema, &.{}));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregatePlanParsedSqlAlloc(alloc, &aggregate_sql, schema, &.{}));

    var set_operation_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT id FROM usage_records UNION SELECT id FROM usage_records");
    defer set_operation_sql.deinit(alloc);
    try corruptRuntimeTestGeneratedReadKind(&set_operation_sql, .query);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &set_operation_sql, schema, null, &.{}, .{}));

    var join_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT u.id FROM usage_records AS u JOIN usage_records AS v ON u.id = v.id");
    defer join_sql.deinit(alloc);
    try corruptRuntimeTestGeneratedReadKind(&join_sql, .query);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerJoinWithSchemasParsedSqlAlloc(alloc, &join_sql, schema, schema, &.{}));

    var window_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records");
    defer window_sql.deinit(alloc);
    try corruptRuntimeTestGeneratedReadKind(&window_sql, .query);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerWindowPlanParsedSqlAlloc(alloc, &window_sql, schema, &.{}));
}

test "sql runtime specialized dml lowerers fail closed on generated write family mismatch" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);

    const NoopResolver = struct {
        fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
            return .{
                .ptr = self,
                .resolve = resolve,
            };
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
            return null;
        }
    };
    var resolver_ctx = NoopResolver{};
    const resolver = resolver_ctx.resolver();
    const txn_id = [_]u8{ 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f };
    const claim: db_mod.types.RowClaimRequest = .{
        .owner_id = "sql-runtime-generated-dml-guard",
        .txn_id = txn_id,
    };

    var corrupted_insert = try sql_adapter.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')");
    defer corrupted_insert.deinit(alloc);
    try corruptRuntimeTestGeneratedDmlKind(&corrupted_insert, .update);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerInsertWithResolverParsedSqlAlloc(alloc, &corrupted_insert, schema, &.{}, resolver),
    );

    var insert_source = try sql_adapter.ParsedSql.initAlloc(alloc, "INSERT INTO usage_records (id, status) SELECT id, status FROM usage_records");
    defer insert_source.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerInsertWithResolverParsedSqlAlloc(alloc, &insert_source, schema, &.{}, resolver),
    );

    var point_update = try sql_adapter.ParsedSql.initAlloc(alloc, "UPDATE usage_records SET status = 'closed' WHERE id = 'u1'");
    defer point_update.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerUpdateMutationSourceParsedSqlAlloc(alloc, &point_update, schema, &.{}, claim),
    );

    var point_delete = try sql_adapter.ParsedSql.initAlloc(alloc, "DELETE FROM usage_records WHERE id = 'u1'");
    defer point_delete.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerDeleteMutationSourceParsedSqlAlloc(alloc, &point_delete, schema, &.{}, claim),
    );

    var merge_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "MERGE INTO usage_records USING usage_records ON usage_records.id = usage_records.id WHEN MATCHED THEN UPDATE SET status = 'closed'");
    defer merge_sql.deinit(alloc);
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerRecursiveMergeMutationWithSchemasParsedSqlAlloc(alloc, &merge_sql, schema, schema, &.{}),
    );
}

test "sql runtime non catalog document reads use conservative capabilities" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };

    var full_text_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT _id FROM docs WHERE full_text_search('title:alpha') LIMIT 10",
    );
    defer full_text_sql.deinit(alloc);
    try std.testing.expectError(
        error.DocumentSqlIndexUnavailable,
        lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &full_text_sql, schema, null, &.{}, .{}),
    );

    var scalar_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT _id FROM docs WHERE status = 'active' LIMIT 10",
    );
    defer scalar_sql.deinit(alloc);
    var lowered = try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &scalar_sql, schema, null, &.{}, .{});
    defer lowered.deinit(alloc);
    switch (lowered) {
        .document_query => |plan| {
            try std.testing.expectEqual(sql_adapter.default_document_sql_bounded_scan_rows, plan.producer.bounded_scan.max_rows);
            try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", plan.producer.bounded_scan.residual_filter_json.?);
        },
        else => return error.TestExpectedEqual,
    }
}

test "sql runtime catalog document aggregate lowers schema-derived materialization" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":0,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"body":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"note":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"metadata":{"type":"json"}},"additionalProperties":true}}}}
    ;
    const indexes_json =
        \\{"full_text_index_v0":{"name":"full_text_index_v0","type":"full_text"},"amount_alg":{"version":2,"table":"docs","schema_version":0,"capability_fingerprint":"8a6d29a74f129f6b","capability_lifecycle_status":"current","group_fields":[{"name":"status","path":"status","type":"string"},{"name":"amount","path":"amount","type":"number"},{"name":"note","path":"note","type":"string"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"time_fields":[],"dynamic_field_rules":[],"laws":[{"name":"count","id":"count","structure":"group","invertible":true},{"name":"sum","id":"sum","structure":"group","invertible":true},{"name":"avg","id":"avg","structure":"group","invertible":true},{"name":"min","id":"min","structure":"lattice","invertible":false},{"name":"max","id":"max","structure":"lattice","invertible":false}],"joins":[],"adaptive":{"observe":true,"lazy_materialization":true,"dematerialization":false,"min_observations":3},"materializations":[{"name":"auto_count_0","op":"count","group_by":["status"]},{"name":"auto_sum_3","op":"sum","group_by":["status"],"measure":"amount"},{"name":"auto_avg_4","op":"avg","group_by":["status"],"measure":"amount"}],"type":"algebraic","name":"amount_alg"}}
    ;

    const FakeCatalog = struct {
        table: metadata_table_manager.TableRecord,

        fn init() @This() {
            return .{ .table = .{
                .table_id = 7,
                .name = "docs",
                .database_name = catalog_resources.default_database_name,
                .namespace_name = catalog_resources.default_namespace_name,
                .placement_role = "data",
                .schema_json = schema_json,
                .indexes_json = indexes_json,
            } };
        }

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{self.table})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 10");
    defer parsed_sql.deinit(alloc);
    var catalog = FakeCatalog.init();

    var lowered = try lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, &.{}, catalog.iface(), .{});
    defer lowered.deinit(alloc);
    switch (lowered) {
        .document_aggregate => |plan| {
            try std.testing.expectEqualStrings("amount_alg", plan.index_name.?);
            try std.testing.expectEqualStrings("auto_avg_4", plan.materialization_name.?);
        },
        else => return error.TestExpectedEqual,
    }
}

test "sql runtime catalog lowerers bind explicit session for source schemas" {
    const alloc = std.testing.allocator;
    const usage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const incoming_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"source":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    const TenantCatalog = struct {
        tables: [2]metadata_table_manager.TableRecord,

        fn init() @This() {
            return .{ .tables = .{
                .{
                    .table_id = 1,
                    .name = "usage_records",
                    .database_name = "tenant_ops",
                    .namespace_name = "analytics",
                    .placement_role = "data",
                    .schema_json = usage_schema_json,
                },
                .{
                    .table_id = 2,
                    .name = "incoming_usage",
                    .database_name = "tenant_ops",
                    .namespace_name = "analytics",
                    .placement_role = "data",
                    .schema_json = incoming_schema_json,
                },
            } };
        }

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const tenant_path = [_][]const u8{"analytics"};
    const tenant_session: catalog_resources.SqlCatalogSession = .{
        .current_database_name = "tenant_ops",
        .search_path = tenant_path[0..],
    };
    var catalog = TenantCatalog.init();
    const catalog_source = catalog.iface();
    const target_schema = try sql_adapter.runtimeSchemaForCatalogTableWithSessionAlloc(alloc, catalog_source, "usage_records", tenant_session);
    defer runtime_schema.freeSchema(alloc, target_schema);

    var read_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "SELECT usage_records.id, incoming_usage.status FROM usage_records JOIN incoming_usage ON usage_records.id = incoming_usage.id",
    );
    defer read_sql.deinit(alloc);
    try std.testing.expectError(
        error.TableNotFound,
        lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(alloc, &read_sql, target_schema, &.{}, catalog_source, .{}),
    );
    var lowered_read = try lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, &read_sql, target_schema, &.{}, catalog_source, tenant_session, .{});
    defer lowered_read.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(LoweredReadPlan), .join), std.meta.activeTag(lowered_read));

    var write_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) SELECT id, status FROM incoming_usage",
    );
    defer write_sql.deinit(alloc);
    try std.testing.expectError(
        error.TableNotFound,
        lowerWritePlanWithCatalogParsedSqlAlloc(alloc, &write_sql, target_schema, &.{}, .{}, catalog_source),
    );
    var lowered_write = try lowerWritePlanWithCatalogSessionParsedSqlAlloc(alloc, &write_sql, target_schema, &.{}, .{}, catalog_source, tenant_session);
    defer lowered_write.deinit(alloc);
    try std.testing.expectEqual(@as(std.meta.Tag(LoweredWritePlan), .insert_source), std.meta.activeTag(lowered_write));
}

pub fn lowerExplainPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredExplainPlan {
    return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, .{}, null, .{});
}

pub fn lowerExplainPlanWithOptionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
) !LoweredExplainPlan {
    return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, options, null, .{});
}

fn lowerExplainPlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
) !LoweredExplainPlan {
    return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, .{}, catalog, .{});
}

pub fn lowerExplainPlanWithOptionsCatalogAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredExplainPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        params,
        options,
        catalog,
        function_bindings,
    );
}

pub fn lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredExplainPlan {
    var context = sql_adapter.ExplainPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .options = options,
        .catalog = catalog,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_read_with_catalog = lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc,
            .lower_read_without_catalog = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
            .lower_write_with_catalog = lowerWritePlanWithCatalogParsedSqlAlloc,
            .lower_write_without_catalog = lowerWritePlanParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerRelationPopulationPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredRelationPopulationPlan {
    return try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, null, .{});
}

pub fn lowerRelationPopulationPlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
) !LoweredRelationPopulationPlan {
    return try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, .{});
}

pub fn lowerRelationPopulationPlanWithCatalogAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredRelationPopulationPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        params,
        catalog,
        function_bindings,
    );
}

pub fn lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredRelationPopulationPlan {
    var context = sql_adapter.RelationPopulationLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .catalog = catalog,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_read_with_catalog = lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc,
            .lower_read_without_catalog = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerWindowPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWindowPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerWindowPlanParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

fn lowerWindowPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWindowPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedReadAstForParsedSql(parsed_sql, .window),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseWindowPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.windowPlanParserHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsWindowPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

pub fn lowerInsertAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredInsert {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerInsertParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

fn lowerInsertParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
    };
    return Parser.ContextAccessors.parseInsert(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerInsertWithResolverAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerInsertWithResolverParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

pub fn lowerInsertWithResolverParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    return try lowerInsertWithResolverAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, unique_resolver, .{});
}

fn lowerInsertWithResolverAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseInsert(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerInsertWithResolverStrictAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerInsertWithResolverStrictParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

pub fn lowerInsertWithResolverStrictParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try Parser.ContextAccessors.parseInsert(&parser);
}

pub fn lowerInsertSourceWithResolverAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsertSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerInsertSourceWithResolverParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

fn lowerInsertSourceWithResolverParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsertSource {
    return try lowerInsertSourceWithResolverAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, unique_resolver, .{});
}

fn lowerInsertSourceWithResolverAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredInsertSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return sql_adapter.parseInsertSourceAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.joinCteSelectParserHooks(&parser), Parser.ContextAccessors.insertSourceParserHooks(&parser)) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerInsertSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsertSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerInsertSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, unique_resolver);
}

fn lowerInsertSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsertSource {
    return try lowerInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, unique_resolver, .{});
}

fn lowerInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredInsertSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .insert_source_allows_different_table = true,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return sql_adapter.parseInsertSourceAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.joinCteSelectParserHooks(&parser), Parser.ContextAccessors.insertSourceParserHooks(&parser)) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerRecursiveInsertSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredRecursiveInsertSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveInsertSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, unique_resolver);
}

fn lowerRecursiveInsertSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredRecursiveInsertSource {
    return try lowerRecursiveInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, unique_resolver, .{});
}

fn lowerRecursiveInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveInsertSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.insert_source}, true);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .insert_source_allows_different_table = true,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseRecursiveInsertSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerRecursiveUpdateJoinedMutationSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredRecursiveJoinedMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, row_claim);
}

fn lowerRecursiveUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredRecursiveJoinedMutationSource {
    return try lowerRecursiveUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, row_claim, .{});
}

fn lowerRecursiveUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveJoinedMutationSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{ .update_source, .update_joined_source }, true);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseRecursiveUpdateJoinedMutationSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerRecursiveDeleteJoinedMutationSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredRecursiveJoinedMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, row_claim);
}

fn lowerRecursiveDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredRecursiveJoinedMutationSource {
    return try lowerRecursiveDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, row_claim, .{});
}

fn lowerRecursiveDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveJoinedMutationSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{ .delete_source, .delete_joined_source }, true);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseRecursiveDeleteJoinedMutationSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerUpdateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerUpdateParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

pub fn lowerUpdateParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    return try lowerUpdateWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, unique_resolver, .{});
}

fn lowerUpdateWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.update}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseUpdate(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerUpdateStrictAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerUpdateStrictParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

pub fn lowerUpdateStrictParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.update}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try Parser.ContextAccessors.parseUpdate(&parser);
}

pub fn lowerDeleteAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerDeleteParsedSqlAlloc(alloc, &parsed_sql, schema, params, unique_resolver);
}

pub fn lowerDeleteParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    return try lowerDeleteWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, unique_resolver, .{});
}

fn lowerDeleteWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    function_bindings: SqlFunctionBindings,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.delete}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseDelete(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerUpdateMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerUpdateMutationSourceParsedSqlAlloc(alloc, &parsed_sql, schema, params, row_claim);
}

pub fn lowerUpdateMutationSourceParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    return try lowerUpdateMutationSourceWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, row_claim, .{});
}

fn lowerUpdateMutationSourceWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredMutationSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.update_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseUpdateMutationSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedRowsQuery,
        else => return err,
    };
}

pub fn lowerDeleteMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerDeleteMutationSourceParsedSqlAlloc(alloc, &parsed_sql, schema, params, row_claim);
}

fn lowerDeleteMutationSourceParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    return try lowerDeleteMutationSourceWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, row_claim, .{});
}

fn lowerDeleteMutationSourceWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredMutationSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.delete_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseDeleteMutationSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedRowsQuery,
        else => return err,
    };
}

pub fn lowerTruncateMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerTruncateMutationSourceParsedSqlAlloc(alloc, &parsed_sql, schema, row_claim);
}

fn lowerTruncateMutationSourceParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.truncate}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .mutation_claim = row_claim,
    };
    return Parser.ContextAccessors.parseTruncateMutationSource(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerMergeMutationPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredMergeMutationPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerMergeMutationPlanParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params);
}

pub fn lowerMergeMutationPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredMergeMutationPlan {
    return try lowerMergeMutationPlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, .{});
}

fn lowerMergeMutationPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredMergeMutationPlan {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.merge}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    return try sql_adapter.parseMergeMutationPlanAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.joinCteSelectParserHooks(&parser), Parser.ContextAccessors.mergeMutationParserOptions(&parser));
}

pub fn lowerRecursiveMergeMutationWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredRecursiveMergeMutation {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveMergeMutationWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params);
}

fn lowerRecursiveMergeMutationWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredRecursiveMergeMutation {
    return try lowerRecursiveMergeMutationWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, .{});
}

fn lowerRecursiveMergeMutationWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveMergeMutation {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.merge}, true);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    return Parser.ContextAccessors.parseRecursiveMergeMutation(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn buildMergeMutationBatchFromDbAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    plan: LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    source_query: db_mod.types.RelationalRowsQueryRequest,
) !relational_rows.OwnedRowsBatchRequest {
    return try buildMergeMutationBatchFromDbAcrossRangesAlloc(alloc, db, target_schema, source_schema, plan, target_query, &.{}, source_query, &.{});
}

pub fn buildMergeMutationBatchFromDbAcrossRangesAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    plan: LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) !relational_rows.OwnedRowsBatchRequest {
    return try buildMergeMutationBatchFromDbsAcrossRangesAlloc(alloc, db, db, target_schema, source_schema, plan, target_query, target_ranges, source_query, source_ranges);
}

pub fn buildMergeMutationBatchFromDbsAcrossRangesAlloc(
    alloc: std.mem.Allocator,
    target_db: *db_mod.DB,
    source_db: *db_mod.DB,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    plan: LoweredMergeMutationPlan,
    target_query: db_mod.types.RelationalRowsQueryRequest,
    target_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
    source_query: db_mod.types.RelationalRowsQueryRequest,
    source_ranges: []const db_mod.types.RelationalRowsDocKeyRange,
) !relational_rows.OwnedRowsBatchRequest {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;

    const target_preimages = try target_db.collectRelationalRowsPreimagesAcrossRangesAlloc(alloc, target_schema, target_query, target_ranges);
    defer db_mod.types.freeRelationalRowsCollectedRows(alloc, target_preimages);

    var source_preimages: []const db_mod.types.RelationalRowsCollectedRow = &.{};
    var source_result: ?db_mod.types.RelationalRowsQueryResult = null;
    defer if (source_preimages.len > 0) db_mod.types.freeRelationalRowsCollectedRows(alloc, source_preimages);
    defer if (source_result) |*result| result.deinit(alloc);

    const source_rows = if (plan.source.source_cte.len != 0) blk: {
        if (!mergeSourceQueryIsDefault(source_query)) return error.InvalidQueryRequest;
        source_result = try source_db.queryRelationalRowsPlan(alloc, source_schema, .{
            .ctes = plan.ctes,
            .ranges = source_ranges,
            .query = plan.source,
        });
        break :blk source_result.?.rows;
    } else blk: {
        source_preimages = try source_db.collectRelationalRowsPreimagesAcrossRangesAlloc(alloc, source_schema, source_query, source_ranges);
        break :blk try mergeSourceRowsFromPreimagesAlloc(alloc, source_preimages);
    };
    defer if (source_preimages.len > 0) alloc.free(source_rows);

    const target_rows = try alloc.alloc(MergeExecutionTargetRow, target_preimages.len);
    defer alloc.free(target_rows);
    for (target_preimages, 0..) |row, i| {
        target_rows[i] = .{
            .key = row.key,
            .json = row.json,
            .version = row.version,
        };
    }

    return try buildMergeMutationBatchAlloc(alloc, target_schema, source_schema, plan, target_rows, source_rows);
}

fn mergeSourceRowsFromPreimagesAlloc(
    alloc: std.mem.Allocator,
    source_preimages: []const db_mod.types.RelationalRowsCollectedRow,
) ![]const []const u8 {
    if (source_preimages.len == 0) return &.{};
    const source_rows = try alloc.alloc([]const u8, source_preimages.len);
    errdefer alloc.free(source_rows);
    for (source_preimages, 0..) |row, i| source_rows[i] = row.json;
    return source_rows;
}

pub fn lowerUpdateJoinedMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    return try lowerUpdateJoinedMutationSourceWithSchemasAlloc(alloc, sql, schema, schema, params, row_claim);
}

pub fn lowerUpdateJoinedMutationSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, row_claim);
}

pub fn lowerUpdateJoinedMutationSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    return try lowerUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, row_claim, .{});
}

fn lowerUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredJoinedMutationSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.update_joined_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return sql_adapter.parseJoinedMutationSourceAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.joinCteSelectParserHooks(&parser), Parser.ContextAccessors.updateJoinedMutationSourceParserHooks(&parser)) catch |err| switch (err) {
        error.InvalidRowsRequest => error.UnsupportedRowsQuery,
        else => err,
    };
}

pub fn lowerDeleteJoinedMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    return try lowerDeleteJoinedMutationSourceWithSchemasAlloc(alloc, sql, schema, schema, params, row_claim);
}

pub fn lowerDeleteJoinedMutationSourceWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc(alloc, &parsed_sql, target_schema, source_schema, params, row_claim);
}

pub fn lowerDeleteJoinedMutationSourceWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredJoinedMutationSource {
    return try lowerDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, target_schema, source_schema, params, row_claim, .{});
}

fn lowerDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
    function_bindings: SqlFunctionBindings,
) !LoweredJoinedMutationSource {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    try requireGeneratedDmlWriteFamily(parsed_sql, &.{.delete_joined_source}, false);
    const tokens = parsed_sql.items();

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = target_schema,
        .joined_source_schema = source_schema,
        .params = params,
        .mutation_claim = row_claim,
        .function_bindings = function_bindings,
    };
    return sql_adapter.parseJoinedMutationSourceAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.joinCteSelectParserHooks(&parser), Parser.ContextAccessors.deleteJoinedMutationSourceParserHooks(&parser)) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedRowsQuery,
        else => return err,
    };
}

pub fn lowerWritePlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
) !LoweredWritePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerWritePlanParsedSqlAlloc(alloc, &parsed_sql, schema, params, options);
}

pub fn lowerWritePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
) !LoweredWritePlan {
    return try lowerWritePlanWithFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, options, .{});
}

fn lowerWritePlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    function_bindings: SqlFunctionBindings,
) !LoweredWritePlan {
    var context = sql_adapter.WritePlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_generated_dml = sql_adapter.lowerWritePlanFromGeneratedDmlAstDirectWithFunctionBindingsAlloc,
            .lower_recursive_insert_source_with_schemas = lowerRecursiveInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_recursive_update_joined_source_with_schemas = lowerRecursiveUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_recursive_delete_joined_source_with_schemas = lowerRecursiveDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_recursive_merge_mutation_with_schemas = lowerRecursiveMergeMutationWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_insert_with_resolver = lowerInsertWithResolverAndFunctionBindingsParsedSqlAlloc,
            .lower_insert_source_with_resolver = lowerInsertSourceWithResolverAndFunctionBindingsParsedSqlAlloc,
            .lower_insert_source_with_schemas = lowerInsertSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_update_joined_source_with_schemas = lowerUpdateJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .classify_update_selector = sql_adapter.classifyUpdateSelectorParsedSqlAlloc,
            .lower_update_with_resolver = lowerUpdateWithFunctionBindingsParsedSqlAlloc,
            .lower_update_source = lowerUpdateMutationSourceWithFunctionBindingsParsedSqlAlloc,
            .lower_delete_joined_source_with_schemas = lowerDeleteJoinedMutationSourceWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .classify_delete_selector = sql_adapter.classifyDeleteSelectorParsedSqlAlloc,
            .lower_delete_with_resolver = lowerDeleteWithFunctionBindingsParsedSqlAlloc,
            .lower_delete_source = lowerDeleteMutationSourceWithFunctionBindingsParsedSqlAlloc,
            .lower_truncate_source = lowerTruncateMutationSourceParsedSqlAlloc,
            .lower_merge_mutation_with_schemas = lowerMergeMutationPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql, options);
}

pub fn lowerWritePlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !LoweredWritePlan {
    return try lowerWritePlanWithCatalogSessionAlloc(alloc, sql, schema, params, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn lowerWritePlanWithCatalogSessionAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !LoweredWritePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerWritePlanWithCatalogSessionParsedSqlAlloc(alloc, &parsed_sql, schema, params, options, catalog, session);
}

pub fn lowerWritePlanWithCatalogParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
) !LoweredWritePlan {
    return try lowerWritePlanWithCatalogSessionParsedSqlAlloc(alloc, parsed_sql, schema, params, options, catalog, catalog_resources.SqlCatalogSession.default());
}

pub fn lowerWritePlanWithCatalogSessionParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
) !LoweredWritePlan {
    return try lowerWritePlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, options, catalog, session, .{});
}

pub fn lowerWritePlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: SqlFunctionBindings,
) !LoweredWritePlan {
    if (schema.storage_mode == .document) return error.DocumentSqlWriteUnsupported;
    var context = sql_adapter.CatalogWritePlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_with_options = lowerWritePlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsedWithSession(parsed_sql, options, catalog, session);
}

pub fn lowerWritePlanWithBoundStatementAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    bound: *sql_adapter.BoundSqlStatement,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWritePlan {
    return try lowerWritePlanWithBoundStatementAndFunctionBindingsAlloc(alloc, parsed_sql, bound, schema, params, .{});
}

pub fn lowerWritePlanWithBoundStatementAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    bound: *sql_adapter.BoundSqlStatement,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredWritePlan {
    if (schema.storage_mode == .document) return error.DocumentSqlWriteUnsupported;
    var context = sql_adapter.CatalogWritePlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_with_options = lowerWritePlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerBoundParsed(parsed_sql, bound);
}

pub fn lowerWritePlanWithLogicalPlanAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    logical: *sql_adapter.LogicalSqlPlan,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredWritePlan {
    if (schema.storage_mode == .document) return error.DocumentSqlWriteUnsupported;
    var context = sql_adapter.CatalogWritePlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_with_options = lowerWritePlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerLogicalParsed(parsed_sql, logical);
}

pub fn lowerAggregateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAggregateParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

fn lowerAggregateParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedReadAstForParsedSql(parsed_sql, .aggregate),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    return Parser.ContextAccessors.parseAggregate(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregatePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAggregatePlanParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

fn lowerAggregatePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregatePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedReadAstForParsedSql(parsed_sql, .aggregate),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseAggregatePlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.aggregatePlanParserHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsAggregatePlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

pub fn lowerJoinAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    return try lowerJoinWithSchemasAlloc(alloc, sql, schema, schema, params);
}

pub fn lowerJoinWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerJoinWithSchemasParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params);
}

fn lowerJoinWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedReadAstForParsedSql(parsed_sql, .join),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseJoinPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.joinCteSelectParserHooks(&parser),
        Parser.ContextAccessors.joinPlanParserHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsJoinPlanCteOutputAlloc(alloc, schema, lowered.asPlan()) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

pub fn lowerLateralPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    return try lowerLateralPlanWithSchemasAlloc(alloc, sql, schema, schema, params);
}

pub fn lowerLateralPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerLateralPlanWithSchemasParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params);
}

fn lowerLateralPlanWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = try generatedReadAstOrUnsupported(
        parsed_sql,
        if (cte_adapter_shape) generatedCteReadAstForParsedSql(parsed_sql) else generatedReadAstForParsedSql(parsed_sql, .lateral),
    );

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseLateralPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.lateralPlanParserHooks(&parser),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsLateralPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

pub const lowerAntflyQueryFunctionSqlAlloc = sql_adapter.lowerAntflyQueryFunctionSqlAlloc;

const Parser = sql_adapter.ParserState;
