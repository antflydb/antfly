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

//! Public session construction and inspection compatibility boundary.
//! Prepared artifact inventories live in models, backend creation in backends,
//! and architecture execution in execution.architecture_session. New loading
//! or kernel implementations belong to those owners.
const implementation = @import("../execution/architecture_session.zig");

pub const CudaRuntimeStats = implementation.CudaRuntimeStats;
pub const estimateNativeWeightBytes = implementation.estimateNativeWeightBytes;
pub const estimateBackendWeightResidencyBytes = implementation.estimateBackendWeightResidencyBytes;
pub const refineGptConfigFromGgufTensorInfo = implementation.refineGptConfigFromGgufTensorInfo;
pub const TaskOverride = implementation.TaskOverride;
pub const GenericEncoderArchConfig = implementation.GenericEncoderArchConfig;
pub const metalDebertaFastPathAdmissionAmounts = implementation.metalDebertaFastPathAdmissionAmounts;
pub const GlinerBoundaryResidentLoadAmounts = implementation.GlinerBoundaryResidentLoadAmounts;
pub const glinerBoundaryResidentLoadAmounts = implementation.glinerBoundaryResidentLoadAmounts;
pub const isPackedLayaModel = implementation.isPackedLayaModel;
pub const layaResidentLoadAmounts = implementation.layaResidentLoadAmounts;
pub const prepareLayaResident = implementation.prepareLayaResident;
pub const UnsupportedTensorTypeCount = implementation.UnsupportedTensorTypeCount;
pub const GgufTensorSample = implementation.GgufTensorSample;
pub const GgufInspectionReport = implementation.GgufInspectionReport;
pub const resolveA4bInferenceConfigForModelListing = implementation.resolveA4bInferenceConfigForModelListing;
pub const resolveCudaA4bInferenceConfigForModelListing = implementation.resolveCudaA4bInferenceConfigForModelListing;
pub const inspectGgufModel = implementation.inspectGgufModel;
pub const inspectGgufModelForListing = implementation.inspectGgufModelForListing;
pub const createNativeSession = implementation.createNativeSession;
pub const createNativeSessionWithTaskOverride = implementation.createNativeSessionWithTaskOverride;
pub const createPjrtSession = implementation.createPjrtSession;
pub const createPjrtSessionWithTaskOverride = implementation.createPjrtSessionWithTaskOverride;
pub const getPjrtClientPtr = implementation.getPjrtClientPtr;
pub const createMetalSession = implementation.createMetalSession;
pub const createMetalSessionWithTaskOverride = implementation.createMetalSessionWithTaskOverride;
pub const createMetalSessionWithKernelJit = implementation.createMetalSessionWithKernelJit;
pub const createMetalSessionWithKernelJitAndLoadContext = implementation.createMetalSessionWithKernelJitAndLoadContext;
pub const createMetalSessionWithKernelJitAndLoadContextAndA4bRequest = implementation.createMetalSessionWithKernelJitAndLoadContextAndA4bRequest;
pub const createMetalSessionWithTaskOverrideAndKernelJit = implementation.createMetalSessionWithTaskOverrideAndKernelJit;
pub const createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext = implementation.createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext;
pub const createCudaSession = implementation.createCudaSession;
pub const GlinerCudaPrecision = implementation.GlinerCudaPrecision;
pub const createGlinerCudaSessionWithPrecision = implementation.createGlinerCudaSessionWithPrecision;
pub const createCudaSessionWithGlinerPrecision = implementation.createCudaSessionWithGlinerPrecision;
pub const createCudaSessionWithGlinerPrecisionForTest = implementation.createCudaSessionWithGlinerPrecisionForTest;
pub const createCudaSessionWithTaskOverride = implementation.createCudaSessionWithTaskOverride;
pub const createCudaSessionWithKernelJit = implementation.createCudaSessionWithKernelJit;
pub const createCudaSessionWithKernelJitAndLoadContext = implementation.createCudaSessionWithKernelJitAndLoadContext;
pub const createCudaSessionWithKernelJitAndLoadContextAndA4bRequest = implementation.createCudaSessionWithKernelJitAndLoadContextAndA4bRequest;
pub const createCudaSessionWithTaskOverrideAndKernelJit = implementation.createCudaSessionWithTaskOverrideAndKernelJit;
pub const createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext = implementation.createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext;
pub const createGemma4CudaTrainingSession = implementation.createGemma4CudaTrainingSession;
pub const writeCudaA4bPreparedPack = implementation.writeCudaA4bPreparedPack;
pub const verifyCudaA4bPreparedPack = implementation.verifyCudaA4bPreparedPack;
pub const ggufTensorTypeSupportsBackend = implementation.ggufTensorTypeSupportsBackend;
pub const ggufInspectionSupportsBackend = implementation.ggufInspectionSupportsBackend;
pub const widenBudgetLimitsForModelPath = implementation.widenBudgetLimitsForModelPath;
pub const layaTrunkCacheStats = implementation.layaTrunkCacheStats;
pub const setLayaTrunkCachePrecision = implementation.setLayaTrunkCachePrecision;
pub const setLayaTrunkCacheLimit = implementation.setLayaTrunkCacheLimit;
pub const attachIo = implementation.attachIo;
pub const attachGraphRuntimeStrategy = implementation.attachGraphRuntimeStrategy;
pub const MetalWorkloadProfileExport = implementation.MetalWorkloadProfileExport;
pub const qwenMetalWorkspaceCredit = implementation.qwenMetalWorkspaceCredit;
pub const trimQwenMetalWorkspace = implementation.trimQwenMetalWorkspace;
pub const beginQwenMetalWorkspace = implementation.beginQwenMetalWorkspace;
pub const finishQwenMetalWorkspace = implementation.finishQwenMetalWorkspace;
pub const beginMetalWorkloadProfile = implementation.beginMetalWorkloadProfile;
pub const endMetalWorkloadProfile = implementation.endMetalWorkloadProfile;
pub const getRecurrentExecution = implementation.getRecurrentExecution;
pub const getGptConfig = implementation.getGptConfig;
pub const getEmbeddingGemma2ResidentConfig = implementation.getEmbeddingGemma2ResidentConfig;
pub const loadGptConfigFromModelDir = implementation.loadGptConfigFromModelDir;
pub const loadGptConfigMetadataFromModelDir = implementation.loadGptConfigMetadataFromModelDir;
pub const getEmbeddingGemma2CudaPolicy = implementation.getEmbeddingGemma2CudaPolicy;
pub const getCudaRuntimeStats = implementation.getCudaRuntimeStats;
pub const drainCudaProfile = implementation.drainCudaProfile;
pub const formatCudaPrefillProfileLine = implementation.formatCudaPrefillProfileLine;
pub const cudaOpProfileLoggingEnabled = implementation.cudaOpProfileLoggingEnabled;
pub const cudaStatsDelta = implementation.cudaStatsDelta;
pub const formatCudaDecodeProfileLine = implementation.formatCudaDecodeProfileLine;
pub const MetalExactJitDispatchStats = implementation.MetalExactJitDispatchStats;
pub const getMetalExactJitDispatchStats = implementation.getMetalExactJitDispatchStats;
pub const recommendedKvDTypeForGptConfig = implementation.recommendedKvDTypeForGptConfig;
pub const recommendedKvDTypeForSession = implementation.recommendedKvDTypeForSession;
pub const getClipConfig = implementation.getClipConfig;
pub const getClapConfig = implementation.getClapConfig;
pub const getWhisperConfig = implementation.getWhisperConfig;
pub const getFlorenceConfig = implementation.getFlorenceConfig;
pub const runFlorenceEncoderResident = implementation.runFlorenceEncoderResident;
pub const runFlorenceDecoderResident = implementation.runFlorenceDecoderResident;
pub const getComputeBackend = implementation.getComputeBackend;
pub const getEmbeddingGemma2Config = implementation.getEmbeddingGemma2Config;
pub const reclaimEmbeddingGemma2VisionWeights = implementation.reclaimEmbeddingGemma2VisionWeights;
pub const reclaimVideoPreparationCoefficients = implementation.reclaimVideoPreparationCoefficients;
pub const reclaimEmbeddingGemma2AudioWeights = implementation.reclaimEmbeddingGemma2AudioWeights;
pub const reclaimEmbeddingGemma2LanguageWeights = implementation.reclaimEmbeddingGemma2LanguageWeights;
pub const videoPreparationContext = implementation.videoPreparationContext;
pub const WhisperNativeDecoder = implementation.WhisperNativeDecoder;
pub const whisperNativeDecoder = implementation.whisperNativeDecoder;
pub const getComputeBackendBorrowingSharedProvider = implementation.getComputeBackendBorrowingSharedProvider;
pub const ManagedComputeBackend = implementation.ManagedComputeBackend;
pub const getComputeBackendWithControl = implementation.getComputeBackendWithControl;
pub const getManagedComputeBackend = implementation.getManagedComputeBackend;
pub const prepareGlinerBoundaryResident = implementation.prepareGlinerBoundaryResident;
pub const isGlinerBoundaryResidentReady = implementation.isGlinerBoundaryResidentReady;
pub const GlinerBoundaryResidentStats = implementation.GlinerBoundaryResidentStats;
pub const glinerBoundaryResidentStats = implementation.glinerBoundaryResidentStats;
pub const glinerBoundaryWorkspaceAdmissionAmounts = implementation.glinerBoundaryWorkspaceAdmissionAmounts;
pub const GlinerBoundaryWorkspacePlan = implementation.GlinerBoundaryWorkspacePlan;
pub const planGlinerBoundaryWorkspace = implementation.planGlinerBoundaryWorkspace;
pub const getManagedGlinerBoundaryComputeBackend = implementation.getManagedGlinerBoundaryComputeBackend;
pub const replaceBlasResidentWeight = implementation.replaceBlasResidentWeight;
pub const getComputeBackendWithBudget = implementation.getComputeBackendWithBudget;
pub const getGenericEncoderArchConfig = implementation.getGenericEncoderArchConfig;
pub const getGlinerBoundaryConfig = implementation.getGlinerBoundaryConfig;
pub const GlinerSpanConfig = implementation.GlinerSpanConfig;
pub const getGlinerSpanConfig = implementation.getGlinerSpanConfig;
pub const getGlinerBoundaryIdentity = implementation.getGlinerBoundaryIdentity;
pub const sealGlinerDecisionTokenizerDigest = implementation.sealGlinerDecisionTokenizerDigest;
pub const getGlinerDecisionIdentity = implementation.getGlinerDecisionIdentity;
pub const TestGlinerBoundaryMetalOwner = implementation.TestGlinerBoundaryMetalOwner;
pub const getLayaConfig = implementation.getLayaConfig;
pub const widenBudgetLimitsForSession = implementation.widenBudgetLimitsForSession;
pub const configureReservedGenerationWorkspace = implementation.configureReservedGenerationWorkspace;
pub const configureSharedCacheAdmissionForSession = implementation.configureSharedCacheAdmissionForSession;
pub const sharedCacheAdmissionAmounts = implementation.sharedCacheAdmissionAmounts;
pub const reclaimOneHostCacheEntryForSession = implementation.reclaimOneHostCacheEntryForSession;
pub const memoryBudgetExceededDetail = implementation.memoryBudgetExceededDetail;
pub const attachSharedPrefetchState = implementation.attachSharedPrefetchState;
pub const layaResidentStats = implementation.layaResidentStats;

pub const NativeSessionOptions = implementation.NativeSessionOptions;
pub const createNativeSessionWithOptions = implementation.createNativeSessionWithOptions;
