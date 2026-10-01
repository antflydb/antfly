// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Boolean-only retained optimizer checks. Integer loads preserve subnormals,
// signed zeros and all NaN payloads regardless of floating-point math flags.
struct TrainingValidationEntry {
    unsigned long long pointer;
    unsigned count, first_block;
};
struct TrainingValidationBatch { TrainingValidationEntry entries[128]; };
static_assert(sizeof(TrainingValidationEntry) == 16, "validation entry ABI");
static_assert(sizeof(TrainingValidationBatch) == 2048, "validation batch ABI");

// All 256 threads participate; only thread zero's return value is consumed.
__device__ __forceinline__ unsigned training_validation_block_or(unsigned flags) {
    __shared__ unsigned warp_flags[8];
    for (unsigned offset = 16; offset; offset >>= 1)
        flags |= __shfl_down_sync(0xffffffffu, flags, offset);
    if ((threadIdx.x & 31u) == 0) warp_flags[threadIdx.x / 32u] = flags;
    __syncthreads();
    if (threadIdx.x < 32u) {
        flags = threadIdx.x < 8u ? warp_flags[threadIdx.x] : 0u;
        for (unsigned offset = 16; offset; offset >>= 1)
            flags |= __shfl_down_sync(0xffffffffu, flags, offset);
    }
    return flags;
}

extern "C" __global__ void termite_training_validate_f32(
    unsigned* partials, const __grid_constant__ TrainingValidationBatch batch,
    unsigned tensors) {
    unsigned lo = 0, hi = tensors;
    while (lo + 1 < hi) {
        const unsigned mid = (lo + hi) / 2;
        if (batch.entries[mid].first_block <= blockIdx.x) lo = mid;
        else hi = mid;
    }
    const TrainingValidationEntry entry = batch.entries[lo];
    const unsigned* input = (const unsigned*)entry.pointer;
    const unsigned first = (blockIdx.x - entry.first_block) * 65536u;
    const unsigned end = min(entry.count, first + 65536u);
    unsigned flags = 0;
    for (unsigned i = first + threadIdx.x; i < end; i += 256u) {
        const unsigned value = input[i];
        flags |= ((value & 0x7f800000u) == 0x7f800000u ? 1u : 0u) |
                 ((value & 0x7fffffffu) != 0u ? 2u : 0u);
    }
    flags = training_validation_block_or(flags);
    // Every partial is written on every invocation, including all-zero input.
    if (threadIdx.x == 0u) partials[blockIdx.x] = flags;
}

extern "C" __global__ void termite_training_validate_finish(
    unsigned* output, const unsigned* partials, unsigned count) {
    unsigned flags = 0;
    for (unsigned i = threadIdx.x; i < count; i += 256u) flags |= partials[i];
    flags = training_validation_block_or(flags);
    if (threadIdx.x == 0u) output[0] = flags;
}
