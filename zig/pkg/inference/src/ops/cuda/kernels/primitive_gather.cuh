// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope traversal; no include guard. Integer index math stays full width.
    unsigned int out_idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (out_idx >= output_count) return;
    unsigned int suffix_coord = out_idx % suffix_size;
    unsigned int outer = out_idx / suffix_size;
    unsigned int index_pos = outer % index_count;
    unsigned int prefix = outer / index_count;
#ifdef TERMITE_PRIMITIVE_INTEGER_INDICES
    long long gather_index = (long long)indices[index_pos];
    if (gather_index < 0) gather_index += (long long)axis_extent;
    if (gather_index < 0 || (unsigned long long)gather_index >= (unsigned long long)axis_extent) {
#else
    int gather_index = (int)indices[index_pos];
    if (gather_index < 0) gather_index += (int)axis_extent;
    if (gather_index < 0 || (unsigned int)gather_index >= axis_extent) {
#endif
        output[out_idx] = NAN;
        return;
    }
#ifdef TERMITE_PRIMITIVE_INTEGER_INDICES
    unsigned long long input_idx = ((unsigned long long)prefix * axis_extent + (unsigned int)gather_index) * suffix_size + suffix_coord;
#else
    unsigned int input_idx = (prefix * axis_extent + (unsigned int)gather_index) * suffix_size + suffix_coord;
#endif
    output[out_idx] = termite_dense_storage_f32<Format>(input, input_idx);
