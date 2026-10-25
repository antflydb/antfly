// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope traversal; no include guard. Integer index math stays full width.
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * cols;
    if (idx >= count) return;
    unsigned int row = idx / cols;
    unsigned int col = idx - row * cols;
#ifdef TERMITE_PRIMITIVE_INTEGER_INDICES
    long long output_row = (long long)indices[row];
    if (output_row < 0) output_row += (long long)output_rows;
    if (output_row < 0 || (unsigned long long)output_row >= (unsigned long long)output_rows) return;
#else
    int output_row = (int)indices[row];
    if (output_row < 0) output_row += (int)output_rows;
    if (output_row < 0 || (unsigned int)output_row >= output_rows) return;
#endif
#ifdef TERMITE_PRIMITIVE_INTEGER_INDICES
    atomicAdd(output + (unsigned long long)(unsigned int)output_row * cols + col, input[idx]);
#else
    atomicAdd(output + (unsigned int)output_row * cols + col, input[idx]);
#endif
