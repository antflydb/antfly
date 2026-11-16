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

// SM12.1 BF16 recurrent prefill. Device scratch is supplied by the caller.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <tvm/ffi/c_api.h>
#include <tvm/ffi/extra/c_env_api.h>
extern "C" int __tvm_ffi_antfly_lighton_prefill(void *, const TVMFFIAny *,
                                                int32_t, TVMFFIAny *);
struct Page {
  float *history;
  float *next_history;
  float *state;
};
struct Batch {
  float *mixed;
  float *conv_weight;
  float *conv;
  float *z;
  float *beta;
  float *a;
  float *a_log;
  float *dt_bias;
  float *core;
  float *norm_weight;
  float *out;
  int key_heads, value_heads, items;
  float eps;
  Page pages[64];
};
struct Ragged {
  Batch base;
  int offsets[64], lengths[64], initialized[64];
};
static_assert(sizeof(Ragged) == 2408, "native packet ABI");
struct Claim {
  Ragged p;
  int count = 0, rows = 0, indices[16]{}, offsets[17]{};
  uintptr_t state_view = 0;
  int64_t state_pointers[16]{};
};
static bool claim(const Ragged *p, Claim &c) {
  if (!p || p->base.items < 1 || p->base.items > 64 ||
      p->base.key_heads != 16 ||
      (p->base.value_heads != 16 && p->base.value_heads != 32))
    return false;
  c.p = *p;
  int total = 0;
  for (int i = 0; i < p->base.items; ++i) {
    if (p->offsets[i] != total || p->lengths[i] < 1 ||
        p->lengths[i] > 4096 - total)
      return false;
    total += p->lengths[i];
    if (p->lengths[i] > 1) {
      if (c.count == 16)
        return false;
      c.indices[c.count] = i;
      c.offsets[c.count++] = c.rows;
      c.rows += p->lengths[i];
    }
  }
  c.offsets[c.count] = c.rows;
  if (c.count) {
    // Every table entry names an independently owned native K,V state. The
    // one-page DLTensor supplies only the common layout; the CuTe kernel uses
    // the selected page pointer directly, never an offset across allocations.
    for (int i = 0; i < c.count; ++i) {
      uintptr_t state =
          reinterpret_cast<uintptr_t>(p->base.pages[c.indices[i]].state);
      if (!state || (state & 255))
        return false;
      c.state_pointers[i] = static_cast<int64_t>(state);
    }
    c.state_view = static_cast<uintptr_t>(c.state_pointers[0]);
  }
  return p->base.conv && p->base.out && p->base.core;
}
static size_t aligned(size_t n) { return (n + 255) & ~size_t(255); }
struct Scratch {
  __nv_bfloat16 *q, *k, *v, *o;
  float *alpha, *beta;
  int64_t *slots;
  int64_t *cu;
  uint8_t *tm;
  size_t size;
  Scratch(void *raw, int rows, int b, int h) {
    size_t offset = 0;
    auto take = [&](size_t n) {
      void *p =
          reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(raw) + offset);
      offset += aligned(n);
      return p;
    };
    q = static_cast<__nv_bfloat16 *>(take(size_t(rows) * 16 * 128 * 2));
    k = static_cast<__nv_bfloat16 *>(take(size_t(rows) * 16 * 128 * 2));
    v = static_cast<__nv_bfloat16 *>(take(size_t(rows) * h * 128 * 2));
    o = static_cast<__nv_bfloat16 *>(take(size_t(rows) * h * 128 * 2));
    alpha = static_cast<float *>(take(size_t(rows) * h * 4));
    beta = static_cast<float *>(take(size_t(rows) * h * 4));
    slots = static_cast<int64_t *>(take(size_t(b) * 8));
    cu = static_cast<int64_t *>(take(size_t(b + 1) * 8));
    tm = static_cast<uint8_t *>(take(256 * 128));
    size = offset;
  }
};
static size_t workspace_bytes(const Ragged *p) {
  Claim c;
  if (!claim(p, c))
    return SIZE_MAX;
  if (!c.count)
    return 0;
  return Scratch(nullptr, c.rows, c.count, p->base.value_heads).size;
}
__global__ void conv_pack(const __grid_constant__ Claim c, Scratch s) {
  __shared__ float partial[4];
  int row = blockIdx.y, head = blockIdx.x, d = threadIdx.x, seq = 0;
  while (seq + 1 < c.count && c.offsets[seq + 1] <= row)
    ++seq;
  int page = c.indices[seq], t = row - c.offsets[seq], h = c.p.base.value_heads,
      ch = (32 + h) * 128, channel = head * 128 + d;
  int offset = c.p.offsets[page], length = c.p.lengths[page],
      init = c.p.initialized[page];
  float sum = 0.f;
#pragma unroll
  for (int k = 0; k < 4; ++k) {
    int source = t + k - 3;
    float v =
        source >= 0
            ? c.p.base.mixed[(offset + source) * ch + channel]
            : (init ? c.p.base.pages[page].history[channel * 4 + source + 4]
                    : 0.f);
    sum += v * c.p.base.conv_weight[channel * 4 + k];
  }
  float value = sum / (1.f + expf(-sum));
  if (head < 32) {
    float square = value * value;
    for (int step = 16; step; step >>= 1)
      square += __shfl_down_sync(0xffffffffu, square, step);
    if ((d & 31) == 0)
      partial[d >> 5] = square;
    __syncthreads();
    if (d == 0)
      partial[0] = 1.f / sqrtf(partial[0] + partial[1] + partial[2] +
                               partial[3] + 1e-6f);
    __syncthreads();
    value = value * partial[0] * (head < 16 ? 0.08838834764831845f : 1.f);
  }
  if (head < 16)
    s.q[(row * 16 + head) * 128 + d] = __float2bfloat16_rn(value);
  else if (head < 32)
    s.k[(row * 16 + head - 16) * 128 + d] = __float2bfloat16_rn(value);
  else
    s.v[(row * h + head - 32) * 128 + d] = __float2bfloat16_rn(value);
  if (t == 0)
    for (int k = 0; k < 4; ++k) {
      int source = length + k - 4;
      c.p.base.pages[page].next_history[channel * 4 + k] =
          source >= 0 && source < length
              ? c.p.base.mixed[(offset + source) * ch + channel]
              : (init && source < 0
                     ? c.p.base.pages[page].history[channel * 4 + source + 4]
                     : 0.f);
    }
  if (d == 0 && head < h) {
    int source = offset + t;
    float a = c.p.base.a[source * h + head] + c.p.base.dt_bias[head];
    float softplus = a > 20.f ? a : log1pf(expf(a));
    s.alpha[row * h + head] = expf(-expf(c.p.base.a_log[head]) * softplus);
    s.beta[row * h + head] =
        1.f / (1.f + expf(-c.p.base.beta[source * h + head]));
  }
  if (t == 0 && head < h && !init) {
    for (int i = d; i < 128 * 128; i += 128)
      c.p.base.pages[page].state[head * 128 * 128 + i] = 0.f;
  }
  if (row == 0 && head == 0) {
    if (d <= c.count)
      s.cu[d] = c.offsets[d];
    if (d < c.count)
      s.slots[d] = c.state_pointers[d];
  }
}
__global__ void scatter_output(const __grid_constant__ Claim c, Scratch s) {
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  int width = c.p.base.value_heads * 128;
  if (idx >= c.rows * width)
    return;
  int row = idx / width, elem = idx % width, seq = 0;
  while (seq + 1 < c.count && c.offsets[seq + 1] <= row)
    ++seq;
  int target = c.p.offsets[c.indices[seq]] + row - c.offsets[seq];
  c.p.base.core[target * width + elem] = __bfloat162float(s.o[idx]);
}
struct Tensor {
  DLTensor dl{};
  int64_t dims[4]{}, strides[4]{};
  Tensor(void *data, std::initializer_list<int64_t> shape,
         std::initializer_list<int64_t> stride, uint8_t code, uint8_t bits,
         int device) {
    std::copy(shape.begin(), shape.end(), dims);
    std::copy(stride.begin(), stride.end(), strides);
    dl.data = data;
    dl.device = {kDLCUDA, device};
    dl.ndim = shape.size();
    dl.dtype = {code, bits, 1};
    dl.shape = dims;
    dl.strides = strides;
  }
};
static int prefill_run(const Ragged *p, void *scratch, size_t bytes, int device,
                       cudaStream_t stream) {
  Claim c;
  if (!claim(p, c))
    return 1;
  if (!c.count)
    return 0;
  if (!scratch || (reinterpret_cast<uintptr_t>(scratch) & 255))
    return 2;
  Scratch s(scratch, c.rows, c.count, p->base.value_heads);
  if (bytes < s.size)
    return 3;
  int sm = 0, major = 0, minor = 0, current = -1;
  if (cudaGetDevice(&current) != cudaSuccess || current != device ||
      cudaDeviceGetAttribute(&sm, cudaDevAttrMultiProcessorCount, device) !=
          cudaSuccess ||
      sm > 256 ||
      cudaDeviceGetAttribute(&major, cudaDevAttrComputeCapabilityMajor,
                             device) != cudaSuccess ||
      cudaDeviceGetAttribute(&minor, cudaDevAttrComputeCapabilityMinor,
                             device) != cudaSuccess ||
      major != 12 || minor != 1)
    return 4;
  int rows = c.rows, b = c.count, h = p->base.value_heads, d = 128;
  TVMFFIAny args[23]{}, result{};
  Tensor q(s.q, {rows, d, 16}, {16 * d, 1, d}, kDLBfloat, 16, device),
      k(s.k, {d, rows, 16}, {1, 16 * d, d}, kDLBfloat, 16, device),
      v(s.v, {d, rows, h}, {1, h * d, d}, kDLBfloat, 16, device),
      o(s.o, {d, rows, h}, {1, h * d, d}, kDLBfloat, 16, device);
  Tensor alpha(s.alpha, {rows * h}, {1}, kDLFloat, 32, device),
      beta(s.beta, {rows * h}, {1}, kDLFloat, 32, device),
      state(reinterpret_cast<void *>(c.state_view), {1, h, d, d},
            {h * d * d, d * d, 1, d}, kDLFloat, 32, device),
      initial(reinterpret_cast<void *>(c.state_view), {1, h, d, d},
              {h * d * d, d * d, 1, d}, kDLFloat, 32, device),
      slots(s.slots, {b}, {1}, kDLInt, 64, device),
      tm(s.tm, {256 * 128}, {1}, kDLUInt, 8, device),
      cu(s.cu, {b + 1}, {1}, kDLInt, 64, device);
  auto tensor = [&](int i, Tensor &t) {
    args[i].type_index = kTVMFFIDLTensorPtr;
    args[i].v_ptr = &t.dl;
  };
  auto integer = [&](int i, int64_t x) {
    args[i].type_index = kTVMFFIInt;
    args[i].v_int64 = x;
  };
  tensor(0, q);
  tensor(1, k);
  tensor(2, v);
  tensor(3, o);
  tensor(4, alpha);
  tensor(5, beta);
  tensor(6, state);
  tensor(7, initial);
  tensor(8, slots);
  tensor(11, tm);
  tensor(12, cu);
  args[13].type_index = kTVMFFIFloat;
  args[13].v_float64 = 1.;
  integer(14, 16);
  integer(15, 16);
  integer(16, h);
  integer(17, h);
  integer(18, b);
  integer(19, 1);
  integer(20, 0);
  integer(21, b * h);
  args[22].type_index = kTVMFFIOpaquePtr;
  args[22].v_ptr = stream;
  TVMFFIStreamHandle previous = nullptr;
  if (TVMFFIEnvSetStream(kDLCUDA, device, stream, &previous))
    return 5;
  conv_pack<<<dim3(32 + h, rows), 128, 0, stream>>>(c, s);
  int rc = static_cast<int>(cudaGetLastError());
  if (!rc)
    rc = __tvm_ffi_antfly_lighton_prefill(nullptr, args, 23, &result);
  int restore = TVMFFIEnvSetStream(kDLCUDA, device, previous, nullptr);
  if (rc || restore)
    return 6;
  scatter_output<<<(rows * h * d + 255) / 256, 256, 0, stream>>>(c, s);
  return cudaGetLastError() == cudaSuccess ? 0 : 7;
}

extern "C" uint64_t antfly_lighton_prefill_abi() {
  return (uint64_t(1) << 32) | sizeof(Ragged);
}
extern "C" size_t antfly_lighton_prefill_workspace(const Ragged *p) {
  return workspace_bytes(p);
}
extern "C" int antfly_lighton_prefill_execute(const Ragged *p, void *scratch,
                                              size_t bytes, int device,
                                              cudaStream_t stream) {
  if (!p || !p->base.mixed || !p->base.conv_weight || !p->base.a ||
      !p->base.beta || !p->base.a_log || !p->base.dt_bias)
    return 8;
  return prefill_run(p, scratch, bytes, device, stream);
}
