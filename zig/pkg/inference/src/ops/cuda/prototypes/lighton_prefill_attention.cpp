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

// Native optional prefill attention ABI. The caller owns every CUDA buffer
// and the execution stream; Python and Torch are build-time dependencies only.
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <tvm/ffi/container/array.h>
#include <tvm/ffi/container/tensor.h>
#include <tvm/ffi/extra/c_env_api.h>
using tvm::ffi::Array;
using tvm::ffi::Optional;
using tvm::ffi::Tensor;
using tvm::ffi::TensorView;

Array<int64_t> BatchPrefillWithKVCachePlan(TensorView, TensorView, TensorView,
                                           TensorView, TensorView, TensorView,
                                           int64_t, int64_t, int64_t, int64_t,
                                           int64_t, bool, int64_t, int64_t,
                                           bool, int64_t, int64_t, bool,
                                           int64_t, int64_t);
void BatchPrefillWithPagedKVCacheRun(
    TensorView, TensorView, Array<int64_t>, TensorView, TensorView, TensorView,
    TensorView, TensorView, TensorView, TensorView, TensorView,
    Optional<TensorView>, int64_t, int64_t, int64_t, bool, Optional<Tensor>,
    Optional<Tensor>, Optional<Tensor>, Optional<Tensor>, Optional<Tensor>,
    Optional<Tensor>, Optional<Tensor>, Optional<Tensor>, double, double,
    double, double, int64_t);

struct PlanRequest {
  uint64_t float_workspace, int_workspace, pinned_workspace;
  uint64_t float_bytes, int_bytes, pinned_bytes;
  uint64_t cpu_query_indptr, cpu_kv_indptr, cpu_kv_lengths;
  int32_t batch, heads, kv_heads, rows, page_size, device;
  uint64_t stream;
};
struct RunRequest {
  uint64_t plan, q, k, v, out, query_indptr, kv_indptr, kv_indices,
      last_page_lengths;
  uint64_t pages, stream;
};
static_assert(sizeof(PlanRequest) == 104);
static_assert(sizeof(RunRequest) == 88);
struct View {
  std::array<int64_t, 4> shape{}, strides{};
  DLTensor tensor{};
  View(uint64_t data, uint8_t code, uint8_t bits, DLDevice device,
       std::initializer_list<int64_t> dimensions) {
    tensor.data = reinterpret_cast<void *>(data);
    tensor.device = device;
    tensor.ndim = dimensions.size();
    tensor.dtype = {code, bits, 1};
    std::copy(dimensions.begin(), dimensions.end(), shape.begin());
    int64_t stride = 1;
    for (int i = tensor.ndim - 1; i >= 0; --i) {
      strides[i] = stride;
      stride *= shape[i];
    }
    tensor.shape = shape.data();
    tensor.strides = strides.data();
  }
  View(const View &) = delete;
  operator TensorView() const { return TensorView(&tensor); }
};
struct StreamScope {
  int device;
  TVMFFIStreamHandle previous = nullptr;
  bool active = false;
  StreamScope(int d, uint64_t stream) : device(d) {
    if (TVMFFIEnvSetStream(kDLCUDA, device, reinterpret_cast<void *>(stream),
                           &previous))
      throw std::runtime_error("cannot bind native CUDA stream");
    active = true;
  }
  int close() {
    if (!active)
      return 0;
    active = false;
    return TVMFFIEnvSetStream(kDLCUDA, device, previous, nullptr);
  }
  ~StreamScope() {
    if (active)
      TVMFFIEnvSetStream(kDLCUDA, device, previous, nullptr);
  }
};
struct Plan {
  Array<int64_t> info;
  uint64_t fw, iw, fb, ib, indices, stream;
  int32_t batch, heads, kv_heads, rows, page_size, device;
};
static thread_local char last_error[1024]{};
static int fail(const char *what) {
  std::strncpy(last_error, what, sizeof(last_error) - 1);
  last_error[sizeof(last_error) - 1] = 0;
  return 1;
}
extern "C" const char *antfly_attention_error() { return last_error; }
extern "C" uint64_t antfly_attention_abi() {
  return (uint64_t(1) << 32) | (sizeof(PlanRequest) << 16) | sizeof(RunRequest);
}
extern "C" int antfly_attention_plan(const PlanRequest *r, void **output) {
  if (!output)
    return fail("missing plan output");
  *output = nullptr;
  try {
    if (!r || !r->float_workspace || !r->int_workspace ||
        !r->pinned_workspace || !r->cpu_query_indptr || !r->cpu_kv_indptr ||
        !r->cpu_kv_lengths || r->batch < 1 || r->batch > 16 || r->rows < 2 ||
        r->rows > 4096 || r->page_size != 16 ||
        !((r->heads == 8 && r->kv_heads == 2) ||
          (r->heads == 16 && r->kv_heads == 4)) ||
        r->float_bytes < 64 * 1024 * 1024 || r->int_bytes < 128 * 1024 ||
        r->pinned_bytes < r->int_bytes)
      return fail("unsupported native attention plan");
    auto qp = reinterpret_cast<const int32_t *>(r->cpu_query_indptr);
    auto kp = reinterpret_cast<const int32_t *>(r->cpu_kv_indptr);
    auto lens = reinterpret_cast<const int32_t *>(r->cpu_kv_lengths);
    if (qp[0] || kp[0] || qp[r->batch] != r->rows || kp[r->batch] > 4096)
      return fail("invalid prefix bounds");
    for (int i = 0; i < r->batch; ++i)
      if (qp[i + 1] - qp[i] < 2 || lens[i] < qp[i + 1] - qp[i] ||
          lens[i] > 4096 || kp[i + 1] - kp[i] != (lens[i] + 15) / 16)
        return fail("invalid causal prefix lengths");
    DLDevice gpu{kDLCUDA, r->device}, cpu{kDLCPU, 0};
    View fw(r->float_workspace, kDLUInt, 8, gpu, {int64_t(r->float_bytes)});
    View iw(r->int_workspace, kDLUInt, 8, gpu, {int64_t(r->int_bytes)});
    View pw(r->pinned_workspace, kDLUInt, 8, cpu, {int64_t(r->pinned_bytes)});
    View qo(r->cpu_query_indptr, kDLInt, 32, cpu, {r->batch + 1});
    View kv(r->cpu_kv_indptr, kDLInt, 32, cpu, {r->batch + 1});
    View kl(r->cpu_kv_lengths, kDLInt, 32, cpu, {r->batch});
    StreamScope stream(r->device, r->stream);
    auto result = std::make_unique<Plan>();
    result->info = BatchPrefillWithKVCachePlan(
        fw, iw, pw, qo, kv, kl, r->rows, r->batch, r->heads, r->kv_heads, 16,
        false, 256, 256, true, -1, -1, false, 0, 0);
    if (result->info.size() != 15 ||
        (result->info[3] != 16 && result->info[3] != 64 &&
         result->info[3] != 128))
      return fail("unsupported native HD256 plan dispatch");
    result->stream = r->stream;
    result->fw = r->float_workspace;
    result->iw = r->int_workspace;
    result->fb = r->float_bytes;
    result->ib = r->int_bytes;
    result->indices = kp[r->batch];
    result->batch = r->batch;
    result->heads = r->heads;
    result->kv_heads = r->kv_heads;
    result->rows = r->rows;
    result->page_size = 16;
    result->device = r->device;
    if (stream.close())
      return fail("cannot restore native CUDA stream");
    *output = result.release();
    return 0;
  } catch (const std::exception &e) {
    return fail(e.what());
  } catch (...) {
    return fail("unknown native attention plan failure");
  }
}
extern "C" int antfly_attention_run(const RunRequest *r) {
  try {
    if (!r || !r->plan || !r->q || !r->k || !r->v || !r->out ||
        !r->query_indptr || !r->kv_indptr || !r->kv_indices ||
        !r->last_page_lengths || !r->pages)
      return fail("invalid native attention buffers");
    const auto &p = *reinterpret_cast<const Plan *>(r->plan);
    if (r->stream != p.stream)
      return fail("native plan requires its owning execution stream");
    DLDevice gpu{kDLCUDA, p.device};
    View fw(p.fw, kDLUInt, 8, gpu, {int64_t(p.fb)}),
        iw(p.iw, kDLUInt, 8, gpu, {int64_t(p.ib)});
    View q(r->q, kDLFloat, 16, gpu, {p.rows, p.heads, 256});
    View k(r->k, kDLFloat, 16, gpu, {int64_t(r->pages), 16, p.kv_heads, 256});
    View v(r->v, kDLFloat, 16, gpu, {int64_t(r->pages), 16, p.kv_heads, 256});
    View o(r->out, kDLFloat, 32, gpu, {p.rows, p.heads, 256});
    View qo(r->query_indptr, kDLInt, 32, gpu, {p.batch + 1});
    View kv(r->kv_indptr, kDLInt, 32, gpu, {p.batch + 1});
    View idx(r->kv_indices, kDLInt, 32, gpu, {int64_t(p.indices)});
    View tail(r->last_page_lengths, kDLInt, 32, gpu, {p.batch});
    StreamScope stream(p.device, r->stream);
    BatchPrefillWithPagedKVCacheRun(
        fw, iw, p.info, q, k, v, qo, kv, idx, tail, o, {}, 1, 0, -1, false, {},
        {}, {}, {}, {}, {}, {}, {}, 0.0, 1.0 / 16, 1.0, 1.0 / 10000, 0);
    if (stream.close())
      return fail("cannot restore native CUDA stream");
    return 0;
  } catch (const std::exception &e) {
    return fail(e.what());
  } catch (...) {
    return fail("unknown native attention execution failure");
  }
}
extern "C" void antfly_attention_drop(void *plan) {
  delete reinterpret_cast<Plan *>(plan);
}
