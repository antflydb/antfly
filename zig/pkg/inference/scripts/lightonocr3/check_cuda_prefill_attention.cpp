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

// Native ABI addressing, causality, plan-lifetime and workspace qualification.
// This constructed saturated-softmax oracle does not measure general accuracy
// or throughput. Build beside the optional library, then run with its native
// dependencies available (no Python or Torch runtime required):
// g++ -std=c++17 -O3 check_cuda_prefill_attention.cpp -I/usr/local/cuda/include
//   -L/usr/local/cuda/lib64 -lcudart -L<library-dir> -lantfly_lighton_prefill
//   -Wl,-rpath,/usr/local/cuda/lib64 -Wl,-rpath,<absolute-library-dir> -o
//   <output>
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <cuda_fp16.h>
#include <cuda_runtime_api.h>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>
struct PlanRequest {
  uint64_t float_workspace, int_workspace, pinned_workspace, float_bytes,
      int_bytes, pinned_bytes;
  uint64_t cpu_query_indptr, cpu_kv_indptr, cpu_kv_lengths;
  int32_t batch, heads, kv_heads, rows, page_size, device;
  uint64_t stream;
};
struct RunRequest {
  uint64_t plan, q, k, v, out, query_indptr, kv_indptr, kv_indices,
      last_page_lengths, pages, stream;
};
extern "C" int antfly_attention_plan(const PlanRequest *, void **);
extern "C" int antfly_attention_run(const RunRequest *);
extern "C" void antfly_attention_drop(void *);
extern "C" const char *antfly_attention_error();
static void check(cudaError_t rc) {
  if (rc != cudaSuccess)
    throw std::runtime_error(cudaGetErrorString(rc));
}
static uint64_t pointer(const void *p) {
  return reinterpret_cast<uintptr_t>(p);
}
struct Owned {
  cudaStream_t stream{};
  void *pinned = nullptr;
  void *plan = nullptr;
  std::vector<void *> allocations;
  Owned() {
    check(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
    check(cudaHostAlloc(&pinned, 128 * 1024 + 64, cudaHostAllocDefault));
    std::memset(pinned, 0x5a, 128 * 1024 + 64);
  }
  ~Owned() {
    cudaStreamSynchronize(stream);
    antfly_attention_drop(plan);
    for (auto p : allocations)
      cudaFree(p);
    cudaFreeHost(pinned);
    cudaStreamDestroy(stream);
  }
  void *alloc(size_t bytes) {
    void *p = nullptr;
    check(cudaMalloc(&p, bytes));
    try {
      allocations.push_back(p);
    } catch (...) {
      cudaFree(p);
      throw;
    }
    return p;
  }
  void *guarded(size_t bytes) {
    void *p = alloc(bytes + 64);
    check(cudaMemset(static_cast<uint8_t *>(p) + bytes, 0x5a, 64));
    return p;
  }
  static void checkGuard(uint64_t p, size_t bytes) {
    std::array<uint8_t, 64> actual{};
    check(cudaMemcpy(actual.data(), reinterpret_cast<void *>(p + bytes),
                     actual.size(), cudaMemcpyDeviceToHost));
    for (auto x : actual)
      if (x != 0x5a)
        throw std::runtime_error("floating workspace guard overwritten");
  }
  template <class T> void *upload(const std::vector<T> &values) {
    void *p = alloc(values.size() * sizeof(T));
    check(cudaMemcpy(p, values.data(), values.size() * sizeof(T),
                     cudaMemcpyHostToDevice));
    return p;
  }
};
int main() {
  try {
    for (int heads : {8, 16})
      for (int batch : {1, 2}) {
        int kh = heads / 4;
        Owned owned;
        std::vector<int32_t> lengths{33}, queries{17}, query_indptr{0, 17},
            kv_indptr{0, 3}, indices{3, 1, 5}, tails{1};
        if (batch == 2) {
          lengths.push_back(25);
          queries.push_back(9);
          query_indptr.push_back(26);
          kv_indptr.push_back(5);
          indices.push_back(7);
          indices.push_back(9);
          tails.push_back(9);
        }
        int rows = query_indptr.back(), pages = batch == 1 ? 6 : 10,
            tokens = pages * 16;
        std::vector<__half> q(rows * heads * 256, __float2half_rn(0));
        auto nan = __float2half_rn(std::numeric_limits<float>::quiet_NaN());
        std::vector<__half> k(tokens * kh * 256, nan), v(k.size(), nan);
        for (int row = 0; row < rows; ++row)
          for (int h = 0; h < heads; ++h)
            q[(row * heads + h) * 256] = __float2half_rn(16);
        auto value = [](int sequence, int token, int h, int d) {
          return float((sequence * 11 + token * 7 + h * 5 + d) % 31 - 15) / 32;
        };
        for (int sequence = 0; sequence < batch; ++sequence)
          for (int token = 0; token < lengths[sequence]; ++token) {
            int physical =
                indices[kv_indptr[sequence] + token / 16] * 16 + token % 16;
            for (int h = 0; h < kh; ++h)
              for (int d = 0; d < 256; ++d) {
                k[(physical * kh + h) * 256 + d] =
                    __float2half_rn(d == 0 ? token * 128.f : 0);
                v[(physical * kh + h) * 256 + d] =
                    __float2half_rn(value(sequence, token, h, d));
              }
          }
        std::vector<float> out(rows * heads * 256 + 16, 123456.f),
            actual(out.size()), first;
        auto qg = owned.upload(q), kg = owned.upload(k), vg = owned.upload(v),
             og = owned.upload(out);
        auto qip = owned.upload(query_indptr), kip = owned.upload(kv_indptr),
             idx = owned.upload(indices), tail = owned.upload(tails);
        PlanRequest plan{};
        plan.float_bytes = 64 * 1024 * 1024;
        plan.int_bytes = 128 * 1024;
        plan.pinned_bytes = 128 * 1024;
        plan.float_workspace = pointer(owned.guarded(plan.float_bytes));
        plan.int_workspace = pointer(
            owned.upload(std::vector<uint8_t>(plan.int_bytes + 64, 0x5a)));
        plan.pinned_workspace = pointer(owned.pinned);
        plan.cpu_query_indptr = pointer(query_indptr.data());
        plan.cpu_kv_indptr = pointer(kv_indptr.data());
        plan.cpu_kv_lengths = pointer(lengths.data());
        plan.batch = batch;
        plan.heads = heads;
        plan.kv_heads = kh;
        plan.rows = rows;
        plan.page_size = 16;
        plan.device = 0;
        plan.stream = pointer(owned.stream);
        if (antfly_attention_plan(&plan, &owned.plan))
          throw std::runtime_error(antfly_attention_error());
        RunRequest run{pointer(owned.plan), pointer(qg),          pointer(kg),
                       pointer(vg),         pointer(og),          pointer(qip),
                       pointer(kip),        pointer(idx),         pointer(tail),
                       uint64_t(pages),     pointer(owned.stream)};
        auto invalid = plan;
        void *rejected = nullptr;
        invalid.rows = 4097;
        if (antfly_attention_plan(&invalid, &rejected) == 0 || rejected)
          throw std::runtime_error("oversized plan admitted");
        invalid = plan;
        invalid.float_bytes--;
        if (antfly_attention_plan(&invalid, &rejected) == 0 || rejected)
          throw std::runtime_error("insufficient workspace admitted");
        auto invalid_run = run;
        invalid_run.stream = 0;
        if (antfly_attention_run(&invalid_run) == 0)
          throw std::runtime_error("foreign stream accepted");
        // The native plan must not retain the caller's CPU metadata arrays.
        auto original_queries = query_indptr;
        auto original_kv = kv_indptr;
        auto original_lengths = lengths;
        std::fill(query_indptr.begin(), query_indptr.end(), -123);
        std::fill(kv_indptr.begin(), kv_indptr.end(), -123);
        std::fill(lengths.begin(), lengths.end(), -123);
        check(cudaStreamSynchronize(owned.stream));
        invalid = plan;
        invalid.int_bytes--;
        if (antfly_attention_plan(&invalid, &rejected) == 0 || rejected)
          throw std::runtime_error("short integer workspace admitted");
        invalid = plan;
        invalid.pinned_bytes--;
        if (antfly_attention_plan(&invalid, &rejected) == 0 || rejected)
          throw std::runtime_error("short pinned workspace admitted");
        float worst = 0;
        for (int repeat = 0; repeat < 3; ++repeat) {
          if (antfly_attention_run(&run))
            throw std::runtime_error(antfly_attention_error());
          check(cudaStreamSynchronize(owned.stream));
          check(cudaMemcpy(actual.data(), og, actual.size() * 4,
                           cudaMemcpyDeviceToHost));
          for (int sequence = 0; sequence < batch; ++sequence)
            for (int row = 0; row < queries[sequence]; ++row) {
              int absolute =
                  original_lengths[sequence] - queries[sequence] + row;
              for (int h = 0; h < heads; ++h)
                for (int d = 0; d < 256; ++d) {
                  float x =
                      actual[((original_queries[sequence] + row) * heads + h) *
                                 256 +
                             d];
                  float expected = value(sequence, absolute, h / 4, d);
                  if (!std::isfinite(x))
                    throw std::runtime_error("non-finite native output");
                  worst = std::max(worst, std::abs(x - expected));
                }
            }
          if (worst > 2e-5)
            throw std::runtime_error(
                "native causal/addressing oracle mismatch");
          for (size_t i = actual.size() - 16; i < actual.size(); ++i)
            if (actual[i] != 123456.f)
              throw std::runtime_error("output canary overwritten");
          if (repeat == 0)
            first = actual;
          else if (std::memcmp(first.data(), actual.data(), actual.size() * 4))
            throw std::runtime_error("plan reuse changed native output");
        }
        for (auto pair :
             {std::pair<void *, const std::vector<__half> *>{qg, &q},
              {kg, &k},
              {vg, &v}}) {
          std::vector<__half> copy(pair.second->size());
          check(cudaMemcpy(copy.data(), pair.first,
                           copy.size() * sizeof(__half),
                           cudaMemcpyDeviceToHost));
          if (std::memcmp(copy.data(), pair.second->data(),
                          copy.size() * sizeof(__half)))
            throw std::runtime_error("native kernel changed Q/K/V");
        }
        Owned::checkGuard(plan.float_workspace, plan.float_bytes);
        auto pinned_bytes = static_cast<uint8_t *>(owned.pinned);
        for (size_t i = 128 * 1024; i < 128 * 1024 + 64; ++i)
          if (pinned_bytes[i] != 0x5a)
            throw std::runtime_error("pinned workspace canary overwritten");
        std::vector<uint8_t> int_guard(64);
        check(cudaMemcpy(
            int_guard.data(),
            reinterpret_cast<void *>(plan.int_workspace + plan.int_bytes), 64,
            cudaMemcpyDeviceToHost));
        for (auto x : int_guard)
          if (x != 0x5a)
            throw std::runtime_error("integer workspace canary overwritten");
        std::cout << "heads=" << heads << " pages=" << batch << " rows=" << rows
                  << " repeats=3 max_abs=" << worst
                  << " canaries=1 readonly=1 holes=1 native_only=1\n";
      }
    int planning_cases = 0;
    for (int heads : {8, 16}) {
      Owned owned;
      PlanRequest p{};
      p.float_bytes = 64 * 1024 * 1024;
      p.int_bytes = 128 * 1024;
      p.pinned_bytes = 128 * 1024;
      p.float_workspace = pointer(owned.guarded(p.float_bytes));
      p.int_workspace =
          pointer(owned.upload(std::vector<uint8_t>(p.int_bytes + 64, 0x5a)));
      p.pinned_workspace = pointer(owned.pinned);
      p.batch = 1;
      p.heads = heads;
      p.kv_heads = heads / 4;
      p.page_size = 16;
      p.device = 0;
      p.stream = pointer(owned.stream);
      for (int queries : {2, 17, 32, 256, 432, 662, 2048, 4096})
        for (int total : {queries, std::min(4096, queries + 17), 4096}) {
          std::vector<int32_t> q{0, queries}, kv{0, (total + 15) / 16},
              length{total};
          p.rows = queries;
          p.cpu_query_indptr = pointer(q.data());
          p.cpu_kv_indptr = pointer(kv.data());
          p.cpu_kv_lengths = pointer(length.data());
          antfly_attention_drop(owned.plan);
          owned.plan = nullptr;
          if (antfly_attention_plan(&p, &owned.plan))
            throw std::runtime_error(antfly_attention_error());
          check(cudaStreamSynchronize(owned.stream));
          auto pinned = static_cast<uint8_t *>(owned.pinned);
          for (size_t i = 128 * 1024; i < 128 * 1024 + 64; ++i)
            if (pinned[i] != 0x5a)
              throw std::runtime_error("planning pinned guard overwritten");
          std::vector<uint8_t> guard(64);
          check(cudaMemcpy(
              guard.data(),
              reinterpret_cast<void *>(p.int_workspace + p.int_bytes), 64,
              cudaMemcpyDeviceToHost));
          for (auto x : guard)
            if (x != 0x5a)
              throw std::runtime_error("planning device guard overwritten");
          Owned::checkGuard(p.float_workspace, p.float_bytes);
          ++planning_cases;
        }
    }
    std::cout << "bounded_workspace_plans=" << planning_cases
              << " int_bytes=131072 pinned_bytes=131072 guards=1\n";
    std::cout << "Native ABI addressing qualification passed; randomized "
                 "mixed-precision accuracy remains separate.\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
