// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Compare every output bit with a pre-change packaged CUDA module. The module
// must be retained and hashed before artifact regeneration; never compare the
// optimized kernel to itself. Usage: qualify-q4-staged BASELINE.cubin [--quick]
// [--candidate CANDIDATE.cubin]. Without --candidate, compare four staging
// widths compiled from the canonical header; with it, test the shipped export.
#include <cuda.h>
#include <cuda_runtime.h>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>
#include "../../src/graph/templates/cuda_q4_0_bf16_staged.cuh"

static void check(cudaError_t e) { if (e != cudaSuccess) { std::fprintf(stderr, "%s\n", cudaGetErrorString(e)); std::exit(2); } }
static void check(CUresult e) { if (e != CUDA_SUCCESS) { const char* s = nullptr; cuGetErrorString(e, &s); std::fprintf(stderr, "%s\n", s); std::exit(2); } }
template <class T> struct Device {
    T* p = nullptr;
    explicit Device(size_t n) { check(cudaMalloc(&p, n * sizeof(T))); }
    explicit Device(const std::vector<T>& v) : Device(v.size()) { check(cudaMemcpy(p, v.data(), v.size() * sizeof(T), cudaMemcpyHostToDevice)); }
    ~Device() { cudaFree(p); }
    Device(const Device&) = delete;
    std::vector<T> read(size_t n) { std::vector<T> v(n); check(cudaMemcpy(v.data(), p, n * sizeof(T), cudaMemcpyDeviceToHost)); return v; }
};
template <unsigned K> __global__ void candidate(float* dst, const float* x, const unsigned char* w, unsigned m, unsigned k, unsigned n) {
    antfly_q4_0_bf16_staged<K>(dst, x, w, m, k, n);
}
template <class F> static float time_ms(F launch, unsigned repeats) {
    for (unsigned i = 0; i < 3; ++i) launch();
    cudaEvent_t start, end;
    check(cudaEventCreate(&start)); check(cudaEventCreate(&end));
    check(cudaEventRecord(start));
    for (unsigned i = 0; i < repeats; ++i) launch();
    check(cudaEventRecord(end)); check(cudaEventSynchronize(end));
    float elapsed; check(cudaEventElapsedTime(&elapsed, start, end));
    check(cudaEventDestroy(start)); check(cudaEventDestroy(end));
    return elapsed / repeats;
}
static bool run(CUfunction baseline, CUfunction packaged, unsigned rows, unsigned in_dim, unsigned out_dim, unsigned pattern, unsigned repeats) {
    std::mt19937 rng(71 + rows + in_dim + out_dim + pattern);
    std::normal_distribution<float> normal(0.f, 1.f);
    std::vector<float> input(size_t(rows) * in_dim);
    for (auto& x : input) x = pattern == 2 ? -0.0f : normal(rng) * (pattern == 1 ? 1e6f : 1.f);
    const size_t blocks = size_t(out_dim) * in_dim / 32;
    std::vector<unsigned char> packed(blocks * 18);
    for (size_t i = 0; i < blocks; ++i) {
        // Signed scales, small scales, and all nibble values; large finite
        // activations exercise the BF16 range which motivated this route.
        const auto scale = __half_as_ushort(__float2half_rn(normal(rng) * (pattern == 1 ? 1e-3f : 0.1f)));
        packed[2 * i] = scale & 255; packed[2 * i + 1] = scale >> 8;
        for (unsigned j = 0; j < 16; ++j) packed[blocks * 2 + i * 16 + j] = rng() & 255;
    }
    Device<float> x(input), ref(input.size() / in_dim * out_dim), got(input.size() / in_dim * out_dim);
    Device<unsigned char> w(packed);
    void* args[] = {&ref.p, &x.p, &w.p, &rows, &in_dim, &out_dim};
    auto original = [&] { check(cuLaunchKernel(baseline, (out_dim + 31) / 32, (rows + 63) / 64, 1, 256, 1, 1, 0, nullptr, args, nullptr)); };
    original(); auto expected = ref.read(size_t(rows) * out_dim);
    const float baseline_ms = time_ms(original, repeats);
    bool passed = true;
    for (unsigned stage : {16u, 32u, 64u, 128u}) {
        if (packaged && stage != 32) continue;
        const void* fn = stage == 16 ? (void*)candidate<16> : stage == 32 ? (void*)candidate<32> : stage == 64 ? (void*)candidate<64> : (void*)candidate<128>;
        void* actual_args[] = {&got.p, &x.p, &w.p, &rows, &in_dim, &out_dim};
        auto launch = [&] {
            if (packaged) check(cuLaunchKernel(packaged, (out_dim + 31) / 32, (rows + 63) / 64, 1, 256, 1, 1, 0, nullptr, actual_args, nullptr));
            else check(cudaLaunchKernel(fn, dim3((out_dim + 31) / 32, (rows + 63) / 64), dim3(256), actual_args));
        };
        check(cudaMemset(got.p, 0xff, size_t(rows) * out_dim * sizeof(float)));
        launch(); auto actual = got.read(size_t(rows) * out_dim);
        size_t mismatches = 0; bool finite = true;
        for (size_t i = 0; i < actual.size(); ++i) { mismatches += std::memcmp(&actual[i], &expected[i], 4) != 0; finite &= std::isfinite(actual[i]) && std::isfinite(expected[i]); }
        const float elapsed = time_ms(launch, repeats);
        passed &= mismatches == 0 && finite;
        std::printf("{\"rows\":%u,\"in\":%u,\"out\":%u,\"pattern\":%u,\"stage\":%u,\"mismatches\":%zu,\"finite\":%s,\"baseline_ms\":%.6f,\"candidate_ms\":%.6f,\"speedup\":%.4f}\n", rows, in_dim, out_dim, pattern, stage, mismatches, finite ? "true" : "false", baseline_ms, elapsed, baseline_ms / elapsed);
        std::fflush(stdout);
    }
    return passed;
}
int main(int argc, char** argv) {
    if (argc < 2) return 2;
    bool quick = false;
    const char* candidate_path = nullptr;
    for (int i = 2; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--quick")) quick = true;
        else if (!std::strcmp(argv[i], "--candidate") && i + 1 < argc) candidate_path = argv[++i];
        else return 2;
    }
    check(cudaSetDevice(0)); check(cudaFree(nullptr));
    CUmodule module; CUfunction baseline;
    check(cuModuleLoad(&module, argv[1]));
    check(cuModuleGetFunction(&baseline, module, "termite_linear_q4_0_f32_tc_hmma_bf16"));
    CUmodule candidate_module = nullptr; CUfunction packaged = nullptr;
    if (candidate_path) {
        check(cuModuleLoad(&candidate_module, candidate_path));
        check(cuModuleGetFunction(&packaged, candidate_module, "antfly_linear_q4_0_bf16_staged"));
    }
    bool passed = true;
    // Output tails, K-stage tails, row/tile boundaries, E4B projection shapes.
    const unsigned shapes[][3] = {{1,32,1},{2,96,17},{17,256,33},{63,288,65},{65,160,31},{253,2560,512},{256,2560,10240},{256,2560,2560},{256,2048,2560},{256,4096,2560},{256,2560,1024},{256,2560,2048},{256,2560,4096},{512,2560,10240}};
    for (const auto& s : shapes) {
        if (quick && s[0] >= 253 && s[2] != 512) continue;
        for (unsigned pattern = 0; pattern < 3; ++pattern) passed &= run(baseline, packaged, s[0], s[1], s[2], pattern, quick ? 1 : 10);
    }
    if (candidate_module) check(cuModuleUnload(candidate_module));
    check(cuModuleUnload(module));
    return passed ? 0 : 1;
}
