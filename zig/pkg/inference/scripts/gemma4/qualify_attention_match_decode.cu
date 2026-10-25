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

#include "../../src/ops/cuda/attention_match_decode_kernels.cuh"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <vector>

static void check(cudaError_t e) {
    if (e != cudaSuccess) { std::fprintf(stderr, "%s\n", cudaGetErrorString(e)); std::exit(2); }
}
template<class T> struct Device {
    T* p = nullptr;
    size_t n;
    explicit Device(size_t count) : n(count) { check(cudaMalloc(&p, n * sizeof(T))); }
    explicit Device(const std::vector<T>& x) : Device(x.size()) { check(cudaMemcpy(p, x.data(), n * sizeof(T), cudaMemcpyHostToDevice)); }
    ~Device() { cudaFree(p); }
    Device(const Device&) = delete;
    std::vector<T> read() const { std::vector<T> x(n); check(cudaMemcpy(x.data(), p, n * sizeof(T), cudaMemcpyDeviceToHost)); return x; }
};
template<class T> T stored(float x) { return T(x); }
template<> __half stored(float x) { return __float2half(x); }
template<class T> float loaded(T x) { return float(x); }
template<> float loaded(__half x) { return __half2float(x); }

template<class T> void run(int dim, int rows, int qr, float cap, uint64_t window, bool masked, bool wrap) {
    constexpr int qh = 8, kh = 2;
    const uint64_t origin = uint64_t(1) << 35; // no truncation of logical positions
    const uint64_t first_query = origin + 2 * rows - 1;
    const float scale = 1 / std::sqrt(float(dim));
    const int splits = (rows + 255) / 256;
    std::vector<float> q(size_t(qr) * qh * dim);
    for (size_t i = 0; i < q.size(); ++i) q[i] = std::sin(double(i % 991) * .17) * .7;
    std::vector<T> k[kh], v[kh];
    std::vector<float> bias[kh];
    std::vector<uint64_t> positions[kh], ends[kh];
    std::unique_ptr<Device<T>> dk[kh], dv[kh];
    std::unique_ptr<Device<float>> db[kh];
    std::unique_ptr<Device<uint64_t>> dp[kh], de[kh];
    std::vector<AntflyMatchHead> heads(kh);
    for (int h = 0; h < kh; ++h) {
        const int n = h ? std::max(1, rows * 2 / 3) : rows;
        const int capacity = n + 13, start = wrap ? capacity - 7 : 0;
        k[h].resize(size_t(capacity) * dim); v[h].resize(k[h].size());
        bias[h].resize(capacity); positions[h].resize(capacity); ends[h].resize(capacity);
        for (int r = 0; r < n; ++r) {
            const int physical = (start + r) % capacity;
            positions[h][physical] = origin + 3 * r + h;
            ends[h][physical] = masked ? first_query + qr + 100 : positions[h][physical] + 1;
            if (!masked && r < n / 2) ends[h][physical] = origin + 2 * n;
            bias[h][physical] = r < n / 2 ? std::log(float(2 + r % 7)) + (h ? .5f : -.5f) : 0;
            for (int d = 0; d < dim; ++d) {
                const size_t i = size_t(r) * dim + d;
                k[h][size_t(physical) * dim + d] = stored<T>(std::cos(double((i + h * 7) % 997) * .13) * .8);
                v[h][size_t(physical) * dim + d] = stored<T>(std::sin(double((i + h * 13) % 983) * .11) * .9);
            }
        }
        dk[h] = std::make_unique<Device<T>>(k[h]); dv[h] = std::make_unique<Device<T>>(v[h]);
        db[h] = std::make_unique<Device<float>>(bias[h]); dp[h] = std::make_unique<Device<uint64_t>>(positions[h]); de[h] = std::make_unique<Device<uint64_t>>(ends[h]);
        heads[h] = {dk[h]->p, dv[h]->p, db[h]->p, dp[h]->p, de[h]->p, uint64_t(n), uint64_t(capacity), uint64_t(start)};
    }
    Device<AntflyMatchHead> dh(heads);
    Device<float> dq(q), partials(size_t(qr) * qh * splits * (dim + 2)), output(q.size()), mass(qr * qh);
    if constexpr (sizeof(T) == 2)
        attention_match_partial_f16<<<dim3(splits, qr * qh), 256>>>(dq.p, dh.p, partials.p, qh, kh, dim, splits, first_query, scale, cap, window);
    else
        attention_match_partial_f32<<<dim3(splits, qr * qh), 256>>>(dq.p, dh.p, partials.p, qh, kh, dim, splits, first_query, scale, cap, window);
    check(cudaGetLastError());
    attention_match_merge<<<qr * qh, 256>>>(partials.p, output.p, mass.p, dim, splits);
    check(cudaGetLastError()); check(cudaDeviceSynchronize());
    const auto actual = output.read(), actual_mass = mass.read();
    double max_error = 0, max_mass_error = 0;
    for (int qi = 0; qi < qr * qh; ++qi) {
        const int h = (qi % qh) / (qh / kh), n = heads[h].rows;
        const uint64_t qp = first_query + qi / qh;
        const uint64_t first = window && qp >= window - 1 ? qp - (window - 1) : 0;
        std::vector<double> scores(n), expected(dim, 0);
        double maximum = -INFINITY;
        for (int r = 0; r < n; ++r) {
            const int p = (heads[h].start + r) % heads[h].capacity;
            if (!ends[h][p] || ends[h][p] - 1 > qp || positions[h][p] > qp || (window && positions[h][p] < first)) { scores[r] = -INFINITY; continue; }
            double dot = 0;
            for (int d = 0; d < dim; ++d) dot += double(q[size_t(qi) * dim + d]) * loaded(k[h][size_t(p) * dim + d]);
            dot *= scale;
            if (cap > 0) dot = cap * std::tanh(dot / cap);
            scores[r] = dot + bias[h][p]; maximum = std::max(maximum, scores[r]);
        }
        double sum = 0;
        for (double& x : scores) { x = maximum == -INFINITY ? 0 : std::exp(x - maximum); sum += x; }
        if (sum > 0) for (int r = 0; r < n; ++r) {
            const int p = (heads[h].start + r) % heads[h].capacity;
            for (int d = 0; d < dim; ++d) expected[d] += scores[r] / sum * loaded(v[h][size_t(p) * dim + d]);
        }
        for (int d = 0; d < dim; ++d) {
            const double error = std::abs(expected[d] - actual[size_t(qi) * dim + d]);
            if (!std::isfinite(error)) std::exit(3);
            max_error = std::max(max_error, error);
        }
        if (sum > 0) {
            const double error = std::abs(maximum + std::log(sum) - actual_mass[qi]);
            if (!std::isfinite(error)) std::exit(3);
            max_mass_error = std::max(max_mass_error, error);
        } else if (actual_mass[qi] != -INFINITY) std::exit(3);
    }
    std::printf("dtype=f%zu dim=%d rows=%d queries=%d cap=%.1f window=%llu masked=%d wrap=%d max_abs=%.9g mass_abs=%.9g\n", sizeof(T) * 8, dim, rows, qr, cap, (unsigned long long)window, masked, wrap, max_error, max_mass_error);
    if (max_error > 3e-5 || max_mass_error > 3e-5) std::exit(1);
}
int main(int argc, char**) {
    const bool quick = argc > 1;
    int cases = 0;
    for (int dim : {64, 256, 512}) for (int rows : {1, 17, 255, 257, 2053, 8192, 32768}) {
        if (quick && rows > 257) continue;
        for (int qr : {1, 3}) for (int variant = 0; variant < 4; ++variant) {
            const float cap = variant & 1 ? 2 : 0;
            const uint64_t window = variant == 2 ? 512 : 0;
            const bool masked = variant == 3, wrap = variant != 0;
            run<float>(dim, rows, qr, cap, window, masked, wrap);
            run<__half>(dim, rows, qr, cap, window, masked, wrap);
            cases += 2;
        }
    }
    run<__half>(512, 513, 17, 2, 512, false, true); ++cases;
    std::printf("PASS: %d ragged attention cases; serving_qualified=false\n", cases);
}
