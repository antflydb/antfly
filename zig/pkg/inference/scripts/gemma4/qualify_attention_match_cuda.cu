// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Standalone differential qualification, independent of serving dispatch.
// Build in the worktree's zig/zig-out/bin; see native-kv-compaction.md.
#include "../../src/graph/templates/cuda_attention_match_reference.cuh"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <vector>

static void check(cudaError_t error) {
    if (error != cudaSuccess) {
        std::fprintf(stderr, "CUDA failure: %s\n", cudaGetErrorString(error));
        std::exit(2);
    }
}

template<class T> struct Device {
    T* data = nullptr;
    size_t count;
    explicit Device(size_t n) : count(n) { check(cudaMalloc(&data, n * sizeof(T))); }
    explicit Device(const std::vector<T>& values) : Device(values.size()) {
        check(cudaMemcpy(data, values.data(), count * sizeof(T), cudaMemcpyHostToDevice));
    }
    ~Device() { cudaFree(data); }
    Device(const Device&) = delete;
    Device& operator=(const Device&) = delete;
    std::vector<T> read() const {
        std::vector<T> result(count);
        check(cudaMemcpy(result.data(), data, count * sizeof(T), cudaMemcpyDeviceToHost));
        return result;
    }
};

static void run(int dim, int rows, int qr, float cap, uint64_t window, bool masked) {
    constexpr int qh = 8, kh = 2;
    const float scale = 1.0f / std::sqrt(float(dim));
    std::vector<float> q(size_t(qr) * qh * dim), k(size_t(kh) * rows * dim), v(k.size()), bias(kh * rows);
    std::vector<uint64_t> positions(kh * rows), ends(kh * rows), query_positions(qr);
    for (size_t i = 0; i < q.size(); ++i) q[i] = std::sin(double(i % 991) * 0.17) * 0.7;
    for (size_t i = 0; i < k.size(); ++i) {
        k[i] = std::cos(double(i % 997) * 0.13) * 0.8;
        v[i] = std::sin(double(i % 983) * 0.11) * 0.9;
    }
    for (int h = 0; h < kh; ++h) for (int r = 0; r < rows; ++r) {
        const int i = h * rows + r;
        positions[i] = 3 * r + h;
        ends[i] = masked ? uint64_t(rows * 3 + qr + 100) : positions[i] + 1;
        // Old fitted rows represent a whole block; exact appended rows have no bias.
        if (!masked && r < rows / 2) ends[i] = rows * 2;
        bias[i] = r < rows / 2 ? std::log(float(2 + r % 7)) + (h ? 0.5f : -0.5f) : 0;
    }
    for (int i = 0; i < qr; ++i) query_positions[i] = rows * 2 + i - 1;
    Device<float> dq(q), dk(k), dv(v), db(bias), scores(size_t(qr) * qh * rows), output(q.size()), mass(qr * qh);
    Device<uint64_t> dp(positions), de(ends), dqp(query_positions);
    antfly_attention_match_scores<<<(scores.count + 127) / 128, 128>>>(dq.data, dk.data, db.data, dp.data, de.data, dqp.data,
        scores.data, qr, qh, kh, rows, dim, scale, cap, window);
    check(cudaGetLastError());
    antfly_attention_match_normalize<<<qr * qh, 256>>>(scores.data, mass.data, rows);
    check(cudaGetLastError());
    antfly_attention_match_values<<<(output.count + 127) / 128, 128>>>(scores.data, dv.data, output.data, qr, qh, kh, rows, dim);
    check(cudaGetLastError());
    check(cudaDeviceSynchronize());
    const auto actual = output.read();
    const auto actual_mass = mass.read();
    double max_error = 0, max_mass_error = 0;
    std::vector<double> logits(rows), expected(dim);
    for (int qi = 0; qi < qr * qh; ++qi) {
        const int head = (qi % qh) / (qh / kh);
        const uint64_t qp = query_positions[qi / qh];
        const uint64_t first = window && qp >= window - 1 ? qp - (window - 1) : 0;
        double maximum = -INFINITY;
        for (int r = 0; r < rows; ++r) {
            const int kr = head * rows + r;
            if (!ends[kr] || ends[kr] - 1 > qp || positions[kr] > qp || (window && positions[kr] < first)) {
                logits[r] = -INFINITY;
                continue;
            }
            double dot = 0;
            for (int d = 0; d < dim; ++d) dot += double(q[size_t(qi) * dim + d]) * k[size_t(kr) * dim + d];
            dot *= scale;
            if (cap > 0) dot = cap * std::tanh(dot / cap);
            logits[r] = dot + bias[kr];
            maximum = std::max(maximum, logits[r]);
        }
        double sum = 0;
        for (auto& x : logits) { x = maximum == -INFINITY ? 0 : std::exp(x - maximum); sum += x; }
        std::fill(expected.begin(), expected.end(), 0);
        if (sum > 0) for (int r = 0; r < rows; ++r) for (int d = 0; d < dim; ++d)
            expected[d] += logits[r] / sum * v[(size_t(head) * rows + r) * dim + d];
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
    std::printf("dim=%d rows=%d queries=%d softcap=%.1f window=%llu masked=%d max_abs=%.9g log_mass_abs=%.9g\n",
        dim, rows, qr, cap, (unsigned long long)window, masked, max_error, max_mass_error);
    if (max_error > 3e-5 || max_mass_error > 3e-5) std::exit(1);
}

int main() {
    int device_count = 0;
    check(cudaGetDeviceCount(&device_count));
    if (!device_count) { std::fprintf(stderr, "No CUDA device\n"); return 2; }
    cudaDeviceProp properties{};
    check(cudaGetDeviceProperties(&properties, 0));
    std::printf("device=%s\n", properties.name);
    for (int dim : {256, 512}) for (int rows : {1, 17, 511, 513, 2053}) for (int qr : {1, 3}) {
        run(dim, rows, qr, 0, 0, false);
        run(dim, rows, qr, 2, 0, false);
        run(dim, rows, qr, 2, 512, false);
        run(dim, rows, qr, 0, 0, true);
    }
    for (int dim : {256, 512}) for (int rows : {8192, 32768}) for (int qr : {1, 3}) {
        run(dim, rows, qr, 0, 0, false);
        run(dim, rows, qr, 2, 0, false);
    }
    std::puts("PASS: 96 CUDA attention mass/output cases (8 query heads, 2 KV heads)");
}
