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

#include <metal_stdlib>
using namespace metal;

struct Params {
    uint op, n, dim, groups, position, stride, mode, index;
    float epsilon, scale, theta, reserved;
};
// groups = original rows | (retained rows << 16), position = training query count, stride = held-out
// query count, mode = iteration bound | (matching pursuit << 16),
// index = (KV heads << 16) | KV head.
// Workspace offsets are floats and mirrored exactly in context_compaction.zig.
struct Workspace {
    uint scores, targets, mass, selected, weights, a, row_norm, gram, rhs;
    uint ridge, validation, errors, validation_mass, end;
    Workspace(constant Params &p) {
        const uint n = p.groups & 65535, m = p.groups >> 16, q = p.position, h = p.stride, d = p.dim;
        scores = 0; targets = scores + q * n; mass = targets + q * d;
        selected = mass + n; weights = selected + m; a = weights + m;
        row_norm = a + q * m; gram = row_norm + q; rhs = gram + m * m;
        ridge = rhs + m * d; validation = ridge + 1;
        errors = validation + h * (n + m); validation_mass = errors + 2 * h * d;
        end = validation_mass + h;
    }
};

inline void fail(device float *report, uint reason) {
    atomic_fetch_or_explicit(reinterpret_cast<device atomic_uint *>(report), reason, memory_order_relaxed);
}
inline uint failure(device float *report) {
    return atomic_load_explicit(reinterpret_cast<device atomic_uint *>(report), memory_order_relaxed);
}
// A validation partition can contain millions of nonnegative squared terms.
// Straight f32 accumulation loses small terms once the partial sum grows,
// which can move the aggregate RRMSE beyond the strict native/FP64 parity
// tolerance even though every consumer-sized reduction remains accurate.
inline void compensated_add(thread float &sum, thread float &correction, float value) {
    const float adjusted = value - correction;
    const float next = sum + adjusted;
    correction = (next - sum) - adjusted;
    sum = next;
}

// This bounded reference kernel is intended for correctness qualification.
// Each opcode is encoded in a separate command encoder, giving device memory
// visibility between stages without a CPU readback. Only the 32-byte report
// is inspected on the host; candidate KV and mass stay device-resident.
kernel void context_compaction(
    device const half *keys [[buffer(0)]],
    device const half *values [[buffer(1)]],
    device const float *queries [[buffer(2)]],
    device const float *held_out [[buffer(3)]],
    device float *scratch [[buffer(4)]],
    device half *candidate_keys [[buffer(5)]],
    device half *candidate_values [[buffer(6)]],
    device float *report [[buffer(7)]],
    constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]) {
    if (tid >= p.n) return;
    if (p.op == 16) {
        // Pack one completed head into an unpublished owner generation.
        // scratch is the destination bias and queries the source log mass.
        const uint row = tid / p.dim, col = tid % p.dim;
        const uint heads = p.index >> 16, head = p.index & 65535;
        const uint dst = (row * heads + head) * p.dim + col;
        candidate_keys[dst] = keys[tid];
        candidate_values[dst] = values[tid];
        if (col == 0) scratch[row * heads + head] = queries[row];
        return;
    }
    // SIMD operations must make the same early-exit decision in every lane,
    // even if another group reports an invalid input concurrently.
    const bool collective = p.op == 0 || p.op == 4 || p.op == 8 || p.op == 11 || p.op == 17 || (p.op == 3 && (p.mode & (1u << 16)) != 0);
    const uint status = collective ? simd_broadcast(failure(report), 0) : failure(report);
    if (status != 0) return;
    const uint n = p.groups & 65535, m = p.groups >> 16, q = p.position, d = p.dim, held = p.stride;
    const uint kv_heads = p.index >> 16, kv_head = p.index & 65535;
    const uint local_rows = (q + 31) / 32;
    const Workspace w(p);
    device uint *selected = reinterpret_cast<device uint *>(scratch + w.selected);
    device float *bias = report + 8;
    if (p.op == 0) {
        const uint element = tid / 32, row = element / n, key = element % n;
        float dot = 0;
        for (uint col = lane; col < d; col += 32)
            dot += queries[row * d + col] * float(keys[(key * kv_heads + kv_head) * d + col]);
        const float score = simd_sum(dot) * p.scale;
        if (lane == 0) {
            if (!isfinite(score)) fail(report, 1);
            scratch[w.scores + element] = score;
        }
    } else if (p.op == 1) {
        const uint row = tid;
        float maximum = -INFINITY, z = 0;
        for (uint i = 0; i < n; ++i) maximum = max(maximum, scratch[w.scores + row * n + i]);
        for (uint i = 0; i < n; ++i) {
            const float weight = exp(scratch[w.scores + row * n + i] - maximum);
            scratch[w.scores + row * n + i] = weight; z += weight;
        }
        if (!isfinite(z) || z <= 0) { fail(report, 1); return; }
        for (uint i = 0; i < n; ++i) scratch[w.scores + row * n + i] /= z;
    } else if (p.op == 2) {
        const uint row = tid / d, col = tid % d;
        float sum = 0;
        for (uint i = 0; i < n; ++i) sum += scratch[w.scores + row * n + i] * float(values[(i * kv_heads + kv_head) * d + col]);
        if (!isfinite(sum)) fail(report, 1);
        scratch[w.targets + tid] = sum;
    } else if (p.op == 3) {
        if ((p.mode & (1u << 16)) != 0) {
            // One SIMD group owns periodic-refit matching pursuit. Selection
            // and weights are lane-0-owned; broadcasts make all cross-lane
            // dependencies explicit without relying on device-memory races.
            float residual[128];
            for (uint k = 0; k < local_rows; ++k) residual[k] = 1;
            if (lane == 0) {
                for (uint key = 0; key < n; ++key) scratch[w.mass + key] = 0;
                for (uint j = 0; j < m; ++j) scratch[w.weights + j] = 0;
            }
            const uint batch_size = min(8u, max(1u, m / 4));
            for (uint count = 0; count < m;) {
                for (uint key = 0; key < n; ++key) {
                    const uint used = simd_broadcast(lane == 0 ? uint(scratch[w.mass + key] == -INFINITY) : 0u, 0);
                    if (used != 0) continue;
                    float correlation = 0;
                    for (uint k = 0; k < local_rows; ++k) {
                        const uint row = lane + 32 * k;
                        if (row < q) correlation += scratch[w.scores + row * n + key] * residual[k];
                    }
                    correlation = simd_sum(correlation);
                    if (lane == 0) scratch[w.mass + key] = correlation;
                }
                const uint end = min(count + batch_size, m);
                if (lane == 0) for (uint j = count; j < end; ++j) {
                    uint winner = 0; float best = -INFINITY;
                    for (uint key = 0; key < n; ++key) if (scratch[w.mass + key] > best) {
                        best = scratch[w.mass + key]; winner = key;
                    }
                    selected[j] = winner; scratch[w.mass + winner] = -INFINITY;
                }
                count = end;
                for (uint sweep = 0; sweep < 8; ++sweep) for (uint j = 0; j < count; ++j) {
                    const uint key = simd_broadcast(lane == 0 ? selected[j] : 0u, 0);
                    float gradient = 0, norm = 0;
                    for (uint k = 0; k < local_rows; ++k) {
                        const uint row = lane + 32 * k;
                        if (row < q) {
                            const float a = scratch[w.scores + row * n + key];
                            gradient += a * residual[k]; norm += a * a;
                        }
                    }
                    gradient = simd_sum(gradient); norm = simd_sum(norm);
                    float delta = 0;
                    if (lane == 0 && norm >= 1e-20f) {
                        const float old = scratch[w.weights + j];
                        const float next = max(1e-6f, old + gradient / norm);
                        if (!isfinite(next) || next > 1e6f) fail(report, 2);
                        delta = next - old; scratch[w.weights + j] = next;
                    }
                    delta = simd_broadcast(delta, 0);
                    for (uint k = 0; k < local_rows; ++k) {
                        const uint row = lane + 32 * k;
                        if (row < q) residual[k] -= scratch[w.scores + row * n + key] * delta;
                    }
                }
            }
            // Negligible-norm columns may never receive an NNLS update. Keep
            // their log mass finite; opcode 4 rebuilds the residual afterward.
            if (lane == 0) for (uint j = 0; j < m; ++j)
                scratch[w.weights + j] = max(scratch[w.weights + j], 1e-6f);
            if (lane == 0) for (uint j = 1; j < m; ++j) {
                const uint key = selected[j]; const float weight = scratch[w.weights + j]; uint at = j;
                while (at > 0 && selected[at - 1] > key) {
                    selected[at] = selected[at - 1];
                    scratch[w.weights + at] = scratch[w.weights + at - 1]; --at;
                }
                selected[at] = key; scratch[w.weights + at] = weight;
            }
            return;
        }
        if (tid != 0) return;
        for (uint key = 0; key < n; ++key) {
            float mass = 0;
            for (uint row = 0; row < q; ++row) {
                const float probability = scratch[w.scores + row * n + key];
                mass += probability * probability;
            }
            scratch[w.mass + key] = mass;
        }
        for (uint j = 0; j < m; ++j) {
            uint winner = 0; float best = -INFINITY;
            for (uint i = 0; i < n; ++i) if (scratch[w.mass + i] > best) {
                best = scratch[w.mass + i]; winner = i;
            }
            selected[j] = winner; scratch[w.mass + winner] = -INFINITY;
        }
        // Stable original order makes both candidate layout and tie-breaking reproducible.
        for (uint j = 1; j < m; ++j) {
            const uint index = selected[j]; uint at = j;
            while (at > 0 && selected[at - 1] > index) { selected[at] = selected[at - 1]; --at; }
            selected[at] = index;
        }
        for (uint j = 0; j < m; ++j) scratch[w.weights + j] = float(n) / float(m);
    } else if (p.op == 4) {
        // One SIMD group owns the bounded NNLS solve. Each lane keeps at most
        // 128 residuals; key coordinates are updated in deterministic order.
        float residual[128];
        for (uint k = 0; k < local_rows; ++k) {
            const uint row = lane + 32 * k; float sum = -1;
            if (row < q) for (uint j = 0; j < m; ++j) sum += scratch[w.weights + j] * scratch[w.scores + row * n + selected[j]];
            residual[k] = sum;
        }
        for (uint iteration = 0; iteration < (p.mode & 65535u); ++iteration) {
            for (uint j = 0; j < m; ++j) {
                float gradient = 0, norm = 0;
                const uint key = selected[j];
                for (uint k = 0; k < local_rows; ++k) {
                    const uint row = lane + 32 * k;
                    if (row < q) {
                        const float a = scratch[w.scores + row * n + key];
                        gradient += a * residual[k]; norm += a * a;
                    }
                }
                gradient = simd_sum(gradient); norm = simd_sum(norm);
                float delta = 0;
                if (lane == 0 && norm >= 1e-20f) {
                    const float old = scratch[w.weights + j];
                    const float next = max(1e-6f, old - gradient / norm);
                    if (!isfinite(next) || next > 1e6f) fail(report, 2);
                    delta = next - old; scratch[w.weights + j] = next;
                }
                delta = simd_broadcast(delta, 0);
                for (uint k = 0; k < local_rows; ++k) {
                    const uint row = lane + 32 * k;
                    if (row < q) residual[k] += scratch[w.scores + row * n + key] * delta;
                }
            }
        }
        float max_training_error = 0;
        for (uint k = 0; k < local_rows; ++k) if (lane + 32 * k < q) {
            if (!isfinite(residual[k]) || abs(residual[k]) > p.theta) fail(report, 4);
            max_training_error = max(max_training_error, isfinite(residual[k]) ? abs(residual[k]) : INFINITY);
        }
        max_training_error = simd_max(max_training_error);
        // Lane 0 performed every weight write; only it reads those weights
        // here, avoiding cross-lane memory visibility requirements.
        if (lane == 0) {
            report[4] = max_training_error;
            reinterpret_cast<device uint *>(report)[5] = 1;
            for (uint j = 0; j < m; ++j) bias[j] = log(scratch[w.weights + j]);
            reinterpret_cast<device uint *>(report)[1] = p.mode & 65535u;
        }
    } else if (p.op == 5) {
        const uint row = tid;
        float z = 0, norm = 0;
        for (uint j = 0; j < m; ++j) z += scratch[w.scores + row * n + selected[j]] * scratch[w.weights + j];
        if (!isfinite(z) || z <= 0) { fail(report, 2); return; }
        for (uint j = 0; j < m; ++j) {
            const float value = scratch[w.scores + row * n + selected[j]] * scratch[w.weights + j] / z;
            scratch[w.a + row * m + j] = value; norm += value * value;
        }
        scratch[w.row_norm + row] = norm;
    } else if (p.op == 6) {
        const uint i = tid / m, j = tid % m;
        float dot = 0;
        for (uint row = 0; row < q; ++row) dot += scratch[w.a + row * m + i] * scratch[w.a + row * m + j];
        scratch[w.gram + tid] = dot;
        if (tid == 0) {
            float trace = 0;
            for (uint row = 0; row < q; ++row) trace += scratch[w.row_norm + row];
            scratch[w.ridge] = p.epsilon * max(trace / float(m), 1e-6f);
        }
    } else if (p.op == 7) {
        const uint i = tid / d, col = tid % d;
        float dot = 0;
        for (uint row = 0; row < q; ++row) dot += scratch[w.a + row * m + i] * scratch[w.targets + row * d + col];
        scratch[w.rhs + tid] = dot;
    } else if (p.op == 8) {
        device float *g = scratch + w.gram;
        for (uint i = lane; i < m; i += 32) g[i * m + i] += scratch[w.ridge];
        simdgroup_barrier(mem_flags::mem_device);
        for (uint j = 0; j < m; ++j) {
            float norm = 0;
            for (uint k = lane; k < j; k += 32) norm += g[j * m + k] * g[j * m + k];
            const float original_diag = simd_broadcast(lane == 0 ? g[j * m + j] : 0.0f, 0);
            const float diag = original_diag - simd_sum(norm);
            if (!isfinite(diag) || diag <= 0) { if (lane == 0) fail(report, 2); return; }
            const float root = sqrt(diag);
            if (lane == 0) g[j * m + j] = root;
            for (uint i = j + 1 + lane; i < m; i += 32) {
                float dot = 0;
                for (uint k = 0; k < j; ++k) dot += g[i * m + k] * g[j * m + k];
                g[i * m + j] = (g[i * m + j] - dot) / root;
            }
            simdgroup_barrier(mem_flags::mem_device);
        }
    } else if (p.op == 9) {
        const uint col = tid;
        device float *g = scratch + w.gram;
        device float *x = scratch + w.rhs;
        for (uint i = 0; i < m; ++i) {
            float value = x[i * d + col];
            for (uint k = 0; k < i; ++k) value -= g[i * m + k] * x[k * d + col];
            x[i * d + col] = value / g[i * m + i];
        }
        for (uint at = m; at > 0; --at) {
            const uint i = at - 1; float value = x[i * d + col];
            for (uint k = i + 1; k < m; ++k) value -= g[k * m + i] * x[k * d + col];
            value /= g[i * m + i]; x[i * d + col] = value;
            value += float(values[(selected[i] * kv_heads + kv_head) * d + col]);
            if (!isfinite(value) || abs(value) > 65504) fail(report, 2);
            candidate_values[i * d + col] = half(value);
        }
    } else if (p.op == 10) {
        candidate_keys[tid] = keys[(selected[tid / d] * kv_heads + kv_head) * d + tid % d];
    } else if (p.op == 17) {
        // Training partition functions for arbitrary stored candidate bytes.
        // Original and candidate scores use the existing disjoint q*n and q*m
        // workspace regions; no fitted selection state is consulted.
        const uint element = tid / 32, row = element / (n + m), key = element % (n + m);
        float dot = 0;
        for (uint col = lane; col < d; col += 32) {
            const float k = key < n ? float(keys[(key * kv_heads + kv_head) * d + col]) : float(candidate_keys[(key - n) * d + col]);
            dot += queries[row * d + col] * k;
        }
        const float score = simd_sum(dot) * p.scale + (key < n ? 0 : bias[key - n]);
        if (lane == 0) {
            if (!isfinite(score)) fail(report, 1);
            if (key < n) scratch[w.scores + row * n + key] = score;
            else scratch[w.a + row * m + key - n] = score;
        }
    } else if (p.op == 18) {
        const uint row = tid;
        float full_max = -INFINITY, fit_max = -INFINITY, full_z = 0, fit_z = 0;
        for (uint i = 0; i < n; ++i) full_max = max(full_max, scratch[w.scores + row * n + i]);
        for (uint i = 0; i < m; ++i) fit_max = max(fit_max, scratch[w.a + row * m + i]);
        for (uint i = 0; i < n; ++i) full_z += exp(scratch[w.scores + row * n + i] - full_max);
        for (uint i = 0; i < m; ++i) fit_z += exp(scratch[w.a + row * m + i] - fit_max);
        const float mass_error = abs(exp(fit_max + log(fit_z) - full_max - log(full_z)) - 1);
        atomic_fetch_max_explicit(reinterpret_cast<device atomic_uint *>(report + 4), as_type<uint>(isfinite(mass_error) ? mass_error : INFINITY), memory_order_relaxed);
        if (!isfinite(mass_error)) fail(report, 1);
    } else if (p.op == 19) {
        if (tid == 0)
            atomic_fetch_or_explicit(reinterpret_cast<device atomic_uint *>(report + 5), 1u, memory_order_relaxed);
    } else if (p.op == 20) {
        // For this operation only, buffer(2) is the caller-owned f32 bias.
        const float value = queries[tid];
        if (!isfinite(value)) fail(report, 1);
        else bias[tid] = value;
    } else if (p.op == 11) {
        const uint element = tid / 32, row = element / (n + m), key = element % (n + m);
        float dot = 0;
        for (uint col = lane; col < d; col += 32) {
            const float k = key < n ? float(keys[(key * kv_heads + kv_head) * d + col]) : float(candidate_keys[(key - n) * d + col]);
            dot += held_out[row * d + col] * k;
        }
        const float score = simd_sum(dot) * p.scale + (key < n ? 0 : bias[key - n]);
        if (lane == 0) {
            if (!isfinite(score)) fail(report, 1);
            scratch[w.validation + element] = score;
        }
    } else if (p.op == 12) {
        const uint row = tid;
        device float *s = scratch + w.validation + row * (n + m);
        float full_max = -INFINITY, fit_max = -INFINITY, full_z = 0, fit_z = 0;
        for (uint i = 0; i < n; ++i) full_max = max(full_max, s[i]);
        for (uint i = 0; i < m; ++i) fit_max = max(fit_max, s[n + i]);
        for (uint i = 0; i < n; ++i) { s[i] = exp(s[i] - full_max); full_z += s[i]; }
        for (uint i = 0; i < m; ++i) { s[n + i] = exp(s[n + i] - fit_max); fit_z += s[n + i]; }
        const float mass_error = abs(exp(fit_max + log(fit_z) - full_max - log(full_z)) - 1);
        scratch[w.validation_mass + row] = mass_error;
        atomic_fetch_max_explicit(reinterpret_cast<device atomic_uint *>(report + 2), as_type<uint>(isfinite(mass_error) ? mass_error : INFINITY), memory_order_relaxed);
        // Defer quality rejection until all held-out rows and output errors
        // have been evaluated. An early failure here made the reported maximum
        // depend on which thread observed another row's failure first.
        if (!isfinite(mass_error)) fail(report, 1);
        for (uint i = 0; i < n; ++i) s[i] /= full_z;
        for (uint i = 0; i < m; ++i) s[n + i] /= fit_z;
    } else if (p.op == 13) {
        const uint row = tid / d, col = tid % d;
        device float *s = scratch + w.validation + row * (n + m);
        float full = 0, fit = 0;
        for (uint i = 0; i < n; ++i) full += s[i] * float(values[(i * kv_heads + kv_head) * d + col]);
        for (uint i = 0; i < m; ++i) fit += s[n + i] * float(candidate_values[i * d + col]);
        scratch[w.errors + tid] = (full - fit) * (full - fit);
        scratch[w.errors + held * d + tid] = full * full;
    } else if (p.op == 15) {
        // Solve for a correction to original V. Forming A^T Y + ridge V
        // directly loses the ridge anchor in nearly rank-deficient blocks.
        const uint row = tid / d, col = tid % d;
        float predicted = 0;
        for (uint i = 0; i < m; ++i)
            predicted += scratch[w.a + row * m + i] * float(values[(selected[i] * kv_heads + kv_head) * d + col]);
        scratch[w.targets + tid] -= predicted;
    } else if (p.op == 14) {
        if (tid != 0) return;
        float squared_error = 0, squared_error_correction = 0;
        float squared_target = 0, squared_target_correction = 0;
        float mass_error = 0;
        for (uint i = 0; i < held * d; ++i) {
            compensated_add(squared_error, squared_error_correction, scratch[w.errors + i]);
            compensated_add(squared_target, squared_target_correction, scratch[w.errors + held * d + i]);
        }
        for (uint i = 0; i < held; ++i) mass_error = max(mass_error, scratch[w.validation_mass + i]);
        const float relative = sqrt(squared_error / max(squared_target, 1e-12f));
        report[2] = mass_error; report[3] = relative;
        const uint group_rows = (p.mode >> 17) == 0 ? held : (p.mode >> 17);
        float max_consumer_error = 0;
        for (uint start = 0; start < held; start += group_rows) {
            float group_error = 0, group_error_correction = 0;
            float group_target = 0, group_target_correction = 0;
            for (uint i = start * d; i < (start + group_rows) * d; ++i) {
                compensated_add(group_error, group_error_correction, scratch[w.errors + i]);
                compensated_add(group_target, group_target_correction, scratch[w.errors + held * d + i]);
            }
            const float consumer_error = sqrt(group_error / max(group_target, 1e-12f));
            max_consumer_error = max(max_consumer_error, isfinite(consumer_error) ? consumer_error : INFINITY);
        }
        report[6] = max_consumer_error;
        reinterpret_cast<device uint *>(report)[7] = held / group_rows;
        const uint evaluated = atomic_load_explicit(reinterpret_cast<device atomic_uint *>(report + 5), memory_order_relaxed);
        if ((evaluated & 1u) == 0) { fail(report, 4); return; }
        atomic_fetch_or_explicit(reinterpret_cast<device atomic_uint *>(report + 5), 6u, memory_order_relaxed);
        if (!isfinite(relative) || relative > p.reserved || mass_error > p.theta || report[4] > p.theta ||
            !isfinite(max_consumer_error) || max_consumer_error > p.reserved) fail(report, 4);
    }
}
