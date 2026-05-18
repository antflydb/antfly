// Copyright 2026 Antfly, Inc.
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

// WebGPU extern declarations for JS bridge.
//
// These functions are imported from the "webgpu" module when running WASM.
// The JS host provides implementations via WebGPU compute shaders.
// GPU buffers are identified by opaque u32 handles managed on the JS side.

pub const GpuBufferId = u32;

pub const GqaCachedKeyFormat = enum(u32) {
    f32 = 0,
    polar4 = 1,
    turbo3 = 2,
};

pub const GqaCachedValueFormat = enum(u32) {
    f32 = 0,
    int8_per_head = 1,
};
pub const invalid_buffer: GpuBufferId = 0;

extern "webgpu" fn gpu_is_available() u32;
extern "webgpu" fn gpu_create_buffer(size_bytes: u32) GpuBufferId;
extern "webgpu" fn gpu_free_buffer(id: GpuBufferId) void;
extern "webgpu" fn gpu_upload(id: GpuBufferId, ptr: [*]const u8, size_bytes: u32) void;
extern "webgpu" fn gpu_download(id: GpuBufferId, ptr: [*]u8, size_bytes: u32) void;
extern "webgpu" fn gpu_copy_buffer_to_buffer(src: GpuBufferId, src_offset_bytes: u32, dst: GpuBufferId, dst_offset_bytes: u32, size_bytes: u32) void;
extern "webgpu" fn gpu_matmul(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_add(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_add_broadcast(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void;
extern "webgpu" fn gpu_mul(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void;
extern "webgpu" fn gpu_sub(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void;
extern "webgpu" fn gpu_div(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void;
extern "webgpu" fn gpu_less_than(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void;
extern "webgpu" fn gpu_where_select(cond: GpuBufferId, on_true: GpuBufferId, on_false: GpuBufferId, out: GpuBufferId, len: u32, true_len: u32, false_len: u32) void;
extern "webgpu" fn gpu_neg(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_sqrt(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_rsqrt(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_exp(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_log(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_sin(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_cos(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_tanh(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_abs(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_erf(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_gelu(input: GpuBufferId, out: GpuBufferId, len: u32) void;
extern "webgpu" fn gpu_softmax(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void;
extern "webgpu" fn gpu_log_softmax(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void;
extern "webgpu" fn gpu_reduce_sum_last_dim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void;
extern "webgpu" fn gpu_reduce_max_last_dim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void;
extern "webgpu" fn gpu_reduce_mean_last_dim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void;
extern "webgpu" fn gpu_reduce_sum(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: [*]const u32, out_shape: [*]const u32, reduced: [*]const u32, in_strides: [*]const u32, out_strides: [*]const u32, kept_axes: [*]const u32, reduced_axes: [*]const u32) void;
extern "webgpu" fn gpu_reduce_max(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: [*]const u32, out_shape: [*]const u32, reduced: [*]const u32, in_strides: [*]const u32, out_strides: [*]const u32, kept_axes: [*]const u32, reduced_axes: [*]const u32) void;
extern "webgpu" fn gpu_reduce_mean(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: [*]const u32, out_shape: [*]const u32, reduced: [*]const u32, in_strides: [*]const u32, out_strides: [*]const u32, kept_axes: [*]const u32, reduced_axes: [*]const u32) void;
extern "webgpu" fn gpu_broadcast_in_dim(input: GpuBufferId, out: GpuBufferId, out_len: u32, out_rank: u32, in_rank: u32, target_shape: [*]const u32, input_shape: [*]const u32, axes: [*]const u32, out_strides: [*]const u32, in_strides: [*]const u32) void;
extern "webgpu" fn gpu_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void;
extern "webgpu" fn gpu_causal_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void;
extern "webgpu" fn gpu_cross_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    num_heads: u32,
    head_dim: u32,
) void;
extern "webgpu" fn gpu_deberta_disentangled_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    q_r: GpuBufferId,
    k_r: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void;

extern "webgpu" fn gpu_gqa_causal_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
) void;

extern "webgpu" fn gpu_gqa_cached_attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
) void;

extern "webgpu" fn gpu_gqa_cached_attention_ex(
    q: GpuBufferId,
    k_main: GpuBufferId,
    k_aux: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    k_format: u32,
    v_format: u32,
    k_row_bytes: u32,
    v_row_bytes: u32,
    flags: u32,
) void;

extern "webgpu" fn gpu_write_buffer_at_offset(id: GpuBufferId, offset_bytes: u32, ptr: [*]const u8, size_bytes: u32) void;

extern "webgpu" fn gpu_matmul_transb_q4_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q4_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq4_nl(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq4_xs(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q2_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q3_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q4_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q6_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_k(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_i2_s(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_i8_s(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q1_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_tq1_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_tq2_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_mxfp4(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_nvfp4(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq1_s(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq1_m(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_xxs(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_xs(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_s(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq3_xxs(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq3_s(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;

// MMV (matrix-vector) variants for decode-time qLen=1 dispatch. Same buffer
// contract as the GEMM ones above; the JS bridge selects the corresponding
// MMV pipeline. Backed by web/shaders/matmul_transb_<fmt>_mmv.wgsl which is
// generated by scripts/gen_mmv_shaders.py.
extern "webgpu" fn gpu_matmul_transb_q4_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q4_1_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_1_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_1_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq4_nl_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq4_xs_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q2_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q3_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q4_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q5_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q6_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q8_k_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_i8_s_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_q1_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_tq1_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_tq2_0_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_mxfp4_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_nvfp4_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq1_s_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq1_m_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_xxs_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_xs_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq2_s_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq3_xxs_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;
extern "webgpu" fn gpu_matmul_transb_iq3_s_mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void;

extern "webgpu" fn gpu_rms_norm(input: GpuBufferId, weight: GpuBufferId, out: GpuBufferId, total_rows: u32, dim: u32, eps_bits: u32) void;
extern "webgpu" fn gpu_layer_norm(input: GpuBufferId, gamma: GpuBufferId, beta: GpuBufferId, out: GpuBufferId, total_rows: u32, dim: u32, eps_bits: u32) void;

pub fn isAvailable() bool {
    return gpu_is_available() != 0;
}

pub fn createBuffer(size_bytes: u32) GpuBufferId {
    return gpu_create_buffer(size_bytes);
}

pub fn freeBuffer(id: GpuBufferId) void {
    gpu_free_buffer(id);
}

pub fn upload(id: GpuBufferId, data: []const u8) void {
    gpu_upload(id, data.ptr, @intCast(data.len));
}

pub fn download(id: GpuBufferId, data: []u8) void {
    gpu_download(id, data.ptr, @intCast(data.len));
}

pub fn copyBufferToBuffer(src: GpuBufferId, src_offset_bytes: u32, dst: GpuBufferId, dst_offset_bytes: u32, size_bytes: u32) void {
    gpu_copy_buffer_to_buffer(src, src_offset_bytes, dst, dst_offset_bytes, size_bytes);
}

pub fn matmul(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul(a, b, out, m, n, k);
}

pub fn matmulTransB(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb(a, b, out, m, n, k);
}

pub fn add(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_add(a, b, out, len);
}

pub fn addBroadcast(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void {
    gpu_add_broadcast(a, b, out, len, a_len, b_len);
}

pub fn mul(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void {
    gpu_mul(a, b, out, len, a_len, b_len);
}

pub fn sub(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void {
    gpu_sub(a, b, out, len, a_len, b_len);
}

pub fn div(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void {
    gpu_div(a, b, out, len, a_len, b_len);
}

pub fn lessThan(a: GpuBufferId, b: GpuBufferId, out: GpuBufferId, len: u32, a_len: u32, b_len: u32) void {
    gpu_less_than(a, b, out, len, a_len, b_len);
}

pub fn whereSelect(cond: GpuBufferId, on_true: GpuBufferId, on_false: GpuBufferId, out: GpuBufferId, len: u32, true_len: u32, false_len: u32) void {
    gpu_where_select(cond, on_true, on_false, out, len, true_len, false_len);
}

pub fn neg(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_neg(input, out, len);
}

pub fn sqrt(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_sqrt(input, out, len);
}

pub fn rsqrt(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_rsqrt(input, out, len);
}

pub fn exp(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_exp(input, out, len);
}

pub fn log(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_log(input, out, len);
}

pub fn sin(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_sin(input, out, len);
}

pub fn cos(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_cos(input, out, len);
}

pub fn tanh(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_tanh(input, out, len);
}

pub fn abs(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_abs(input, out, len);
}

pub fn erf(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_erf(input, out, len);
}

pub fn gelu(input: GpuBufferId, out: GpuBufferId, len: u32) void {
    gpu_gelu(input, out, len);
}

pub fn softmax(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void {
    gpu_softmax(input, out, rows, dim);
}

pub fn logSoftmax(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void {
    gpu_log_softmax(input, out, rows, dim);
}

pub fn reduceSumLastDim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void {
    gpu_reduce_sum_last_dim(input, out, rows, dim);
}

pub fn reduceMaxLastDim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void {
    gpu_reduce_max_last_dim(input, out, rows, dim);
}

pub fn reduceMeanLastDim(input: GpuBufferId, out: GpuBufferId, rows: u32, dim: u32) void {
    gpu_reduce_mean_last_dim(input, out, rows, dim);
}

pub fn reduceSum(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: []const u32, out_shape: []const u32, reduced: []const u32, in_strides: []const u32, out_strides: []const u32, kept_axes: []const u32, reduced_axes: []const u32) void {
    gpu_reduce_sum(input, out, out_len, reduce_count, in_rank, out_rank, input_shape.ptr, out_shape.ptr, reduced.ptr, in_strides.ptr, out_strides.ptr, kept_axes.ptr, reduced_axes.ptr);
}

pub fn reduceMax(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: []const u32, out_shape: []const u32, reduced: []const u32, in_strides: []const u32, out_strides: []const u32, kept_axes: []const u32, reduced_axes: []const u32) void {
    gpu_reduce_max(input, out, out_len, reduce_count, in_rank, out_rank, input_shape.ptr, out_shape.ptr, reduced.ptr, in_strides.ptr, out_strides.ptr, kept_axes.ptr, reduced_axes.ptr);
}

pub fn reduceMean(input: GpuBufferId, out: GpuBufferId, out_len: u32, reduce_count: u32, in_rank: u32, out_rank: u32, input_shape: []const u32, out_shape: []const u32, reduced: []const u32, in_strides: []const u32, out_strides: []const u32, kept_axes: []const u32, reduced_axes: []const u32) void {
    gpu_reduce_mean(input, out, out_len, reduce_count, in_rank, out_rank, input_shape.ptr, out_shape.ptr, reduced.ptr, in_strides.ptr, out_strides.ptr, kept_axes.ptr, reduced_axes.ptr);
}

pub fn broadcastInDim(input: GpuBufferId, out: GpuBufferId, out_len: u32, out_rank: u32, in_rank: u32, target_shape: []const u32, input_shape: []const u32, axes: []const u32, out_strides: []const u32, in_strides: []const u32) void {
    gpu_broadcast_in_dim(input, out, out_len, out_rank, in_rank, target_shape.ptr, input_shape.ptr, axes.ptr, out_strides.ptr, in_strides.ptr);
}

pub fn attention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void {
    gpu_attention(q, k, v, mask_buf, out_buf, batch, seq_len, num_heads, head_dim);
}

pub fn disentangledRelativeAttention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    q_r: GpuBufferId,
    k_r: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void {
    gpu_deberta_disentangled_attention(q, k, v, q_r, k_r, mask_buf, out_buf, batch, seq_len, num_heads, head_dim);
}

pub fn causalAttention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    head_dim: u32,
) void {
    gpu_causal_attention(q, k, v, out_buf, batch, seq_len, num_heads, head_dim);
}

pub fn matmulTransBQ4_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ4_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_1(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_1(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_1(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_1(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ4_NL(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq4_nl(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ4_XS(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq4_xs(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ2_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q2_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ3_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q3_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ4_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ6_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q6_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_K(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_k(a, b_quant, out, m, n, k);
}

pub fn matmulTransBI2_S(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_i2_s(a, b_quant, out, m, n, k);
}

pub fn matmulTransBI8_S(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_i8_s(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ1_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q1_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBTQ1_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_tq1_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBTQ2_0(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_tq2_0(a, b_quant, out, m, n, k);
}

pub fn matmulTransBMXFP4(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_mxfp4(a, b_quant, out, m, n, k);
}

pub fn matmulTransBNVFP4(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_nvfp4(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ1_S(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq1_s(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ1_M(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq1_m(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_XXS(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_xxs(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_XS(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_xs(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_S(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_s(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ3_XXS(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq3_xxs(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ3_S(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq3_s(a, b_quant, out, m, n, k);
}

// MMV (qLen=1 / decode) wrappers. Same signature as the GEMM wrappers; a separate
// dispatch routes here when M==1 to avoid wasting threads on a 16x16 GEMM tile.
pub fn matmulTransBQ4_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ4_1Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_1_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_1Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_1_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_1Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_1_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ4_NLMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq4_nl_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ4_XSMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq4_xs_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ2_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q2_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ3_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q3_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ4_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q4_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ5_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q5_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ6_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q6_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ8_KMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q8_k_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBI8_SMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_i8_s_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBQ1_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_q1_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBTQ1_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_tq1_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBTQ2_0Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_tq2_0_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBMXFP4Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_mxfp4_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBNVFP4Mmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_nvfp4_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ1_SMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq1_s_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ1_MMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq1_m_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_XXSMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_xxs_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_XSMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_xs_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ2_SMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq2_s_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ3_XXSMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq3_xxs_mmv(a, b_quant, out, m, n, k);
}

pub fn matmulTransBIQ3_SMmv(a: GpuBufferId, b_quant: GpuBufferId, out: GpuBufferId, m: u32, n: u32, k: u32) void {
    gpu_matmul_transb_iq3_s_mmv(a, b_quant, out, m, n, k);
}

pub fn gqaCausalAttention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    seq_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
) void {
    gpu_gqa_causal_attention(q, k, v, out_buf, batch, seq_len, num_heads, num_kv_heads, head_dim);
}

pub fn gqaCachedAttention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
) void {
    gpu_gqa_cached_attention(q, k, v, out_buf, batch, q_len, kv_len, num_heads, num_kv_heads, head_dim);
}

pub fn gqaCachedAttentionEx(
    q: GpuBufferId,
    k_main: GpuBufferId,
    k_aux: GpuBufferId,
    v: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    k_format: GqaCachedKeyFormat,
    v_format: GqaCachedValueFormat,
    k_row_bytes: u32,
    v_row_bytes: u32,
    flags: u32,
) void {
    gpu_gqa_cached_attention_ex(
        q,
        k_main,
        k_aux,
        v,
        out_buf,
        batch,
        q_len,
        kv_len,
        num_heads,
        num_kv_heads,
        head_dim,
        @intFromEnum(k_format),
        @intFromEnum(v_format),
        k_row_bytes,
        v_row_bytes,
        flags,
    );
}

pub fn writeBufferAtOffset(id: GpuBufferId, offset_bytes: u32, data: []const u8) void {
    gpu_write_buffer_at_offset(id, offset_bytes, data.ptr, @intCast(data.len));
}

pub fn crossAttention(
    q: GpuBufferId,
    k: GpuBufferId,
    v: GpuBufferId,
    mask_buf: GpuBufferId,
    out_buf: GpuBufferId,
    batch: u32,
    dec_seq: u32,
    enc_seq: u32,
    num_heads: u32,
    head_dim: u32,
) void {
    gpu_cross_attention(q, k, v, mask_buf, out_buf, batch, dec_seq, enc_seq, num_heads, head_dim);
}

pub fn rmsNorm(input: GpuBufferId, weight: GpuBufferId, out: GpuBufferId, total_rows: u32, dim: u32, eps_bits: u32) void {
    gpu_rms_norm(input, weight, out, total_rows, dim, eps_bits);
}

pub fn layerNorm(input: GpuBufferId, gamma: GpuBufferId, beta: GpuBufferId, out: GpuBufferId, total_rows: u32, dim: u32, eps_bits: u32) void {
    gpu_layer_norm(input, gamma, beta, out, total_rows, dim, eps_bits);
}
