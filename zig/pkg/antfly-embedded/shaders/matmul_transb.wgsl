// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Tiled matrix multiplication with B transposed.
// C[M,N] = A[M,K] @ B^T[N,K]
// B is stored as [N,K] row-major, i.e. B^T[i,j] = B[j,i] = B_data[j*K+i].
//
// Uses 16x16 tiles with workgroup shared memory.

struct Params {
    M: u32,
    N: u32,
    K: u32,
    _pad: u32,
};

@group(0) @binding(0) var<storage, read> A: array<f32>;
@group(0) @binding(1) var<storage, read> B: array<f32>;
@group(0) @binding(2) var<storage, read_write> C: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

const TILE: u32 = 16;

var<workgroup> tile_a: array<f32, 256>;
var<workgroup> tile_b: array<f32, 256>;

@compute @workgroup_size(TILE, TILE)
fn matmul_transb(
    @builtin(global_invocation_id) gid: vec3<u32>,
    @builtin(local_invocation_id) lid: vec3<u32>,
) {
    let row = gid.y;  // output row (indexes into A)
    let col = gid.x;  // output col (indexes into B rows = B^T cols)
    let local_row = lid.y;
    let local_col = lid.x;

    var sum: f32 = 0.0;
    let num_tiles = (params.K + TILE - 1) / TILE;

    for (var t: u32 = 0; t < num_tiles; t++) {
        // Load A tile: A[row, t*TILE + local_col]
        let a_col = t * TILE + local_col;
        if (row < params.M && a_col < params.K) {
            tile_a[local_row * TILE + local_col] = A[row * params.K + a_col];
        } else {
            tile_a[local_row * TILE + local_col] = 0.0;
        }

        // Load B^T tile: B^T[t*TILE + local_row, col] = B[col, t*TILE + local_row]
        // = B_data[col * K + t*TILE + local_row]
        let b_k = t * TILE + local_row;
        if (col < params.N && b_k < params.K) {
            tile_b[local_row * TILE + local_col] = B[col * params.K + b_k];
        } else {
            tile_b[local_row * TILE + local_col] = 0.0;
        }

        workgroupBarrier();

        for (var i: u32 = 0; i < TILE; i++) {
            sum += tile_a[local_row * TILE + i] * tile_b[i * TILE + local_col];
        }

        workgroupBarrier();
    }

    if (row < params.M && col < params.N) {
        C[row * params.N + col] = sum;
    }
}
