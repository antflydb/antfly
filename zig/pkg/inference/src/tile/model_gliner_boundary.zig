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

//! GLiNER 2.5 boundary ops: one `switch (p.kind)` kernel, written once in
//! MSL, rendered as Metal `termite_gliner_boundary_f32` and as CUDA
//! `termite_gliner25_boundary_f32` in kernels/gliner25_boundary.cuh.
//!
//! The CUDA port takes `cuda_rules` spellings (including erff for the Metal
//! erf approximation), its own ABI with a `work_items` bound, and lacks the
//! Metal-only training cases 42..55.

const dialect = @import("model_dialect.zig");

const cuda_rules = [_]dialect.Rule{
    .{ .metal = "device const ", .cuda = "const " },
    .{ .metal = "simd_sum(", .cuda = "gliner25_warp_sum(" },
    .{ .metal = "simd_max(", .cuda = "gliner25_warp_max(" },
    .{ .metal = "rsqrt(", .cuda = "rsqrtf(" },
    .{ .metal = "clamp(", .cuda = "gliner25_clamp(" },
    .{ .metal = "exp(", .cuda = "expf(" },
    .{ .metal = "log(", .cuda = "logf(" },
    .{ .metal = "pow(", .cuda = "powf(" },
    .{ .metal = "cos(", .cuda = "cosf(" },
    .{ .metal = "sin(", .cuda = "sinf(" },
    .{ .metal = "fabs(", .cuda = "fabsf(" },
    .{ .metal = "termite_erf_approx(", .cuda = "erff(" },
};

pub fn metalSource() []const u8 {
    return metal_signature ++ head ++ metal_only ++ tail;
}

pub fn cudaSource() []const u8 {
    return cuda_signature ++ dialect.translate(head ++ tail, &cuda_rules);
}

const cuda_signature =
    \\extern "C" __global__ void termite_gliner25_boundary_f32(
    \\    const float *a0, const float *a1, const float *a2, const float *a3,
    \\    const float *a4, const float *a5, const float *a6, const float *a7,
    \\    const float *a8, const float *a9, float *out,
    \\    Gliner25BoundaryParams p, unsigned int work_items) {
    \\    const uint gid = blockIdx.x * blockDim.x + threadIdx.x;
    \\    if (gid >= work_items) return;
    \\    const uint lane = threadIdx.x & 31u;
    \\
;

const metal_signature =
    \\kernel void termite_gliner_boundary_f32(
    \\    device const float *a0 [[buffer(0)]], device const float *a1 [[buffer(1)]],
    \\    device const float *a2 [[buffer(2)]], device const float *a3 [[buffer(3)]],
    \\    device const float *a4 [[buffer(4)]], device const float *a5 [[buffer(5)]],
    \\    device const float *a6 [[buffer(6)]], device const float *a7 [[buffer(7)]],
    \\    device const float *a8 [[buffer(8)]], device const float *a9 [[buffer(9)]],
    \\    device float *out [[buffer(10)]], constant termite_metal_gliner_boundary_params &p [[buffer(11)]],
    \\    uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]) {
    \\
;

/// Cases shared by both backends, up to the Metal-only block.
const head =
    \\    uint B=p.dims[0], N=p.dims[1], C=p.dims[2], D=p.dims[3];
    \\    switch (p.kind) {
    \\    case 0u: { out[gid]=a0[gid]+a1[gid%N]; break; }
    \\    case 1u: {
    \\        uint row=gid/32u; if(row>=B) return;
    \\        float sum=0.0f; for(uint d=lane;d<N;d+=32u) sum+=a0[row*N+d];
    \\        float mean=simd_sum(sum)/float(N); float sq=0.0f;
    \\        for(uint d=lane;d<N;d+=32u) { float v=a0[row*N+d]-mean; sq+=v*v; }
    \\        float inv=rsqrt(simd_sum(sq)/float(N)+p.scalars[0]);
    \\        for(uint d=lane;d<N;d+=32u) out[row*N+d]=(a0[row*N+d]-mean)*inv*a1[d]+a2[d];
    \\        break;
    \\    }
    \\    case 2u: {
    \\        uint h=gid%C, row=gid/C, pos=row%(N+1u), b=row/(N+1u);
    \\        int raw=((device const int*)a3)[b]; uint len=uint(clamp(raw,0,int(N)));
    \\        if(D==0u) out[gid]=pos==0u?a1[h]:a0[(b*N+pos-1u)*C+h];
    \\        else out[gid]=(pos==N||pos==len)?a2[h]:a0[(b*N+pos)*C+h];
    \\        break;
    \\    }
    \\    case 3u: {
    \\        uint dim=N+C, row=gid/dim, d=gid%dim;
    \\        out[gid]=d<N?a0[row*N+d]:a1[row*C+d-N]; break;
    \\    }
    \\    case 4u: out[gid]=a0[gid]+a1[gid]; break;
    \\    case 5u: out[gid]=((device const int*)a1)[gid/N]!=0?a0[gid]:0.0f; break;
    \\    case 6u: {
    \\        uint group=gid/32u, h=group%C, q=(group/C)%N, b=group/(C*N), hidden=C*D;
    \\        if(b>=B) return;
    \\        uint len=uint(clamp(((device const int*)a1)[b],0,int(N)));
    \\        float query=lane<D?a0[(b*N+q)*3u*hidden+h*D+lane]:0.0f;
    \\        float best=-INFINITY, denom=0.0f, acc=0.0f;
    \\        uint lo=q>p.dims[4]?q-p.dims[4]:0u, hi=min(N-1u,q+p.dims[4]);
    \\        for(uint k=lo;k<=hi;++k) {
    \\            if(k>=len&&k!=q) continue;
    \\            float key=lane<D?a0[(b*N+k)*3u*hidden+hidden+h*D+lane]:0.0f;
    \\            float value=lane<D?a0[(b*N+k)*3u*hidden+2u*hidden+h*D+lane]:0.0f;
    \\            float score=simd_sum(query*key)*rsqrt(float(D));
    \\            float next=max(best,score), old=exp(best-next), prob=exp(score-next);
    \\            acc=acc*old+prob*value; denom=denom*old+prob; best=next;
    \\        }
    \\        if(lane<D) out[(b*N+q)*hidden+h*D+lane]=q<len?acc/denom:0.0f;
    \\        break;
    \\    }
    \\    case 7u: {
    \\        uint row=gid/N, d=gid%N; float gate=a0[row*2u*N+N+d];
    \\        out[gid]=a0[row*2u*N+d]*(gate/(1.0f+exp(-gate))); break;
    \\    }
    \\    case 8u: {
    \\        uint pos=gid%N, qi=gid/N, b=qi/C;
    \\        int len=((device const int*)a2)[b];
    \\        bool valid=((device const int*)a3)[qi]!=0&&int(pos)<len+int(p.dims[4]);
    \\        if(!valid) {out[gid]=-10000.0f;break;}
    \\        float sum=0.0f; for(uint d=0;d<D;++d) sum+=a0[(b*N+pos)*D+d]*a1[qi*D+d];
    \\        out[gid]=sum*rsqrt(float(D)); break;
    \\    }
    \\    case 9u: {
    \\        uint qi=gid, b=qi/C, len=uint(clamp(((device const int*)a1)[b],0,int(N)));
    \\        if(((device const int*)a2)[qi]==0) len=0u;
    \\        float sum=0.0f; for(uint t=0;t<len;++t) sum+=a0[qi*N+t];
    \\        float mean=sum/float(max(len,1u)), running=0.0f; uint base=qi*(N+2u);
    \\        out[base]=0.0f; for(uint t=0;t<N;++t) {if(t<len)running+=a0[qi*N+t]-mean;out[base+t+1u]=running;}
    \\        out[base+N+1u]=mean; break;
    \\    }
    \\    case 10u: {
    \\        uint b=gid/C, d=gid%C, len=uint(clamp(((device const int*)a1)[b],0,int(N)));
    \\        float sum=0.0f; out[b*(N+1u)*C+d]=0.0f;
    \\        for(uint t=0;t<N;++t) {if(t<len)sum+=a0[(b*N+t)*C+d];out[(b*(N+1u)+t+1u)*C+d]=sum;} break;
    \\    }
    \\    case 11u: {
    \\        uint d=gid%(2u*D), ci=gid/(2u*D), b=ci/C;
    \\        int pos=((device const int*)a1)[ci*2u+uint(d>=D)];
    \\        out[gid]=(pos>=0&&uint(pos)<N)?a0[(b*N+uint(pos))*D+d%D]:NAN; break;
    \\    }
    \\    case 12u: {
    \\        uint ci=gid/3u, d=gid%3u, b=ci/N;
    \\        int start=((device const int*)a0)[ci*2u], end=((device const int*)a0)[ci*2u+1u];
    \\        float len=float(max(end-start,1));
    \\        out[gid]=d==0u?log(1.0f+len):(d==1u?len/float(max(((device const int*)a1)[b],1)):rsqrt(len)); break;
    \\    }
    \\    case 13u: {
    \\        uint d=gid%D, ci=gid/D, b=ci/C;
    \\        int start=((device const int*)a1)[ci*2u], end=((device const int*)a1)[ci*2u+1u];
    \\        out[gid]=(start>=0&&end>=start&&uint(end)<N)?(a0[(b*N+uint(end))*D+d]-a0[(b*N+uint(start))*D+d])/float(max(end-start,1)):NAN; break;
    \\    }
    \\    case 14u: {
    \\        uint d=gid%D, ci=gid/D, b=ci/C;
    \\        int start=((device const int*)a4)[ci*2u], end=((device const int*)a4)[ci*2u+1u];
    \\        out[gid]=(start>=0&&end>=start&&uint(end)<N)?a0[(b*N+uint(start))*D+d]+a1[(b*N+uint(end))*D+d]+a2[gid]+a3[gid]:NAN; break;
    \\    }
    \\    case 15u: {
    \\        uint d=gid%D, q=(gid/D)%p.dims[5], ci=gid/(D*p.dims[5]), b=ci/N;
    \\        uint qi=b*C+p.dims[4]+q, off=qi*2u*D;
    \\        out[gid]=a0[ci*D+d]*(1.0f+a1[off+d])+a1[off+D+d]; break;
    \\    }
    \\    case 16u: { float x=a0[gid];out[gid]=0.5f*x*(1.0f+termite_erf_approx(x*0.7071067811865476f));break; }
    \\    case 17u: {
    \\        uint cap=C, queries=D, dim=p.dims[4], count=p.dims[6];
    \\        uint c=gid%cap, local=(gid/cap)%count, b=gid/(cap*count), qi=b*queries+p.dims[5]+local, ci=b*cap+c;
    \\        if(((device const int*)a7)[ci]==0||((device const int*)a8)[qi]==0){out[gid]=-10000.0f;break;}
    \\        int start=((device const int*)a6)[ci*2u],end=((device const int*)a6)[ci*2u+1u];
    \\        if(start<0||end<=start||uint(end)>=N){out[gid]=NAN;break;}
    \\        float score=0.0f;for(uint d=0;d<dim;++d)score+=a0[ci*dim+d]*a1[qi*dim+d];
    \\        score=score*rsqrt(float(dim))+a2[ci*count+local];
    \\        score+=a3[qi*N+uint(start)];score+=a4[qi*N+uint(end)];
    \\        if(p.dims[7]!=0u){uint base=qi*(N+1u);float len=float(end-start);score+=(a5[base+uint(end)]-a5[base+uint(start)]+a5[base+N]*len)*rsqrt(len);}
    \\        out[gid]=score;break;
    \\    }
    \\    case 18u: {
    \\        uint stride=(N+C)*D, b=gid/stride, offset=gid%stride;
    \\        out[gid]=offset<N*D?a0[b*N*D+offset]:a1[b*C*D+offset-N*D];break;
    \\    }
    \\    case 19u: out[gid]=float(((device const half*)a0)[gid]);break;
    \\    case 29u: {
    \\        uint row=gid/N,d=gid%N;int index=((device const int*)a1)[row];
    \\        out[gid]=index==-1?0.0f:((index>=0&&uint(index)<B)?a0[uint(index)*N+d]:NAN);break;
    \\    }
    \\    case 30u: {
    \\        uint group=gid/32u,h=group%C,qi=(group/C)%N,b=group/(C*N),hidden=C*D;
    \\        if(b>=B)return;
    \\        float query[4]={0.0f,0.0f,0.0f,0.0f},acc[4]={0.0f,0.0f,0.0f,0.0f};
    \\        uint pieces=(D+31u)/32u;
    \\        for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D)query[j]=a0[(b*N+qi)*hidden+h*D+d];}
    \\        float best=-INFINITY,denom=0.0f;
    \\        for(uint ki=0;ki<N;++ki){
    \\            if(((device const int*)a6)[b*N+ki]==0)continue;
    \\            int relative=((device const int*)a5)[qi+N-1u-ki];
    \\            bool legal=relative>=0&&uint(relative)<p.dims[4];uint r=legal?uint(relative):0u;
    \\            float cc=0.0f,cp=0.0f,pc=0.0f,values[4]={0.0f,0.0f,0.0f,0.0f};
    \\            for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D){
    \\                uint key=(b*N+ki)*hidden+h*D+d,rel=r*hidden+h*D+d;float kv=a1[key];
    \\                cc+=query[j]*kv;cp+=query[j]*a4[rel];pc+=a3[rel]*kv;values[j]=a2[key];}}
    \\            float score=(simd_sum(cc)+simd_sum(cp)+simd_sum(pc))*rsqrt(3.0f*float(D));
    \\            if(!legal)score=NAN;
    \\            float next=max(best,score),old=exp(best-next),prob=exp(score-next);
    \\            for(uint j=0;j<pieces;++j)acc[j]=acc[j]*old+prob*values[j];
    \\            denom=denom*old+prob;best=next;
    \\        }
    \\        bool valid=((device const int*)a6)[b*N+qi]!=0;
    \\        for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D)out[(b*N+qi)*hidden+h*D+d]=valid?(denom>0.0f?acc[j]/denom:NAN):0.0f;}
    \\        break;
    \\    }
    \\    case 31u: out[gid]=isnan(a0[gid])?NAN:max(a0[gid],0.0f);break;
    \\    case 32u: {uint row=gid/N,d=gid%N;float width=a1[row];out[gid]=width>0.0f?(a0[(row*2u+1u)*N+d]-a0[row*2u*N+d])/width:NAN;break;}
    \\    case 33u: {
    \\        if(((device const int*)a5)[gid]==0){out[gid]=0.0f;break;}
    \\        float sum=0.0f;for(uint d=0;d<N;++d){uint i=gid*N+d;sum+=a1[i]*(1.0f/(1.0f+exp(-a3[i])))*a2[i];}
    \\        out[gid]=(a0[gid]+sum*rsqrt(float(N)))+a4[gid];break;
    \\    }
    \\    case 34u: {
    \\        uint row=gid/32u;if(row>=B)return;uint qpieces=(C+31u)/32u,vpieces=(D+31u)/32u;
    \\        float query[8],acc[24];for(uint j=0;j<qpieces;++j){uint d=uint(lane)+j*32u;query[j]=d<C?a0[row*C+d]:0.0f;}
    \\        for(uint j=0;j<vpieces;++j)acc[j]=0.0f;float best=-INFINITY,denom=0.0f;
    \\        for(uint k=0;k<N;++k){float dot=0.0f;for(uint j=0;j<qpieces;++j){uint d=uint(lane)+j*32u;if(d<C)dot+=query[j]*a1[k*C+d];}
    \\            float score=simd_sum(dot)*rsqrt(float(C)),next=max(best,score),old=exp(best-next),prob=exp(score-next);
    \\            for(uint j=0;j<vpieces;++j){uint d=uint(lane)+j*32u;if(d<D)acc[j]=acc[j]*old+prob*a2[k*D+d];}
    \\            denom=denom*old+prob;best=next;}
    \\        for(uint j=0;j<vpieces;++j){uint d=uint(lane)+j*32u;if(d<D)out[row*D+d]=a3[row*D+d]+acc[j]/denom;}break;
    \\    }
    \\    case 35u: {
    \\        uint lo=0u,hi=C;while(lo<hi){uint mid=lo+(hi-lo)/2u;int off=((device const int*)a4)[mid];
    \\            if(off<0||uint(off)>D){out[gid]=NAN;return;}if(B*(uint(off)+mid)<=gid)lo=mid+1u;else hi=mid;}
    \\        if(lo==0u){out[gid]=NAN;break;}uint f=lo-1u;int begin=((device const int*)a4)[f],end=((device const int*)a4)[f+1u];
    \\        if(begin<0||end<begin||uint(end)>D){out[gid]=NAN;break;}uint cols=uint(end-begin)+1u,local=gid-B*(uint(begin)+f),row=local/cols,col=local%cols;
    \\        if(row>=B){out[gid]=NAN;break;}float sum=0.0f;for(uint d=0;d<N;++d){float key=col==0u?a3[d]:a2[(uint(begin)+col-1u)*N+d];sum+=(a0[row*N+d]+a1[f*N+d])*key;}out[gid]=sum;break;
    \\    }
    \\    case 36u: {
    \\        uint slot=gid/N,col=gid%N;int row=((device const int*)a1)[slot];if(row<0)row+=int(B);
    \\        out[gid]=(row>=0&&uint(row)<B)?a0[uint(row)*N+col]:NAN;break;
    \\    }
    \\    case 37u: {
    \\        uint row=gid/N,col=gid%N;float sum=0.0f;
    \\        for(uint i=0u;i<C;++i){int target=((device const int*)a1)[i];if(target<0)target+=int(B);if(target>=0&&uint(target)==row)sum+=a0[i*N+col];}
    \\        out[gid]=sum;break;
    \\    }
    \\    case 38u: out[gid]=0.0f;break;
    \\    case 39u: {
    \\        uint group=gid/N,col=gid%N;int row=((device const int*)a1)[group];
    \\        int lo=((device const int*)a2)[group],hi=((device const int*)a2)[group+1u];
    \\        if(row<0||uint(row)>=B)return;if(lo<0||hi<lo||uint(hi)>C){out[uint(row)*N+col]=NAN;return;}
    \\        float sum=0.0f;for(int i=lo;i<hi;++i){int v=((device const int*)a3)[i];if(v<0||uint(v)>=C){sum=NAN;break;}sum+=a0[uint(v)*N+col];}
    \\        out[uint(row)*N+col]=sum;break;
    \\    }
    \\    case 40u: {
    \\        uint chunk=gid/32u,lo=chunk*N,hi=min(B,lo+N);float scale=0.0f;uint invalid=0u;
    \\        for(uint i=lo+lane;i<hi;i+=32u){float x=a0[i];if(isfinite(x))scale=max(scale,fabs(x));else invalid=1u;}
    \\        scale=simd_max(scale);invalid=simd_max(invalid);float sum=0.0f;
    \\        if(scale>0.0f)for(uint i=lo+lane;i<hi;i+=32u){float x=a0[i];if(isfinite(x)){float v=x/scale;sum+=v*v;}}
    \\        sum=simd_sum(sum);if(lane==0u){out[chunk*3u]=scale;out[chunk*3u+1u]=sum;out[chunk*3u+2u]=float(invalid);}break;
    \\    }
    \\
;

/// Metal-only cases 42..55.
const metal_only =
    \\    case 42u: {
    \\        uint row=gid/N,d=gid%N,hd=N/C,j=d%hd,base=row*3u*N+p.dims[4]*N;
    \\        float x=a0[base+d];
    \\        if(p.dims[5]==0u){out[gid]=x;break;}
    \\        uint half_dim=hd/2u,k=j%half_dim,other=d-j+(j<half_dim?j+half_dim:j-half_dim);
    \\        float angle=float(row%D)*pow(p.scalars[0],-2.0f*float(k)/float(hd));
    \\        out[gid]=x*cos(angle)+(j<half_dim?-a0[base+other]:a0[base+other])*sin(angle);break;
    \\    }
    \\    case 43u: {uint row=gid/N,d=gid%N;float x=a0[row*2u*N+d];out[gid]=termite_gelu_exact(x)*a0[row*2u*N+N+d];break;}
    \\    case 44u: {uint q=(gid/N)%N,k=gid%N;out[gid]=(q>k?q-k:k-q)>C?-INFINITY:0.0f;break;}
    \\    case 45u: {uint b=gid/(N*C),d=gid%C;int kind=((device const int*)a2)[b];out[gid]=kind>=0&&kind<3?a0[gid]+a1[uint(kind)*C+d]:NAN;break;}
    \\    case 46u: {
    \\        uint b=gid/(C+4u),d=gid%(C+4u);if(d<C){out[gid]=a0[b*N*C+d];break;}
    \\        float best=-INFINITY;uint valid=0u;
    \\        for(uint i=0;i<D;++i){bool ok=((device const int*)a2)[b*D+i]>=0;valid+=uint(ok);float z=ok?a1[b*D+i]:-1e4f;best=max(best,z);}
    \\        float sum=0.0f;for(uint i=0;i<D;++i){float z=((device const int*)a2)[b*D+i]>=0?a1[b*D+i]:-1e4f;sum+=exp(z-best);}
    \\        float first=0.0f,second=0.0f,entropy=0.0f;
    \\        for(uint i=0;i<D;++i){float z=((device const int*)a2)[b*D+i]>=0?a1[b*D+i]:-1e4f;float v=exp(z-best)/sum;entropy-=v*log(max(v,1e-9f));if(v>first){second=first;first=v;}else second=max(second,v);}
    \\        out[gid]=d==C?first:d==C+1u?first-second:d==C+2u?entropy/log(float(valid)):float(valid)/255.0f;break;
    \\    }
    \\    case 47u: {
    \\        uint b=gid,base=b*(N+6u),valid=0u;int kind=((device const int*)a3)[b];float best=-INFINITY;bool ok=kind>=0&&kind<3;
    \\        for(uint i=0;i<N;++i)if(((device const int*)a2)[b*N+i]>=0){++valid;float z=a0[b*N+i];ok=ok&&isfinite(z);best=max(best,z);}
    \\        uint bucket=valid<=2u?0u:valid<=5u?1u:valid<=10u?2u:3u;float scale=ok?a4[uint(kind)*4u+bucket]:1.0f,sum=0.0f;
    \\        for(uint i=0;i<N;++i)if(((device const int*)a2)[b*N+i]>=0)sum+=exp((a0[b*N+i]-best)/scale);
    \\        uint winner=0u;float top=-1.0f,entropy=0.0f,expected=0.0f;
    \\        for(uint i=0;i<N;++i){float v=((device const int*)a2)[b*N+i]>=0?exp((a0[b*N+i]-best)/scale)/sum:0.0f;out[base+i]=v;if(v>top){top=v;winner=i;}entropy-=v*log(max(v,1e-12f));expected+=float(i)*v;}
    \\        float abest=-INFINITY;for(uint i=0;i<C;++i){float z=a1[b*C+i];ok=ok&&isfinite(z);abest=max(abest,z);}float asum=0.0f;for(uint i=0;i<C;++i)asum+=exp(a1[b*C+i]-abest);
    \\        float truth=out[base+1u];out[base+N]=float(winner);out[base+N+1u]=kind==2?max(truth,1.0f-truth):clamp(1.0f-entropy/log(float(valid)),0.0f,1.0f);
    \\        out[base+N+2u]=expected;out[base+N+3u]=truth;out[base+N+4u]=exp(a1[b*C]-abest)/asum;
    \\        out[base+N+5u]=ok&&valid>=2u&&isfinite(sum)&&sum>0.0f&&isfinite(asum)&&asum>0.0f?0.0f:1.0f;break;
    \\    }
    \\    case 48u: {float x=a0[gid];out[gid]=termite_gelu_exact(x);break;}
    \\    case 55u: {ulong seed=ulong(p.dims[1])|(ulong(p.dims[2])<<32);uint bits=uint(termite_dropout_mix(seed^termite_dropout_mix(ulong(gid)))>>32);out[gid]=bits<p.dims[3]?0.0f:p.scalars[0];break;}
    \\    case 54u: {uint part,row,col;if(D!=0u){row=gid/(3u*N);uint c=gid%(3u*N);part=c/N;col=c%N;}else{part=gid/(B*N);uint r=gid%(B*N);row=r/N;col=r%N;}uint src=D!=0u?(part*B+row)*N:row*3u*N+part*N;if(part==2u){out[gid]=a0[src+col];break;}uint half_dim=C/2u,d=col%C,pair=d%half_dim,head=col-d;uint t=(row*(N/C)+head/C)*half_dim+pair;float lo=a0[src+head+pair],hi=a0[src+head+half_dim+pair],c=a1[t],s=a2[t];if(D!=0u)out[gid]=d<half_dim?lo*c+hi*s:hi*c-lo*s;else out[gid]=d<half_dim?lo*c-hi*s:lo*s+hi*c;break;}
    \\    case 53u: {uint row=gid/(2u*N),c=gid%(2u*N),i=c<N?c:c-N;float x=a0[row*2u*N+i],g=a1[row*N+i];if(c<N){float dg=!isfinite(x)?0.0f:(x>10.0f?1.0f:(x<-10.0f?0.0f:0.5f*(1.0f+termite_erf_approx(x*0.7071067811865476f))+x*0.3989422804014327f*exp(-0.5f*x*x)));out[gid]=g*a0[row*2u*N+N+i]*dg;}else out[gid]=g*termite_gelu_exact(x);break;}
    \\    case 52u: {uint row=gid/N,col=gid%N,half_dim=C/2u,d=col%C,pair=d%half_dim,head=col-d;uint t=(row*(N/C)+head/C)*half_dim+pair;float lo=a0[row*N+head+pair],hi=a0[row*N+head+half_dim+pair],c=a1[t],s=a2[t];if(D!=0u)out[gid]=d<half_dim?lo*c+hi*s:hi*c-lo*s;else out[gid]=d<half_dim?lo*c-hi*s:lo*s+hi*c;break;}
    \\    case 49u: {uint b=gid/(N+C),i=gid%(N+C);out[gid]=i<N?(((device const int*)a2)[b*N+i]>=0?a0[b*N+i]:-1e4f):a1[b*C+i-N];break;}
    \\    case 50u: {uint plane=B*N,part=gid/plane,idx=gid%plane,row=idx/N,col=idx%N,base=row*3u*N+part*N; if(part==2u){out[gid]=a0[base+col];break;} uint hd=C,half_dim=hd/2u,d=col%hd,pair=d%half_dim,head=col-d;float pos=float(((device const int*)a1)[row]);float frequency=1.0f/pow(p.scalars[0],float(2u*pair)/float(hd));float angle=pos*frequency;float cosine=cos(angle),sine=sin(angle);float left=a0[base+head+pair],right=a0[base+head+half_dim+pair];out[gid]=d<half_dim?left*cosine-right*sine:left*sine+right*cosine;break;}
    \\    case 51u: {uint row=gid/32u;if(row>=B)return;uint base=row*N;float sum=0.0f;for(uint d=lane;d<N;d+=32u)sum+=a0[base+d]+a1[base+d];float mean=simd_sum(sum)/float(N);float sq=0.0f;for(uint d=lane;d<N;d+=32u){float v=(a0[base+d]+a1[base+d])-mean;sq+=v*v;}float inv=rsqrt(simd_sum(sq)/float(N)+p.scalars[0]);for(uint d=lane;d<N;d+=32u){float v=a0[base+d]+a1[base+d];out[base+d]=(v-mean)*inv*a2[d]+a3[d];if(C!=0u)out[B*N+base+d]=v;}break;}
    \\
;

const tail =
    \\    case 41u: {
    \\        float scale=0.0f;uint invalid=0u;for(uint i=lane;i<B;i+=32u){scale=max(scale,a0[i*3u]);if(a0[i*3u+2u]!=0.0f)invalid=1u;}
    \\        scale=simd_max(scale);invalid=simd_max(invalid);float sum=0.0f;
    \\        if(scale>0.0f)for(uint i=lane;i<B;i+=32u){float v=a0[i*3u]/scale;sum+=v*v*a0[i*3u+1u];}
    \\        sum=simd_sum(sum);if(lane==0u){out[0]=scale;out[1]=sum;out[2]=float(invalid);}break;
    \\    }
    \\    case 20u: {
    \\        uint d=gid%C, pos=(gid/C)%N, base=gid-d, even=d&~1u;
    \\        float angle=float(pos)/pow(p.scalars[0],float(even)/float(C));
    \\        float cosine=cos(angle),sine=sin(angle),x=a0[base+even],y=a0[base+even+1u];
    \\        out[gid]=(d&1u)==0u?x*cosine-y*sine:x*sine+y*cosine;break;
    \\    }
    \\    case 21u: out[gid]=1.0f/(1.0f+exp(-a0[gid]));break;
    \\    case 22u: case 23u: {
    \\        uint width=p.dims[4],heads=p.kind==23u?p.dims[7]:1u,local=gid/heads,head=gid%heads;
    \\        uint slot=p.dims[5]+local,qi=slot/D,b=qi/C;
    \\        int start=((device const int*)a3)[slot*2u],end=((device const int*)a3)[slot*2u+1u];
    \\        if(start<0||end<start||uint(end)>=N){out[gid]=NAN;break;}
    \\        if(p.kind==22u&&((device const int*)a4)[slot]==0){out[gid]=0.0f;break;}
    \\        float sum=0.0f;uint per=width/heads;
    \\        for(uint d=head*per;d<(head+1u)*per;++d)sum+=a0[(b*N+uint(start))*width+d]*a2[qi*(width/2u)+d/2u]*a1[(b*N+uint(end))*width+d];
    \\        out[gid]=p.kind==22u?sum*rsqrt(float(width)):sum;break;
    \\    }
    \\    case 24u: {
    \\        uint width=p.dims[4],local=gid/(2u*width),d=gid%(2u*width),slot=p.dims[5]+local,qi=slot/D,b=qi/C;
    \\        int start=((device const int*)a2)[slot*2u],end=((device const int*)a2)[slot*2u+1u];
    \\        if(start<0||end<start||uint(end)>=N){out[gid]=NAN;break;}
    \\        float delta=a0[(b*N+uint(start))*width+d%width]-a1[(b*N+uint(end))*width+d%width];
    \\        out[gid]=d<width?delta:fabs(delta);break;
    \\    }
    \\    case 25u: {
    \\        uint width=p.dims[4],local=gid/width,d=gid%width,slot=p.dims[5]+local,qi=slot/D,b=qi/C;
    \\        int start=((device const int*)a1)[slot*2u],end=((device const int*)a1)[slot*2u+1u];
    \\        out[gid]=(start>=0&&end>=start&&uint(end)<N)?(a0[(b*N+uint(end))*width+d]-a0[(b*N+uint(start))*width+d])/float(max(end-start,1)):NAN;break;
    \\    }
    \\    case 26u: {
    \\        uint slot=p.dims[5]+gid,qi=slot/D;
    \\        int start=((device const int*)a5)[slot*2u],end=((device const int*)a5)[slot*2u+1u];
    \\        out[gid]=(start>=0&&end>=start&&uint(end)<N)?a0[gid]*rsqrt(float(p.dims[4]))+a1[gid]+a2[gid]+a3[qi*N+uint(start)]+a4[qi*N+uint(end)]:NAN;break;
    \\    }
    \\    case 27u: {
    \\        uint width=p.dims[4],qi=(p.dims[5]+gid)/D;float sum=0.0f;
    \\        for(uint d=0;d<width;++d)sum+=a1[gid*width+d]*a2[qi*width+d];
    \\        out[gid]=a0[gid]+sum*rsqrt(float(width))+a3[gid];break;
    \\    }
    \\    case 28u: {
    \\        uint slot=p.dims[5]+gid,qi=slot/D,b=qi/C;
    \\        if(((device const int*)a5)[slot]==0){out[gid]=-10000.0f;break;}
    \\        int start=((device const int*)a4)[slot*2u],end=((device const int*)a4)[slot*2u+1u];
    \\        if(start<0||end<=start||uint(end)>=N){out[gid]=NAN;break;}
    \\        float len=float(end-start),inv=rsqrt(len);uint base=qi*(N+1u);
    \\        float score=a0[gid]+a2[qi]*(a1[base+uint(end)]-a1[base+uint(start)]+a1[base+N]*len)*inv;
    \\        score+=a3[qi*3u]*log(1.0f+len)+a3[qi*3u+1u]*len/float(max(((device const int*)a6)[b],1))+a3[qi*3u+2u]*inv;
    \\        out[gid]=score;break;
    \\    }
    \\    }
    \\}
    \\
;
