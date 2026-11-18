// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// CUDA implementation of gliner_boundary_device_ops.zig. All descriptors are
// validated by the shared Zig layout planner before launch. The integer ABI
// matches Metal, including the distinct padding and exact-gather operations.
#pragma once
namespace {
using uint = unsigned int;
template<class T> __device__ __forceinline__ T gliner25_clamp(T v, T lo, T hi) { return min(max(v, lo), hi); }
__device__ __forceinline__ float gliner25_warp_sum(float v) {
    for (int d=16; d; d>>=1) v += __shfl_xor_sync(0xffffffffu, v, d);
    return v;
}
template<class T> __device__ __forceinline__ T gliner25_warp_max(T v) {
    for (int d=16; d; d>>=1) v = max(v, __shfl_xor_sync(0xffffffffu, v, d));
    return v;
}
struct Gliner25BoundaryParams { uint kind; uint dims[8]; float scalars[4]; };
static_assert(sizeof(Gliner25BoundaryParams) == 52, "boundary descriptor ABI");
}
extern "C" __global__ void termite_gliner25_boundary_f32(
    const float *a0, const float *a1, const float *a2, const float *a3,
    const float *a4, const float *a5, const float *a6, const float *a7,
    const float *a8, const float *a9, float *out,
    Gliner25BoundaryParams p, unsigned int work_items) {
    const uint gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid >= work_items) return;
    const uint lane = threadIdx.x & 31u;
    uint B=p.dims[0], N=p.dims[1], C=p.dims[2], D=p.dims[3];
    switch (p.kind) {
    case 0u: { out[gid]=a0[gid]+a1[gid%N]; break; }
    case 1u: {
        uint row=gid/32u; if(row>=B) return;
        float sum=0.0f; for(uint d=lane;d<N;d+=32u) sum+=a0[row*N+d];
        float mean=gliner25_warp_sum(sum)/float(N); float sq=0.0f;
        for(uint d=lane;d<N;d+=32u) { float v=a0[row*N+d]-mean; sq+=v*v; }
        float inv=rsqrtf(gliner25_warp_sum(sq)/float(N)+p.scalars[0]);
        for(uint d=lane;d<N;d+=32u) out[row*N+d]=(a0[row*N+d]-mean)*inv*a1[d]+a2[d];
        break;
    }
    case 51u: {
        uint row=gid/32u;if(row>=B)return;uint base=row*N;
        float sum=0.0f;for(uint d=lane;d<N;d+=32u)sum+=a0[base+d]+a1[base+d];
        float mean=gliner25_warp_sum(sum)/float(N),sq=0.0f;
        for(uint d=lane;d<N;d+=32u){float x=(a0[base+d]+a1[base+d])-mean;sq+=x*x;}
        float inv=rsqrtf(gliner25_warp_sum(sq)/float(N)+p.scalars[0]);
        for(uint d=lane;d<N;d+=32u){float x=a0[base+d]+a1[base+d];out[base+d]=(x-mean)*inv*a2[d]+a3[d];if(C)out[B*N+base+d]=x;}
        break;
    }
    case 2u: {
        uint h=gid%C, row=gid/C, pos=row%(N+1u), b=row/(N+1u);
        int raw=((const int*)a3)[b]; uint len=uint(gliner25_clamp(raw,0,int(N)));
        if(D==0u) out[gid]=pos==0u?a1[h]:a0[(b*N+pos-1u)*C+h];
        else out[gid]=(pos==N||pos==len)?a2[h]:a0[(b*N+pos)*C+h];
        break;
    }
    case 3u: {
        uint dim=N+C, row=gid/dim, d=gid%dim;
        out[gid]=d<N?a0[row*N+d]:a1[row*C+d-N]; break;
    }
    case 4u: out[gid]=a0[gid]+a1[gid]; break;
    case 5u: out[gid]=((const int*)a1)[gid/N]!=0?a0[gid]:0.0f; break;
    case 6u: {
        uint group=gid/32u, h=group%C, q=(group/C)%N, b=group/(C*N), hidden=C*D;
        if(b>=B) return;
        uint len=uint(gliner25_clamp(((const int*)a1)[b],0,int(N)));
        float query=lane<D?a0[(b*N+q)*3u*hidden+h*D+lane]:0.0f;
        float best=-INFINITY, denom=0.0f, acc=0.0f;
        uint lo=q>p.dims[4]?q-p.dims[4]:0u, hi=min(N-1u,q+p.dims[4]);
        for(uint k=lo;k<=hi;++k) {
            if(k>=len&&k!=q) continue;
            float key=lane<D?a0[(b*N+k)*3u*hidden+hidden+h*D+lane]:0.0f;
            float value=lane<D?a0[(b*N+k)*3u*hidden+2u*hidden+h*D+lane]:0.0f;
            float score=gliner25_warp_sum(query*key)*rsqrtf(float(D));
            float next=max(best,score), old=expf(best-next), prob=expf(score-next);
            acc=acc*old+prob*value; denom=denom*old+prob; best=next;
        }
        if(lane<D) out[(b*N+q)*hidden+h*D+lane]=q<len?acc/denom:0.0f;
        break;
    }
    case 7u: {
        uint row=gid/N, d=gid%N; float gate=a0[row*2u*N+N+d];
        out[gid]=a0[row*2u*N+d]*(gate/(1.0f+expf(-gate))); break;
    }
    case 8u: {
        uint pos=gid%N, qi=gid/N, b=qi/C;
        int len=((const int*)a2)[b];
        bool valid=((const int*)a3)[qi]!=0&&int(pos)<len+int(p.dims[4]);
        if(!valid) {out[gid]=-10000.0f;break;}
        float sum=0.0f; for(uint d=0;d<D;++d) sum+=a0[(b*N+pos)*D+d]*a1[qi*D+d];
        out[gid]=sum*rsqrtf(float(D)); break;
    }
    case 9u: {
        uint qi=gid, b=qi/C, len=uint(gliner25_clamp(((const int*)a1)[b],0,int(N)));
        if(((const int*)a2)[qi]==0) len=0u;
        float sum=0.0f; for(uint t=0;t<len;++t) sum+=a0[qi*N+t];
        float mean=sum/float(max(len,1u)), running=0.0f; uint base=qi*(N+2u);
        out[base]=0.0f; for(uint t=0;t<N;++t) {if(t<len)running+=a0[qi*N+t]-mean;out[base+t+1u]=running;}
        out[base+N+1u]=mean; break;
    }
    case 10u: {
        uint b=gid/C, d=gid%C, len=uint(gliner25_clamp(((const int*)a1)[b],0,int(N)));
        float sum=0.0f; out[b*(N+1u)*C+d]=0.0f;
        for(uint t=0;t<N;++t) {if(t<len)sum+=a0[(b*N+t)*C+d];out[(b*(N+1u)+t+1u)*C+d]=sum;} break;
    }
    case 11u: {
        uint d=gid%(2u*D), ci=gid/(2u*D), b=ci/C;
        int pos=((const int*)a1)[ci*2u+uint(d>=D)];
        out[gid]=(pos>=0&&uint(pos)<N)?a0[(b*N+uint(pos))*D+d%D]:NAN; break;
    }
    case 12u: {
        uint ci=gid/3u, d=gid%3u, b=ci/N;
        int start=((const int*)a0)[ci*2u], end=((const int*)a0)[ci*2u+1u];
        float len=float(max(end-start,1));
        out[gid]=d==0u?logf(1.0f+len):(d==1u?len/float(max(((const int*)a1)[b],1)):rsqrtf(len)); break;
    }
    case 13u: {
        uint d=gid%D, ci=gid/D, b=ci/C;
        int start=((const int*)a1)[ci*2u], end=((const int*)a1)[ci*2u+1u];
        out[gid]=(start>=0&&end>=start&&uint(end)<N)?(a0[(b*N+uint(end))*D+d]-a0[(b*N+uint(start))*D+d])/float(max(end-start,1)):NAN; break;
    }
    case 14u: {
        uint d=gid%D, ci=gid/D, b=ci/C;
        int start=((const int*)a4)[ci*2u], end=((const int*)a4)[ci*2u+1u];
        out[gid]=(start>=0&&end>=start&&uint(end)<N)?a0[(b*N+uint(start))*D+d]+a1[(b*N+uint(end))*D+d]+a2[gid]+a3[gid]:NAN; break;
    }
    case 15u: {
        uint d=gid%D, q=(gid/D)%p.dims[5], ci=gid/(D*p.dims[5]), b=ci/N;
        uint qi=b*C+p.dims[4]+q, off=qi*2u*D;
        out[gid]=a0[ci*D+d]*(1.0f+a1[off+d])+a1[off+D+d]; break;
    }
    case 16u: { float x=a0[gid];out[gid]=0.5f*x*(1.0f+erff(x*0.7071067811865476f));break; }
    case 17u: {
        uint cap=C, queries=D, dim=p.dims[4], count=p.dims[6];
        uint c=gid%cap, local=(gid/cap)%count, b=gid/(cap*count), qi=b*queries+p.dims[5]+local, ci=b*cap+c;
        if(((const int*)a7)[ci]==0||((const int*)a8)[qi]==0){out[gid]=-10000.0f;break;}
        int start=((const int*)a6)[ci*2u],end=((const int*)a6)[ci*2u+1u];
        if(start<0||end<=start||uint(end)>=N){out[gid]=NAN;break;}
        float score=0.0f;for(uint d=0;d<dim;++d)score+=a0[ci*dim+d]*a1[qi*dim+d];
        score=score*rsqrtf(float(dim))+a2[ci*count+local];
        score+=a3[qi*N+uint(start)];score+=a4[qi*N+uint(end)];
        if(p.dims[7]!=0u){uint base=qi*(N+1u);float len=float(end-start);score+=(a5[base+uint(end)]-a5[base+uint(start)]+a5[base+N]*len)*rsqrtf(len);}
        out[gid]=score;break;
    }
    case 18u: {
        uint stride=(N+C)*D, b=gid/stride, offset=gid%stride;
        out[gid]=offset<N*D?a0[b*N*D+offset]:a1[b*C*D+offset-N*D];break;
    }
    case 19u: out[gid]=float(((const half*)a0)[gid]);break;
    case 29u: {
        uint row=gid/N,d=gid%N;int index=((const int*)a1)[row];
        out[gid]=index==-1?0.0f:((index>=0&&uint(index)<B)?a0[uint(index)*N+d]:NAN);break;
    }
    case 30u: {
        uint group=gid/32u,h=group%C,qi=(group/C)%N,b=group/(C*N),hidden=C*D;
        if(b>=B)return;
        float query[4]={0.0f,0.0f,0.0f,0.0f},acc[4]={0.0f,0.0f,0.0f,0.0f};
        uint pieces=(D+31u)/32u;
        for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D)query[j]=a0[(b*N+qi)*hidden+h*D+d];}
        float best=-INFINITY,denom=0.0f;
        for(uint ki=0;ki<N;++ki){
            if(((const int*)a6)[b*N+ki]==0)continue;
            int relative=((const int*)a5)[qi+N-1u-ki];
            bool legal=relative>=0&&uint(relative)<p.dims[4];uint r=legal?uint(relative):0u;
            float cc=0.0f,cp=0.0f,pc=0.0f,values[4]={0.0f,0.0f,0.0f,0.0f};
            for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D){
                uint key=(b*N+ki)*hidden+h*D+d,rel=r*hidden+h*D+d;float kv=a1[key];
                cc+=query[j]*kv;cp+=query[j]*a4[rel];pc+=a3[rel]*kv;values[j]=a2[key];}}
            float score=(gliner25_warp_sum(cc)+gliner25_warp_sum(cp)+gliner25_warp_sum(pc))*rsqrtf(3.0f*float(D));
            if(!legal)score=NAN;
            float next=max(best,score),old=expf(best-next),prob=expf(score-next);
            for(uint j=0;j<pieces;++j)acc[j]=acc[j]*old+prob*values[j];
            denom=denom*old+prob;best=next;
        }
        bool valid=((const int*)a6)[b*N+qi]!=0;
        for(uint j=0;j<pieces;++j){uint d=uint(lane)+32u*j;if(d<D)out[(b*N+qi)*hidden+h*D+d]=valid?(denom>0.0f?acc[j]/denom:NAN):0.0f;}
        break;
    }
    case 31u: out[gid]=isnan(a0[gid])?NAN:max(a0[gid],0.0f);break;
    case 32u: {uint row=gid/N,d=gid%N;float width=a1[row];out[gid]=width>0.0f?(a0[(row*2u+1u)*N+d]-a0[row*2u*N+d])/width:NAN;break;}
    case 33u: {
        if(((const int*)a5)[gid]==0){out[gid]=0.0f;break;}
        float sum=0.0f;for(uint d=0;d<N;++d){uint i=gid*N+d;sum+=a1[i]*(1.0f/(1.0f+expf(-a3[i])))*a2[i];}
        out[gid]=(a0[gid]+sum*rsqrtf(float(N)))+a4[gid];break;
    }
    case 34u: {
        uint row=gid/32u;if(row>=B)return;uint qpieces=(C+31u)/32u,vpieces=(D+31u)/32u;
        float query[8],acc[24];for(uint j=0;j<qpieces;++j){uint d=uint(lane)+j*32u;query[j]=d<C?a0[row*C+d]:0.0f;}
        for(uint j=0;j<vpieces;++j)acc[j]=0.0f;float best=-INFINITY,denom=0.0f;
        for(uint k=0;k<N;++k){float dot=0.0f;for(uint j=0;j<qpieces;++j){uint d=uint(lane)+j*32u;if(d<C)dot+=query[j]*a1[k*C+d];}
            float score=gliner25_warp_sum(dot)*rsqrtf(float(C)),next=max(best,score),old=expf(best-next),prob=expf(score-next);
            for(uint j=0;j<vpieces;++j){uint d=uint(lane)+j*32u;if(d<D)acc[j]=acc[j]*old+prob*a2[k*D+d];}
            denom=denom*old+prob;best=next;}
        for(uint j=0;j<vpieces;++j){uint d=uint(lane)+j*32u;if(d<D)out[row*D+d]=a3[row*D+d]+acc[j]/denom;}break;
    }
    case 35u: {
        uint lo=0u,hi=C;while(lo<hi){uint mid=lo+(hi-lo)/2u;int off=((const int*)a4)[mid];
            if(off<0||uint(off)>D){out[gid]=NAN;return;}if(B*(uint(off)+mid)<=gid)lo=mid+1u;else hi=mid;}
        if(lo==0u){out[gid]=NAN;break;}uint f=lo-1u;int begin=((const int*)a4)[f],end=((const int*)a4)[f+1u];
        if(begin<0||end<begin||uint(end)>D){out[gid]=NAN;break;}uint cols=uint(end-begin)+1u,local=gid-B*(uint(begin)+f),row=local/cols,col=local%cols;
        if(row>=B){out[gid]=NAN;break;}float sum=0.0f;for(uint d=0;d<N;++d){float key=col==0u?a3[d]:a2[(uint(begin)+col-1u)*N+d];sum+=(a0[row*N+d]+a1[f*N+d])*key;}out[gid]=sum;break;
    }
    case 36u: {
        uint slot=gid/N,col=gid%N;int row=((const int*)a1)[slot];if(row<0)row+=int(B);
        out[gid]=(row>=0&&uint(row)<B)?a0[uint(row)*N+col]:NAN;break;
    }
    case 37u: {
        uint row=gid/N,col=gid%N;float sum=0.0f;
        for(uint i=0u;i<C;++i){int target=((const int*)a1)[i];if(target<0)target+=int(B);if(target>=0&&uint(target)==row)sum+=a0[i*N+col];}
        out[gid]=sum;break;
    }
    case 38u: out[gid]=0.0f;break;
    case 39u: {
        uint group=gid/N,col=gid%N;int row=((const int*)a1)[group];
        int lo=((const int*)a2)[group],hi=((const int*)a2)[group+1u];
        if(row<0||uint(row)>=B)return;if(lo<0||hi<lo||uint(hi)>C){out[uint(row)*N+col]=NAN;return;}
        float sum=0.0f;for(int i=lo;i<hi;++i){int v=((const int*)a3)[i];if(v<0||uint(v)>=C){sum=NAN;break;}sum+=a0[uint(v)*N+col];}
        out[uint(row)*N+col]=sum;break;
    }
    case 40u: {
        uint chunk=gid/32u,lo=chunk*N,hi=min(B,lo+N);float scale=0.0f;uint invalid=0u;
        for(uint i=lo+lane;i<hi;i+=32u){float x=a0[i];if(isfinite(x))scale=max(scale,fabsf(x));else invalid=1u;}
        scale=gliner25_warp_max(scale);invalid=gliner25_warp_max(invalid);float sum=0.0f;
        if(scale>0.0f)for(uint i=lo+lane;i<hi;i+=32u){float x=a0[i];if(isfinite(x)){float v=x/scale;sum+=v*v;}}
        sum=gliner25_warp_sum(sum);if(lane==0u){out[chunk*3u]=scale;out[chunk*3u+1u]=sum;out[chunk*3u+2u]=float(invalid);}break;
    }
    case 41u: {
        float scale=0.0f;uint invalid=0u;for(uint i=lane;i<B;i+=32u){scale=max(scale,a0[i*3u]);if(a0[i*3u+2u]!=0.0f)invalid=1u;}
        scale=gliner25_warp_max(scale);invalid=gliner25_warp_max(invalid);float sum=0.0f;
        if(scale>0.0f)for(uint i=lane;i<B;i+=32u){float v=a0[i*3u]/scale;sum+=v*v*a0[i*3u+1u];}
        sum=gliner25_warp_sum(sum);if(lane==0u){out[0]=scale;out[1]=sum;out[2]=float(invalid);}break;
    }
    case 20u: {
        uint d=gid%C, pos=(gid/C)%N, base=gid-d, even=d&~1u;
        float angle=float(pos)/powf(p.scalars[0],float(even)/float(C));
        float cosine=cosf(angle),sine=sinf(angle),x=a0[base+even],y=a0[base+even+1u];
        out[gid]=(d&1u)==0u?x*cosine-y*sine:x*sine+y*cosine;break;
    }
    case 21u: out[gid]=1.0f/(1.0f+expf(-a0[gid]));break;
    case 22u: case 23u: {
        uint width=p.dims[4],heads=p.kind==23u?p.dims[7]:1u,local=gid/heads,head=gid%heads;
        uint slot=p.dims[5]+local,qi=slot/D,b=qi/C;
        int start=((const int*)a3)[slot*2u],end=((const int*)a3)[slot*2u+1u];
        if(start<0||end<start||uint(end)>=N){out[gid]=NAN;break;}
        if(p.kind==22u&&((const int*)a4)[slot]==0){out[gid]=0.0f;break;}
        float sum=0.0f;uint per=width/heads;
        for(uint d=head*per;d<(head+1u)*per;++d)sum+=a0[(b*N+uint(start))*width+d]*a2[qi*(width/2u)+d/2u]*a1[(b*N+uint(end))*width+d];
        out[gid]=p.kind==22u?sum*rsqrtf(float(width)):sum;break;
    }
    case 24u: {
        uint width=p.dims[4],local=gid/(2u*width),d=gid%(2u*width),slot=p.dims[5]+local,qi=slot/D,b=qi/C;
        int start=((const int*)a2)[slot*2u],end=((const int*)a2)[slot*2u+1u];
        if(start<0||end<start||uint(end)>=N){out[gid]=NAN;break;}
        float delta=a0[(b*N+uint(start))*width+d%width]-a1[(b*N+uint(end))*width+d%width];
        out[gid]=d<width?delta:fabsf(delta);break;
    }
    case 25u: {
        uint width=p.dims[4],local=gid/width,d=gid%width,slot=p.dims[5]+local,qi=slot/D,b=qi/C;
        int start=((const int*)a1)[slot*2u],end=((const int*)a1)[slot*2u+1u];
        out[gid]=(start>=0&&end>=start&&uint(end)<N)?(a0[(b*N+uint(end))*width+d]-a0[(b*N+uint(start))*width+d])/float(max(end-start,1)):NAN;break;
    }
    case 26u: {
        uint slot=p.dims[5]+gid,qi=slot/D;
        int start=((const int*)a5)[slot*2u],end=((const int*)a5)[slot*2u+1u];
        out[gid]=(start>=0&&end>=start&&uint(end)<N)?a0[gid]*rsqrtf(float(p.dims[4]))+a1[gid]+a2[gid]+a3[qi*N+uint(start)]+a4[qi*N+uint(end)]:NAN;break;
    }
    case 27u: {
        uint width=p.dims[4],qi=(p.dims[5]+gid)/D;float sum=0.0f;
        for(uint d=0;d<width;++d)sum+=a1[gid*width+d]*a2[qi*width+d];
        out[gid]=a0[gid]+sum*rsqrtf(float(width))+a3[gid];break;
    }
    case 28u: {
        uint slot=p.dims[5]+gid,qi=slot/D,b=qi/C;
        if(((const int*)a5)[slot]==0){out[gid]=-10000.0f;break;}
        int start=((const int*)a4)[slot*2u],end=((const int*)a4)[slot*2u+1u];
        if(start<0||end<=start||uint(end)>=N){out[gid]=NAN;break;}
        float len=float(end-start),inv=rsqrtf(len);uint base=qi*(N+1u);
        float score=a0[gid]+a2[qi]*(a1[base+uint(end)]-a1[base+uint(start)]+a1[base+N]*len)*inv;
        score+=a3[qi*3u]*logf(1.0f+len)+a3[qi*3u+1u]*len/float(max(((const int*)a6)[b],1))+a3[qi*3u+2u]*inv;
        out[gid]=score;break;
    }
    }
}

// Explicit BF16 experiment: F32 boundary-head attention, parallel over the
// bounded band. Eight warps share one query; no dense score workspace.
extern "C" __global__ void termite_gliner25_boundary_banded_f32_v1(
    const float *a0, const float *a1, const float *a2, const float *a3,
    const float *a4, const float *a5, const float *a6, const float *a7,
    const float *a8, const float *a9, float *out,
    Gliner25BoundaryParams p, unsigned int work_items) {
 const unsigned lane=threadIdx.x%32,warp=threadIdx.x/32,N=p.dims[1],C=p.dims[2],D=p.dims[3],h=blockIdx.x%C,q=(blockIdx.x/C)%N,b=blockIdx.x/(C*N),hidden=C*D;
 const unsigned len=min(N,unsigned(max(0,((const int*)a1)[b])));
 if(q>=len){if(!warp&&lane<D)out[(b*N+q)*hidden+h*D+lane]=0;return;}
 const unsigned lo=q>p.dims[4]?q-p.dims[4]:0,hi=min(N-1,q+p.dims[4]),count=hi-lo+1;
 __shared__ float scores[257],reductions[8],parts[8*32];
 const float query=lane<D?a0[(b*N+q)*3*hidden+h*D+lane]:0;
 for(unsigned k=lo+warp;k<=hi;k+=8){float key=lane<D?a0[(b*N+k)*3*hidden+hidden+h*D+lane]:0;float s=gliner25_warp_sum(query*key)*rsqrtf(float(D));if(!lane)scores[k-lo]=k<len?s:-INFINITY;}
 __syncthreads();
 float v=-INFINITY;for(unsigned i=threadIdx.x;i<count;i+=256)v=fmaxf(v,scores[i]);v=gliner25_warp_max(v);if(!lane)reductions[warp]=v;__syncthreads();
 v=lane<8?reductions[lane]:-INFINITY;v=gliner25_warp_max(v);__syncthreads();float s=0;for(unsigned i=threadIdx.x;i<count;i+=256){float e=expf(scores[i]-v);scores[i]=e;s+=e;}s=gliner25_warp_sum(s);if(!lane)reductions[warp]=s;__syncthreads();
 s=lane<8?reductions[lane]:0;s=gliner25_warp_sum(s);
 float acc=0;for(unsigned k=lo+warp;k<=hi;k+=8){float value=lane<D?a0[(b*N+k)*3*hidden+2*hidden+h*D+lane]:0;acc+=scores[k-lo]*value;}parts[warp*32+lane]=acc;__syncthreads();
 if(!warp&&lane<D){float value=0;for(unsigned w=0;w<8;w++)value+=parts[w*32+lane];out[(b*N+q)*hidden+h*D+lane]=value/s;}
}

// Preserve each existing F32 normalization boundary and emit its RNE BF16
// value in the same pass. Flavors 0/1 use the boundary head's centered warp
// reduction; 2/3 preserve the generic add/plain block reduction respectively.
extern "C" __global__ void termite_gliner25_norm_bf16_mirror_v1(
    float* output, __nv_bfloat16* mirror, const float* a, const float* b,
    const float* gamma, const float* beta, unsigned rows, unsigned width,
    float eps, unsigned flavor) {
    const bool add = flavor == 1u || flavor == 2u;
    if (flavor < 2u) {
        const unsigned lane = threadIdx.x & 31u;
        const unsigned row = blockIdx.x * 8u + (threadIdx.x >> 5u);
        if (row >= rows) return;
        const unsigned base = row * width;
        float sum = 0.0f;
        for (unsigned d = lane; d < width; d += 32u)
            sum += (add ? a[base+d] + b[base+d] : a[base+d]);
        const float mean = gliner25_warp_sum(sum) / float(width);
        float square = 0.0f;
        for (unsigned d = lane; d < width; d += 32u) {
            const float x = ((add ? a[base+d] + b[base+d] : a[base+d])) - mean;
            square += x*x;
        }
        const float inv = rsqrtf(gliner25_warp_sum(square) / float(width) + eps);
        for (unsigned d = lane; d < width; d += 32u) {
            const float x = (add ? a[base+d] + b[base+d] : a[base+d]);
            const float value = (x-mean) * inv * gamma[d] + beta[d];
            output[base+d] = value;
            mirror[base+d] = __float2bfloat16_rn(value);
        }
        return;
    }
    const unsigned row = blockIdx.x, tid = threadIdx.x;
    if (row >= rows) return;
    const unsigned base = row * width;
    __shared__ float sums[256], squares[256];
    float sum = 0.0f, square = 0.0f;
    for (unsigned d = tid; d < width; d += blockDim.x) {
        const float x = (add ? a[base+d] + b[base+d] : a[base+d]);
        sum += x;
        square += x*x;
    }
    sums[tid] = sum;
    squares[tid] = square;
    __syncthreads();
    for (unsigned stride = blockDim.x >> 1u; stride; stride >>= 1u) {
        if (tid < stride) {
            sums[tid] += sums[tid+stride];
            squares[tid] += squares[tid+stride];
        }
        __syncthreads();
    }
    const float mean = sums[0] / float(width);
    // Keep the original kernel's rounded square/subtraction. With the extra
    // BF16 output, ptxas otherwise contracts these into a different FFMA.
    const float mean_square = __fmul_rn(mean, mean);
    const float variance = fmaxf(__fsub_rn(__fdiv_rn(squares[0], float(width)), mean_square), 0.0f);
    const float inv = rsqrtf(variance + eps);
    for (unsigned d = tid; d < width; d += blockDim.x) {
        const float x = (add ? a[base+d] + b[base+d] : a[base+d]);
        const float value = (x-mean) * inv * gamma[d] + beta[d];
        output[base+d] = value;
        mirror[base+d] = __float2bfloat16_rn(value);
    }
}

// Explicit BF16 experiment only: bounded head reductions and scans.
// Padded words are never loaded; prefix padding carries the last valid sum.
__device__ __forceinline__ float gliner25_warp_prefix(float x){unsigned lane=threadIdx.x&31;for(unsigned d=1;d<32;d*=2){float y=__shfl_up_sync(0xffffffff,x,d);if(lane>=d)x+=y;}return x;}
__device__ __forceinline__ float gliner25_block_prefix(float x,float* sums){unsigned lane=threadIdx.x&31,warp=threadIdx.x>>5;x=gliner25_warp_prefix(x);if(lane==31)sums[warp]=x;__syncthreads();if(!warp){float y=lane<16?sums[lane]:0;y=gliner25_warp_prefix(y);if(lane<16)sums[lane]=y;}__syncthreads();return x+(warp?sums[warp-1]:0);}
extern "C" __global__ void termite_gliner25_boundary_parallel_f32_v1(const float* a0,const float* a1,const float* a2,const float* a3,const float* a4,const float* a5,const float* a6,const float* a7,const float* a8,const float* a9,float* out,Gliner25BoundaryParams p,unsigned work){
 const unsigned lane=threadIdx.x&31,B=p.dims[0],N=p.dims[1],C=p.dims[2],D=p.dims[3];
 if(p.kind==8){unsigned row=blockIdx.x*8+(threadIdx.x>>5);if(row>=B*C*N)return;unsigned pos=row%N,qi=row/N,b=qi/C;int len=((const int*)a2)[b];if(((const int*)a3)[qi]==0||int(pos)>=len+int(p.dims[4])){if(!lane)out[row]=-10000;return;}float x=0;for(unsigned d=lane;d<D;d+=32)x+=a0[(b*N+pos)*D+d]*a1[qi*D+d];for(unsigned d=16;d;d>>=1)x+=__shfl_xor_sync(0xffffffff,x,d);if(!lane)out[row]=x*rsqrtf(float(D));return;}
 __shared__ float sums[16];const unsigned t=threadIdx.x,row=blockIdx.x;
 if(p.kind==9){unsigned b=row/C;unsigned len=unsigned(min(max(((const int*)a1)[b],0),int(N)));if(((const int*)a2)[row]==0)len=0;float x=t<len?a0[row*N+t]:0;float inclusive=gliner25_block_prefix(x,sums);float mean=sums[15]/float(max(len,1u));__syncthreads();float centered=t<len?x-mean:0;inclusive=gliner25_block_prefix(centered,sums);if(!t){out[row*(N+2)]=0;out[row*(N+2)+N+1]=mean;}if(t<N)out[row*(N+2)+t+1]=inclusive;return;}
 if(p.kind==10){unsigned b=row/C,d=row%C,len=unsigned(min(max(((const int*)a1)[b],0),int(N)));float x=t<len?a0[(b*N+t)*C+d]:0;float inclusive=gliner25_block_prefix(x,sums);if(!t)out[b*(N+1)*C+d]=0;if(t<N)out[(b*(N+1)+t+1)*C+d]=inclusive;}
}
