// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Compare E4B repeat prefill with the serving F32 route and FP64 spot oracles.
// --memcheck skips the slow scalar GPU reference and checks candidate memory
// accesses, finite outputs and independent FP64 spots; run both modes.
#include <cuda.h>
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

static void ck(cudaError_t e) { if (e != cudaSuccess) { std::fprintf(stderr, "%s\n", cudaGetErrorString(e)); std::exit(2); } }
static void ck(CUresult e) { if (e != CUDA_SUCCESS) { const char* s = nullptr; cuGetErrorString(e, &s); std::fprintf(stderr, "%s\n", s); std::exit(2); } }
template<class T> struct Device {
    T* p = nullptr;
    explicit Device(size_t n) { ck(cudaMalloc(&p, n*sizeof(T))); }
    explicit Device(const std::vector<T>& v) : Device(v.size()) { ck(cudaMemcpy(p,v.data(),v.size()*sizeof(T),cudaMemcpyHostToDevice)); }
    ~Device() { cudaFree(p); }
    Device(const Device&) = delete;
    std::vector<T> read(size_t n) { std::vector<T> v(n); ck(cudaMemcpy(v.data(),p,n*sizeof(T),cudaMemcpyDeviceToHost)); return v; }
};

static bool run(CUmodule module, CUmodule repeat_module, unsigned dim, unsigned qr, unsigned offset, bool permuted, bool sharp, bool memory_check, bool split) {
    unsigned batch=1, heads=8, kh=2, rows=offset+qr, page=16, capacity=(rows+15)/16*16;
    unsigned blocks=permuted ? capacity/page : 0, window=dim==256 ? 512 : 0;
    unsigned zero=0, format=2, row_bytes=kh*dim*2, total=rows;
    std::mt19937 rng(19+dim+qr+offset);
    std::normal_distribution<float> normal(0,1);
    std::vector<float> q(size_t(qr)*heads*dim);
    for (auto& x:q) x=normal(rng)*(sharp ? std::sqrt(float(dim)) : 1.f);
    std::vector<unsigned> table(capacity/page);
    for(unsigned i=0;i<table.size();++i) table[i]=permuted ? unsigned(table.size()-1-i) : i;
    std::vector<half> k(size_t(capacity)*kh*dim),v(k.size());
    for(auto& x:k) x=__float2half(normal(rng));
    for(auto& x:v) x=__float2half(normal(rng));
    Device<float> dq(q), reference(q.size()), candidate(q.size());
    Device<half> dk(k),dv(v);
    Device<unsigned> dt(table);
    void* kp=dk.p; void* vp=dv.p; void* tp=permuted ? dt.p : nullptr; void* nil=nullptr;
    CUfunction fast,mma;
    ck(cuModuleGetFunction(&fast,module,"termite_gqa_attention_prefill_turboquant_fast_f32"));
    ck(cuModuleGetFunction(&mma,repeat_module ? repeat_module : module,repeat_module ? (dim==256 ? "antfly_gqa_repeat_prefill_exact_hd256_f32" : "antfly_gqa_repeat_prefill_exact_hd512_f32") : dim==256 ? "termite_gqa_attention_prefill_turboquant_mma_m32_f32" : "termite_gqa_attention_prefill_turboquant_mma_f32"));
    CUfunction scores_fn=nullptr, coefficients_fn=nullptr, values_fn=nullptr;
    const bool use_split=split && dim==512;
    if(use_split) {
        ck(cuModuleGetFunction(&scores_fn,repeat_module,"antfly_gqa_repeat_split_scores_f32"));
        ck(cuModuleGetFunction(&coefficients_fn,repeat_module,"antfly_gqa_repeat_split_coefficients_f32"));
        ck(cuModuleGetFunction(&values_fn,repeat_module,"antfly_gqa_repeat_split_values_f32"));
    }
    Device<float> alpha(use_split ? size_t(qr)*heads*4096 : 1);
    Device<float> beta(use_split ? size_t(qr)*heads*4096 : 1);
    Device<float> state(use_split ? size_t(qr)*heads*2 : 1);
    Device<float> trace(use_split && !memory_check ? size_t(rows)*4*5 : 1);
    std::vector<float> expected_trace;
    bool check_intermediates=use_split && !memory_check, intermediates_equal=true;
    if(check_intermediates) {
        CUfunction oracle;
        ck(cuModuleGetFunction(&oracle,repeat_module,"antfly_repeat_trace_oracle"));
        void* oracle_args[]={&trace.p,&dq.p,&kp,&tp,&qr,&offset,&rows,&capacity};
        ck(cuLaunchKernel(oracle,4,1,1,32,1,1,0,nullptr,oracle_args,nullptr));
        expected_trace=trace.read(size_t(rows)*4*5);
    }
    auto check_stage=[&](unsigned begin,unsigned count,bool coefficients) {
        if(!check_intermediates) return;
        for(unsigned test=0;test<4;++test) {
            const unsigned qi=test/2 ? qr-1 : 0, head=test%2 ? 4 : 0, row=qi*8+head;
            const unsigned end=std::min(begin+count,offset+qi+1);
            std::vector<float> a(count), b(count);
            ck(cudaMemcpy(a.data(),alpha.p+size_t(row)*count,count*sizeof(float),cudaMemcpyDeviceToHost));
            if(coefficients) ck(cudaMemcpy(b.data(),beta.p+size_t(row)*count,count*sizeof(float),cudaMemcpyDeviceToHost));
            for(unsigned key=begin;key<end;++key) {
                const float* expected=&expected_trace[(size_t(test)*rows+key)*5];
                intermediates_equal &= std::memcmp(&a[key-begin],&expected[coefficients ? 1 : 0],sizeof(float))==0;
                if(coefficients) intermediates_equal &= std::memcmp(&b[key-begin],&expected[2],sizeof(float))==0;
            }
            if(coefficients) {
                float recurrence[2];
                ck(cudaMemcpy(recurrence,state.p+row*2,sizeof(recurrence),cudaMemcpyDeviceToHost));
                const float* expected=&expected_trace[(size_t(test)*rows+end-1)*5+3];
                intermediates_equal &= std::memcmp(recurrence,expected,sizeof(recurrence))==0;
            }
        }
    };
    unsigned smem=repeat_module ? 0 : (dim==256 ? 320*dim+14848 : 160*dim+7424);
    if(smem) ck(cuFuncSetAttribute(mma,CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,int(smem)));
    auto launch=[&](bool optimized) {
        float* out=optimized ? candidate.p : reference.p;
        void* args[]={&out,&dq.p,&kp,&vp,&tp,&nil,&nil,&batch,&qr,&rows,&heads,&kh,&dim,
            &offset,&zero,&window,&total,&zero,&zero,&row_bytes,&row_bytes,&row_bytes,
            &blocks,&page,&format,&format,&capacity,&nil};
        if (optimized && use_split) {
            for(unsigned begin=0;begin<rows;begin+=4096) {
                unsigned count=std::min(4096u,rows-begin), last=begin+count==rows;
                void* score_args[]={&alpha.p,&dq.p,&kp,&tp,&qr,&offset,&begin,&count,&capacity};
                void* coefficient_args[]={&alpha.p,&beta.p,&state.p,&qr,&offset,&begin,&count};
                void* value_args[]={&out,&alpha.p,&beta.p,&state.p,&vp,&tp,&qr,&offset,&begin,&count,&capacity,&last};
                ck(cuLaunchKernel(scores_fn,((count+255)/256)*((qr+7)/8)*heads,1,1,256,1,1,0,nullptr,score_args,nullptr));
                check_stage(begin,count,false);
                ck(cuLaunchKernel(coefficients_fn,(qr*heads+31)/32,1,1,32,1,1,0,nullptr,coefficient_args,nullptr));
                check_stage(begin,count,true);
                ck(cuLaunchKernel(values_fn,((qr+7)/8)*heads,1,1,256,1,1,0,nullptr,value_args,nullptr));
            }
            return;
        }
        unsigned tile=repeat_module ? 8 : (dim==256 ? 32 : 16);
        ck(cuLaunchKernel(optimized ? mma : fast,optimized ? heads : qr*heads,
            optimized ? (qr+tile-1)/tile : 1,1,optimized ? 256 : dim,1,1,
            optimized ? smem : 0,nullptr,args,nullptr));
    };
    if (!memory_check) launch(false);
    launch(true); ck(cudaDeviceSynchronize());
    check_intermediates=false;
    const auto b=candidate.read(q.size());
    const auto a=memory_check ? b : reference.read(q.size());
    double max_abs=0,max_row_relative=0;
    bool finite=true;
    for(unsigned row=0;row<qr*heads;++row) {
        double err=0,norm=0;
        for(unsigned d=0;d<dim;++d) {
            size_t i=size_t(row)*dim+d;
            finite=finite && std::isfinite(a[i]) && std::isfinite(b[i]);
            double delta=double(b[i])-a[i]; err+=delta*delta; norm+=double(a[i])*a[i];
            max_abs=std::max(max_abs,std::abs(delta));
        }
        max_row_relative=std::max(max_row_relative,std::sqrt(err/std::max(norm,1e-30)));
    }
    // Independently check both KV groups and the first/last query against FP64.
    double oracle_abs=0;
    for(unsigned qi : {0u,qr-1}) for(unsigned head : {0u,4u}) {
        unsigned end=offset+qi+1,begin=window && end>window ? end-window : 0;
        std::vector<double> scores(end-begin), expected(dim,0);
        double maximum=-INFINITY;
        for(unsigned t=begin;t<end;++t) {
            unsigned physical=table[t/page]*page+t%page;
            double dot=0;
            for(unsigned d=0;d<dim;++d) dot+=double(q[(size_t(qi)*heads+head)*dim+d])*__half2float(k[(size_t(physical)*kh+head/4)*dim+d]);
            scores[t-begin]=dot/std::sqrt(double(dim)); maximum=std::max(maximum,scores[t-begin]);
        }
        double denom=0;
        for(auto& x:scores) { x=std::exp(x-maximum); denom+=x; }
        for(unsigned t=begin;t<end;++t) {
            unsigned physical=table[t/page]*page+t%page;
            for(unsigned d=0;d<dim;++d) expected[d]+=scores[t-begin]/denom*__half2float(v[(size_t(physical)*kh+head/4)*dim+d]);
        }
        for(unsigned d=0;d<dim;++d) oracle_abs=std::max(oracle_abs,std::abs(expected[d]-a[(size_t(qi)*heads+head)*dim+d]));
    }
    cudaEvent_t start,stop; ck(cudaEventCreate(&start)); ck(cudaEventCreate(&stop));
    auto timing=[&](bool optimized) { ck(cudaEventRecord(start)); launch(optimized); ck(cudaEventRecord(stop)); ck(cudaEventSynchronize(stop)); float ms; ck(cudaEventElapsedTime(&ms,start,stop)); return ms; };
    float fast_ms=memory_check ? 0.f : timing(false),mma_ms=timing(true);
    ck(cudaEventDestroy(start)); ck(cudaEventDestroy(stop));
    const bool bitwise_equal = !memory_check && std::memcmp(a.data(),b.data(),a.size()*sizeof(float))==0;
    bool pass=intermediates_equal && (!repeat_module || memory_check || bitwise_equal) && finite && oracle_abs<1e-4 && max_abs<(repeat_module ? 0.005 : 0.025) && max_row_relative<(repeat_module ? 0.001 : 0.01);
    std::printf("{\"dim\":%u,\"queries\":%u,\"offset\":%u,\"permuted\":%s,\"sharp\":%s,\"oracle_abs\":%.9g,\"max_abs\":%.9g,\"max_row_relative\":%.9g,\"fast_ms\":%.6g,\"candidate_ms\":%.6g,\"intermediate_spots_equal\":%s,\"full_reference_compared\":%s,\"bitwise_equal\":%s,\"passed\":%s}\n",dim,qr,offset,permuted?"true":"false",sharp?"true":"false",oracle_abs,max_abs,max_row_relative,fast_ms,mma_ms,use_split && !memory_check ? (intermediates_equal?"true":"false") : "null",memory_check?"false":"true",memory_check?"null":(bitwise_equal?"true":"false"),pass?"true":"false");
    std::fflush(stdout);
    return pass;
}
int main(int argc,char** argv) {
    bool memory_check=false, boundary=false, split=false, quick=false;
    if(argc<2 || argc>7) { std::fprintf(stderr,"usage: qualify-repeat-attention SERVING_CUBIN [REPEAT_CUBIN [--memcheck] [--boundary] [--split] [--quick]]\n"); return 2; }
    for(int i=3;i<argc;++i) {
        if(std::strcmp(argv[i],"--memcheck")==0) memory_check=true;
        else if(std::strcmp(argv[i],"--boundary")==0) boundary=true;
        else if(std::strcmp(argv[i],"--split")==0) split=true;
        else if(std::strcmp(argv[i],"--quick")==0) quick=true;
        else return 2;
    }
    ck(cudaFree(nullptr)); CUmodule module; ck(cuModuleLoad(&module,argv[1]));
    CUmodule repeat_module=nullptr; if(argc>=3) ck(cuModuleLoad(&repeat_module,argv[2]));
    if (memory_check) std::fprintf(stderr, "memcheck mode: candidate-only launches; FP64 spot oracle; no full GPU-reference comparison\n");
    const std::vector<std::pair<unsigned,unsigned>> cases = boundary
        ? (memory_check ? std::vector<std::pair<unsigned,unsigned>>{{2,0},{512,512},{2,65534}}
                        : std::vector<std::pair<unsigned,unsigned>>{{2,0},{512,65024}})
        : std::vector<std::pair<unsigned,unsigned>>{{3,0},{17,509},{17,4088},{17,8184},{253,7936},{256,8192},{256,32512}};
    bool pass=true;
    for(unsigned dim : {256u,512u}) for(bool permuted : {false,true}) for(bool sharp : {false,true})
        for(auto test : cases)
            if (!quick || (dim==512 && test.first==256 && test.second==8192 && !permuted && !sharp))
                pass=run(module,repeat_module,dim,test.first,test.second,permuted,sharp,memory_check,split) && pass;
    if(repeat_module) ck(cuModuleUnload(repeat_module));
    ck(cuModuleUnload(module)); return pass ? 0 : 1;
}
