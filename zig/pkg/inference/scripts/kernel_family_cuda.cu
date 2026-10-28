// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include <cuda.h>
#include <cuda_runtime.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>
#define RT(call) do { auto e=(call); if(e!=cudaSuccess) { fprintf(stderr,"%s: %s\n",#call,cudaGetErrorString(e));exit(2); } } while(0)
#define DR(call) do { auto e=(call); if(e!=CUDA_SUCCESS) { const char *s;cuGetErrorString(e,&s);fprintf(stderr,"%s: %s\n",#call,s);exit(2); } } while(0)
static void require(bool ok,const char *text) { if(!ok) {fprintf(stderr,"%s\n",text);exit(2);} }
template<typename T> static T read(FILE *file) { T value;require(fread(&value,sizeof(T),1,file)==1,"Truncated manifest");return value; }
static std::string string(FILE *file) {
    unsigned length=read<unsigned>(file);require(length<=256,"Oversized name");std::string value(length,' ');
    require(fread(&value[0],1,length,file)==length,"Truncated name");return value;
}
struct Argument { unsigned role; size_t written; double absolute=0, relative=0; std::vector<unsigned char> initial; CUdeviceptr device[2]={0,0}; };
// Oracle precision does not relax A/B equality. F32 poison is nonfinite for
// finite expectations, and finite for nonfinite expectations, to catch omissions.
static void poison(const Argument& arg, std::vector<unsigned char>& bytes) {
    if(arg.role==3)for(size_t i=0;i<arg.written;i++)bytes[i]^=0xff;
    if(arg.role==4)for(size_t i=0;i<arg.written;i+=4) {
        float want;memcpy(&want,arg.initial.data()+i,4);
        uint32_t value=std::isfinite(want)?0x7fc00000u:0u;
        memcpy(bytes.data()+i,&value,4);
    }
}
static bool reference_matches(const Argument& arg, const std::vector<unsigned char>& bytes) {
    if(arg.role==3)return memcmp(bytes.data(),arg.initial.data(),arg.written)==0;
    if(arg.role!=4)return true;
    for(size_t i=0;i<arg.written;i+=4) {
        float want,got;memcpy(&want,arg.initial.data()+i,4);memcpy(&got,bytes.data()+i,4);
        bool ok=std::isfinite(want)?std::isfinite(got)&&std::abs(double(got)-want)<=arg.absolute+arg.relative*std::abs(double(want))
            :std::isnan(want)?std::isnan(got):got==want;
        if(!ok){fprintf(stderr,"Reference element %zu: got %.9g expected %.9g\n",i/4,got,want);return false;}
    }
    return true;
}
int main(int argc,char **argv) {
    require(argc==5 || (argc==6 && (std::string(argv[5])=="--long" || std::string(argv[5])=="--stable" || std::string(argv[5])=="--inplace-stable")),"Usage: kernel-family-cuda baseline.fatbin candidate.fatbin cases.bin receipt.json [--long|--stable|--inplace-stable]");
    const bool inplace=argc==6 && std::string(argv[5])=="--inplace-stable";
    const bool extended=argc==6, stable=extended && (std::string(argv[5])=="--stable" || inplace);
    const int launches=stable?200:(extended?40:10);
    RT(cudaSetDevice(0));RT(cudaFree(nullptr));CUmodule modules[2];DR(cuModuleLoad(&modules[0],argv[1]));DR(cuModuleLoad(&modules[1],argv[2]));
    FILE *manifest=fopen(argv[3],"rb");require(manifest!=nullptr,"Cannot read manifest");
    unsigned magic=read<unsigned>(manifest);
    require(magic==0x4b464331u || magic==0x4b464332u || magic==0x4b464333u,"Wrong manifest version");unsigned count=read<unsigned>(manifest);
    cudaStream_t stream;RT(cudaStreamCreate(&stream));std::vector<std::string> receipts;bool passed=true;
    for(unsigned test=0;test<count;test++) {
        std::string name=string(manifest),kernel=string(manifest);unsigned grid[3],block[3];
        for(auto &v:grid)v=read<unsigned>(manifest);for(auto &v:block)v=read<unsigned>(manifest);
        unsigned shared=read<unsigned>(manifest),argc=read<unsigned>(manifest);require(argc<=64,"Too many arguments");
        std::vector<Argument> arguments(argc);std::vector<void*> params[2];
        for(auto &arg:arguments) {
            arg.role=read<unsigned>(manifest);arg.written=read<unsigned long long>(manifest);
            require(arg.role<= (magic==0x4b464333u?4u:magic==0x4b464332u?3u:2u),"Invalid argument role");
            size_t size=read<unsigned long long>(manifest);require(size>0&&size<(1ull<<32),"Invalid buffer size");arg.initial.resize(size);
            if(magic==0x4b464333u){arg.absolute=read<double>(manifest);arg.relative=read<double>(manifest);}
            require(arg.role!=4 || (arg.written%4==0 && std::isfinite(arg.absolute) && arg.absolute>=0 && std::isfinite(arg.relative) && arg.relative>=0),"Invalid F32 reference precision");
            require(fread(arg.initial.data(),1,size,manifest)==size,"Truncated fixture");require(arg.written<=size,"Invalid output extent");
            std::vector<unsigned char> initial=arg.initial;
            // Poison every reference byte so an omitted write cannot pass.
            poison(arg,initial);
            if(arg.role) {DR(cuMemAlloc(&arg.device[0],size));DR(cuMemcpyHtoD(arg.device[0],initial.data(),size));arg.device[1]=arg.device[0];
                if(arg.role>=2) {DR(cuMemAlloc(&arg.device[1],size));DR(cuMemcpyHtoD(arg.device[1],initial.data(),size));}}
        }
        for(auto &arg:arguments)for(int v=0;v<2;v++)params[v].push_back(arg.role?(void*)&arg.device[v]:(void*)arg.initial.data());
        CUfunction functions[2];cudaGraph_t graphs[2];cudaGraphExec_t execs[2];
        int active_blocks[2], registers[2];
        for(int version=0;version<2;version++) {
            DR(cuModuleGetFunction(&functions[version],modules[version],kernel.c_str()));
            if(shared>48u*1024u) DR(cuFuncSetAttribute(functions[version],CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES,(int)shared));
            DR(cuFuncGetAttribute(&registers[version],CU_FUNC_ATTRIBUTE_NUM_REGS,functions[version]));
            DR(cuOccupancyMaxActiveBlocksPerMultiprocessor(&active_blocks[version],functions[version],block[0]*block[1]*block[2],shared));
            DR(cuLaunchKernel(functions[version],grid[0],grid[1],grid[2],block[0],block[1],block[2],shared,(CUstream)stream,params[version].data(),nullptr));
        }
        auto verify_outputs = [&]() {
        RT(cudaStreamSynchronize(stream));unsigned outputs=0;
        for(auto &arg:arguments)if(arg.role>=2) {
            ++outputs;std::vector<unsigned char> bytes[2];
            for(int v=0;v<2;v++) {bytes[v].resize(arg.initial.size());DR(cuMemcpyDtoH(bytes[v].data(),arg.device[v],bytes[v].size()));
                require(memcmp(bytes[v].data()+arg.written,arg.initial.data()+arg.written,arg.initial.size()-arg.written)==0,"Output guard changed");
                require(reference_matches(arg,bytes[v]),"Independent reference mismatch");}
            if(bytes[0]!=bytes[1]) {fprintf(stderr,"Output mismatch %s\n",name.c_str());exit(3);}
        }
        require(outputs>0,"No output bindings");
        };
        verify_outputs();
        // Fixed addresses for both versions during captured graph timing.
        for(int version=0;version<2;version++) {
            RT(cudaStreamBeginCapture(stream,cudaStreamCaptureModeThreadLocal));
            for(int node=0;node<100;node++)DR(cuLaunchKernel(functions[version],grid[0],grid[1],grid[2],block[0],block[1],block[2],shared,(CUstream)stream,params[0].data(),nullptr));
            RT(cudaStreamEndCapture(stream,&graphs[version]));RT(cudaGraphInstantiate(&execs[version],graphs[version],nullptr,nullptr,0));
        }
        // In-place fixtures (role 2) must start every 100-node graph from
        // their declared input, avoiding drift to zero/nonfinite values. Copies
        // precede each graph's start event and are excluded from kernel timing.
        auto reset_inplace = [&]() {
            if(inplace)for(auto &arg:arguments)if(arg.role==2)
                RT(cudaMemcpyAsync((void*)arg.device[0],arg.initial.data(),arg.initial.size(),cudaMemcpyHostToDevice,stream));
        };
        for(int i=0;i<(extended?80:40);i++){reset_inplace();RT(cudaGraphLaunch(execs[i%2],stream));RT(cudaStreamSynchronize(stream));}
        cudaEvent_t begin,end;RT(cudaEventCreate(&begin));RT(cudaEventCreate(&end));
        float warmup_ms=0.0f;
        if(stable) {
            // Tiny kernels otherwise finish their fixed-count warmup before
            // the device's clock governor reaches a steady operating point.
            auto started=std::chrono::steady_clock::now();int version=0;
            RT(cudaEventRecord(begin,stream));
            do {
                reset_inplace();RT(cudaGraphLaunch(execs[version],stream));RT(cudaStreamSynchronize(stream));
                version^=1;
            } while(std::chrono::steady_clock::now()-started<std::chrono::milliseconds(500));
            RT(cudaEventRecord(end,stream));RT(cudaEventSynchronize(end));
            RT(cudaEventElapsedTime(&warmup_ms,begin,end));
        }
        std::vector<double> times[2];std::string arms="[";
        std::string order="ABBABAABABBAABBA"; if(extended)order+=order;
        std::vector<cudaEvent_t> starts(inplace?launches:0), stops(inplace?launches:0);
        for(size_t i=0;i<starts.size();++i){RT(cudaEventCreate(&starts[i]));RT(cudaEventCreate(&stops[i]));}
        for(char arm:order) {
            int version=arm=='B';float ms=0.0f;
            if(inplace) {
                for(int repeat=0;repeat<launches;repeat++) {
                    reset_inplace();RT(cudaEventRecord(starts[repeat],stream));
                    RT(cudaGraphLaunch(execs[version],stream));RT(cudaEventRecord(stops[repeat],stream));
                }
                RT(cudaEventSynchronize(stops.back()));
                for(int repeat=0;repeat<launches;repeat++){float elapsed;RT(cudaEventElapsedTime(&elapsed,starts[repeat],stops[repeat]));ms+=elapsed;}
            } else {
                RT(cudaEventRecord(begin,stream));for(int repeat=0;repeat<launches;repeat++)RT(cudaGraphLaunch(execs[version],stream));
                RT(cudaEventRecord(end,stream));RT(cudaEventSynchronize(end));RT(cudaEventElapsedTime(&ms,begin,end));
            }
            double ns=ms*1e6/(100*launches);times[version].push_back(ns);
            char row[120];snprintf(row,sizeof(row),"%s{\"arm\":%d,\"ns\":%.9f}",arms.size()>1?",":"",version,ns);arms+=row;
        }
        // Verify each captured version separately against the candidate's
        // correctness buffers. Reset/poison outputs so a missing replay write
        // cannot be hidden by the other version's previous result. Persistent
        // counters start from their declared fixture state, then must re-arm
        // correctly throughout the graph's 100 consecutive kernel launches.
        std::vector<std::vector<unsigned char>> replay;
        for(int version=0;version<2;version++) {
            for(auto &arg:arguments)if(arg.role>=2) {
                std::vector<unsigned char> initial=arg.initial;
                poison(arg,initial);
                DR(cuMemcpyHtoD(arg.device[0],initial.data(),initial.size()));
            }
            RT(cudaGraphLaunch(execs[version],stream));
            if(inplace) {
                // Both captured graphs use the same physical addresses. Compare
                // independently reset graph replays, rather than comparing a
                // hundred in-place updates with the single-launch oracle buffer.
                RT(cudaStreamSynchronize(stream));size_t slot=0;
                for(auto &arg:arguments)if(arg.role>=2) {
                    std::vector<unsigned char> bytes(arg.initial.size());DR(cuMemcpyDtoH(bytes.data(),arg.device[0],bytes.size()));
                    require(memcmp(bytes.data()+arg.written,arg.initial.data()+arg.written,arg.initial.size()-arg.written)==0,"Replay guard changed");
                    require(reference_matches(arg,bytes),"Replay reference mismatch");
                    if(version==0)replay.push_back(bytes);
                    else require(replay[slot]==bytes,"In-place replay mismatch");
                    ++slot;
                }
            } else verify_outputs();
        }
        arms+="]";double median[2];for(int v=0;v<2;v++){auto &t=times[v];std::sort(t.begin(),t.end());median[v]=(t[t.size()/2-1]+t[t.size()/2])/2;}
        double ratio=median[1]/median[0];bool ok=ratio<=1.05;passed&=ok;char row[1000];
        snprintf(row,sizeof(row),"{\"name\":\"%s\",\"kernel\":\"%s\",\"exact\":true,\"replay_exact\":true,\"guards\":true,\"baseline_ns\":%.9f,\"candidate_ns\":%.9f,\"ratio\":%.9f,\"pass\":%s,\"arms\":",name.c_str(),kernel.c_str(),median[0],median[1],ratio,ok?"true":"false");
        char protocol[384];snprintf(protocol,sizeof(protocol),",\"launches_per_arm\":%d,\"additional_warmup_gpu_ms\":%.6f,\"baseline_registers\":%d,\"candidate_registers\":%d,\"baseline_active_blocks_per_sm\":%d,\"candidate_active_blocks_per_sm\":%d}",100*launches,warmup_ms,registers[0],registers[1],active_blocks[0],active_blocks[1]);
        receipts.push_back(std::string(row)+arms+protocol);FILE *output=fopen(argv[4],"wb");require(output!=nullptr,"Cannot save receipt");fputs("[\n",output);
        for(size_t i=0;i<receipts.size();i++)fprintf(output,"%s%s\n",receipts[i].c_str(),i+1<receipts.size()?",":"");fputs("]\n",output);fclose(output);
        printf("%s exact=1 guards=1 ratio=%.6f pass=%d\n",name.c_str(),ratio,ok);fflush(stdout);
        for(size_t i=0;i<starts.size();++i){RT(cudaEventDestroy(starts[i]));RT(cudaEventDestroy(stops[i]));}
        RT(cudaEventDestroy(begin));RT(cudaEventDestroy(end));for(int v=0;v<2;v++){RT(cudaGraphExecDestroy(execs[v]));RT(cudaGraphDestroy(graphs[v]));}
        for(auto &arg:arguments)if(arg.role){DR(cuMemFree(arg.device[0]));if(arg.role>=2)DR(cuMemFree(arg.device[1]));}
    }
    fclose(manifest);RT(cudaStreamDestroy(stream));for(auto m:modules)DR(cuModuleUnload(m));
    printf("KERNEL_FAMILY_CUDA %s %u cases\n",passed?"PASSED":"FAILED",count);return passed?0:1;
}
