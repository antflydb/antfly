// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include <cuda.h>
#include <cuda_runtime.h>
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
struct Argument { unsigned role; size_t written; std::vector<unsigned char> initial; CUdeviceptr device[2]={0,0}; };
int main(int argc,char **argv) {
    require(argc==5 || (argc==6 && std::string(argv[5])=="--long"),"Usage: kernel-family-cuda baseline.fatbin candidate.fatbin cases.bin receipt.json [--long]");
    const bool extended=argc==6; const int launches=extended?40:10;
    RT(cudaSetDevice(0));RT(cudaFree(nullptr));CUmodule modules[2];DR(cuModuleLoad(&modules[0],argv[1]));DR(cuModuleLoad(&modules[1],argv[2]));
    FILE *manifest=fopen(argv[3],"rb");require(manifest!=nullptr,"Cannot read manifest");
    unsigned magic=read<unsigned>(manifest);
    require(magic==0x4b464331u || magic==0x4b464332u,"Wrong manifest version");unsigned count=read<unsigned>(manifest);
    cudaStream_t stream;RT(cudaStreamCreate(&stream));std::vector<std::string> receipts;bool passed=true;
    for(unsigned test=0;test<count;test++) {
        std::string name=string(manifest),kernel=string(manifest);unsigned grid[3],block[3];
        for(auto &v:grid)v=read<unsigned>(manifest);for(auto &v:block)v=read<unsigned>(manifest);
        unsigned shared=read<unsigned>(manifest),argc=read<unsigned>(manifest);require(argc<=64,"Too many arguments");
        std::vector<Argument> arguments(argc);std::vector<void*> params[2];
        for(auto &arg:arguments) {
            arg.role=read<unsigned>(manifest);arg.written=read<unsigned long long>(manifest);
            require(arg.role<= (magic==0x4b464332u?3u:2u),"Invalid argument role");
            size_t size=read<unsigned long long>(manifest);require(size>0&&size<(1ull<<32),"Invalid buffer size");arg.initial.resize(size);
            require(fread(arg.initial.data(),1,size,manifest)==size,"Truncated fixture");require(arg.written<=size,"Invalid output extent");
            std::vector<unsigned char> initial=arg.initial;
            // Poison every reference byte so an omitted write cannot pass.
            if(arg.role==3)for(size_t i=0;i<arg.written;i++)initial[i]^=0xff;
            if(arg.role) {DR(cuMemAlloc(&arg.device[0],size));DR(cuMemcpyHtoD(arg.device[0],initial.data(),size));arg.device[1]=arg.device[0];
                if(arg.role>=2) {DR(cuMemAlloc(&arg.device[1],size));DR(cuMemcpyHtoD(arg.device[1],initial.data(),size));}}
        }
        for(auto &arg:arguments)for(int v=0;v<2;v++)params[v].push_back(arg.role?(void*)&arg.device[v]:(void*)arg.initial.data());
        CUfunction functions[2];cudaGraph_t graphs[2];cudaGraphExec_t execs[2];
        for(int version=0;version<2;version++) {
            DR(cuModuleGetFunction(&functions[version],modules[version],kernel.c_str()));
            DR(cuLaunchKernel(functions[version],grid[0],grid[1],grid[2],block[0],block[1],block[2],shared,(CUstream)stream,params[version].data(),nullptr));
        }
        auto verify_outputs = [&]() {
        RT(cudaStreamSynchronize(stream));unsigned outputs=0;
        for(auto &arg:arguments)if(arg.role>=2) {
            ++outputs;std::vector<unsigned char> bytes[2];
            for(int v=0;v<2;v++) {bytes[v].resize(arg.initial.size());DR(cuMemcpyDtoH(bytes[v].data(),arg.device[v],bytes[v].size()));
                require(memcmp(bytes[v].data()+arg.written,arg.initial.data()+arg.written,arg.initial.size()-arg.written)==0,"Output guard changed");
                if(arg.role==3)require(memcmp(bytes[v].data(),arg.initial.data(),arg.written)==0,"Independent reference mismatch");}
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
        for(int i=0;i<(extended?80:40);i++){RT(cudaGraphLaunch(execs[i%2],stream));RT(cudaStreamSynchronize(stream));}
        cudaEvent_t begin,end;RT(cudaEventCreate(&begin));RT(cudaEventCreate(&end));std::vector<double> times[2];std::string arms="[";
        std::string order="ABBABAABABBAABBA"; if(extended)order+=order;
        for(char arm:order) {
            int version=arm=='B';RT(cudaEventRecord(begin,stream));for(int repeat=0;repeat<launches;repeat++)RT(cudaGraphLaunch(execs[version],stream));
            RT(cudaEventRecord(end,stream));RT(cudaEventSynchronize(end));float ms;RT(cudaEventElapsedTime(&ms,begin,end));double ns=ms*1e6/(100*launches);times[version].push_back(ns);
            char row[120];snprintf(row,sizeof(row),"%s{\"arm\":%d,\"ns\":%.9f}",arms.size()>1?",":"",version,ns);arms+=row;
        }
        // Verify each captured version separately against the candidate's
        // correctness buffers. Reset/poison outputs so a missing replay write
        // cannot be hidden by the other version's previous result. Persistent
        // counters start from their declared fixture state, then must re-arm
        // correctly throughout the graph's 100 consecutive kernel launches.
        for(int version=0;version<2;version++) {
            for(auto &arg:arguments)if(arg.role>=2) {
                std::vector<unsigned char> initial=arg.initial;
                if(arg.role==3)for(size_t i=0;i<arg.written;i++)initial[i]^=0xff;
                DR(cuMemcpyHtoD(arg.device[0],initial.data(),initial.size()));
            }
            RT(cudaGraphLaunch(execs[version],stream));
            verify_outputs();
        }
        arms+="]";double median[2];for(int v=0;v<2;v++){auto &t=times[v];std::sort(t.begin(),t.end());median[v]=(t[t.size()/2-1]+t[t.size()/2])/2;}
        double ratio=median[1]/median[0];bool ok=ratio<=1.05;passed&=ok;char row[1000];
        snprintf(row,sizeof(row),"{\"name\":\"%s\",\"kernel\":\"%s\",\"exact\":true,\"replay_exact\":true,\"guards\":true,\"baseline_ns\":%.9f,\"candidate_ns\":%.9f,\"ratio\":%.9f,\"pass\":%s,\"arms\":",name.c_str(),kernel.c_str(),median[0],median[1],ratio,ok?"true":"false");
        receipts.push_back(std::string(row)+arms+"}");FILE *output=fopen(argv[4],"wb");require(output!=nullptr,"Cannot save receipt");fputs("[\n",output);
        for(size_t i=0;i<receipts.size();i++)fprintf(output,"%s%s\n",receipts[i].c_str(),i+1<receipts.size()?",":"");fputs("]\n",output);fclose(output);
        printf("%s exact=1 guards=1 ratio=%.6f pass=%d\n",name.c_str(),ratio,ok);fflush(stdout);
        RT(cudaEventDestroy(begin));RT(cudaEventDestroy(end));for(int v=0;v<2;v++){RT(cudaGraphExecDestroy(execs[v]));RT(cudaGraphDestroy(graphs[v]));}
        for(auto &arg:arguments)if(arg.role){DR(cuMemFree(arg.device[0]));if(arg.role>=2)DR(cuMemFree(arg.device[1]));}
    }
    fclose(manifest);RT(cudaStreamDestroy(stream));for(auto m:modules)DR(cuModuleUnload(m));
    printf("KERNEL_FAMILY_CUDA %s %u cases\n",passed?"PASSED":"FAILED",count);return passed?0:1;
}
