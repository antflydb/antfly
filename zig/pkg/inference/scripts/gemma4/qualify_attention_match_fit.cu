// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Qualification-only CUDA numerical backend for native attention matching.
// Not linked into serving. Owns its process-local CUDA/library resources.
// FP64 cuBLAS/cuSOLVER provide matrix primitives; key selection, constrained
// mass fitting, QR updates/downdates, acceptance and ownership are our code.
// Build: nvcc -O3 -std=c++17 -arch=sm_89 --shared -Xcompiler -fPIC \
//   qualify_attention_match_fit.cu -lcublas -lcusolver -o libattention_match_fit.so
// Inputs/outputs are host arrays; am_select returns selection-order indices and
// nonnegative weights. Callers must still validate storage rounding and every
// complete head on held-out queries. This file does not install or export KV.
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <cusolverDn.h>
#include <vector>
#include <cmath>
#include <cstdio>
#include <algorithm>
#define CHECK(x) do { int e=int(x); if(e) {fprintf(stderr,"accelerator error %d at %d\n",e,__LINE__); return e+1000;} }while(0)
struct Memory { void* p=nullptr; ~Memory(){if(p)cudaFree(p);} int allocate(size_t bytes){return int(cudaMalloc(&p,bytes));} double* data(){return static_cast<double*>(p);} };
struct Handles { cublasHandle_t blas=nullptr; cusolverDnHandle_t solver=nullptr; int error=0; Handles(){error=int(cublasCreate(&blas));if(!error)error=int(cusolverDnCreate(&solver));if(!error)error=int(cublasSetMathMode(blas,CUBLAS_PEDANTIC_MATH));} ~Handles(){if(solver)cusolverDnDestroy(solver);if(blas)cublasDestroy(blas);} };
static thread_local Handles handles;
__global__ void normalize(double* p,const double* bias,int n,double cap,double* masses){
 int row=blockIdx.x,lane=threadIdx.x; double* v=p+size_t(row)*n;
 __shared__ double temp[128]; double maximum=-INFINITY;
 for(int j=lane;j<n;j+=blockDim.x){double z=v[j];if(cap>0)z=cap*tanh(z/cap);if(bias)z+=bias[j];v[j]=z;maximum=fmax(maximum,z);}
 temp[lane]=maximum;__syncthreads();
 for(int k=64;k;k/=2){if(lane<k)temp[lane]=fmax(temp[lane],temp[lane+k]);__syncthreads();}
 // All warps must load the maximum before shared storage is reused for sums.
 maximum=temp[0];__syncthreads();double sum=0;
 for(int j=lane;j<n;j+=blockDim.x){v[j]=exp(v[j]-maximum);sum+=v[j];}
 temp[lane]=sum;__syncthreads();
 for(int k=64;k;k/=2){if(lane<k)temp[lane]+=temp[lane+k];__syncthreads();}
 sum=temp[0];if(lane==0)masses[row]=maximum+log(sum);
 for(int j=lane;j<n;j+=blockDim.x)v[j]/=sum;
}
extern "C" int am_attend(size_t n,size_t q,size_t d,double scale,double cap,const double* keys,const double* values,const double* bias,const double* queries,double* probabilities,double* targets,double* masses){
 if(!n||n>32768||!q||q>8192||!d||d>1024||!keys||!queries||!probabilities)return 1;
 CHECK(handles.error); Memory k,query,p,v,b,y,mass;
 CHECK(k.allocate(n*d*8)); CHECK(query.allocate(q*d*8)); CHECK(p.allocate(n*q*8)); CHECK(mass.allocate(q*8));
 CHECK(cudaMemcpy(k.p,keys,n*d*8,cudaMemcpyHostToDevice)); CHECK(cudaMemcpy(query.p,queries,q*d*8,cudaMemcpyHostToDevice));
 if(bias){CHECK(b.allocate(n*8));CHECK(cudaMemcpy(b.p,bias,n*8,cudaMemcpyHostToDevice));}
 double zero=0,one=1;
 CHECK(cublasDgemm(handles.blas,CUBLAS_OP_T,CUBLAS_OP_N,n,q,d,&scale,k.data(),d,query.data(),d,&zero,p.data(),n));
 normalize<<<q,128>>>(p.data(),b.data(),n,cap,mass.data());CHECK(cudaGetLastError());
 if(targets){if(!values)return 1;CHECK(v.allocate(n*d*8));CHECK(y.allocate(q*d*8));CHECK(cudaMemcpy(v.p,values,n*d*8,cudaMemcpyHostToDevice));
 CHECK(cublasDgemm(handles.blas,CUBLAS_OP_N,CUBLAS_OP_N,d,q,n,&one,v.data(),d,p.data(),n,&zero,y.data(),d));
 CHECK(cudaMemcpy(targets,y.p,q*d*8,cudaMemcpyDeviceToHost));}
 CHECK(cudaMemcpy(probabilities,p.p,n*q*8,cudaMemcpyDeviceToHost));if(masses)CHECK(cudaMemcpy(masses,mass.p,q*8,cudaMemcpyDeviceToHost));
 return 0;
}
extern "C" int am_qr(size_t q,size_t m,size_t d,double ridge,const double* a,const double* y,double* out){
 if(!q||q>8192||!m||m>2048||!d||d>1024||!a||!y||!out||!std::isfinite(ridge)||ridge<0)return 1;
 CHECK(handles.error);const int rows=q+m; std::vector<double> ha(size_t(rows)*m,0),hy(size_t(rows)*d,0);
 for(size_t j=0;j<m;j++){for(size_t i=0;i<q;i++)ha[j*rows+i]=a[i*m+j];ha[j*rows+q+j]=std::sqrt(ridge);}
 for(size_t j=0;j<d;j++)for(size_t i=0;i<q;i++)hy[j*rows+i]=y[i*d+j];
 Memory da,dy,tau,info,workspace;CHECK(da.allocate(ha.size()*8));CHECK(dy.allocate(hy.size()*8));CHECK(tau.allocate(m*8));CHECK(info.allocate(sizeof(int)));
 CHECK(cudaMemcpy(da.p,ha.data(),ha.size()*8,cudaMemcpyHostToDevice));CHECK(cudaMemcpy(dy.p,hy.data(),hy.size()*8,cudaMemcpyHostToDevice));
 int lwork1=0,lwork2=0;
 CHECK(cusolverDnDgeqrf_bufferSize(handles.solver,rows,m,da.data(),rows,&lwork1));
 CHECK(cusolverDnDormqr_bufferSize(handles.solver,CUBLAS_SIDE_LEFT,CUBLAS_OP_T,rows,d,m,da.data(),rows,tau.data(),dy.data(),rows,&lwork2));
 const int lwork=std::max(lwork1,lwork2);CHECK(workspace.allocate(size_t(lwork)*8));
 CHECK(cusolverDnDgeqrf(handles.solver,rows,m,da.data(),rows,tau.data(),workspace.data(),lwork,static_cast<int*>(info.p)));
 int result=0;CHECK(cudaMemcpy(&result,info.p,sizeof(int),cudaMemcpyDeviceToHost));if(result)return 2;
 CHECK(cusolverDnDormqr(handles.solver,CUBLAS_SIDE_LEFT,CUBLAS_OP_T,rows,d,m,da.data(),rows,tau.data(),dy.data(),rows,workspace.data(),lwork,static_cast<int*>(info.p)));
 CHECK(cudaMemcpy(&result,info.p,sizeof(int),cudaMemcpyDeviceToHost));if(result)return 3;
 const double one=1;
 CHECK(cublasDtrsm(handles.blas,CUBLAS_SIDE_LEFT,CUBLAS_FILL_MODE_UPPER,CUBLAS_OP_N,CUBLAS_DIAG_NON_UNIT,m,d,&one,da.data(),rows,dy.data(),rows));
 CHECK(cudaMemcpy(hy.data(),dy.p,hy.size()*8,cudaMemcpyDeviceToHost));
 for(size_t j=0;j<d;j++)for(size_t i=0;i<m;i++){double v=hy[j*rows+i];if(!std::isfinite(v))return 4;out[i*d+j]=v;}
 return 0;
}
// GPU arithmetic for native residual OMP. Constrained fallback remains the
// caller's native active-set solver; selection never clips a negative optimum.
__global__ void pick_column(const double* p,int n,int q,int index,double norm,double* basis,double* orth){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q){double x=p[size_t(i)*n+index];basis[i]=x;orth[i]=x/norm;}}
__global__ void pack_basis(const double* b,double* packed,int q,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q*count)packed[i]=b[size_t(i%count)*q+i/count];}
using NnlsCallback=int(*)(void*,const double*,size_t,size_t,double*,double*);
using CheckCallback=int(*)(void*);
__global__ void pack_active(const double* basis,const int* active,const double* norms,double* orth,int q,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q*count){int column=i/q,src=active[column];orth[i]=basis[size_t(src)*q+i%q]/norms[src];}}
__global__ void save_upper(const double* qr,double* r,int q,int m,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count*count){int row=i%count,col=i/count;r[size_t(col)*m+row]=row<=col?qr[size_t(col)*q+row]:0;}}
static int rebuild(const double* basis,double* orth,double* r,double* projected,const double* ones,int q,int m,const std::vector<int>& active,const std::vector<double>& norms){
 const int count=active.size();if(!count||count>q)return 1;
 Memory ix,dnorm,tau,info,work;CHECK(ix.allocate(count*sizeof(int)));CHECK(dnorm.allocate(m*8));CHECK(tau.allocate(count*8));CHECK(info.allocate(sizeof(int)));
 CHECK(cudaMemcpy(ix.p,active.data(),count*sizeof(int),cudaMemcpyHostToDevice));CHECK(cudaMemcpy(dnorm.p,norms.data(),m*8,cudaMemcpyHostToDevice));
 pack_active<<<(q*count+127)/128,128>>>(basis,static_cast<int*>(ix.p),dnorm.data(),orth,q,count);CHECK(cudaGetLastError());
 int w1=0,w2=0;CHECK(cusolverDnDgeqrf_bufferSize(handles.solver,q,count,orth,q,&w1));CHECK(cusolverDnDorgqr_bufferSize(handles.solver,q,count,count,orth,q,tau.data(),&w2));int nw=std::max(w1,w2);CHECK(work.allocate(size_t(nw)*8));
 CHECK(cusolverDnDgeqrf(handles.solver,q,count,orth,q,tau.data(),work.data(),nw,static_cast<int*>(info.p)));int result=0;CHECK(cudaMemcpy(&result,info.p,sizeof(int),cudaMemcpyDeviceToHost));if(result)return 2;
 save_upper<<<(count*count+127)/128,128>>>(orth,r,q,m,count);CHECK(cudaGetLastError());
 CHECK(cusolverDnDorgqr(handles.solver,q,count,count,orth,q,tau.data(),work.data(),nw,static_cast<int*>(info.p)));CHECK(cudaMemcpy(&result,info.p,sizeof(int),cudaMemcpyDeviceToHost));if(result)return 3;
 const double one=1,zero=0;CHECK(cublasDgemv(handles.blas,CUBLAS_OP_T,q,count,&one,orth,q,ones,1,&zero,projected,1));return 0;
}
__global__ void delete_r_column(const double* r,double* tmp,int m,int count,int removed){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count*(count-1)){int row=i%count,col=i/count;tmp[size_t(col)*m+row]=r[size_t(col+(col>=removed))*m+row];}}
// A single cooperating block applies the dependent Givens rotations. All rows
// of Q and the affected rows of R are complete before the next rotation reads.
__global__ void retriangularize(double* orth,double* r,double* projected,int q,int m,int count,int removed){
 __shared__ double c,s;
 for(int j=removed;j<count-1;j++){
  if(threadIdx.x==0){double a=r[size_t(j)*m+j],b=r[size_t(j)*m+j+1],norm=hypot(a,b);c=norm>0?a/norm:1;s=norm>0?b/norm:0;double x=projected[j],y=projected[j+1];projected[j]=c*x+s*y;projected[j+1]=-s*x+c*y;}
  __syncthreads();
  for(int col=j+threadIdx.x;col<count-1;col+=blockDim.x){double x=r[size_t(col)*m+j],y=r[size_t(col)*m+j+1];r[size_t(col)*m+j]=c*x+s*y;r[size_t(col)*m+j+1]=-s*x+c*y;}
  for(int row=threadIdx.x;row<q;row+=blockDim.x){double x=orth[size_t(j)*q+row],y=orth[size_t(j+1)*q+row];orth[size_t(j)*q+row]=c*x+s*y;orth[size_t(j+1)*q+row]=-s*x+c*y;}
  __syncthreads();
 }
}
__global__ void copy_scaled_column(const double* source,double* target,int q,double inverse){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q)target[i]=source[i]*inverse;}
__global__ void finalize_column(double* column,int q,double* diagonal,double* projection,int* status){
 __shared__ double squares[256],sums[256],inverse;
 __shared__ int valid;
 double square=0,sum=0;
 for(int i=threadIdx.x;i<q;i+=blockDim.x){double x=column[i];square+=x*x;sum+=x;}
 squares[threadIdx.x]=square;sums[threadIdx.x]=sum;__syncthreads();
 for(int stride=128;stride;stride/=2){if(threadIdx.x<stride){squares[threadIdx.x]+=squares[threadIdx.x+stride];sums[threadIdx.x]+=sums[threadIdx.x+stride];}__syncthreads();}
 if(threadIdx.x==0){double norm=sqrt(squares[0]);valid=isfinite(norm)&&norm>1e-12;*status=valid?0:7;inverse=valid?1/norm:0;if(valid){*diagonal=norm;*projection=sums[0]*inverse;}}__syncthreads();
 if(valid)for(int i=threadIdx.x;i<q;i+=blockDim.x)column[i]*=inverse;
}
extern "C" int am_select_v2(size_t q,size_t n,size_t m,const double* p,const double* norms_squared,size_t* indices,double* weights,double tolerance,void* context,NnlsCallback fallback,size_t max_iterations,CheckCallback check){
 if(!q||q>8192||!n||n>8192||!m||m>n||m>2048||!p||!norms_squared||!indices||!weights||!fallback||!max_iterations||!std::isfinite(tolerance)||tolerance<=0)return 1;
 if(check){int status=check(context);if(status)return status;}
 CHECK(handles.error);Memory dp,basis,orth,r,rtmp,projected,dots,residual,trial,correlation,ones,packed,column_status;
 CHECK(column_status.allocate(sizeof(int)));CHECK(dp.allocate(q*n*8));CHECK(basis.allocate(q*m*8));CHECK(orth.allocate(q*m*8));CHECK(r.allocate(m*m*8));CHECK(rtmp.allocate(m*m*8));CHECK(projected.allocate(m*8));CHECK(dots.allocate(m*8));CHECK(residual.allocate(q*8));CHECK(trial.allocate(m*8));CHECK(correlation.allocate(n*8));CHECK(ones.allocate(q*8));CHECK(packed.allocate(q*m*8));
 CHECK(cudaMemcpy(dp.p,p,q*n*8,cudaMemcpyHostToDevice));CHECK(cudaMemset(r.p,0,m*m*8));
 std::vector<double> host_ones(q,1),scores(n),norms(m),proposal(m),solution(m),gradient(m),host_basis(q*m),host_residual(q);std::vector<bool> used(n,false),is_active(m,false);std::vector<int> active;
 CHECK(cudaMemcpy(ones.p,host_ones.data(),q*8,cudaMemcpyHostToDevice));CHECK(cudaMemcpy(residual.p,host_ones.data(),q*8,cudaMemcpyHostToDevice));
 const double one=1,zero=0,minus=-1;const double kkt=tolerance*std::sqrt(double(q));std::fill(weights,weights+m,0);size_t updates=0,downdates=0,fallbacks=0;
 auto add_active=[&](size_t index)->int {
  const size_t column_index=active.size();if(column_index>=q)return 7;
  double* column=orth.data()+column_index*q;CHECK(cudaMemset(r.data()+column_index*m,0,m*8));
  copy_scaled_column<<<(q+127)/128,128>>>(basis.data()+index*q,column,q,1/norms[index]);CHECK(cudaGetLastError());
  if(column_index)for(int pass=0;pass<2;pass++){
   CHECK(cublasDgemv(handles.blas,CUBLAS_OP_T,q,column_index,&one,orth.data(),q,column,1,&zero,dots.data(),1));CHECK(cublasDaxpy(handles.blas,column_index,&one,dots.data(),1,r.data()+column_index*m,1));CHECK(cublasDgemv(handles.blas,CUBLAS_OP_N,q,column_index,&minus,orth.data(),q,dots.data(),1,&one,column,1));
  }
  finalize_column<<<1,256>>>(column,q,r.data()+column_index*m+column_index,projected.data()+column_index,static_cast<int*>(column_status.p));CHECK(cudaGetLastError());int status=0;CHECK(cudaMemcpy(&status,column_status.p,sizeof(int),cudaMemcpyDeviceToHost));if(status)return status;
  active.push_back(index);is_active[index]=true;updates++;return 0;
 };
 auto remove_active=[&](size_t column)->int {
  const size_t count=active.size();is_active[active[column]]=false;weights[active[column]]=0;
  if(count>1){delete_r_column<<<(count*(count-1)+127)/128,128>>>(r.data(),rtmp.data(),m,count,column);CHECK(cudaGetLastError());CHECK(cudaMemcpy(r.p,rtmp.p,m*m*8,cudaMemcpyDeviceToDevice));retriangularize<<<1,128>>>(orth.data(),r.data(),projected.data(),q,m,count,column);CHECK(cudaGetLastError());}
  active.erase(active.begin()+column);downdates++;return 0;
 };
 auto native_fallback=[&](size_t count)->int {
  pack_basis<<<(q*count+127)/128,128>>>(basis.data(),packed.data(),q,count);CHECK(cudaGetLastError());CHECK(cudaMemcpy(host_basis.data(),packed.p,q*count*8,cudaMemcpyDeviceToHost));int status=fallback(context,host_basis.data(),q,count,weights,host_residual.data());if(status)return status;CHECK(cudaMemcpy(residual.p,host_residual.data(),q*8,cudaMemcpyHostToDevice));
  active.clear();std::fill(is_active.begin(),is_active.end(),false);for(size_t j=0;j<count;j++)if(weights[j]>0){active.push_back(j);is_active[j]=true;}
  if(!active.empty()){CHECK(rebuild(basis.data(),orth.data(),r.data(),projected.data(),ones.data(),q,m,active,norms));}fallbacks++;return 0;
 };
 for(size_t selected=0;selected<m;selected++){
  if(check){int status=check(context);if(status)return status;}
  CHECK(cublasDgemv(handles.blas,CUBLAS_OP_N,n,q,&one,dp.data(),n,residual.data(),1,&zero,correlation.data(),1));CHECK(cudaMemcpy(scores.data(),correlation.p,n*8,cudaMemcpyDeviceToHost));size_t best=n;double best_score=-INFINITY;for(size_t j=0;j<n;j++)if(!used[j]){double score=norms_squared[j]>0?scores[j]/std::sqrt(norms_squared[j]):0;if(best==n||score>best_score){best=j;best_score=score;}}
  if(best==n||!std::isfinite(best_score))return 5;used[best]=true;indices[selected]=best;norms[selected]=std::sqrt(norms_squared[best]);weights[selected]=0;const size_t count=selected+1;
  // The auxiliary orth destination is unused here; the active-factor update
  // copies from the raw basis only when its gradient warrants admission.
  pick_column<<<(q+127)/128,128>>>(dp.data(),n,q,best,1,basis.data()+selected*q,packed.data());CHECK(cudaGetLastError());
  bool converged=false;size_t iterations=0;
  while(iterations++<max_iterations){
   if(check){int status=check(context);if(status)return status;}
   CHECK(cudaMemcpy(trial.p,weights,count*8,cudaMemcpyHostToDevice));CHECK(cudaMemcpy(residual.p,ones.p,q*8,cudaMemcpyDeviceToDevice));CHECK(cublasDgemv(handles.blas,CUBLAS_OP_N,q,count,&minus,basis.data(),q,trial.data(),1,&one,residual.data(),1));CHECK(cublasDgemv(handles.blas,CUBLAS_OP_T,q,count,&one,basis.data(),q,residual.data(),1,&zero,dots.data(),1));CHECK(cudaMemcpy(gradient.data(),dots.p,count*8,cudaMemcpyDeviceToHost));
   double violation=0,best_gradient=kkt;size_t entering=count;
   for(size_t j=0;j<count;j++){if(norms[j]==0)continue;double g=gradient[j]/norms[j];if(!std::isfinite(g))return 6;violation=std::max(violation,weights[j]>0?std::abs(g):std::max(0.,g));if(!is_active[j]&&g>best_gradient){best_gradient=g;entering=j;}}
   if(violation<=kkt){converged=true;break;}
   if(entering==count){CHECK(native_fallback(count));converged=true;break;}
   int added=add_active(entering);if(added==7){CHECK(native_fallback(count));converged=true;break;}if(added)return added;
   while(!active.empty()&&iterations++<max_iterations){
    if(check){int status=check(context);if(status)return status;}
    const size_t ac=active.size();CHECK(cudaMemcpy(trial.p,projected.p,ac*8,cudaMemcpyDeviceToDevice));CHECK(cublasDtrsv(handles.blas,CUBLAS_FILL_MODE_UPPER,CUBLAS_OP_N,CUBLAS_DIAG_NON_UNIT,ac,r.data(),m,trial.data(),1));CHECK(cudaMemcpy(solution.data(),trial.p,ac*8,cudaMemcpyDeviceToHost));std::fill(proposal.begin(),proposal.end(),0);bool feasible=true;
    for(size_t j=0;j<ac;j++){int idx=active[j];proposal[idx]=solution[j]/norms[idx];if(!std::isfinite(proposal[idx]))return 6;if(proposal[idx]<=0)feasible=false;}
    if(feasible){std::copy(proposal.begin(),proposal.begin()+count,weights);break;}
    double alpha=1;size_t drop=count;
    for(int idx:active)if(proposal[idx]<=0){double fraction=weights[idx]==0?0:weights[idx]/(weights[idx]-proposal[idx]);if(drop==count||fraction<alpha){alpha=fraction;drop=idx;}}
    if(drop==count||!std::isfinite(alpha)||alpha<0||alpha>1)return 6;
    for(size_t j=0;j<count;j++){weights[j]+=alpha*(proposal[j]-weights[j]);if(!std::isfinite(weights[j]))return 6;}
    for(size_t j=active.size();j>0;j--){int idx=active[j-1];if(weights[idx]<=0||size_t(idx)==drop){CHECK(remove_active(j-1));}}
   }
  }
  if(!converged)return 8;
 }
 fprintf(stderr,"am_updates additions=%zu removals=%zu full_refits=%zu\n",updates,downdates,fallbacks);return 0;
}

// Compatibility entry for existing numerical fixture runners. New callers
// should provide explicit bounded work and cancellation through am_select_v2.
extern "C" int am_select(size_t q,size_t n,size_t m,const double* p,const double* norms_squared,size_t* indices,double* weights,double tolerance,void* context,NnlsCallback fallback){
 return am_select_v2(q,n,m,p,norms_squared,indices,weights,tolerance,context,fallback,4096,nullptr);
}
