// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native KV fitting kernels; FP64, no fast-math compilation.

extern "C" __global__ void normalize(double* p,const double* bias,int n,double cap,double* masses){
 int row=blockIdx.x,lane=threadIdx.x; double* v=p+size_t(row)*n;
 __shared__ double temp[128]; double maximum=-__longlong_as_double(0x7ff0000000000000LL);
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

extern "C" __global__ void pick_column(const double* p,int n,int q,int index,double norm,double* basis,double* orth){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q){double x=p[size_t(i)*n+index];basis[i]=x;orth[i]=x/norm;}}

extern "C" __global__ void pack_basis(const double* b,double* packed,int q,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q*count)packed[i]=b[size_t(i%count)*q+i/count];}

extern "C" __global__ void pack_active(const double* basis,const int* active,const double* norms,double* orth,int q,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q*count){int column=i/q,src=active[column];orth[i]=basis[size_t(src)*q+i%q]/norms[src];}}

extern "C" __global__ void save_upper(const double* qr,double* r,int q,int m,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count*count){int row=i%count,col=i/count;r[size_t(col)*m+row]=row<=col?qr[size_t(col)*q+row]:0;}}

extern "C" __global__ void delete_r_column(const double* r,double* tmp,int m,int count,int removed){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count*(count-1)){int row=i%count,col=i/count;tmp[size_t(col)*m+row]=r[size_t(col+(col>=removed))*m+row];}}

extern "C" __global__ void retriangularize(double* orth,double* r,double* projected,int q,int m,int count,int removed){
 __shared__ double c,s;
 for(int j=removed;j<count-1;j++){
  if(threadIdx.x==0){double a=r[size_t(j)*m+j],b=r[size_t(j)*m+j+1],norm=hypot(a,b);c=norm>0?a/norm:1;s=norm>0?b/norm:0;double x=projected[j],y=projected[j+1];projected[j]=c*x+s*y;projected[j+1]=-s*x+c*y;}
  __syncthreads();
  for(int col=j+threadIdx.x;col<count-1;col+=blockDim.x){double x=r[size_t(col)*m+j],y=r[size_t(col)*m+j+1];r[size_t(col)*m+j]=c*x+s*y;r[size_t(col)*m+j+1]=-s*x+c*y;}
  for(int row=threadIdx.x;row<q;row+=blockDim.x){double x=orth[size_t(j)*q+row],y=orth[size_t(j+1)*q+row];orth[size_t(j)*q+row]=c*x+s*y;orth[size_t(j+1)*q+row]=-s*x+c*y;}
  __syncthreads();
 }
}

extern "C" __global__ void copy_scaled_column(const double* source,double* target,int q,double inverse){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<q)target[i]=source[i]*inverse;}

extern "C" __global__ void finalize_column(double* column,int q,double* diagonal,double* projection,int* status){
 __shared__ double squares[256],sums[256],inverse;
 __shared__ int valid;
 double square=0,sum=0;
 for(int i=threadIdx.x;i<q;i+=blockDim.x){double x=column[i];square+=x*x;sum+=x;}
 squares[threadIdx.x]=square;sums[threadIdx.x]=sum;__syncthreads();
 for(int stride=128;stride;stride/=2){if(threadIdx.x<stride){squares[threadIdx.x]+=squares[threadIdx.x+stride];sums[threadIdx.x]+=sums[threadIdx.x+stride];}__syncthreads();}
 if(threadIdx.x==0){double norm=sqrt(squares[0]);valid=isfinite(norm)&&norm>1e-12;*status=valid?0:7;inverse=valid?1/norm:0;if(valid){*diagonal=norm;*projection=sums[0]*inverse;}}__syncthreads();
 if(valid)for(int i=threadIdx.x;i<q;i+=blockDim.x)column[i]*=inverse;
}

extern "C" __global__ void zero_values(double* p,int count){int i=blockIdx.x*blockDim.x+threadIdx.x;if(i<count)p[i]=0;}

// Bounded device-resident OMP/NNLS; host checks cancellation between columns.
// Cooperative launch must be capped by driver-reported occupancy.
#include <cooperative_groups.h>
namespace cg=cooperative_groups;
struct ControlState { int status,active_count,entering,feasible,drop,remove; double alpha,c,s; };
__device__ double block_sum_baseline(double x,double* temp){
 temp[threadIdx.x]=x;__syncthreads();
 for(int stride=blockDim.x/2;stride;stride/=2){if(threadIdx.x<stride)temp[threadIdx.x]+=temp[threadIdx.x+stride];__syncthreads();}
 double result=temp[0];__syncthreads();return result;
}
// Preserve the exact 128-thread binary addition tree. Only the final
// five levels move to warp registers. Shuffles are not memory barriers;
// all shared-memory reads/writes retain explicit block synchronization.
// https://docs.nvidia.com/cuda/archive/12.9.1/cuda-c-programming-guide/index.html#warp-shuffle-functions
__device__ double block_sum(double x,double* temp){
 if(blockDim.x!=128)return block_sum_baseline(x,temp);
 const int lane=threadIdx.x;
 temp[lane]=x;__syncthreads();
 if(lane<64)temp[lane]+=temp[lane+64];
 __syncthreads();
 if(lane<32){
  double value=temp[lane]+temp[lane+32];
  for(int stride=16;stride;stride/=2){
   const double other=__shfl_down_sync(0xffffffffu,value,stride);
   if(lane<stride)value+=other;
  }
  if(lane==0)temp[0]=value;
 }
 __syncthreads();
 const double result=temp[0];__syncthreads();return result;
}
struct Best { double score; int index; };
__device__ Best block_best(Best value,double* scores,int* indices){
 scores[threadIdx.x]=value.score;indices[threadIdx.x]=value.index;__syncthreads();
 for(int stride=blockDim.x/2;stride;stride/=2){if(threadIdx.x<stride){int i=threadIdx.x,j=i+stride;bool better=indices[j]>=0&&(indices[i]<0||scores[j]>scores[i]||(scores[j]==scores[i]&&indices[j]<indices[i]));if(better){scores[i]=scores[j];indices[i]=indices[j];}}__syncthreads();}
 Best result={scores[0],indices[0]};__syncthreads();return result;
}
__device__ double block_max(double value,double* temp){temp[threadIdx.x]=value;__syncthreads();for(int stride=blockDim.x/2;stride;stride/=2){if(threadIdx.x<stride)temp[threadIdx.x]=fmax(temp[threadIdx.x],temp[threadIdx.x+stride]);__syncthreads();}double result=temp[0];__syncthreads();return result;}
extern "C" __global__ void transpose_probabilities(const double* p,double* columns,int q,int n){size_t i=size_t(blockIdx.x)*blockDim.x+threadIdx.x;if(i<size_t(q)*n)columns[i]=p[(i%q)*n+i/q];}
extern "C" __global__ void select_column_cooperative(const double* columns,const double* norms_squared,size_t* indices,double* weights,double* residual,double* scores,double* orth,double* r,double* rtmp,double* projected,double* dots,double* trial,double* proposal,int* active,int* used,int* active_flags,ControlState* control,int q,int n,int m,int selected,double tolerance,int maximum_iterations){
 auto grid=cg::this_grid();const int thread=blockIdx.x*blockDim.x+threadIdx.x,stride=gridDim.x*blockDim.x;
 __shared__ double reduction[128];__shared__ int reduction_index[128];__shared__ double scalar;
 if(selected==0){
  for(int i=thread;i<q;i+=stride)residual[i]=1;
  for(int j=thread;j<n;j+=stride)used[j]=0;
  for(int j=thread;j<m;j+=stride){weights[j]=0;active_flags[j]=0;}
  for(int j=thread;j<m*m;j+=stride){r[j]=0;rtmp[j]=0;}
  if(thread==0){control->status=0;control->active_count=0;}
 }
 grid.sync();
 for(int j=blockIdx.x;j<n;j+=gridDim.x){
  double dot=0;for(int i=threadIdx.x;i<q;i+=blockDim.x)dot+=columns[size_t(j)*q+i]*residual[i];
  dot=block_sum(dot,reduction);if(threadIdx.x==0)scores[j]=used[j]?-INFINITY:(norms_squared[j]>0?dot/sqrt(norms_squared[j]):0);
 }
 grid.sync();
 if(blockIdx.x==0){Best best={-INFINITY,-1};for(int j=threadIdx.x;j<n;j+=blockDim.x)if(!used[j]&&(best.index<0||scores[j]>best.score)){best={scores[j],j};}best=block_best(best,reduction,reduction_index);if(threadIdx.x==0){if(best.index<0||!isfinite(best.score))control->status=5;else{indices[selected]=best.index;used[best.index]=1;weights[selected]=0;}}}
 grid.sync();{const int status=control->status;grid.sync();if(status)return;}
 const int count=selected+1;int iterations=0;
 while(true){
  if(thread==0&&iterations>=maximum_iterations)control->status=8;
  grid.sync();{const int status=control->status;grid.sync();if(status)return;}iterations++;
  for(int i=thread;i<q;i+=stride){double value=1;for(int j=0;j<count;j++)value-=columns[indices[j]*q+i]*weights[j];residual[i]=value;}
  grid.sync();
  for(int j=blockIdx.x;j<count;j+=gridDim.x){double dot=0;for(int i=threadIdx.x;i<q;i+=blockDim.x)dot+=columns[indices[j]*q+i]*residual[i];dot=block_sum(dot,reduction);if(threadIdx.x==0)scores[j]=norms_squared[indices[j]]>0?dot/sqrt(norms_squared[indices[j]]):0;}
  grid.sync();
  if(blockIdx.x==0){double violation=0;Best best={tolerance,-1};for(int j=threadIdx.x;j<count;j+=blockDim.x){double g=scores[j];if(!isfinite(g))atomicExch(&control->status,6);violation=fmax(violation,weights[j]>0?fabs(g):fmax(0.,g));if(!active_flags[j]&&g>best.score)best={g,j};}best=block_best(best,reduction,reduction_index);violation=block_max(violation,reduction);if(threadIdx.x==0){control->entering=violation<=tolerance?-2:best.index;if(violation>tolerance&&best.index<0)control->status=7;}}
  grid.sync();{const int status=control->status;grid.sync();if(status)return;}if(control->entering==-2)return;
  const int entering=control->entering,ac=control->active_count;
  if(thread==0&&ac>=q)control->status=7;
  grid.sync();{const int status=control->status;grid.sync();if(status)return;}
  double* column=orth+size_t(ac)*q;
  for(int i=thread;i<q;i+=stride)column[i]=columns[indices[entering]*q+i]/sqrt(norms_squared[indices[entering]]);
  for(int j=thread;j<m;j+=stride)r[size_t(ac)*m+j]=0;
  grid.sync();
  for(int pass=0;pass<2;pass++){
   for(int j=blockIdx.x;j<ac;j+=gridDim.x){double dot=0;for(int i=threadIdx.x;i<q;i+=blockDim.x)dot+=orth[size_t(j)*q+i]*column[i];dot=block_sum(dot,reduction);if(threadIdx.x==0){dots[j]=dot;r[size_t(ac)*m+j]+=dot;}}
   grid.sync();
   for(int i=thread;i<q;i+=stride){double dot=0;for(int j=0;j<ac;j++)dot+=orth[size_t(j)*q+i]*dots[j];column[i]-=dot;}
   grid.sync();
  }
  if(blockIdx.x==0){double square=0,sum=0;for(int i=threadIdx.x;i<q;i+=blockDim.x){double x=column[i];square+=x*x;sum+=x;}square=block_sum(square,reduction);sum=block_sum(sum,reduction);if(threadIdx.x==0){double norm=sqrt(square);if(!isfinite(norm)||norm<=1e-12)control->status=7;else{r[size_t(ac)*m+ac]=norm;projected[ac]=sum/norm;active[ac]=entering;active_flags[entering]=1;control->active_count=ac+1;}}}
  grid.sync();{const int status=control->status;grid.sync();if(status)return;}
  for(int i=thread;i<q;i+=stride)column[i]/=r[size_t(ac)*m+ac];
  grid.sync();
  while(control->active_count>0){
   if(thread==0&&iterations>=maximum_iterations)control->status=8;
   grid.sync();{const int status=control->status;grid.sync();if(status)return;}iterations++;
   const int current=control->active_count;
   if(blockIdx.x==0){
    for(int i=threadIdx.x;i<current;i+=blockDim.x)trial[i]=projected[i];__syncthreads();
    for(int j=current-1;j>=0;j--){if(threadIdx.x==0){scalar=trial[j]/r[size_t(j)*m+j];trial[j]=scalar;}__syncthreads();for(int i=threadIdx.x;i<j;i+=blockDim.x)trial[i]-=r[size_t(j)*m+i]*scalar;__syncthreads();}
   }
   for(int i=thread;i<count;i+=stride)proposal[i]=0;
   if(thread==0)control->feasible=1;
   grid.sync();
   for(int i=thread;i<current;i+=stride){int index=active[i];double value=trial[i]/sqrt(norms_squared[indices[index]]);proposal[index]=value;if(!isfinite(value))atomicExch(&control->status,6);if(value<=0)atomicExch(&control->feasible,0);}
   grid.sync();{const int status=control->status;grid.sync();if(status)return;}
   if(control->feasible){for(int i=thread;i<count;i+=stride)weights[i]=proposal[i];grid.sync();break;}
   if(thread==0){double alpha=1;int drop=-1;for(int j=0;j<current;j++){int index=active[j];if(proposal[index]<=0){double fraction=weights[index]==0?0:weights[index]/(weights[index]-proposal[index]);if(drop<0||fraction<alpha){alpha=fraction;drop=index;}}}control->alpha=alpha;control->drop=drop;if(drop<0||!isfinite(alpha)||alpha<0||alpha>1)control->status=6;}
   grid.sync();{const int status=control->status;grid.sync();if(status)return;}
   for(int i=thread;i<count;i+=stride){weights[i]+=control->alpha*(proposal[i]-weights[i]);if(!isfinite(weights[i]))atomicExch(&control->status,6);}
   grid.sync();{const int status=control->status;grid.sync();if(status)return;}
   for(int removed=current-1;removed>=0;removed--){
    if(thread==0)control->remove=weights[active[removed]]<=0||active[removed]==control->drop;
    grid.sync();
    if(control->remove){
     const int size=control->active_count;
     for(int i=thread;i<size*(size-1);i+=stride){int row=i%size,col=i/size;rtmp[size_t(col)*m+row]=r[size_t(col+(col>=removed))*m+row];}
     grid.sync();
     for(int i=thread;i<size*(size-1);i+=stride){int row=i%size,col=i/size;r[size_t(col)*m+row]=rtmp[size_t(col)*m+row];}
     grid.sync();
     for(int j=removed;j<size-1;j++){
      if(thread==0){double a=r[size_t(j)*m+j],b=r[size_t(j)*m+j+1],norm=hypot(a,b);double c=norm>0?a/norm:1,s=norm>0?b/norm:0;control->c=c;control->s=s;double x=projected[j],y=projected[j+1];projected[j]=c*x+s*y;projected[j+1]=-s*x+c*y;}
      grid.sync();double c=control->c,s=control->s;
      for(int col=j+thread;col<size-1;col+=stride){double x=r[size_t(col)*m+j],y=r[size_t(col)*m+j+1];r[size_t(col)*m+j]=c*x+s*y;r[size_t(col)*m+j+1]=-s*x+c*y;}
      for(int row=thread;row<q;row+=stride){double x=orth[size_t(j)*q+row],y=orth[size_t(j+1)*q+row];orth[size_t(j)*q+row]=c*x+s*y;orth[size_t(j+1)*q+row]=-s*x+c*y;}
      grid.sync();
     }
     if(thread==0){int index=active[removed];weights[index]=0;active_flags[index]=0;for(int j=removed;j<size-1;j++)active[j]=active[j+1];control->active_count=size-1;}
     grid.sync();
    }
    // Every block must consume the remove decision before thread zero
    // overwrites it on the next iteration, including the no-removal path.
    grid.sync();
   }
  }
 }
}
