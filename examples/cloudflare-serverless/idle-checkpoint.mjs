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

// Recovery hints run after foreground drain, never as a condition of its ACK.
export class IdleCheckpoint {
 constructor({revision,isIdle,checkpoint,background,onError=()=>{},every=1,delay=250,retryDelay=1100}){Object.assign(this,{revision,isIdle,checkpoint,background,onError,every,delay,retryDelay});this.last=0;this.pending=false;}
 schedule(){
  if(this.pending||!this.isIdle()||this.revision()-this.last<this.every)return;
  this.pending=true;
  const work=(async()=>{
   for(let attempt=0;attempt<2;attempt++){
    await new Promise(resolve=>setTimeout(resolve,attempt?this.retryDelay:this.delay));
    if(!this.isIdle())return;
    const result=await this.checkpoint();if(result.busy)return;
    if(result.done){this.last=result.revision;return;}
   }
  })().catch(error=>this.onError(error)).finally(()=>{this.pending=false;});
  this.background(work);
 }
}
