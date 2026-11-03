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

// Never publish a closed report while a failed phase's peer still sends I/O.
export async function runPairedPhase(sides,operation){
 let failed=false,firstError;
 const results=await Promise.allSettled(sides.map(async side=>{
  try{return await operation(side);}
  catch(error){if(!failed){failed=true;firstError=error;for(const peer of sides)peer.stop(Error('PeerPhaseCancelled'));}throw error;}
 }));
 if(failed)throw firstError;
 return results.map(result=>result.value);
}
