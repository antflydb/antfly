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

// Used only after the lifecycle driver has stopped producers and validated
// disposable scope. Deleting another page is safe after a lost acknowledgement.
const transient=error=>error?.retryable===true||["TimeoutError","AbortError"].includes(error?.name)||error instanceof TypeError&&/fetch failed|terminated|network error/i.test(error.message)||["ECONNRESET","ETIMEDOUT","UND_ERR_SOCKET","UND_ERR_CONNECT_TIMEOUT"].includes(error?.cause?.code);
export async function drainBucket(readPage,{delay=ms=>new Promise(r=>setTimeout(r,ms)),maxPages=10000}={}){
 const report={objects:0,bytes:0,transport_retries:0,counts_complete:true,count_scope:"acknowledged pages in this invocation"};
 for(let i=0;i<maxPages;i++){
  let page;
  for(let attempt=0;attempt<3;attempt++)try{page=await readPage();break;}catch(error){if(!transient(error)||attempt===2)throw error;report.transport_retries++;report.counts_complete=false;await delay(250*(attempt+1));}
  if(typeof page?.done!=="boolean"||!Number.isSafeInteger(page.deleted)||page.deleted<0||page.deleted>1000||!Number.isSafeInteger(page.bytes)||page.bytes<0||page.done!==(page.deleted===0))throw Error("InvalidCleanupDrainPage");
  report.objects+=page.deleted;report.bytes+=page.bytes;
  if(page.done)return report;
 }
 throw Error("CleanupDrainPageLimitExceeded");
}
