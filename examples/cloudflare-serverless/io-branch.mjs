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

export function reserveIoBranch(parent,{operations=512,bytes=16*1024*1024}={}){
 const maxOperations=1024,maxBytes=32*1024*1024;
 if(![parent.operations,parent.bytes,operations,bytes].every(Number.isSafeInteger)||operations<1||bytes<1||parent.operations<0||parent.bytes<0||parent.operations+operations>=maxOperations||parent.bytes+bytes>=maxBytes)throw Error('CoordinatorIoBudgetExceeded');
 parent.operations+=operations;parent.bytes+=bytes;
 const prefixOperations=maxOperations-operations,prefixBytes=maxBytes-bytes,child={operations:prefixOperations,bytes:prefixBytes};let joined=false;
 return{budget:child,join(){if(joined)return;joined=true;if(!Number.isSafeInteger(child.operations)||!Number.isSafeInteger(child.bytes)||child.operations<prefixOperations||child.bytes<prefixBytes||child.operations>maxOperations||child.bytes>maxBytes)throw Error('CoordinatorIoBudgetExceeded');parent.operations+=child.operations-prefixOperations-operations;parent.bytes+=child.bytes-prefixBytes-bytes;if(parent.operations>maxOperations||parent.bytes>maxBytes)throw Error('CoordinatorIoBudgetExceeded');}};
}
