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

// Charge streamed input across requests, retaining its charge while parsed
// objects are dispatched. Request count alone does not bound concurrent bodies.
export class IngressBudget {
 constructor(limit=4*1024*1024){this.limit=limit;this.used=0;}
 open(){let held=0,closed=false;return{
  add:bytes=>{if(closed)throw Error('IngressReservationClosed');if(!Number.isSafeInteger(bytes)||bytes<0||this.used+bytes>this.limit)throw Error('CoordinatorCommitBusy');this.used+=bytes;held+=bytes;},
  release:()=>{if(!closed){this.used-=held;closed=true;}}
 };}
}
