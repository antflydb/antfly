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

export function diagnosticPrefix(env,kind){
 if(!['observer','receiver'].includes(kind))throw Error('ObserverDiagnosticScopeInvalid');
 const id=env.OBSERVATION_CAPTURE_ID;if(id==null)return kind+'-v1/';
 if(typeof id!=='string'||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(id))throw Error('ObserverDiagnosticScopeInvalid');
 return kind+'-v2/'+id+'/';
}
