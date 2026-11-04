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

// Portable request correlation. The token identifies a physical HTTP attempt,
// never a provider invocation. Lowercase base36 avoids URL ID redaction.
export function versionIdentity(value){return typeof value==='string'&&/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(value)?value.toLowerCase():null;}
export function attemptParameter(value){const id=versionIdentity(value);return id?'q'+BigInt('0x'+id.replaceAll('-','')).toString(36).padStart(25,'0'):null;}
export function requestAttemptUuid(value){try{const parameter=new URL(value).searchParams.get('__antfly_qualification_attempt');if(versionIdentity(parameter))return versionIdentity(parameter);if(!/^q[0-9a-z]{25}$/.test(parameter??''))return null;let number=0n;for(const digit of parameter.slice(1))number=number*36n+BigInt(parseInt(digit,36));if(number>=1n<<128n)return null;const hex=number.toString(16).padStart(32,'0'),id=[hex.slice(0,8),hex.slice(8,12),hex.slice(12,16),hex.slice(16,20),hex.slice(20)].join('-');return attemptParameter(id)===parameter?id:null;}catch{return null;}}
export async function portableAttemptIdentity(value){const id=versionIdentity(value);if(!id)return null;const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(id));return Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');}
