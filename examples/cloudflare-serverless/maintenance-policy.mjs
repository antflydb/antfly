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

export function metadataCollectionDue(prior,revision,now){
 if(!prior||now<prior.at)return false;
 const lowYield=prior.stats&&prior.stats.reclaimed_bytes<1024*1024&&prior.stats.deleted<prior.stats.scanned/10;
 const minimumMs=lowYield?900000:300000,minimumRevisions=lowYield?1024:256;
 return now-prior.at>=3600000||(now-prior.at>=minimumMs&&revision-prior.revision>=minimumRevisions);
}
