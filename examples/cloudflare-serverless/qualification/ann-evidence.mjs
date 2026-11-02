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

// Validate completed native corpus work, not just the retained recall samples.
export function validateAnnReport(report,{minimumDocuments=1000}={}){
 if(report.cases?.length!==1)throw Error('ANN evidence requires one declared corpus');
 const corpus=report.cases[0],required=['ingest-and-publication','accepted-visibility','cold-query','cold-vector-query','vector-recall','cold-sparse-query','warm-query','cold-point-updates','cold-delete','compaction'];
 if(!Number.isInteger(corpus.documents)||corpus.documents<minimumDocuments||corpus.accepted_documents!==corpus.documents||BigInt(corpus.published_lsn??0)<BigInt(corpus.documents)||!Array.isArray(corpus.stages)||corpus.stages.some(stage=>stage.ok!==true)||required.some(name=>!corpus.stages.some(stage=>stage.name===name&&stage.ok===true)))throw Error('ANN corpus/stages incomplete');
 for(const probes of[8,32,'all']){
  const rows=corpus.vector_recall?.filter(row=>row.num_probes===probes)??[];
  if(rows.length<10||new Set(rows.map(row=>row.query)).size!==rows.length||rows.some(row=>!Number.isFinite(row.recall_at_10)||row.recall_at_10<0||row.recall_at_10>1||!Number.isFinite(row.r2_gets)||row.r2_gets<0||!Number.isFinite(row.r2_bytes)||row.r2_bytes<0||probes==='all'&&row.recall_at_10!==1))throw Error('ANN recall/physical samples incomplete or invalid');
 }
 return corpus;
}


// The precision protocol may change, but the compared workload may not.
export function validateAnnPair(baseline,candidate,options={}){
 const left=validateAnnReport(baseline,options),right=validateAnnReport(candidate,options);
 for(const key of ['dimensions','body_bytes','publication_records','paged_projections','paged_text','max_put_bytes','compaction_max_documents','recycle_at_bytes','reindex','recall_queries','clock','embedding_fixture_sha256','embedding_model']){
  if(JSON.stringify(baseline[key]??null)!==JSON.stringify(candidate[key]??null))throw Error(`ANN workload mismatch: ${key}`);
 }
 if(left.documents!==right.documents)throw Error('ANN workload mismatch: documents');
 for(const probes of[8,32,'all']){
  const a=left.vector_recall.filter(row=>row.num_probes===probes),b=right.vector_recall.filter(row=>row.num_probes===probes);
  const ids=rows=>rows.map(row=>row.query).sort((x,y)=>x-y);
  if(JSON.stringify(ids(a))!==JSON.stringify(ids(b)))throw Error('ANN workload mismatch: queries');
  for(const row of a){const other=b.find(candidate=>candidate.query===row.query);for(const key of['coarse_probes','search_effort'])if((row[key]??null)!==(other[key]??null))throw Error(`ANN workload mismatch: ${key}`);}
 }
 return [left,right];
}
