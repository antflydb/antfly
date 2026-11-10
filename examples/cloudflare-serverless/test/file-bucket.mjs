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

// Local R2 contract fixture, not production storage. Payload bytes live in one
// append-only file; only the current key/ETag/offset index stays in memory.
import {mkdir,mkdtemp,open,rm} from 'node:fs/promises';
import {resolve} from 'node:path';

export class FileBucket {
  objects=new Map();revision=0;writes=0;reads=0;readBytes=0;storageBytes=0;pending=Promise.resolve();
  static async create(){
    const root=resolve('dist/qualification');await mkdir(root,{recursive:true});
    const directory=await mkdtemp(resolve(root,'file-bucket-'));
    try{return new FileBucket(await open(resolve(directory,'objects.bin'),'wx+',0o600),directory);}catch(error){await rm(directory,{recursive:true,force:true});throw error;}
  }
  constructor(file,directory){this.file=file;this.directory=directory;}
  metadata(key,value){return{key,size:value.size,etag:value.etag,uploaded:new Date(0),httpMetadata:value.httpMetadata??{}};}
  put(key,body,options={}){
    if(typeof body==='string')body=new TextEncoder().encode(body);
    // Own bytes across asynchronous append admission, matching Bucket.put.
    const bytes=Uint8Array.from(body);
    const operation=this.pending.then(async()=>{
      const old=this.objects.get(key),condition=options.onlyIf;
      if(condition?.etagDoesNotMatch==='*'&&old||condition?.etagMatches!=null&&condition.etagMatches!==old?.etag)return null;
      if(this.storageBytes+bytes.length>64*1024*1024*1024||!old&&this.objects.size>=1000000)throw Error('FixtureStorageBudgetExceeded');
      const offset=this.storageBytes;let written=0;
      while(written<bytes.length){const result=await this.file.write(bytes,written,bytes.length-written,offset+written);if(!result.bytesWritten)throw Error('FixtureShortWrite');written+=result.bytesWritten;}
      this.storageBytes+=bytes.length;
      const value={offset,size:bytes.length,etag:'e'+(++this.revision),httpMetadata:structuredClone(options.httpMetadata)};
      this.objects.set(key,value);this.writes++;return this.metadata(key,value);
    });
    this.pending=operation.catch(()=>{});return operation;
  }
  async get(key,options={}){
    this.reads++;const value=this.objects.get(key);if(!value)return null;
    const metadata=this.metadata(key,value);
    if(options.onlyIf?.etagMatches!=null&&options.onlyIf.etagMatches!==value.etag)return metadata;
    const offset=Math.min(value.size,options.range?.offset??0),length=Math.min(value.size-offset,options.range?.length??value.size-offset),bytes=new Uint8Array(length);let read=0;
    while(read<length){const result=await this.file.read(bytes,read,length-read,value.offset+offset+read);if(!result.bytesRead)throw Error('FixtureShortRead');read+=result.bytesRead;}
    this.readBytes+=bytes.length;
    const body=new ReadableStream({start(controller){controller.enqueue(bytes);controller.close();}});
    return{...metadata,range:options.range?{offset,length}:undefined,body,json:()=>new Response(body).json()};
  }
  async head(key){const value=this.objects.get(key);return value?this.metadata(key,value):null;}
  async delete(key){await this.pending;for(const item of Array.isArray(key)?key:[key])this.objects.delete(item);}
  async list(options={}){
    const keys=[...this.objects.keys()].sort().filter(key=>key.startsWith(options.prefix??'')&&(!options.startAfter||key>options.startAfter)&&(!options.cursor||key>options.cursor)),selected=keys.slice(0,options.limit??1000),truncated=selected.length<keys.length;
    return{objects:selected.map(key=>this.metadata(key,this.objects.get(key))),truncated,cursor:truncated?selected.at(-1):undefined};
  }
  async close(){await this.pending;await this.file.close();await rm(this.directory,{recursive:true,force:true});}
}
