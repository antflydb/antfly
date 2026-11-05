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

import {DurableObject} from 'cloudflare:workers';
import {SQLControl,authenticateSQL,boundedSQLBody,consumeSQL,locatorName,scheduleSQLMaintenance,retiredSQLDelivery} from './sql-control.mjs';
import {runSQL} from './sql-kernel.mjs';
import {IngressBudget} from './ingress-budget.mjs';
const ingress=new IngressBudget(2*1024*1024);
let active=0;
export class SQLCoordinator extends DurableObject {
 constructor(ctx,env){super(ctx,env);this.control=new SQLControl(ctx,env,runSQL);}
 async kick(locator){return this.control.kick(locator);}
 async advance(locator){return this.control.advance(locator);}
 async recover(){return this.control.recover();}
 async collect(){return this.control.collect();}
 async alarm(){return this.control.alarm();}
}
const paths=new Map([
 ['/internal/v1/sql/queries/submit','submit'],
 ['/internal/v1/sql/queries/turn','turn'],
 ['/internal/v1/sql/queries/result','result'],
 ['/internal/v1/sql/queries/cancel','cancel'],
 ['/internal/v1/sql/tasks/execute','execute'],
]);
const status=error=>/SqlJoinBudgetExceeded|HostedWorkingSetBudgetExceeded|SqlProgramLimitExceeded/.test(String(error))?422:/SqlQueryExpired/.test(String(error))?410:/Canceled|Cancelled/.test(String(error))?408:/SqlExecutionFailed/.test(String(error))?409:/SqlExecutionUnauthorized/.test(String(error))?403:503;
export default {
 async fetch(request,env){
  const action=paths.get(new URL(request.url).pathname);
  if(!action)return new Response('not found',{status:404});
  if(request.method!=='POST')return new Response('method not allowed',{status:405});
  if(!await authenticateSQL(request,env))return new Response('unauthorized',{status:401});
  if(active>=16)return new Response('SQL runtime busy',{status:action==='execute'?429:503,headers:{'Retry-After':'1'}});
  active++;const reservation=ingress.open();
  try{
   const raw=await boundedSQLBody(request,reservation),body=JSON.parse(raw);
   if(action==='submit'){
    // Preserve exact integers/scalar values in the native plan's original JSON.
    const result=await runSQL(env,{op:'sql_submit',tenant:body.tenant,raw_plan:raw,owner:crypto.randomUUID()});
    const locator={tenant:body.tenant,query:result.query};
    await env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);
    return Response.json(result);
   }
   const locator={tenant:body.tenant,query:body.query};locatorName(locator);
   if(action==='turn')return Response.json(await env.SQL_COORDINATORS.getByName(locatorName(locator)).advance(locator));
   if(action==='cancel'){await runSQL(env,{op:'sql_cancel',...locator,owner:crypto.randomUUID()});await env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);return Response.json({});}
   if(action==='result')return Response.json(await runSQL(env,{op:'sql_result',...locator,owner:crypto.randomUUID()}));
   try{await runSQL(env,{op:'sql_execute',...locator,ticket:body,owner:crypto.randomUUID()});}
   catch(error){if(retiredSQLDelivery(error))return new Response(null,{status:204});throw error;}
   return Response.json({});
  }catch(error){return Response.json({error:String(error)},{status:status(error)});}finally{reservation.release();active--;}
 },
 async queue(batch,env){await consumeSQL(batch,env,runSQL);},
 async scheduled(_controller,env,ctx){
  scheduleSQLMaintenance(env,ctx);
 },
};
