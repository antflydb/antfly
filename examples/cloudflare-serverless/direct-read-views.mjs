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

import observe from "./dist/antfly-serverless-observe.wasm";
import{AppReadViews}from"./app-read-views.mjs";
import{NativeReadViews}from"./native-read-views.mjs";
import{readerKernel,sharedCache,kernelMetrics}from"./kernels.mjs";
// No HTTP ctx/waitUntil is retained by these isolate-local observers.
export const directReadViews=new AppReadViews(env=>new NativeReadViews(observe,env,{kernel:readerKernel,cache:sharedCache,cohortDelayMs:Number(env.READ_VIEW_BATCH_MS??2),onMetrics:sample=>{if(env.LOG_METRICS==="true")console.log(JSON.stringify({event:"antfly.app_metadata",...sample,...kernelMetrics()}));}}));
