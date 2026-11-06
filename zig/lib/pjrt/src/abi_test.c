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

#include "pjrt_c_api.h"

const size_t antfly_pjrt_api_offsets[] = {
    offsetof(PJRT_Api, PJRT_Error_Destroy),
    offsetof(PJRT_Api, PJRT_Error_Message),
    offsetof(PJRT_Api, PJRT_Plugin_Initialize),
    offsetof(PJRT_Api, PJRT_Event_Destroy),
    offsetof(PJRT_Api, PJRT_Event_Await),
    offsetof(PJRT_Api, PJRT_Client_Create),
    offsetof(PJRT_Api, PJRT_Client_Destroy),
    offsetof(PJRT_Api, PJRT_Client_PlatformName),
    offsetof(PJRT_Api, PJRT_Client_AddressableDevices),
    offsetof(PJRT_Api, PJRT_Client_Compile),
    offsetof(PJRT_Api, PJRT_Client_BufferFromHostBuffer),
    offsetof(PJRT_Api, PJRT_Executable_Destroy),
    offsetof(PJRT_Api, PJRT_Executable_NumOutputs),
    offsetof(PJRT_Api, PJRT_Executable_Serialize),
    offsetof(PJRT_Api, PJRT_LoadedExecutable_Destroy),
    offsetof(PJRT_Api, PJRT_LoadedExecutable_GetExecutable),
    offsetof(PJRT_Api, PJRT_LoadedExecutable_Execute),
    offsetof(PJRT_Api, PJRT_Executable_DeserializeAndLoad),
    offsetof(PJRT_Api, PJRT_Buffer_Destroy),
    offsetof(PJRT_Api, PJRT_Buffer_OnDeviceSizeInBytes),
    offsetof(PJRT_Api, PJRT_Buffer_ToHostBuffer),
    offsetof(PJRT_Api, PJRT_Executable_GetCompiledMemoryStats),
};

const size_t antfly_pjrt_arg_layouts[] = {
    sizeof(PJRT_Executable_Destroy_Args),
    offsetof(PJRT_Executable_Destroy_Args, struct_size),
    offsetof(PJRT_Executable_Destroy_Args, extension_start),
    offsetof(PJRT_Executable_Destroy_Args, executable),
    sizeof(PJRT_LoadedExecutable_GetExecutable_Args),
    offsetof(PJRT_LoadedExecutable_GetExecutable_Args, struct_size),
    offsetof(PJRT_LoadedExecutable_GetExecutable_Args, extension_start),
    offsetof(PJRT_LoadedExecutable_GetExecutable_Args, loaded_executable),
    offsetof(PJRT_LoadedExecutable_GetExecutable_Args, executable),
    sizeof(PJRT_Executable_NumOutputs_Args),
    offsetof(PJRT_Executable_NumOutputs_Args, struct_size),
    offsetof(PJRT_Executable_NumOutputs_Args, extension_start),
    offsetof(PJRT_Executable_NumOutputs_Args, executable),
    offsetof(PJRT_Executable_NumOutputs_Args, num_outputs),
};
