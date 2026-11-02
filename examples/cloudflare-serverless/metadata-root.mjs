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

// Native roots inline the WAL/outbox (v2) and optional read capsules (v3).
// Cached rows remain in the ordered tree; only that tree owns page objects.
export function metadataTreeRoot(state){const root=state.root?JSON.parse(state.root):null;if(root?.format!=null){if(![2,3].includes(root.format)||!Object.hasOwn(root,"metadata_tree"))throw new Error("UnsupportedCoordinatorRoot");return root.metadata_tree;}return root;}
