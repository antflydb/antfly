// Copyright 2026 Antfly, Inc.
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

// Package proxy contains the Antfly-aware public gateway seam.
//
// The first responsibility of this package is to provide a stable place for:
//   - backend routing between stateful and serverless products
//   - tenant and namespace-aware request policy
//   - request-level freshness/consistency controls
//   - authn/authz integration
//
// The proxy should stay out of query execution and storage coordination.
package proxy
