// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
