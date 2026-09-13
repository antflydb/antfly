// Copyright 2026 Antfly, Inc.
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

// These imports are file inputs supplied by the owner build constructor.
// Keep the public schema responses byte-for-byte identical to their sources.
pub const ard = @embedFile("ard.yaml");
pub const antfly = @embedFile("antfly.yaml");
pub const metadata = @embedFile("metadata.yaml");
pub const extensions = @embedFile("extensions.yaml");
pub const auth = @embedFile("auth.yaml");
pub const inference_config = @embedFile("inference-config.yaml");
