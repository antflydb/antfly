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

//go:build afrelease

package utils

// AfdbTestBuild is a flag that is set to true if the binary was compiled
// without the 'release' build tag (which is the case for all test targets). This
// flag can be used to enable expensive checks, test randomizations, or other
// perturbations that will not affect test results but will exercise different parts of the code.
const AfdbTestBuild = false
