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

package management

//go:generate go -C ../../sdk tool oapi-codegen --config=../sources/management/cfg.yaml -o ../sources/management/types.gen.go ../../../specs/openapi/sources/management.yaml

//go:generate python3 ../../../../scripts/license_headers.py --group apache types.gen.go
