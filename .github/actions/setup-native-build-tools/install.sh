#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

missing=false
for tool in cc c++ make nm pkg-config xz; do
  if ! command -v "$tool" >/dev/null 2>&1; then missing=true; fi
done
if [[ "$missing" == false ]]; then exit 0; fi

sudo apt-get update
sudo apt-get install --no-install-recommends -y build-essential binutils pkg-config xz-utils
