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

# Refresh all indexes successfully before resolving packages. Proxies and
# mirrors can briefly serve indexes from different publication generations;
# retry fresh metadata while retaining APT's signature and checksum checks.
set -euo pipefail
for attempt in 1 2 3; do
  if sudo apt-get -o Acquire::Retries=3 \
      -o Acquire::http::No-Cache=true -o Acquire::https::No-Cache=true \
      -o APT::Update::Error-Mode=any update; then
    sudo apt-get -o Acquire::Retries=3 install -y "$@"
    exit 0
  else
    status=$?
  fi
  if [ "$attempt" -eq 3 ]; then
    exit "$status"
  fi
  sleep "$((attempt * 5))"
done
