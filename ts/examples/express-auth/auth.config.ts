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

import { authDefaults } from "@antfly/better-auth";
const origin = process.env.APP_ORIGIN ?? "http://localhost:3000";
if (!process.env.BETTER_AUTH_SECRET)
  throw new Error("Set BETTER_AUTH_SECRET to a random secret of at least 32 characters");
export const authOptions = {
  ...authDefaults,
  secret: process.env.BETTER_AUTH_SECRET,
  baseURL: origin,
  trustedOrigins: [origin],
  advanced: { ...authDefaults.advanced, ipAddress: { ipAddressHeaders: ["x-antfly-client-ip"] } },
};
