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

import { queueAfterTransactionHook } from "@better-auth/core/context";
import type { BetterAuthOptions } from "better-auth";
/** Explicit single-process defaults. Use database limiting for remote deployments. */
export const authDefaults = {
  emailAndPassword: { enabled: true },
  session: {
    expiresIn: 7 * 24 * 60 * 60,
    updateAge: 24 * 60 * 60,
    cookieCache: { enabled: false },
  },
  rateLimit: { enabled: true, storage: "memory", window: 60, max: 100 },
  advanced: { defaultCookieAttributes: { httpOnly: true, sameSite: "lax" } },
} satisfies BetterAuthOptions;
export interface AuthMail {
  to: string;
  subject: string;
  text: string;
}
/** Mail delivery stays application-owned; pass a durable production delivery callback. */
export function consumerAuthOptions(config: {
  sendMail: (mail: AuthMail) => Promise<void>;
  google?: { clientId: string; clientSecret: string };
  remote?: boolean;
}): BetterAuthOptions {
  const sendMail = (mail: AuthMail) => queueAfterTransactionHook(() => config.sendMail(mail));
  return {
    ...authDefaults,
    rateLimit: { ...authDefaults.rateLimit, storage: config.remote ? "database" : "memory" },
    emailAndPassword: {
      enabled: true,
      requireEmailVerification: true,
      revokeSessionsOnPasswordReset: true,
      sendResetPassword: async ({ user, url }) =>
        sendMail({
          to: user.email,
          subject: "Reset your password",
          text: `Reset your password: ${url}`,
        }),
    },
    emailVerification: {
      sendOnSignUp: true,
      sendOnSignIn: true,
      autoSignInAfterVerification: false,
      sendVerificationEmail: async ({ user, url }) =>
        sendMail({
          to: user.email,
          subject: "Verify your email",
          text: `Verify your email: ${url}`,
        }),
    },
    user: {
      deleteUser: { enabled: true },
      changeEmail: {
        enabled: true,
        updateEmailWithoutVerification: false,
        sendChangeEmailConfirmation: async ({ user, url }) =>
          sendMail({
            to: user.email,
            subject: "Confirm your email change",
            text: `Confirm your email change: ${url}`,
          }),
      },
    },
    account: {
      accountLinking: { enabled: true, requireLocalEmailVerified: true, trustedProviders: [] },
    },
    ...(config.google ? { socialProviders: { google: config.google } } : {}),
  };
}
