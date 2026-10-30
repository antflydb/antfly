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

"use client";
import { useState } from "react";
import { createAuthClient } from "better-auth/react";
const auth = createAuthClient();
export default function Page() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [text, setText] = useState("");
  const [message, setMessage] = useState("");
  const { data: session } = auth.useSession();
  async function login(signup: boolean) {
    const input = { email, password };
    const result = signup
      ? await auth.signUp.email({ ...input, name: email })
      : await auth.signIn.email(input);
    setMessage(result.error?.message ?? "Signed in");
  }
  async function documents(save: boolean) {
    const response = await fetch(
      save ? "/api/documents" : `/api/documents?q=${encodeURIComponent(text)}`,
      save
        ? {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ title: "Document", body: text }),
          }
        : {}
    );
    setMessage(JSON.stringify(await response.json()));
  }
  return (
    <main>
      <h1>My documents</h1>
      {session ? (
        <>
          <p>{session.user.email}</p>
          <button onClick={() => auth.signOut()}>Sign out</button>
          <textarea
            aria-label="Document or search"
            value={text}
            onChange={(e) => setText(e.target.value)}
          />
          <button onClick={() => documents(true)}>Save</button>
          <button onClick={() => documents(false)}>Search</button>
        </>
      ) : (
        <>
          <input
            aria-label="Email"
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          <input
            aria-label="Password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          <button onClick={() => login(false)}>Sign in</button>
          <button onClick={() => login(true)}>Create account</button>
        </>
      )}
      <pre role="status">{message}</pre>
    </main>
  );
}
