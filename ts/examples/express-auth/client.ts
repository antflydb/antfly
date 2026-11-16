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

import { createAuthClient } from "better-auth/client";
const auth = createAuthClient();
const status = document.querySelector("#status")!;
const list = document.querySelector("#documents")!;
function report(error: unknown) {
  status.textContent = error instanceof Error ? error.message : String(error);
}
async function load(query = "") {
  const response = await fetch(`/api/documents${query ? `?q=${encodeURIComponent(query)}` : ""}`);
  if (!response.ok) throw new Error("Sign in to view your documents");
  const rows = await response.json();
  list.replaceChildren();
  for (const row of rows) {
    const li = document.createElement("li");
    li.textContent = `${row.title}: ${row.body}`;
    list.append(li);
  }
  status.textContent = "";
}
document.querySelector<HTMLFormElement>("#login")!.onsubmit = async (event) => {
  event.preventDefault();
  const data = new FormData(event.currentTarget as HTMLFormElement);
  const input = { email: String(data.get("email")), password: String(data.get("password")) };
  try {
    const result =
      (event.submitter as HTMLButtonElement).value === "signup"
        ? await auth.signUp.email({ ...input, name: input.email })
        : await auth.signIn.email(input);
    if (result.error) throw new Error(result.error.message);
    await load();
  } catch (error) {
    report(error);
  }
};
document.querySelector<HTMLButtonElement>("#logout")!.onclick = async () => {
  await auth.signOut();
  list.replaceChildren();
  report("Signed out");
};
document.querySelector<HTMLFormElement>("#document")!.onsubmit = async (event) => {
  event.preventDefault();
  try {
    const data = Object.fromEntries(new FormData(event.currentTarget as HTMLFormElement));
    const response = await fetch("/api/documents", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    if (!response.ok) throw new Error("Unable to save document");
    await load();
  } catch (error) {
    report(error);
  }
};
document.querySelector<HTMLFormElement>("#search")!.onsubmit = (event) => {
  event.preventDefault();
  void load(String(new FormData(event.currentTarget as HTMLFormElement).get("query"))).catch(
    report
  );
};
void load().catch(report);
