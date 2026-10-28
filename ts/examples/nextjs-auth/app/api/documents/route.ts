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

import { APIError } from "better-auth/api";
import { getAuth } from "../../../auth";
export const runtime = "nodejs";
function failure(error: unknown) {
  return Response.json(
    { error: "Sign in or try again later" },
    { status: error instanceof APIError ? error.statusCode : 500 }
  );
}
export async function GET(request: Request) {
  try {
    const mine = await (await getAuth()).requireUser(request);
    const query = new URL(request.url).searchParams.get("q");
    return Response.json(
      query ? await mine.documents.searchText({ query }) : await mine.documents.list()
    );
  } catch (error) {
    return failure(error);
  }
}
export async function POST(request: Request) {
  if (request.headers.get("origin") !== (process.env.APP_ORIGIN ?? "http://localhost:3000"))
    return new Response(null, { status: 403 });
  try {
    const mine = await (await getAuth()).requireUser(request);
    return Response.json(await mine.documents.create(await request.json()));
  } catch (error) {
    return failure(error);
  }
}
