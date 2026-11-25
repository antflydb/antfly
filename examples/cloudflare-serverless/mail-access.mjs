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

const keysByIssuer = new Map();

function bytes(value) {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw Error("InvalidAccessToken");
  return Uint8Array.from(atob(value.replaceAll("-", "+").replaceAll("_", "/")), (c) => c.charCodeAt(0));
}

export function accessConfig(env) {
  if (!env.MAIL_ACCESS_ISSUER && !env.MAIL_ACCESS_AUDIENCE && !env.MAIL_TEAM_ADDRESS) return null;
  if (!/^https:\/\/[a-z0-9-]+\.cloudflareaccess\.com$/.test(env.MAIL_ACCESS_ISSUER ?? "") ||
      !env.MAIL_ACCESS_AUDIENCE || !env.MAIL_TEAM_ADDRESS?.endsWith("@" + env.MAIL_DOMAIN))
    throw Error("MailAccessConfigurationRequired");
  return { issuer: env.MAIL_ACCESS_ISSUER, audience: env.MAIL_ACCESS_AUDIENCE };
}

async function signingKey(issuer, kid) {
  let cached = keysByIssuer.get(issuer);
  if (!cached || cached.expires <= Date.now()) {
    const response = await fetch(issuer + "/cdn-cgi/access/certs", { redirect: "error" });
    if (!response.ok) throw Error("AccessKeysUnavailable");
    const { keys } = await response.json();
    cached = { keys, expires: Date.now() + 60_000 };
    keysByIssuer.set(issuer, cached);
  }
  const jwk = cached.keys.find((key) => key.kid === kid && key.kty === "RSA");
  if (!jwk) throw Error("UnknownAccessKey");
  return crypto.subtle.importKey("jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
}

export async function accessIdentity(token, { issuer, audience }) {
  if (!token || token.length > 16384) return null;
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const [header, payload, signature] = parts;
    const decode = (part) => JSON.parse(new TextDecoder().decode(bytes(part)));
    const { alg, kid } = decode(header);
    const { iss, aud, exp, nbf, sub, email, type } = decode(payload);
    const now = Date.now() / 1000;
    if (alg !== "RS256" || typeof kid !== "string" || iss !== issuer ||
        !Array.isArray(aud) || !aud.includes(audience) || !Number.isFinite(exp) || exp <= now ||
        (nbf !== undefined && (!Number.isFinite(nbf) || nbf > now)) ||
        type !== "app" || typeof sub !== "string" || !sub ||
        typeof email !== "string" || !email.includes("@")) return null;
    const key = await signingKey(issuer, kid);
    if (!(await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, bytes(signature),
      new TextEncoder().encode(header + "." + payload)))) return null;
    return { id: "access:" + issuer + ":" + sub, email, name: email };
  } catch {
    return null;
  }
}
