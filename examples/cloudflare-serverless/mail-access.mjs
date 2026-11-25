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
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/");
  return Uint8Array.from(atob(base64.padEnd(Math.ceil(base64.length / 4) * 4, "=")), (c) => c.charCodeAt(0));
}

export function accessConfig(env) {
  if (!env.MAIL_ACCESS_ISSUER && !env.MAIL_ACCESS_AUDIENCE && !env.MAIL_TEAM_ADDRESS) return null;
  if (!/^https:\/\/[a-z0-9-]+\.cloudflareaccess\.com$/.test(env.MAIL_ACCESS_ISSUER ?? "") ||
      !env.MAIL_ACCESS_AUDIENCE || !env.MAIL_TEAM_ADDRESS?.endsWith("@" + env.MAIL_DOMAIN))
    throw Error("MailAccessConfigurationRequired");
  const pinnedKeys = env.MAIL_ACCESS_JWKS ? JSON.parse(env.MAIL_ACCESS_JWKS).keys : [];
  if (!Array.isArray(pinnedKeys)) throw Error("MailAccessConfigurationRequired");
  return { issuer: env.MAIL_ACCESS_ISSUER, audience: env.MAIL_ACCESS_AUDIENCE, pinnedKeys };
}

async function signingKey(issuer, kid, pinnedKeys) {
  const pinned = pinnedKeys.find((key) => key.kid === kid && key.kty === "RSA");
  if (pinned) return importSigningKey(pinned);
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
  return importSigningKey(jwk);
}

function importSigningKey(jwk) {
  return crypto.subtle.importKey("jwk", jwk, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
}

export async function accessIdentity(token, { issuer, audience, pinnedKeys = [] }, onReject = () => {}) {
  function reject(reason) {
    onReject(reason);
    return null;
  }
  if (!token) return reject("AccessAssertionMissing");
  if (token.length > 16384) return reject("AccessAssertionMalformed");
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return reject("AccessAssertionMalformed");
    const [header, payload, signature] = parts;
    const decode = (part) => JSON.parse(new TextDecoder().decode(bytes(part)));
    const { alg, kid } = decode(header);
    const { iss, aud, exp, nbf, sub, email, type } = decode(payload);
    const now = Date.now() / 1000;
    if (iss !== issuer) return reject("AccessIssuerMismatch");
    if (!Array.isArray(aud) || !aud.includes(audience)) return reject("AccessAudienceMismatch");
    if (alg !== "RS256" || typeof kid !== "string" || !Number.isFinite(exp) || exp <= now ||
        (nbf !== undefined && (!Number.isFinite(nbf) || nbf > now)) ||
        type !== "app" || typeof sub !== "string" || !sub ||
        typeof email !== "string" || !email.includes("@")) return reject("AccessClaimsInvalid");
    let key;
    try {
      key = await signingKey(issuer, kid, pinnedKeys);
    } catch (error) {
      return reject(error.message === "UnknownAccessKey" ? "AccessSigningKeyUnknown" : "AccessSigningKeysUnavailable");
    }
    if (!(await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, bytes(signature),
      new TextEncoder().encode(header + "." + payload)))) return reject("AccessSignatureInvalid");
    return { id: "access:" + issuer + ":" + sub, email, name: email };
  } catch {
    return reject("AccessAssertionMalformed");
  }
}
