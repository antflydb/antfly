import type { AntflyClient } from "@antfly/sdk";
import { AssetNotFoundError, resolveCatalogResource } from "./catalog.js";
import { type AntflyAssetReference, decodeAssetReference } from "./reference.js";

/**
 * A browser-display route helper for the pattern settled on antfly#1491: a
 * route in the customer's own app authenticates that app's own user, checks
 * access against the catalog, and streams bytes from the server side, with
 * `Range`/`If-Match`/`If-None-Match`/`If-Range` passed through. `FileUIPart.url`
 * points at this route (not at Antfly), so the source companion's capability
 * URL never reaches the browser or a model, and Antfly never fetches through
 * this route -- it only reads from it.
 *
 * This is a plain `Request -> Response` handler (the Fetch API types), so it
 * drops into any framework that exposes that shape (Next.js route handlers,
 * Hono, etc.) without a framework dependency here.
 */
export interface AssetDisplayRouteOptions {
  client: AntflyClient;
  /**
   * The app's own authorization decision for this request -- e.g. checking
   * the caller's session against the asset's tenant/permissions. Resolve or
   * reject before this handler touches the catalog or the source companion.
   * Returning `false` (or throwing) responds 403.
   */
  authorize: (request: Request, reference: AntflyAssetReference) => Promise<boolean> | boolean;
  /**
   * Extracts the opaque `antfly` provider-reference id from the request
   * (e.g. a validated route param), never trusting a client-supplied table
   * or key directly (antfly#1505: "clients only send opaque IDs").
   */
  referenceId: (request: Request) => string;
  /** Defaults to `globalThis.fetch`. */
  fetch?: typeof fetch;
}

// Only passive formats may render on the application's authenticated origin.
const INLINE_MEDIA_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
  "image/avif",
  "image/bmp",
  "video/mp4",
  "video/webm",
  "video/quicktime",
  "video/ogg",
  "audio/mpeg",
  "audio/mp4",
  "audio/wav",
  "audio/ogg",
  "audio/webm",
  "audio/flac",
]);

const FORWARDED_REQUEST_HEADERS = ["range", "if-match", "if-none-match", "if-range"];
const FORWARDED_RESPONSE_HEADERS = [
  "content-type",
  "content-length",
  "content-range",
  "accept-ranges",
  "etag",
  "last-modified",
];

/** Builds the `(Request) => Promise<Response>` handler for one app route. */
export function createAssetDisplayHandler(
  options: AssetDisplayRouteOptions
): (request: Request) => Promise<Response> {
  const fetchFn = options.fetch ?? globalThis.fetch;
  return async (request: Request): Promise<Response> => {
    let reference: AntflyAssetReference;
    try {
      reference = decodeAssetReference(options.referenceId(request));
    } catch {
      return new Response("Not found", { status: 404 });
    }

    let authorized: boolean;
    try {
      authorized = await options.authorize(request, reference);
    } catch {
      authorized = false;
    }
    if (!authorized) return new Response("Forbidden", { status: 403 });

    let resource: Awaited<ReturnType<typeof resolveCatalogResource>>;
    try {
      resource = await resolveCatalogResource(options.client, reference);
    } catch (error) {
      if (error instanceof AssetNotFoundError) return new Response("Not found", { status: 404 });
      throw error;
    }

    const upstreamHeaders = new Headers();
    for (const name of FORWARDED_REQUEST_HEADERS) {
      const value = request.headers.get(name);
      if (value !== null) upstreamHeaders.set(name, value);
    }
    // `resource.uri` is the capability URL; it is read here, server-side,
    // and in no other field of this response.
    const upstream = await fetchFn(resource.uri, {
      headers: upstreamHeaders,
      signal: request.signal,
    });

    const headers = new Headers();
    for (const name of FORWARDED_RESPONSE_HEADERS) {
      const value = upstream.headers.get(name);
      if (value !== null) headers.set(name, value);
    }
    if (!headers.has("content-type")) headers.set("content-type", resource.mime_type);
    const mediaType = headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() ?? "";
    const inline = INLINE_MEDIA_TYPES.has(mediaType);
    headers.set("content-disposition", inline ? "inline" : "attachment");
    if (!inline) headers.set("content-security-policy", "sandbox; default-src 'none'");
    headers.set("cache-control", "private, no-store");
    headers.set("x-content-type-options", "nosniff");

    return new Response(upstream.body, { status: upstream.status, headers });
  };
}
