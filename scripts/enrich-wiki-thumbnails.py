#!/usr/bin/env python3
"""Enrich wiki-articles-1000.json with Wikipedia thumbnail URLs.

Downloads the original dataset, queries the Wikipedia API for page thumbnails
(in batches of 50), downloads the actual thumbnail images, and outputs an
enriched JSONL file with thumbnail_url fields pointing to cdn.antfly.io.

Usage:
    uv run scripts/enrich-wiki-thumbnails.py

Outputs:
    datasets/wiki-articles-10k-v001.json          (JSONL, one article per line)
    datasets/wiki-articles-10k-v001-images/       (downloaded thumbnail images)
"""

import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

SOURCE_URL = "http://fulmicoton.com/tantivy-files/wiki-articles-1000.json"
OUTPUT_DIR = "datasets"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wiki-articles-10k-v001.json")
IMAGES_DIR = os.path.join(OUTPUT_DIR, "wiki-articles-10k-v001-images")
CDN_BASE = "https://cdn.antfly.io/datasets/wiki-articles-10k-v001-images"
BATCH_SIZE = 50  # Wikipedia API max for pageimages
THUMB_WIDTH = 300
USER_AGENT = "AntflyDB-QuickstartDataset/1.0"


def fetch_articles(url: str) -> list[dict]:
    """Download and parse the source JSONL dataset."""
    print(f"Downloading {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as resp:
        lines = resp.read().decode("utf-8").strip().split("\n")
    articles = [json.loads(line) for line in lines]
    print(f"  Downloaded {len(articles)} articles")
    return articles


def fetch_thumbnails(titles: list[str], max_retries: int = 3) -> dict[str, str]:
    """Query Wikipedia API for thumbnail URLs for a batch of titles."""
    params = {
        "action": "query",
        "prop": "pageimages",
        "format": "json",
        "piprop": "thumbnail",
        "pithumbsize": THUMB_WIDTH,
        "titles": "|".join(titles),
    }
    url = "https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode(params)

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                wait = int(e.headers.get("Retry-After", 2))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise

    result = {}
    for page in data.get("query", {}).get("pages", {}).values():
        title = page.get("title", "")
        thumb = page.get("thumbnail", {})
        if thumb.get("source"):
            result[title] = thumb["source"]
    return result


def url_to_local_path(thumb_url: str) -> str:
    """Convert a Wikipedia thumbnail URL to a local filename.

    Preserves the full path under wikipedia/commons/thumb/ so we can
    re-download from the same URL structure later. Components longer
    than 200 bytes are replaced with a SHA-256 hash to avoid filesystem
    limits (macOS: 255 bytes per component).
    """
    parsed = urllib.parse.urlparse(thumb_url)
    # Decode percent-encoding so local filenames (and R2 keys) use real characters.
    # Without this, keys like "file_%281%29.jpg" won't match CDN URL decoding.
    decoded_path = urllib.parse.unquote(parsed.path)
    # Path like: /wikipedia/commons/thumb/d/d3/Albert_Einstein_Head.jpg/330px-...
    parts = decoded_path.lstrip("/").split("/")
    safe_parts = []
    for part in parts:
        if len(part.encode("utf-8")) > 200:
            ext = os.path.splitext(part)[1]
            part = hashlib.sha256(part.encode("utf-8")).hexdigest()[:32] + ext
        safe_parts.append(part)
    return "/".join(safe_parts)


def download_image(thumb_url: str, local_path: str, max_retries: int = 5) -> tuple[bool, int]:
    """Download a thumbnail image to a local path.

    Returns (success, retry_after) where retry_after is the seconds
    the server asked us to wait (0 if no rate limiting occurred).
    """
    full_path = os.path.join(IMAGES_DIR, local_path)
    if os.path.exists(full_path):
        return True, -1  # -1 signals cache hit, caller should skip sleep

    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    last_retry_after = 0
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(thumb_url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req) as resp:
                data = resp.read()
            with open(full_path, "wb") as f:
                f.write(data)
            return True, last_retry_after
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                last_retry_after = int(e.headers.get("Retry-After", 10))
                print(f"    Rate limited, waiting {last_retry_after}s... ({thumb_url[:60]})")
                time.sleep(last_retry_after)
            elif e.code == 404:
                return False, 0
            else:
                print(f"    Failed to download {thumb_url}: {e}")
                return False, 0
        except Exception as e:
            print(f"    Failed to download {thumb_url}: {e}")
            return False, 0
    print(f"    Gave up after {max_retries} retries: {thumb_url[:80]}")
    return False, last_retry_after


def main():
    os.makedirs(IMAGES_DIR, exist_ok=True)

    articles = fetch_articles(SOURCE_URL)

    # Build title -> article index
    title_to_articles: dict[str, list[dict]] = {}
    for article in articles:
        title_to_articles.setdefault(article["title"], []).append(article)

    all_titles = list(title_to_articles.keys())

    # Cache thumbnail URLs to avoid re-fetching from Wikipedia API
    thumb_cache = os.path.join(OUTPUT_DIR, ".thumbnails-cache.json")
    if os.path.exists(thumb_cache):
        print(f"Loading cached thumbnail URLs from {thumb_cache}...")
        with open(thumb_cache) as f:
            thumbnails = json.load(f)
        print(f"  Loaded {len(thumbnails)} cached thumbnail URLs")
    else:
        print(f"Fetching thumbnails for {len(all_titles)} unique titles...")

        # Fetch thumbnails in batches
        thumbnails: dict[str, str] = {}
        for i in range(0, len(all_titles), BATCH_SIZE):
            batch = all_titles[i : i + BATCH_SIZE]
            batch_thumbnails = fetch_thumbnails(batch)
            thumbnails.update(batch_thumbnails)

            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(all_titles) + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"  Batch {batch_num}/{total_batches}: {len(batch_thumbnails)}/{len(batch)} have thumbnails")

            # Be polite to Wikipedia API
            if i + BATCH_SIZE < len(all_titles):
                time.sleep(1.0)

        # Save cache
        with open(thumb_cache, "w") as f:
            json.dump(thumbnails, f)
        print(f"  Saved {len(thumbnails)} thumbnail URLs to cache")

    # Download thumbnail images
    # Count how many are already cached
    cached = sum(1 for thumb_url in thumbnails.values()
                 if os.path.exists(os.path.join(IMAGES_DIR, url_to_local_path(thumb_url))))
    to_download = len(thumbnails) - cached
    print(f"\nDownloading {to_download} thumbnail images ({cached} already cached)...")

    # Wait for rate limit cooldown after API batch phase
    if to_download > 0:
        print("  Waiting 30s for rate limit cooldown...")
        time.sleep(30)

    # Filter to only images that need downloading
    to_fetch = []
    for title, thumb_url in thumbnails.items():
        local_path = url_to_local_path(thumb_url)
        if not os.path.exists(os.path.join(IMAGES_DIR, local_path)):
            to_fetch.append((title, thumb_url, local_path))

    print(f"  {len(to_fetch)} images to download, {len(thumbnails) - len(to_fetch)} already cached")

    downloaded = 0
    failed = 0
    delay = 2.0  # adaptive delay between requests
    for i, (title, thumb_url, local_path) in enumerate(to_fetch):
        ok, retry_after = download_image(thumb_url, local_path)
        if ok:
            downloaded += 1
        else:
            failed += 1
            # Remove from thumbnails so article won't reference a missing image
            thumbnails[title] = None

        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(to_fetch)} downloaded ({downloaded} ok, {failed} failed, delay={delay:.0f}s)")

        # Skip sleep for cache hits (retry_after == 0 and file already existed)
        if retry_after < 0:
            continue

        # Adaptive backoff: if server sent Retry-After, increase delay
        if retry_after > 0:
            delay = min(max(delay, retry_after), 60.0)
        elif delay > 2.0:
            # Gradually reduce delay when requests succeed without rate limiting
            delay = max(2.0, delay * 0.9)

        time.sleep(delay)

    print(f"  Downloaded {downloaded} new images ({failed} failed)")

    # Enrich articles — rewrite thumbnail_url to CDN path
    with_thumb = 0
    without_thumb = 0
    with open(OUTPUT_FILE, "w") as f:
        for article in articles:
            thumb_url = thumbnails.get(article["title"])
            if thumb_url:
                local_path = url_to_local_path(thumb_url)
                # Percent-encode each path segment for the URL so the CDN
                # decodes it back to the actual R2 key.
                cdn_path = "/".join(
                    urllib.parse.quote(seg, safe="") for seg in local_path.split("/")
                )
                article["thumbnail_url"] = f"{CDN_BASE}/{cdn_path}"
                with_thumb += 1
            else:
                without_thumb += 1
            f.write(json.dumps(article, ensure_ascii=False) + "\n")

    print(f"\nWrote {OUTPUT_FILE}")
    print(f"  {with_thumb} articles with thumbnails ({with_thumb * 100 // len(articles)}%)")
    print(f"  {without_thumb} articles without thumbnails")

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"  File size: {size_mb:.1f} MB")

    # Count total image size
    total_image_bytes = 0
    image_count = 0
    for root, _dirs, files in os.walk(IMAGES_DIR):
        for fname in files:
            total_image_bytes += os.path.getsize(os.path.join(root, fname))
            image_count += 1
    print(f"\nImages: {image_count} files, {total_image_bytes / (1024 * 1024):.1f} MB in {IMAGES_DIR}/")


if __name__ == "__main__":
    main()
