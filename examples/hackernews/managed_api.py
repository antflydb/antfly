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

"""Fixed managed API destinations and server-owned credentials for HN tools."""

import ipaddress
import os
import re
from urllib.parse import unquote, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward a tenant credential or retry a write at another URL.
        return None


_opener = build_opener(NoRedirect())


def open_request(request, *, timeout):
    return _opener.open(request, timeout=timeout)


def validate_backend(value, authenticated=False):
    try:
        parsed = urlsplit(value)
        port = parsed.port
        host = parsed.hostname
    except ValueError:
        raise ValueError("Invalid Antfly API base") from None
    if (
        not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(ord(c) < 33 or ord(c) == 127 for c in value)
        or "\\" in value
        or any(p in (".", "..") for p in unquote(parsed.path).split("/"))
        or not parsed.path.rstrip("/").endswith("/db/v1")
        or (port is not None and port == 0)
    ):
        raise ValueError("Expected an Antfly API base ending in /db/v1")
    loopback = host == "localhost"
    try:
        loopback |= ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and loopback and not authenticated
    ):
        raise ValueError("Authenticated or remote Antfly APIs require HTTPS")
    return value.rstrip("/")


def authorization_headers(environ=None):
    environ = os.environ if environ is None else environ
    full = environ.get("ANTFLY_AUTHORIZATION", "")
    key = environ.get("ANTFLY_API_KEY", "")
    if full and key:
        raise ValueError("Configure only one Antfly authorization variable")
    value = full or ("Bearer " + key if key else "")
    if value and (
        len(value) > 8192 or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]* [!-~]+", value)
    ):
        raise ValueError("Invalid Antfly authorization header")
    return {"Authorization": value} if value else {}
