# Copyright 2026 Antfly, Inc.
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

"""Deployment-surface overlays applied while joining the public OpenAPI spec.

The standalone inference server has no built-in auth, so its source spec
(specs/openapi/inference/api.yaml) intentionally documents none. The unified
Antfly server fronts the same routes with its auth middleware, which can
reject a request before any inference handler runs. The overlays here
document that unified-deployment behavior on the joined public contract
only — do not add auth responses to the standalone inference spec, and do
not fold these into generic join machinery: they describe a deployment,
not a merge rule.
"""

from __future__ import annotations


def add_unified_auth_responses(path_item: dict, error_schema_ref: str) -> dict:
    """Document auth failures added by the unified Antfly middleware.

    Adds a 401 for rejected credentials and extends (or adds) the 503 to
    cover the auth backend not being ready. Existing responses win: 401 is
    only set if the operation does not already define one, and an existing
    503 keeps its description with the auth clause appended.
    """

    def auth_error_content() -> dict:
        return {
            "application/json": {
                "schema": {"$ref": error_schema_ref},
            },
        }

    for operation in path_item.values():
        if not isinstance(operation, dict):
            continue
        responses = operation.get("responses")
        if not isinstance(responses, dict):
            continue
        responses.setdefault(
            "401",
            {
                "description": "Authentication is enabled and valid credentials were not supplied",
                "content": auth_error_content(),
            },
        )
        auth_not_ready = (
            "The unified Antfly server also returns this status when authentication "
            "is enabled but its backend is not ready."
        )
        unavailable = responses.get("503")
        if isinstance(unavailable, dict):
            description = unavailable.get(
                "description", "Inference service unavailable"
            ).rstrip(".")
            unavailable["description"] = f"{description}. {auth_not_ready}"
        else:
            responses["503"] = {
                "description": auth_not_ready,
                "content": auth_error_content(),
            }
    return path_item
