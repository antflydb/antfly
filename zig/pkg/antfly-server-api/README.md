# Server API schemas

This Apache-2.0 package owns generated admin, internal, public, metadata, and auth
server API modules used by the Antfly server. Their authored OpenAPI sources
remain in `specs/openapi`.
Run `zig build regen-openapi` from `zig/` to regenerate the checked-in modules.

Embedded Lite, the C API, and standalone inference do not import this package.
Public, metadata, and auth types shared with embedded local APIs are generated
once under `pkg/antfly-embedded`. Their server extractors and routers import
those existing types through the generator's `--external-types-module` option.
