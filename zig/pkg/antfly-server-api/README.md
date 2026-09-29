# Server API schemas

This Apache-2.0 package owns generated admin and internal API modules used by
the Antfly server. Their authored OpenAPI sources remain in `specs/openapi`.
Run `zig build regen-openapi` from `zig/` to regenerate the checked-in modules.

Embedded Lite, the C API, and standalone inference do not import this package.
Types shared with embedded local APIs are generated once under
`pkg/antfly-embedded` until the generator can emit server routers against an
external types module.
