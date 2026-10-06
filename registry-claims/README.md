# Antfly registry setup

The following crates.io names were published as `0.0.0` setup packages on
2026-10-05. These packages contain no runtime API; functional releases come
from the corresponding Rust workspace crates.

| crates.io package | Functional source |
| --- | --- |
| `antfly-sdk` | `rs/crates/sdk` |
| `antfly-embedded` | `rs/crates/embedded` |
| `antfly-embedded-sys` | `rs/crates/embedded-sys` |
| `antfly-postgres` | `rs/crates/postgres` |

The PostgreSQL extension and query-builder schema are `antfly_postgres`.
The unrelated registry package `pgaf` is not an Antfly package.

Crates.io uses GitHub users and teams as owners. Use the existing
`github:antflydb:engineering` team; no separate crates.io organization is
needed. On 2026-10-05, all four crates were verified to have both `ajroetker`
and `github:antflydb:engineering` as owners. A personal owner must remain for
owner administration, which team owners cannot perform. The authenticated crates.io account must be a member
of that GitHub team and authorize crates.io to read the organization.

Verify each crate and add the team if absent:

```sh
cargo owner --list antfly-sdk
cargo owner --add github:antflydb:engineering antfly-sdk
```

Repeat for each package in the table. Check the actual registry owner list;
this repository does not itself grant registry access.

## npm

The following packages have `0.0.0` setup versions and trusted publishers for
repository `antflydb/antfly`, workflow `embedded-release-publish.yml`, environment
`npm`:

- `@antfly/embedded`
- `@antfly/embedded-darwin-arm64`
- `@antfly/embedded-linux-arm64`
- `@antfly/embedded-linux-x64`

These setup versions have no runtime code or native binaries. Functional
releases are produced and verified by the release workflows before publishing.
To inspect an existing configuration:

```sh
npm trust list @antfly/embedded
```

If missing, an authorized npm account with 2FA can configure it:

```sh
npm trust github @antfly/embedded --file embedded-release-publish.yml \
  --repo antflydb/antfly --env npm --allow-publish --yes
```

Repeat for the platform packages. PyPI pending publisher configuration is
separate; see [licensing maintenance](../docs/reference/licensing.md).
