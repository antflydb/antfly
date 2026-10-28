# Current credential policy snapshots

The UserManager snapshot captures current permissions, layered row filters,
transitive roles, metadata, and persistent account identity under one mutation
lock. It carries no password verifier or bearer secret and does not authenticate
a caller. Credential validation and comparison against admitted authority remain
caller responsibilities.

Native macOS qualification used Zig 0.17.0 in ReleaseSafe mode:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/antfly-default-pooling-global \
/Users/ajroetker/bin/zig-0.17.0/zig build antfly-usermgr-test \
-Doptimize=ReleaseSafe -j1 --cache-dir /private/tmp/antfly-default-pooling-cache --summary all
```

The complete standalone target passed all 22 build steps. It executed 16
embedded user-manager/authority tests, four server storage-adapter tests, and
one independent archive ABI test. A strict direct invocation of the local test
artifact with `--suite-filter 'usermgr credential policy snapshots'` executed
the two new named tests; neither was skipped and no allocations leaked.

The new tests cover role and filter changes, retained snapshot ownership,
revocation, account deletion/recreation, exact Basic filter entries (including
null overrides), owner/key filter layering, API-key
revocation/deletion/expiry, and every injected allocation failure for both
Basic and API-key snapshots with transitive roles. The allocation sweep found
and verifies fixes for partial policy-copy ownership in UserManager and Casbin.
The standalone owner now imports the memory ABI needed by its storage fixtures.

The same standalone target passed on GNU AArch64 with the pinned Nix/glibc
Zig 0.17.0 image, two CPUs, and 4 GiB memory with no extra swap: 21/21 build
steps, the same 16 embedded and four server tests, and one archive ABI test.

The embedded production boundary audit passed for 736 authored sources.
Compressed logs and exact source hashes are adjacent. These checks do not
qualify HTTP/PG resume integration, distributed policy fences,
or scheduler defaults; those remain separate requirements.
