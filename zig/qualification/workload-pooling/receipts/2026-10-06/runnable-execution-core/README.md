# Runnable execution core verification

Thirteen named scheduler tests plus the request-admission import test pass on
macOS ReleaseSafe and pinned Nix glibc GNU Linux ReleaseSafe. The complete logs
and tested core hashes are retained here. The production source-boundary audit
passes for 734 sources with no server coordination imports.

The added tests cover the single-worker execution capsule, checkpoint quanta,
original deadlines across parking and resumption, disabled execution, protected
control metadata at full ordinary count and byte limits, and promotion of a
parked completion drain without a new metadata reservation.

This receipt establishes core behavior only. The whole API pooling suite is
under verification; these results do not establish protocol integration, backend
wait release, parallel child ownership, distributed uncertainty accounting,
performance qualification, or automatic defaults. The full design remains active.

The canonical OpenAPI comparison also passes (85/85 build steps; 2/2 generator
tests). This checks generated-source consistency, not the runtime configuration
parser or production scheduler integration.
