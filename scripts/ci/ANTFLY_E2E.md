# Required Antfly E2E timing budget

The executable build remains a separate dependency, with its existing configuration.
The goal for tests after executable availability is less than 25 minutes.

## Partition and isolation

`e2e-base-plan` collects the exact required marker selection once. It balances whole
scheduler isolation groups across three ordinary lanes and three recovery lanes.
Every lane downloads the same immutable plan and timing baseline. A collection
mismatch fails rather than silently dropping a new test. The partition regressions
check exhaustive coverage, unique membership, fixture-group preservation, and
collection-order independence. Six-process recovery cases remain fresh and intact.
Each test runner retains one process-owning worker and diskful test roots.

The planner restores the latest common timing history. The checked-in seed comes
from successful run 36260644890: ordinary costs are approximate gaps between
process-test results (including adjacent teardown); recovery costs are pytest
setup/call/teardown totals. All subsequent observations use actual phase totals.
Unknown groups get conservative defaults. History is merged from disjoint lanes
and cached only after the required test lanes and low-FD gate pass.

## Results and activation

Every lane retains JUnit, duration history, and Antfly phase measurements on success
or failure, with slowest fixture groups in its Actions summary. The low-FD job
retains its regression XML separately. Low-FD regressions remain required by the
aggregate gate but run concurrently instead of following ordinary tests.

Approved PR CI executes the trusted workflow from main. The new job topology takes
effect after this workflow lands on main. Until then, the candidate helper preserves
the complete legacy ordinary + two recovery lane partition. Local `all` selections
remain complete and require no plan.

## Consolidation boundaries

Literal catalog-name cases reuse the existing resettable stateful fixture. Exact
number spelling variants use distinct tables within fresh FK/no-FK and
coordinated/uncoordinated scenarios; all original wire assertions remain. Auth
policy permutations, recovery faults, and transaction lifecycle tests retain fresh
runtime isolation. Pure harness tests already run independently of process slots;
there is no coverage reduction or flaky-case retry policy in this change.

## Validate the budget

Measure test-job startup, phase totals, longest group, and queue delay separately
from executable compilation. Compare several consecutive Linux runs and their
slow tail with the successful baseline: ordinary job 37:24; recovery jobs 23:38 and
22:43. Seed estimates predict about 11 minutes of ordinary work and 14 minutes of
recovery work per lane, before setup. These are projections, not a demonstrated
25-minute guarantee. Ensure runner CPU and disk capacity can sustain the extra
lanes; increasing same-host storage contention can erase the benefit.
