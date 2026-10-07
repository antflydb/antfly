# ARC runner disk measurements

The action samples filesystem usage every five seconds from checkout through
the job's post steps. Its job summary records the starting usage, maximum
observed usage, and minimum available space for the cache PVC (when mounted),
workspace, and runner temp paths. It uses `statfs`, so the measurement adds no
recursive directory walk to build jobs. Workspace and runner temp may be on the
same filesystem; their rows then describe the same capacity rather than two
independent allocations.

Use the peak and minimum-free values across several successful and failing runs
of each job before changing a PVC or scratch limit. A PVC may also contain
cache data left by earlier jobs, so compare the start and peak values instead
of treating peak usage as this job's own footprint. The result is an observed
sample, not an upper bound on a short-lived spike between samples. Jobs with
no cache PVC still report scratch headroom, which is required before moving
more jobs to the PVC-free `arc-antfly-heavy-runtime` profile.

The base and full x86 unit jobs keep their disposable `zig-local` caches and
reusable global dependencies on the cache PVC. Runner temp counts toward the
pod's 10 GiB ephemeral-storage limit; its reported filesystem capacity does
not establish the pod's storage budget. A failing run filled the measured
49 GiB PVC, but filesystem usage alone does not identify the size of compiler
outputs versus other cache contents. Completed phases retire local outputs
and manifests together before starting the next phase, and the full job also
removes its local cache on exit. Measure peak usage with this cleanup before
increasing storage or splitting the unit suite. Do not delete individual
executables while leaving live manifests, or remove a phase cache while one
of its builds, tests, or inventory consumers is still running.
