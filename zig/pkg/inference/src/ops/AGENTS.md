# Native kernel programs

- Name new shared kernels, helpers, source owners and launch APIs for their
  operation, physical layout, precision boundaries and static schedule. Do not
  add a model name to a reusable native program.
- Keep model-specific tensor names, configuration interpretation and checkpoint
  conventions in model adapters. Pass typed workload/layout metadata to planners.
- Extend an existing program family before adding another body. Preserve tuned
  tiles, vector widths, reduction orders, scratch lifetimes, fusion and explicit
  rounding as static policies. A shared family can retain several schedules.
- Historical graph names and exported device symbols can remain compatibility
  adapters; document that role. They are not templates for naming new programs.
- Validate changed tuned programs against frozen originals and independent
  references, then compare native resources and hardware performance. Keep
  failed prototypes and timing controls in the qualification report.
- Count every new production owner. Renames, generated copies and moving text
  between host/device files do not earn source-reduction credit.

See `docs/design/inference/kernel-authoring.md` for the family map and workflow.
