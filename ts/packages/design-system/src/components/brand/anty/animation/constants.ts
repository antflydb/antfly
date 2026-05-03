/**
 * Animation Constants
 *
 * Tuned values that coordinate idle float / breathing / rotation with the
 * shadow's inverse scale response. Changing one generally requires adjusting
 * the others — durations and eases are intentionally shared.
 */

/**
 * Idle float — gentle vertical bob. Duration MUST match IDLE_ROTATION and
 * SHADOW so rotation, breathing, and shadow scale all drift in phase.
 */
export const IDLE_FLOAT = {
  amplitude: 12,
  duration: 2.5,
  ease: "sine.inOut",
} as const;

/**
 * Idle rotation — subtle side-to-side tilt, phase-locked with IDLE_FLOAT.
 */
export const IDLE_ROTATION = {
  degrees: 2.0,
  duration: 2.5,
  ease: "sine.inOut",
  synchronized: true,
} as const;

/**
 * Idle breathing — independent scale pulse, intentionally offset from float
 * duration to avoid mechanical sync.
 */
export const IDLE_BREATHE = {
  scaleMin: 1.0,
  scaleMax: 1.02,
  duration: 3,
  ease: "sine.inOut",
} as const;

/**
 * Shadow — inverse relationship with character Y. When the character floats
 * up, the shadow scales down and fades (less ground contact). Shadow position
 * is fixed on the ground. Duration MUST match IDLE_FLOAT.
 */
export const SHADOW = {
  scaleXWhenUp: 0.7,
  scaleYWhenUp: 0.55,
  opacityWhenUp: 0.2,
  scaleXWhenDown: 1.0,
  scaleYWhenDown: 1.0,
  opacityWhenDown: 0.7,
  xPercent: -50,
  duration: 2.5,
  ease: "sine.inOut",
} as const;
