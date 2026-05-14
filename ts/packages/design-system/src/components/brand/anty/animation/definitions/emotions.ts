/**
 * Declarative Emotion Configurations
 *
 * All emotions defined as DATA, not code.
 * Each emotion is a configuration object that the interpreter uses
 * to build GSAP timelines.
 */

import type { EmotionConfig, EmotionType } from "../types";

/**
 * Glow coordination constants
 * Glows follow character at 75% distance with 0.05s lag
 */
export const GLOW_CONSTANTS = {
  DISTANCE_RATIO: 0.75,
  LAG_SECONDS: 0.05,
} as const;

/**
 * All emotion configurations
 */
export const EMOTION_CONFIGS: Partial<Record<EmotionType, EmotionConfig>> = {
  // ===========================
  // EXCITED (Level 4) - Jump + spin
  // For: Good accomplishments, victories, success
  // ===========================
  excited: {
    id: "excited",
    character: [
      { props: { y: 8 }, duration: 0.15, ease: "power2.in" },
      { props: { y: -50, rotation: 360 }, duration: 0.35, ease: "power2.out" },
      { props: { y: -50, rotation: 360 }, duration: 0.2, ease: "none" },
      { props: { y: 0 }, duration: 0.25, ease: "power2.in" },
      { props: { y: -8 }, duration: 0.1, ease: "power2.out" },
      { props: { y: 0 }, duration: 0.08, ease: "power2.in" },
    ],
    glow: { follow: true },
    totalDuration: 1.1,
    resetRotation: true,
  },

  // ===========================
  // SHOCKED - Jump with bracket separation
  // ===========================
  shocked: {
    id: "shocked",
    eyes: {
      shape: "IDLE",
      duration: 0.2,
      scale: 1.4,
      returnPosition: 1.3,
      returnDuration: 0.3,
    },
    character: [
      { props: { y: -30 }, duration: 0.2, ease: "power2.out" },
      { props: { y: 0 }, duration: 0.3, ease: "power2.in", position: 1.3 },
    ],
    body: {
      leftX: -15,
      leftY: -15,
      rightX: 15,
      rightY: 15,
      duration: 0.2,
      ease: "back.out(2)",
      returnPosition: 1.3,
      returnDuration: 0.3,
      returnEase: "power2.inOut",
    },
    glow: { follow: true },
    totalDuration: 1.6,
  },

  // ===========================
  // BACK-FORTH - Look left then right with "considering" eyes
  // ===========================
  "back-forth": {
    id: "back-forth",
    eyePhases: [
      { position: 0.2, shape: { left: "IDLE", right: "HALF" }, duration: 0.06 },
      { position: 1.1, shape: { left: "HALF", right: "IDLE" }, duration: 0.06 },
      { position: 1.9, shape: "IDLE", duration: 0.06 },
    ],
    character: [
      { props: { rotation: -8, x: -10 }, duration: 0.3, ease: "power2.out" },
      { props: { rotation: -8, x: -10 }, duration: 0.5, ease: "none" },
      { props: { rotation: 8, x: 10 }, duration: 0.4, ease: "power2.inOut" },
      { props: { rotation: 8, x: 10 }, duration: 0.5, ease: "none" },
      { props: { rotation: 0, x: 0 }, duration: 0.3, ease: "power2.in" },
    ],
    totalDuration: 2.0,
  },

  // ===========================
  // WINK - Asymmetric eye wink with tilt
  // ===========================
  wink: {
    id: "wink",
    eyes: {
      shape: { left: "CLOSED", right: "HALF" },
      duration: 0.08,
      yOffset: { left: 0, right: -10 },
    },
    character: [
      { props: { rotation: -3, y: -5 }, duration: 0.19, ease: "power1.inOut" },
      { props: { rotation: -3, y: -5 }, duration: 0.4, ease: "none" },
      { props: { rotation: 0, y: 0 }, duration: 0.25, ease: "power1.inOut" },
    ],
    totalDuration: 0.84,
    resetIdle: false,
  },

  // ===========================
  // NOD - Vertical head bob (yes)
  // ===========================
  nod: {
    id: "nod",
    character: [
      {
        props: { rotationX: -35, y: 8, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.out",
      },
      {
        props: { rotationX: 0, y: 0, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.inOut",
      },
      {
        props: { rotationX: -35, y: 8, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.out",
      },
      {
        props: { rotationX: 0, y: 0, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.inOut",
      },
      {
        props: { rotationX: -35, y: 8, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.out",
      },
      {
        props: { rotationX: 0, y: 0, transformPerspective: 600 },
        duration: 0.15,
        ease: "power2.in",
      },
    ],
    totalDuration: 0.9,
  },

  // ===========================
  // HEADSHAKE - Y-axis rotation shake (no)
  // ===========================
  headshake: {
    id: "headshake",
    character: [
      { props: { rotationY: -35, transformPerspective: 600 }, duration: 0.15, ease: "power4.out" },
      { props: { rotationY: 40, transformPerspective: 600 }, duration: 0.18, ease: "power4.inOut" },
      {
        props: { rotationY: -45, transformPerspective: 600 },
        duration: 0.18,
        ease: "power4.inOut",
      },
      { props: { rotationY: 50, transformPerspective: 600 }, duration: 0.18, ease: "power4.inOut" },
      {
        props: { rotationY: -50, transformPerspective: 600 },
        duration: 0.18,
        ease: "power4.inOut",
      },
      { props: { rotationY: 0, transformPerspective: 600 }, duration: 0.2, ease: "power2.inOut" },
    ],
    totalDuration: 1.07,
    resetRotationY: true,
  },

  // ===========================
  // LOOK-AROUND - Look left then right (eye-only)
  // ===========================
  "look-around": {
    id: "look-around",
    eyes: {
      shape: "LOOK",
      duration: 0.15,
      xOffset: -12,
      bunch: 4,
    },
    eyePhases: [
      { position: 1.1, shape: "LOOK", duration: 0.15, xOffset: 12, bunch: 4 },
      { position: 2.2, shape: "IDLE", duration: 0.15, xOffset: 0, bunch: 0 },
    ],
    character: [],
    totalDuration: 2.35,
    preserveIdle: true,
    resetIdle: false,
  },

  // ===========================
  // LOOK-LEFT - Eye-only look
  // ===========================
  "look-left": {
    id: "look-left",
    eyes: {
      shape: "LOOK",
      duration: 0.15,
      xOffset: -12,
      bunch: 4,
    },
    character: [],
    totalDuration: 0.9,
    holdDuration: 0.6,
  },

  // ===========================
  // LOOK-RIGHT - Eye-only look
  // ===========================
  "look-right": {
    id: "look-right",
    eyes: {
      shape: "LOOK",
      duration: 0.15,
      xOffset: 12,
      bunch: 4,
    },
    character: [],
    totalDuration: 0.9,
    holdDuration: 0.6,
  },
};

/**
 * Get emotion config by type
 */
export function getEmotionConfig(emotion: EmotionType): EmotionConfig | undefined {
  return EMOTION_CONFIGS[emotion];
}
