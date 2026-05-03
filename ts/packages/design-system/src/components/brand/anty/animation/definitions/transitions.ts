/**
 * Transition Animation Definitions
 * Pure functions that create GSAP timelines for state transitions (wake-up, power-off)
 */

import gsap from "gsap";
import type { EyeStyle } from "../types";
import { getEyeDimensions, getEyeShape } from "./eye-shapes";

export interface TransitionAnimationElements {
  character: HTMLElement;
  shadow: HTMLElement;
  innerGlow?: HTMLElement;
  outerGlow?: HTMLElement;
  // Eye elements for morphing
  eyeLeft?: HTMLElement;
  eyeRight?: HTMLElement;
  eyeLeftPath?: SVGPathElement;
  eyeRightPath?: SVGPathElement;
  eyeLeftSvg?: SVGSVGElement;
  eyeRightSvg?: SVGSVGElement;
}

/**
 * Glow lag configuration
 * Glows follow character at 75% distance with 0.05s delay
 */
const _GLOW_DISTANCE_RATIO = 0.75;
const _GLOW_LAG_SECONDS = 0.05;

/**
 * Creates wake-up animation (OFF → ON transition)
 *
 * "Blink Awake" animation (~0.5s):
 * 1. Body smoothly rises (scale 0.65→1, y 50→0, opacity 0.45→1)
 * 2. Eyes snap from OFF arrows to CLOSED partway through
 * 3. Eyes morph from CLOSED → IDLE (opening like waking up)
 *
 * @param elements - Character, shadow, and optional glow elements
 * @param sizeScale - Scale factor for the character (size / 160)
 * @returns GSAP timeline for wake-up animation
 */
export interface TransitionScaleOptions {
  /** Character scale while active (default: 1) */
  activeScale?: number;
  /** Character scale while off (default: 0.65) */
  offScale?: number;
}

export function createWakeUpAnimation(
  elements: TransitionAnimationElements,
  sizeScale: number = 1,
  scaleOptions: TransitionScaleOptions = {},
  eyeStyle: EyeStyle = "alive"
): gsap.core.Timeline {
  const { activeScale = 1, offScale = 0.65 } = scaleOptions;
  const sameSize = activeScale === offScale;
  const {
    character,
    shadow,
    eyeLeft,
    eyeRight,
    eyeLeftPath,
    eyeRightPath,
    eyeLeftSvg,
    eyeRightSvg,
  } = elements;
  // NOTE: Glow animations removed - GlowSystem handles fade in via animation controller

  const timeline = gsap.timeline();

  // Kill any existing animations on character, shadow, eyes
  // NOTE: Don't kill glow tweens - GlowSystem manages glow animations
  gsap.killTweensOf([character, shadow]);
  if (eyeLeft && eyeRight) {
    gsap.killTweensOf([eyeLeft, eyeRight]);
  }
  if (eyeLeftPath && eyeRightPath) {
    gsap.killTweensOf([eyeLeftPath, eyeRightPath]);
  }

  // ============================================
  // WAKE-UP: Mirror of power-off sequence
  // ============================================
  // OFF does: climb to -60 (0.5s) → snap to 50 (0.1s) → fade
  // ON does:  snap to -60 (0.12s) → hang → settle to 0 (0.5s)
  // Scale all durations by sizeScale so smaller characters animate faster
  // ============================================

  // Phase 1: POP UP - quick but readable rise to apex (skipped when same size)
  if (sameSize) {
    // No size difference — just settle in place. Keeps position + size stable.
    timeline.fromTo(
      character,
      { opacity: 1, scale: offScale, y: 0, x: 0, rotation: 0 },
      { scale: activeScale, duration: 0.25, ease: "power2.out" }
    );
  } else {
    timeline.fromTo(
      character,
      {
        opacity: 1,
        scale: offScale,
        y: 50,
        x: 0,
        rotation: 0,
      },
      {
        scale: activeScale,
        y: -40,
        duration: 0.25,
        ease: "power3.out",
      }
    );

    // Phase 3: SETTLE DOWN - smooth descent to idle
    timeline.to(
      character,
      {
        y: 0,
        duration: 0.35,
        ease: "power2.inOut",
        clearProps: "willChange",
      },
      0.75
    );
  }

  if (sameSize) {
    // Shadow: simple fade-in, no size choreography
    timeline.fromTo(
      shadow,
      { xPercent: -50, scaleX: 1, scaleY: 1, opacity: 0 },
      { opacity: 0.7, duration: 0.25, ease: "power2.out" },
      0
    );
  } else {
    // Shadow: starts small/faint, shrinks during rise
    timeline.fromTo(
      shadow,
      {
        xPercent: -50,
        scaleX: 0.65,
        scaleY: 0.65,
        opacity: 0,
      },
      {
        scaleX: 0.5,
        scaleY: 0.35,
        opacity: 0.3,
        duration: 0.25,
        ease: "power3.out",
      },
      0
    );

    // Shadow: grows as character settles
    timeline.to(
      shadow,
      {
        scaleX: 1,
        scaleY: 1,
        opacity: 0.7,
        duration: 0.35,
        ease: "power2.inOut",
      },
      0.75
    );
  }

  // ============================================
  // EYES: Snap to IDLE, then blink-awake before settle
  // Skipped for original eye style — triangles stay as-is, only body moves
  // ============================================
  if (
    eyeStyle !== "original" &&
    eyeLeftPath &&
    eyeRightPath &&
    eyeLeftSvg &&
    eyeRightSvg &&
    eyeLeft &&
    eyeRight
  ) {
    const idleDimensions = getEyeDimensions("IDLE");

    // Kill any existing eye tweens
    gsap.killTweensOf([eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg, eyeLeft, eyeRight]);

    // Start with HALF eyes (half-open) positioned higher
    const halfDimensions = getEyeDimensions("HALF");

    timeline.set(eyeLeftPath, { attr: { d: getEyeShape("HALF", "left") } }, 0);
    timeline.set(eyeRightPath, { attr: { d: getEyeShape("HALF", "right") } }, 0);
    timeline.set([eyeLeftSvg, eyeRightSvg], { attr: { viewBox: halfDimensions.viewBox } }, 0);
    timeline.set(
      [eyeLeft, eyeRight],
      {
        width: halfDimensions.width * sizeScale,
        height: halfDimensions.height * sizeScale,
        x: 0,
        y: -10 * sizeScale, // Higher up (scaled)
        rotation: 0,
        scaleX: 1,
        scaleY: 1,
      },
      0
    );

    // Morph to IDLE eyes — timing depends on whether we did the climb choreography
    const eyeMorphTime = sameSize ? 0 : 0.75;
    timeline.to(
      eyeLeftPath,
      { attr: { d: getEyeShape("IDLE", "left") }, duration: 0.25, ease: "power2.inOut" },
      eyeMorphTime
    );
    timeline.to(
      eyeRightPath,
      { attr: { d: getEyeShape("IDLE", "right") }, duration: 0.25, ease: "power2.inOut" },
      eyeMorphTime
    );
    timeline.to(
      [eyeLeftSvg, eyeRightSvg],
      { attr: { viewBox: idleDimensions.viewBox }, duration: 0.25, ease: "power2.inOut" },
      eyeMorphTime
    );
    timeline.to(
      [eyeLeft, eyeRight],
      {
        width: idleDimensions.width * sizeScale,
        height: idleDimensions.height * sizeScale,
        y: 0,
        duration: 0.35,
        ease: "power2.inOut",
      },
      eyeMorphTime
    );
  }

  return timeline;
}

/**
 * Creates power-off animation (ON → OFF transition)
 *
 * Dramatic three-phase choreography:
 * 1. Climb up (0.5s) - controlled rise to y: -60
 * 2. SNAP down HARD (0.1s) - explosive drop to y: 50, scale: 0.65 with expo.in easing
 * 3. Fade out (0.05-0.06s) - character to 0.45 opacity, glows/shadow to 0
 *
 * Shadow shrinks to 0.65 (NOT zero) and stays on ground
 * Glows follow character movement and fade out
 *
 * Total duration: ~0.66s
 *
 * @param elements - Character, shadow, and optional glow elements
 * @returns GSAP timeline for power-off animation
 */
export function createPowerOffAnimation(
  elements: TransitionAnimationElements,
  sizeScale: number = 1,
  scaleOptions: TransitionScaleOptions = {},
  eyeStyle: EyeStyle = "alive"
): gsap.core.Timeline {
  const { activeScale = 1, offScale = 0.65 } = scaleOptions;
  const sameSize = activeScale === offScale;
  const {
    character,
    shadow,
    eyeLeft,
    eyeRight,
    eyeLeftPath,
    eyeRightPath,
    eyeLeftSvg,
    eyeRightSvg,
  } = elements;
  // NOTE: Glow animations removed - GlowSystem handles fade out via animation controller

  const timeline = gsap.timeline();

  // Kill any existing animations
  // NOTE: Don't kill glow tweens - GlowSystem manages glow animations
  gsap.killTweensOf([character, shadow]);

  // NOTE: Duration scaling disabled - adjust DURATION_SCALE_FACTOR in emotion-interpreter.ts if needed

  if (sameSize) {
    // No size/position change — quick settle to offScale in place
    timeline.to(character, {
      scale: offScale,
      duration: 0.2,
      ease: "power2.inOut",
    });
    // Shadow: just fade out in place
    timeline.to(shadow, { xPercent: -50, opacity: 0, duration: 0.2, ease: "power2.inOut" }, 0);
  } else {
    // Phase 1: Climb up (0.5s) - eyes stay as idle
    timeline.to(character, {
      y: -60,
      duration: 0.5,
      ease: "power2.out",
    });

    // Phase 2: SNAP down HARD - super fast shrink (0.1s)
    timeline.to(character, {
      y: 50,
      scale: offScale,
      duration: 0.1,
      ease: "expo.in",
    });

    // Phase 2c: Shadow shrinks but stays on ground (no Y movement)
    timeline.to(
      shadow,
      {
        xPercent: -50,
        scaleX: 0.65,
        scaleY: 0.65,
        duration: 0.1,
        ease: "expo.in",
      },
      `-=0.1`
    );

    // Phase 3b: Fade out shadow
    timeline.to(
      shadow,
      {
        opacity: 0,
        duration: 0.06,
        ease: "power2.in",
      },
      `-=0.05`
    );
  }

  // CRITICAL: Freeze rotation at 0° for logo state
  timeline.set(character, { rotation: 0 }, ">");

  // INSTANT SNAP: Set eyes to OFF/logo shape at the end
  // Using gsap.set (instant, no morphing) to avoid glitchy point flipping
  if (eyeLeftPath && eyeRightPath && eyeLeftSvg && eyeRightSvg && eyeLeft && eyeRight) {
    const offDimensions = getEyeDimensions("OFF_LEFT"); // Same for both sides

    // Kill any existing eye tweens to prevent conflicts
    gsap.killTweensOf([eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg, eyeLeft, eyeRight]);

    // Snap to OFF shapes instantly (no animation = no morphing glitches)
    timeline.set(eyeLeftPath, { attr: { d: getEyeShape("OFF_LEFT", "left") } }, ">");
    timeline.set(eyeRightPath, { attr: { d: getEyeShape("OFF_RIGHT", "right") } }, "<");

    // Update viewBox to match OFF dimensions
    timeline.set([eyeLeftSvg, eyeRightSvg], { attr: { viewBox: offDimensions.viewBox } }, "<");

    // Update container dimensions and position for logo state
    // Original eyes stay in place (no bunching); alive eyes move closer together
    const eyeOffsetX = eyeStyle === "original" ? 0 : 3;
    const eyeOffsetY = eyeStyle === "original" ? 0 : 3;

    timeline.set(
      eyeLeft,
      {
        width: offDimensions.width * sizeScale,
        height: offDimensions.height * sizeScale,
        rotation: 0,
        scaleX: 1,
        scaleY: 1,
        x: eyeOffsetX * sizeScale, // Move right toward center
        y: eyeOffsetY * sizeScale,
      },
      "<"
    );

    timeline.set(
      eyeRight,
      {
        width: offDimensions.width * sizeScale,
        height: offDimensions.height * sizeScale,
        rotation: 0,
        scaleX: 1,
        scaleY: 1,
        x: -eyeOffsetX * sizeScale, // Move left toward center
        y: eyeOffsetY * sizeScale,
      },
      "<"
    );
  }

  return timeline;
}
