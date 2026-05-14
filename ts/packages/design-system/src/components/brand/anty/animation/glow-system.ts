/**
 * Glow System - Physics-based glow tracking with snake-like oscillation
 *
 * Two glow layers (inner/outer) that:
 * 1. Track character position via spring physics (delayed, natural response)
 * 2. Oscillate on X-axis with phase offsets (snake-like wave effect)
 * 3. Chain together: Anty → Outer glow → Inner glow
 *
 * The chained springs + phase-offset oscillation creates an ethereal,
 * snake-like trailing effect behind the character.
 */

import gsap from "gsap";
import { ENABLE_ANIMATION_DEBUG_LOGS } from "./feature-flags";

// =============================================================================
// TYPES
// =============================================================================

export interface GlowSystemConfig {
  /** Spring stiffness for outer glow following character (0-1, higher = snappier) */
  outerStiffness: number;
  /** Spring stiffness for inner glow following outer glow (0-1, higher = snappier) */
  innerStiffness: number;
  /** Damping factor to prevent infinite oscillation (0-1, higher = more damping) */
  damping: number;

  /** X-axis oscillation amplitude for outer glow (pixels) */
  outerOscillationAmplitudeX: number;
  /** X-axis oscillation amplitude for inner glow (pixels) */
  innerOscillationAmplitudeX: number;
  /** Y-axis oscillation amplitude for outer glow (pixels) - bobs down from base */
  outerOscillationAmplitudeY: number;
  /** Y-axis oscillation amplitude for inner glow (pixels) - bobs down from base */
  innerOscillationAmplitudeY: number;
  /** Oscillation frequency (radians per second) */
  oscillationFrequency: number;
  /** Phase offset for inner glow oscillation (radians) */
  innerPhaseOffset: number;

  /** Base opacity when visible */
  visibleOpacity: number;
  /** Opacity when hidden */
  hiddenOpacity: number;
}

export interface GlowSystemControls {
  /** Start the tracking system (adds to GSAP ticker) */
  start: () => void;
  /** Stop the tracking system completely (removes from ticker) */
  stop: () => void;
  /** Pause tracking (keeps ticker but doesn't update) */
  pause: () => void;
  /** Resume tracking after pause */
  resume: () => void;
  /** Check if system is currently active */
  isActive: () => boolean;
  /** Fade glows in over duration (seconds) */
  fadeIn: (duration?: number) => gsap.core.Tween;
  /** Fade glows out over duration (seconds) */
  fadeOut: (duration?: number) => gsap.core.Tween;
  /** Instantly show glows */
  show: () => void;
  /** Instantly hide glows */
  hide: () => void;
  /** Reset positions to character position (no spring lag) */
  snapToCharacter: () => void;
  /** Update the size scale (recalculates oscillation amplitudes) */
  updateSizeScale: (newSizeScale: number) => void;
}

// =============================================================================
// SPRING STATE
// =============================================================================

interface SpringState {
  x: number;
  y: number;
  velocityX: number;
  velocityY: number;
}

// =============================================================================
// DEFAULT CONFIG
// =============================================================================

const DEFAULT_CONFIG: GlowSystemConfig = {
  // Spring physics
  outerStiffness: 0.08, // Outer glow follows character with slight lag
  innerStiffness: 0.12, // Inner glow follows outer (tighter, but still delayed)
  damping: 0.75, // Prevents excessive bouncing

  // Oscillation (snake wiggle + vertical bob)
  outerOscillationAmplitudeX: 40, // Outer glow swings ±40px horizontally
  innerOscillationAmplitudeX: 28, // Inner glow swings ±28px horizontally
  outerOscillationAmplitudeY: 18, // Outer glow bobs down 18px from base
  innerOscillationAmplitudeY: 10, // Inner glow bobs down 10px from base
  oscillationFrequency: 0.22, // ~4.5 second full cycle
  innerPhaseOffset: 1.8, // ~103 degrees behind outer

  // Visibility
  visibleOpacity: 1,
  hiddenOpacity: 0,
};

// =============================================================================
// GLOW SYSTEM FACTORY
// =============================================================================

/**
 * Creates a glow tracking system with spring physics and oscillation
 *
 * @param character - The character element to track position from
 * @param outerGlow - The outer (larger, softer) glow element
 * @param innerGlow - The inner (smaller, tighter) glow element
 * @param sizeScale - Scale factor for oscillation amplitudes (size / 160)
 * @param config - Optional configuration overrides
 * @returns Controls for the glow system
 */
export function createGlowSystem(
  character: HTMLElement,
  outerGlow: HTMLElement,
  innerGlow: HTMLElement,
  sizeScale: number = 1,
  config: Partial<GlowSystemConfig> = {}
): GlowSystemControls {
  // Mutable config that can be updated when sizeScale changes
  let currentSizeScale = sizeScale;
  const cfg = {
    ...DEFAULT_CONFIG,
    // Scale oscillation amplitudes by sizeScale (frequency stays in Hz, not pixels)
    outerOscillationAmplitudeX: DEFAULT_CONFIG.outerOscillationAmplitudeX * currentSizeScale,
    innerOscillationAmplitudeX: DEFAULT_CONFIG.innerOscillationAmplitudeX * currentSizeScale,
    outerOscillationAmplitudeY: DEFAULT_CONFIG.outerOscillationAmplitudeY * currentSizeScale,
    innerOscillationAmplitudeY: DEFAULT_CONFIG.innerOscillationAmplitudeY * currentSizeScale,
    ...config,
  };

  // State
  let isRunning = false;
  let isPaused = false;
  let time = 0;

  // Spring states for each glow layer
  const outerSpring: SpringState = { x: 0, y: 0, velocityX: 0, velocityY: 0 };
  const innerSpring: SpringState = { x: 0, y: 0, velocityX: 0, velocityY: 0 };

  // =============================================================================
  // PHYSICS UPDATE
  // =============================================================================

  /**
   * Update spring physics and oscillation each frame
   */
  function update(): void {
    if (isPaused) return;

    // Get delta time from GSAP ticker (seconds)
    const deltaTime = gsap.ticker.deltaRatio() / 60;
    time += deltaTime;

    // Get character's current position
    const rawX = gsap.getProperty(character, "x");
    const rawY = gsap.getProperty(character, "y");

    // DEFENSIVE: Ensure we get valid numbers, default to 0
    const characterX = typeof rawX === "number" && Number.isFinite(rawX) ? rawX : 0;
    const characterY = typeof rawY === "number" && Number.isFinite(rawY) ? rawY : 0;

    // --- OUTER GLOW: follows character ---
    updateSpring(outerSpring, characterX, characterY, cfg.outerStiffness, cfg.damping);

    // --- INNER GLOW: follows outer glow (chained) ---
    updateSpring(innerSpring, outerSpring.x, outerSpring.y, cfg.innerStiffness, cfg.damping);

    // --- OSCILLATION: sine wave on X, cosine wave on Y (starts at top, bobs down) ---
    const phase = time * cfg.oscillationFrequency * Math.PI * 2;

    // X oscillation (left-right snake)
    const outerOscillationX = Math.sin(phase) * cfg.outerOscillationAmplitudeX;
    const innerOscillationX =
      Math.sin(phase + cfg.innerPhaseOffset) * cfg.innerOscillationAmplitudeX;

    // Y oscillation (up-down bob) - use (1 - cos) so 0 is top, amplitude is bottom
    const outerOscillationY = (1 - Math.cos(phase)) * cfg.outerOscillationAmplitudeY * 0.5;
    const innerOscillationY =
      (1 - Math.cos(phase + cfg.innerPhaseOffset)) * cfg.innerOscillationAmplitudeY * 0.5;

    // --- APPLY POSITIONS ---
    const outerX = outerSpring.x + outerOscillationX;
    const outerY = outerSpring.y + outerOscillationY;
    const innerX = innerSpring.x + innerOscillationX;
    const innerY = innerSpring.y + innerOscillationY;

    // Debug: log if values are unexpectedly large
    if (ENABLE_ANIMATION_DEBUG_LOGS && (Math.abs(outerX) > 100 || Math.abs(outerY) > 100)) {
      console.warn("[GlowSystem] Large values detected:", {
        outerX,
        outerY,
        innerX,
        innerY,
        characterX,
        characterY,
      });
    }

    gsap.set(outerGlow, { x: outerX, y: outerY });
    gsap.set(innerGlow, { x: innerX, y: innerY });
  }

  /**
   * Update a single spring towards target
   */
  function updateSpring(
    spring: SpringState,
    targetX: number,
    targetY: number,
    stiffness: number,
    damping: number
  ): void {
    // Calculate spring force (difference from target)
    const forceX = (targetX - spring.x) * stiffness;
    const forceY = (targetY - spring.y) * stiffness;

    // Apply force to velocity
    spring.velocityX += forceX;
    spring.velocityY += forceY;

    // Apply damping
    spring.velocityX *= damping;
    spring.velocityY *= damping;

    // Update position
    spring.x += spring.velocityX;
    spring.y += spring.velocityY;
  }

  // =============================================================================
  // LIFECYCLE CONTROLS
  // =============================================================================

  function start(): void {
    if (isRunning) return;
    isRunning = true;
    isPaused = false;
    gsap.ticker.add(update);
  }

  function stop(): void {
    if (!isRunning) return;
    isRunning = false;
    isPaused = false;
    gsap.ticker.remove(update);
  }

  function pause(): void {
    isPaused = true;
  }

  function resume(): void {
    isPaused = false;
  }

  function isActive(): boolean {
    return isRunning && !isPaused;
  }

  // =============================================================================
  // VISIBILITY CONTROLS
  // =============================================================================

  function fadeIn(duration = 0.4): gsap.core.Tween {
    return gsap.to([outerGlow, innerGlow], {
      opacity: cfg.visibleOpacity,
      duration,
      ease: "power2.out",
    });
  }

  function fadeOut(duration = 0.3): gsap.core.Tween {
    return gsap.to([outerGlow, innerGlow], {
      opacity: cfg.hiddenOpacity,
      duration,
      ease: "power2.in",
    });
  }

  function show(): void {
    gsap.set([outerGlow, innerGlow], { opacity: cfg.visibleOpacity });
  }

  function hide(): void {
    gsap.set([outerGlow, innerGlow], { opacity: cfg.hiddenOpacity });
  }

  // =============================================================================
  // UTILITY
  // =============================================================================

  /**
   * Snap glows to character position (reset spring lag)
   * Useful after teleporting character or on initialization
   */
  function snapToCharacter(): void {
    const rawX = gsap.getProperty(character, "x");
    const rawY = gsap.getProperty(character, "y");

    // DEFENSIVE: Ensure we get valid numbers, default to 0
    // gsap.getProperty can return unexpected values before element is fully initialized
    const characterX = typeof rawX === "number" && Number.isFinite(rawX) ? rawX : 0;
    const characterY = typeof rawY === "number" && Number.isFinite(rawY) ? rawY : 0;

    // Debug: Log if we got unexpected values
    if (ENABLE_ANIMATION_DEBUG_LOGS && (rawX !== characterX || rawY !== characterY)) {
      console.warn("[GlowSystem] snapToCharacter got invalid values, using 0:", { rawX, rawY });
    }

    // Reset outer spring
    outerSpring.x = characterX;
    outerSpring.y = characterY;
    outerSpring.velocityX = 0;
    outerSpring.velocityY = 0;

    // Reset inner spring
    innerSpring.x = characterX;
    innerSpring.y = characterY;
    innerSpring.velocityX = 0;
    innerSpring.velocityY = 0;

    // Apply immediately
    gsap.set(outerGlow, { x: characterX, y: characterY });
    gsap.set(innerGlow, { x: characterX, y: characterY });
  }

  /**
   * Update the size scale dynamically (e.g., when character size changes)
   * Recalculates oscillation amplitudes based on new scale
   */
  function updateSizeScale(newSizeScale: number): void {
    currentSizeScale = newSizeScale;
    cfg.outerOscillationAmplitudeX = DEFAULT_CONFIG.outerOscillationAmplitudeX * currentSizeScale;
    cfg.innerOscillationAmplitudeX = DEFAULT_CONFIG.innerOscillationAmplitudeX * currentSizeScale;
    cfg.outerOscillationAmplitudeY = DEFAULT_CONFIG.outerOscillationAmplitudeY * currentSizeScale;
    cfg.innerOscillationAmplitudeY = DEFAULT_CONFIG.innerOscillationAmplitudeY * currentSizeScale;
  }

  // =============================================================================
  // RETURN CONTROLS
  // =============================================================================

  return {
    start,
    stop,
    pause,
    resume,
    isActive,
    fadeIn,
    fadeOut,
    show,
    hide,
    snapToCharacter,
    updateSizeScale,
  };
}
