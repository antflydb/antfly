/**
 * Shadow Tracker - Dynamic shadow that responds to character Y position
 *
 * Instead of animating shadow in multiple disconnected places (idle, emotions, etc.),
 * this tracker continuously watches character Y position and updates shadow accordingly.
 *
 * The relationship is inverse:
 * - Higher Y (jumping up) → shadow shrinks + fades
 * - Lower Y (grounded) → shadow grows + darkens
 */

import gsap from "gsap";
import { SHADOW } from "./constants";

export interface ShadowTrackerConfig {
  /** Y value considered "max height" for scaling (default: 70) */
  maxHeight: number;
  /** Scale when Y = 0 (grounded) */
  groundedScaleX: number;
  groundedScaleY: number;
  /** Scale when Y = -maxHeight (at apex) */
  maxHeightScaleX: number;
  maxHeightScaleY: number;
  /** Opacity when Y = 0 (grounded) */
  groundedOpacity: number;
  /** Opacity when Y = -maxHeight (at apex) */
  maxHeightOpacity: number;
}

export interface ShadowTrackerControls {
  /** Start the tracker (adds to GSAP ticker) */
  start: () => void;
  /** Stop the tracker completely (removes from ticker) */
  stop: () => void;
  /** Pause tracking (keeps ticker but doesn't update) */
  pause: () => void;
  /** Resume tracking after pause */
  resume: () => void;
  /** Check if tracker is currently active */
  isActive: () => boolean;
  /** Force update shadow to specific state (for transitions) */
  setGrounded: () => void;
}

const DEFAULT_CONFIG: ShadowTrackerConfig = {
  maxHeight: 70, // Max jump height in emotions (excited, jump)
  groundedScaleX: SHADOW.scaleXWhenDown,
  groundedScaleY: SHADOW.scaleYWhenDown,
  maxHeightScaleX: SHADOW.scaleXWhenUp,
  maxHeightScaleY: SHADOW.scaleYWhenUp,
  groundedOpacity: SHADOW.opacityWhenDown,
  maxHeightOpacity: 0, // Fully invisible at max height (jump apex)
};

/**
 * Creates a shadow tracker that updates shadow based on character Y position
 *
 * @param character - The character element to track Y position from
 * @param shadow - The shadow element to update
 * @param config - Optional configuration overrides
 * @returns Controls for starting/stopping the tracker
 */
export function createShadowTracker(
  character: HTMLElement,
  shadow: HTMLElement,
  config: Partial<ShadowTrackerConfig> = {}
): ShadowTrackerControls {
  const cfg = { ...DEFAULT_CONFIG, ...config };

  let isRunning = false;
  let isPaused = false;

  /**
   * Calculate shadow properties from character Y position
   * Uses an eased curve to make idle breathing more visible while
   * shadow fully disappears at high jump heights.
   *
   * The power curve (t^0.6) front-loads changes so small movements
   * (idle float) create noticeable shadow breathing.
   */
  function updateShadow(): void {
    if (isPaused) return;

    // Get current Y position (negative = up, 0 = grounded)
    const y = gsap.getProperty(character, "y") as number;

    // Calculate interpolation factor: 0 = grounded, 1 = max height
    // Y is negative when up, so we negate it
    const linearT = Math.min(1, Math.max(0, -y / cfg.maxHeight));

    // Apply power curve to front-load the change
    // At idle float (y=-12, linearT=0.17): easedT ≈ 0.29 → more noticeable shadow shrink
    // At jump apex (y=-70, linearT=1.0): easedT = 1.0 → shadow fully gone
    const easedT = linearT ** 0.6;

    // Interpolate scale and opacity using eased t
    const scaleX = gsap.utils.interpolate(cfg.groundedScaleX, cfg.maxHeightScaleX, easedT);
    const scaleY = gsap.utils.interpolate(cfg.groundedScaleY, cfg.maxHeightScaleY, easedT);
    const opacity = gsap.utils.interpolate(cfg.groundedOpacity, cfg.maxHeightOpacity, easedT);

    // Apply to shadow (using set for immediate update)
    gsap.set(shadow, { scaleX, scaleY, opacity });
  }

  function start(): void {
    if (isRunning) return;
    isRunning = true;
    isPaused = false;
    gsap.ticker.add(updateShadow);
  }

  function stop(): void {
    if (!isRunning) return;
    isRunning = false;
    isPaused = false;
    gsap.ticker.remove(updateShadow);
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

  function setGrounded(): void {
    gsap.set(shadow, {
      scaleX: cfg.groundedScaleX,
      scaleY: cfg.groundedScaleY,
      opacity: cfg.groundedOpacity,
    });
  }

  return {
    start,
    stop,
    pause,
    resume,
    isActive,
    setGrounded,
  };
}
