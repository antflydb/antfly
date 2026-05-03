/**
 * Animation Definitions Index
 */

export type {
  BodyConfig,
  CharacterPhase,
  EmotionConfig,
  EyeConfig,
  GlowConfig,
} from "../types";
export { interpretEmotionConfig, killPendingEyeReset } from "./emotion-interpreter";
export { EMOTION_CONFIGS, GLOW_CONSTANTS, getEmotionConfig } from "./emotions";
export {
  type BlinkAnimationConfig,
  createBlinkAnimation,
  createDoubleBlinkAnimation,
  createEyeAnimation,
  createLookAnimation,
  createReturnFromLookAnimation,
  type EyeAnimationConfig,
  type EyeAnimationElements,
  type EyeShapeSpec,
  type LookAnimationConfig,
} from "./eye-animations";
export {
  EYE_DIMENSIONS,
  EYE_SHAPES,
  type EyeShapeName,
  getEyeDimensions,
  getEyeShape,
} from "./eye-shapes";
export {
  createIdleAnimation,
  type IdleAnimationElements,
  type IdleAnimationOptions,
} from "./idle";
export {
  createPowerOffAnimation,
  createWakeUpAnimation,
  type TransitionAnimationElements,
} from "./transitions";
