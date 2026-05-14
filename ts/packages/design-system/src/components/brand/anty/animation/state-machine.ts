/**
 * Animation State Machine
 *
 * Validates state transitions and enforces state transition rules.
 * Prevents invalid transitions and provides debugging information.
 */

import { ENABLE_ANIMATION_DEBUG_LOGS } from "./feature-flags";
import { AnimationState, type StateTransition } from "./types";

/**
 * Priority levels for different animation states
 */
const STATE_PRIORITIES: Record<AnimationState, number> = {
  [AnimationState.OFF]: 0, // Lowest - can be interrupted by anything
  [AnimationState.IDLE]: 1, // Low - should be interrupted by most things
  [AnimationState.TRANSITION]: 2, // Medium - brief transitions
  [AnimationState.INTERACTION]: 3, // High - user interactions are important
  [AnimationState.EMOTION]: 4, // Highest - emotional expressions complete fully
};

/**
 * Valid state transition rules
 */
const TRANSITION_RULES: StateTransition[] = [
  // From IDLE
  { from: AnimationState.IDLE, to: AnimationState.IDLE, allowed: true, priority: 1 },
  { from: AnimationState.IDLE, to: AnimationState.TRANSITION, allowed: true, priority: 2 },
  { from: AnimationState.IDLE, to: AnimationState.EMOTION, allowed: true, priority: 4 },
  { from: AnimationState.IDLE, to: AnimationState.INTERACTION, allowed: true, priority: 3 },
  { from: AnimationState.IDLE, to: AnimationState.OFF, allowed: true, priority: 0 },

  // From TRANSITION
  { from: AnimationState.TRANSITION, to: AnimationState.IDLE, allowed: true, priority: 1 },
  { from: AnimationState.TRANSITION, to: AnimationState.TRANSITION, allowed: true, priority: 2 },
  { from: AnimationState.TRANSITION, to: AnimationState.EMOTION, allowed: true, priority: 4 },
  { from: AnimationState.TRANSITION, to: AnimationState.INTERACTION, allowed: true, priority: 3 },
  { from: AnimationState.TRANSITION, to: AnimationState.OFF, allowed: true, priority: 0 },

  // From EMOTION
  { from: AnimationState.EMOTION, to: AnimationState.IDLE, allowed: true, priority: 1 },
  { from: AnimationState.EMOTION, to: AnimationState.TRANSITION, allowed: true, priority: 2 },
  { from: AnimationState.EMOTION, to: AnimationState.EMOTION, allowed: true, priority: 4 },
  { from: AnimationState.EMOTION, to: AnimationState.INTERACTION, allowed: false, priority: 3 }, // Emotions should complete
  { from: AnimationState.EMOTION, to: AnimationState.OFF, allowed: true, priority: 0 },

  // From INTERACTION
  { from: AnimationState.INTERACTION, to: AnimationState.IDLE, allowed: true, priority: 1 },
  { from: AnimationState.INTERACTION, to: AnimationState.TRANSITION, allowed: true, priority: 2 },
  { from: AnimationState.INTERACTION, to: AnimationState.EMOTION, allowed: true, priority: 4 },
  { from: AnimationState.INTERACTION, to: AnimationState.INTERACTION, allowed: true, priority: 3 },
  { from: AnimationState.INTERACTION, to: AnimationState.OFF, allowed: true, priority: 0 },

  // From OFF
  { from: AnimationState.OFF, to: AnimationState.IDLE, allowed: true, priority: 1 },
  { from: AnimationState.OFF, to: AnimationState.TRANSITION, allowed: true, priority: 2 },
  { from: AnimationState.OFF, to: AnimationState.EMOTION, allowed: true, priority: 4 },
  { from: AnimationState.OFF, to: AnimationState.INTERACTION, allowed: true, priority: 3 },
  { from: AnimationState.OFF, to: AnimationState.OFF, allowed: true, priority: 0 },
];

export class StateMachine {
  private currentState: AnimationState = AnimationState.IDLE;
  private previousState: AnimationState | null = null;
  private stateHistory: Array<{ state: AnimationState; timestamp: number }> = [];
  private enableLogging: boolean;
  private maxHistorySize = 50;

  constructor(enableLogging = false) {
    this.enableLogging = enableLogging;
    this.recordStateChange(AnimationState.IDLE);
  }

  /**
   * Get current state
   */
  getCurrentState(): AnimationState {
    return this.currentState;
  }

  /**
   * Get previous state
   */
  getPreviousState(): AnimationState | null {
    return this.previousState;
  }

  /**
   * Check if a transition is allowed
   */
  canTransition(from: AnimationState, to: AnimationState): boolean {
    const rule = TRANSITION_RULES.find((r) => r.from === from && r.to === to);
    return rule ? rule.allowed : false;
  }

  /**
   * Get priority for a state
   */
  getPriority(state: AnimationState): number {
    return STATE_PRIORITIES[state];
  }

  /**
   * Check if target state can interrupt current state based on priority
   */
  canInterrupt(targetState: AnimationState, force = false): boolean {
    if (force) return true;

    const currentPriority = this.getPriority(this.currentState);
    const targetPriority = this.getPriority(targetState);

    // Higher priority can interrupt lower priority
    // Equal priority can interrupt (e.g., emotion -> emotion)
    return targetPriority >= currentPriority;
  }

  /**
   * Attempt to transition to a new state
   * Returns true if successful, false if invalid
   */
  transition(to: AnimationState, force = false): boolean {
    const from = this.currentState;

    // Check if transition is allowed
    if (!this.canTransition(from, to)) {
      if (this.enableLogging && ENABLE_ANIMATION_DEBUG_LOGS) {
        console.warn(`[StateMachine] Invalid transition: ${from} → ${to}`);
      }
      return false;
    }

    // Check priority
    if (!this.canInterrupt(to, force)) {
      if (this.enableLogging && ENABLE_ANIMATION_DEBUG_LOGS) {
        console.warn(
          `[StateMachine] Cannot interrupt: ${from} (priority ${this.getPriority(from)}) with ${to} (priority ${this.getPriority(to)})`
        );
      }
      return false;
    }

    // Perform transition
    this.previousState = from;
    this.currentState = to;
    this.recordStateChange(to);

    if (this.enableLogging && ENABLE_ANIMATION_DEBUG_LOGS) {
      console.log(`[StateMachine] Transition: ${from} → ${to}`);
    }

    return true;
  }

  /**
   * Record state change in history
   */
  private recordStateChange(state: AnimationState): void {
    this.stateHistory.push({
      state,
      timestamp: Date.now(),
    });

    // Keep history size manageable
    if (this.stateHistory.length > this.maxHistorySize) {
      this.stateHistory.shift();
    }
  }

  /**
   * Get state history
   */
  getHistory(): Array<{ state: AnimationState; timestamp: number }> {
    return [...this.stateHistory];
  }

  /**
   * Get recent state changes (last N)
   */
  getRecentHistory(count = 10): Array<{ state: AnimationState; timestamp: number }> {
    return this.stateHistory.slice(-count);
  }

  /**
   * Reset state machine to idle
   */
  reset(): void {
    this.previousState = this.currentState;
    this.currentState = AnimationState.IDLE;
    this.recordStateChange(AnimationState.IDLE);

    if (this.enableLogging && ENABLE_ANIMATION_DEBUG_LOGS) {
      console.log("[StateMachine] Reset to idle");
    }
  }

  /**
   * Get debug info
   */
  getDebugInfo(): {
    currentState: AnimationState;
    previousState: AnimationState | null;
    currentPriority: number;
    historySize: number;
    recentHistory: Array<{ state: AnimationState; timestamp: number; ago: string }>;
  } {
    const now = Date.now();
    const recentHistory = this.getRecentHistory(5).map((entry) => ({
      ...entry,
      ago: `${((now - entry.timestamp) / 1000).toFixed(1)}s ago`,
    }));

    return {
      currentState: this.currentState,
      previousState: this.previousState,
      currentPriority: this.getPriority(this.currentState),
      historySize: this.stateHistory.length,
      recentHistory,
    };
  }

  /**
   * Validate transition rules (for testing)
   */
  static validateRules(): boolean {
    const states = Object.values(AnimationState);
    let valid = true;

    // Check that every state has a rule for every possible transition
    states.forEach((from) => {
      states.forEach((to) => {
        const rule = TRANSITION_RULES.find((r) => r.from === from && r.to === to);
        if (!rule) {
          console.error(`Missing transition rule: ${from} → ${to}`);
          valid = false;
        }
      });
    });

    return valid;
  }
}
