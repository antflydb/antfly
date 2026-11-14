// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum DictationMode: String, Codable, CaseIterable, Sendable {
  case verbatim, clean, rewrite
  public var label: String { self == .clean ? "Clean up" : rawValue.capitalized }
  public var description: String {
    switch self {
    case .verbatim:
      return "Keep the recognized wording. Voice formatting can still add line breaks."
    case .clean:
      return
        "Remove fillers, fix punctuation, and apply preferred spellings using a text processor."
    case .rewrite: return "Use a text processor to apply your tone and rewrite instructions."
    }
  }
  public func validateProcessor(_ model: String) throws {
    if self != .verbatim && model.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
      throw DictationError.invalid(
        "Choose a text processor for \(label) in Presets, or select Verbatim.")
    }
  }
}
public enum ProcessingStrategy: String, Codable, CaseIterable, Sendable {
  case automatic, incremental, wholeUtterance
  public var label: String {
    switch self {
    case .automatic: return "Automatic"
    case .incremental: return "Incremental"
    case .wholeUtterance: return "Whole utterance"
    }
  }
}
public enum DictationPhase: String, Sendable {
  case idle, listening, transcribing, cleaning, ready, noSpeech, failed, canceled
  public var label: String {
    switch self {
    case .idle: return "Ready"
    case .listening: return "Listening…"
    case .transcribing: return "Transcribing…"
    case .cleaning: return "Cleaning…"
    case .ready: return "Ready"
    case .noSpeech: return "No speech detected"
    case .failed: return "Needs attention"
    case .canceled: return "Canceled"
    }
  }
}
public struct DictationProgress: Sendable {
  public var phase: DictationPhase
  public var raw: String
  public var draft: String
  public init(phase: DictationPhase, raw: String = "", draft: String = "") {
    self.phase = phase
    self.raw = raw
    self.draft = draft
  }
}
public struct DictationTiming: Sendable {
  public var recognition: TimeInterval
  public var cleanup: TimeInterval
  public var afterRelease: TimeInterval = 0
  public var total: TimeInterval
  public var firstEvent: TimeInterval?
  public var firstDraft: TimeInterval?
  public init(
    recognition: TimeInterval = 0, cleanup: TimeInterval = 0, total: TimeInterval = 0,
    firstEvent: TimeInterval? = nil, firstDraft: TimeInterval? = nil
  ) {
    self.recognition = recognition
    self.cleanup = cleanup
    self.total = total
    self.firstEvent = firstEvent
    self.firstDraft = firstDraft
  }
}
public struct ProcessingPlan: Sendable {
  public var session: Bool
  public var incrementalCleanup: Bool
  public var explanation: String
  public init(preset: Preset, model: DiscoveredModel?, nativeAppleStreaming: Bool = false) throws {
    let stable = nativeAppleStreaming || model?.dictation?.stable_segments == true
    let textCleanup =
      (nativeAppleStreaming || model?.dictation?.text_cleanup == true)
      && preset.usesNamedTranscriber
    let canSession = stable && (preset.outputMode == .verbatim || textCleanup)
    let canIncrement = canSession && preset.outputMode != .rewrite
    if preset.processingStrategy == .incremental && !canIncrement {
      throw DictationError.invalid(
        preset.outputMode == .rewrite
          ? "Rewrite needs the complete utterance. Choose Automatic or Whole utterance."
          : "This transcriber does not advertise stable-segment processing. Choose Automatic or Whole utterance."
      )
    }
    session = canSession
    incrementalCleanup =
      canIncrement && preset.processingStrategy != .wholeUtterance && preset.runsCleanup
    if nativeAppleStreaming {
      explanation =
        incrementalCleanup
        ? "Apple speech runs on this Mac while you speak. Text processing starts when final phrases arrive; release finishes the tail."
        : "Apple speech runs on this Mac while you speak. "
          + (preset.runsCleanup
            ? "Text processing uses the complete utterance on release."
            : "Release assembles the final transcript.")
    } else if session {
      explanation =
        incrementalCleanup
        ? "Recognition and cleanup run while you speak. Release finishes the remaining tail."
        : "Recognition runs while you speak. \(preset.runsCleanup ? "Cleanup uses the complete utterance on release." : "Final segments are assembled on release.")"
    } else {
      explanation =
        "This transcriber processes the recorded clip after release.\(preset.runsCleanup ? " Cleanup follows transcription." : "")"
    }
    if preset.outputMode != .verbatim && !preset.runsCleanup {
      explanation += " Recognition only: choose a cleanup generator to enable cleanup."
    }
  }
}
extension Preset {
  public var outputMode: DictationMode {
    get {
      mode
        ?? (style == "verbatim" || cleanupModel.isEmpty
          ? .verbatim : (style == "clean" && instructions.isEmpty ? .clean : .rewrite))
    }
    set { mode = newValue }
  }
  public var processingStrategy: ProcessingStrategy {
    get { processing ?? .automatic }
    set { processing = newValue }
  }
  public var runsCleanup: Bool { outputMode != .verbatim && !cleanupModel.isEmpty }
  public var requestStyle: String {
    outputMode == .verbatim
      ? "verbatim" : outputMode == .clean ? "clean" : (style == "verbatim" ? "clean" : style)
  }
}

public struct RecognitionEvent: Codable, Sendable {
  public var type: String
  public var sequence: Int
  public var text: String
  public var start_ms: Int
  public var end_ms: Int
  public init(type: String, sequence: Int, text: String, start: Int, end: Int) {
    self.type = type
    self.sequence = sequence
    self.text = text
    start_ms = start
    end_ms = end
  }
}

/// Cancellation-aware FIFO admission for the two recording workers. This keeps
/// recognition and cleanup usable even with server inference concurrency = 1.
actor InferenceGate {
  private let capacity: Int
  private var held = 0
  init(capacity: Int = 1) { self.capacity = capacity }
  private var waiters: [(UUID, CheckedContinuation<Void, any Error>)] = []
  func enter() async throws {
    try Task.checkCancellation()
    if held < capacity {
      held += 1
      return
    }
    let id = UUID()
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        waiters.append((id, continuation))
        if Task.isCancelled { cancel(id) }
      }
    } onCancel: {
      Task { await self.cancel(id) }
    }
  }
  private func cancel(_ id: UUID) {
    guard let index = waiters.firstIndex(where: { $0.0 == id }) else { return }
    waiters.remove(at: index).1.resume(throwing: CancellationError())
  }
  func leave() {
    if waiters.isEmpty { held -= 1 } else { waiters.removeFirst().1.resume() }
  }
}

extension Preset {
  /// A recording must never wait on another preset's processor or on warmup
  /// when its output does not require generation.
  public func canUseCleanupPreparation(for model: String?) -> Bool {
    runsCleanup && model == cleanupModel
  }
}
