// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Immutable ASR finals, rather than speculative stable_text prefixes, own cleanup.
/// Recognition and cleanup have separate workers; only validated complete results
/// become insertable. A final flush covers the tail without re-decoding the clip.
public actor LiveDictation {
  public typealias Observe = @Sendable (DictationProgress) async -> Void
  public typealias Recognize = @Sendable (Data, Bool) async throws -> [RecognitionEvent]
  public typealias Clean = @Sendable (String) async throws -> DictationResult
  private struct Segment {
    var start: Int
    var end: Int
    var raw: String
    var cleaned: String?
  }
  private let recognize: Recognize
  private let clean: Clean
  private let close: @Sendable () async -> Void
  private let observe: Observe
  private let incremental: Bool
  private let needsCleanup: Bool
  private var segments: [Segment] = []
  private var partial = ""
  private var sequence = -1
  private var committed = false
  private var stopped = false
  private var appending = false
  private var worker: Task<Void, Never>?
  // Separately owned disposal never gates transcript delivery or a new recording.
  private var disposal: Task<Void, Never>?
  private func dispose() {
    guard disposal == nil else { return }
    let close = self.close
    disposal = Task(priority: .utility) { await close() }
  }
  private var failure: String?
  private var started = ProcessInfo.processInfo.systemUptime
  private var recognitionSeconds: TimeInterval = 0
  private var cleanupSeconds: TimeInterval = 0

  public init(
    incremental: Bool, needsCleanup: Bool, recognize: @escaping Recognize,
    clean: @escaping Clean, close: @escaping @Sendable () async -> Void = {},
    observe: @escaping Observe = { _ in }
  ) {
    self.incremental = incremental
    self.needsCleanup = needsCleanup
    self.recognize = recognize
    self.clean = clean
    self.close = close
    self.observe = observe
  }
  public static func open(
    client: AntflyClient, preset: Preset, dictionary: [String],
    plan: ProcessingPlan, observe: @escaping Observe
  ) async throws -> LiveDictation {
    let id = try await client.createSession(preset: preset, dictionary: dictionary)
    if Task.isCancelled {
      await client.closeSession(id)
      throw CancellationError()
    }
    return LiveDictation(
      incremental: plan.incrementalCleanup, needsCleanup: preset.runsCleanup,
      recognize: {
        try await client.appendSessionEvents(id, audio: $0, sampleRate: 16000, commit: $1)
      },
      clean: { try await client.cleanupText($0, preset: preset, dictionary: dictionary) },
      close: { await client.closeSession(id) }, observe: observe)
  }
  private var raw: String { segments.map(\.raw).joined(separator: " ") }
  private var draft: String {
    segments.compactMap(\.cleaned).filter { !$0.isEmpty }.joined(separator: " ")
  }
  public func append(_ audio: Data, commit: Bool = false) async throws {
    guard !stopped, !committed, !appending else {
      throw DictationError.invalid("Recording session is closed or busy.")
    }
    appending = true
    defer { appending = false }
    let began = ProcessInfo.processInfo.systemUptime
    let events = try await recognize(audio, commit)
    recognitionSeconds += ProcessInfo.processInfo.systemUptime - began
    try Task.checkCancellation()
    guard !stopped else { throw CancellationError() }
    try accept(events)
    if commit { committed = true }
    await observe(
      DictationProgress(
        phase: commit ? .transcribing : .listening,
        raw: [raw, partial].filter { !$0.isEmpty }.joined(separator: " "), draft: draft))
    startCleanup()
  }
  private func accept(_ events: [RecognitionEvent]) throws {
    for event in events {
      if event.sequence <= sequence { continue }  // Replay is idempotent.
      guard event.start_ms >= 0, event.end_ms >= event.start_ms,
        ["partial", "final"].contains(event.type), event.text.utf8.count <= 65536
      else {
        throw DictationError.invalid("Invalid recognition event.")
      }
      sequence = event.sequence
      if event.type == "partial" {
        partial = event.text
        continue
      }
      partial = ""
      let text = event.text.trimmingCharacters(in: .whitespacesAndNewlines)
      if let existing = segments.first(where: {
        $0.start == event.start_ms && $0.end == event.end_ms
      }) {
        guard existing.raw == text else {
          throw DictationError.invalid("Recognition revised an already final segment.")
        }
        continue
      }
      if text.isEmpty { continue }
      guard segments.count < 2048,
        raw.utf8.count + text.utf8.count + (segments.isEmpty ? 0 : 1) <= 65536,
        event.start_ms >= (segments.last?.end ?? 0)
      else {
        throw DictationError.invalid("Recognition segments overlap or exceed the recording limit.")
      }
      segments.append(Segment(start: event.start_ms, end: event.end_ms, raw: text))
    }
  }
  private func startCleanup() {
    guard incremental, needsCleanup, failure == nil, worker == nil, !stopped,
      segments.contains(where: { $0.cleaned == nil })
    else { return }
    worker = Task { await self.drain() }
  }
  private func drain() async {
    defer { worker = nil }
    while !stopped, !Task.isCancelled, let index = segments.firstIndex(where: { $0.cleaned == nil })
    {
      // Coalesce any backlog to avoid one model invocation per short phrase.
      let end = segments.count
      let input = segments[index..<end].map(\.raw).joined(separator: " ")
      do {
        let began = ProcessInfo.processInfo.systemUptime
        let output = try await clean(input)
        cleanupSeconds += ProcessInfo.processInfo.systemUptime - began
        try Task.checkCancellation()
        guard !stopped else { return }
        guard output.raw == input,
          !output.text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else {
          throw DictationError.invalid("Cleanup returned empty text for speech.")
        }
        segments[index].cleaned = output.text
        if index + 1 < end { for offset in (index + 1)..<end { segments[offset].cleaned = "" } }
        await observe(
          DictationProgress(
            phase: committed ? .cleaning : .listening,
            raw: [raw, partial].filter { !$0.isEmpty }.joined(separator: " "), draft: draft))
      } catch {
        if !stopped && !Task.isCancelled { failure = error.localizedDescription }
        return
      }
    }
  }
  public func finish(_ tail: Data) async throws -> DictationResult {
    do {
      try await append(tail, commit: true)
      if let worker { await worker.value }
      try Task.checkCancellation()
      guard !stopped else { throw CancellationError() }
      let original = raw
      var final = original
      if !original.isEmpty && needsCleanup {
        await observe(DictationProgress(phase: .cleaning, raw: original, draft: draft))
        if let failure { throw DictationError.cleanupFailed(raw: original, message: failure) }
        if incremental {
          guard segments.allSatisfy({ $0.cleaned != nil }) else {
            throw DictationError.cleanupFailed(
              raw: original, message: "Cleanup did not finish every final segment.")
          }
          final = draft
        } else {
          let began = ProcessInfo.processInfo.systemUptime
          do { final = try await clean(original).text } catch is CancellationError {
            throw CancellationError()
          } catch {
            throw DictationError.cleanupFailed(raw: original, message: error.localizedDescription)
          }
          cleanupSeconds += ProcessInfo.processInfo.systemUptime - began
          guard !final.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw DictationError.cleanupFailed(
              raw: original, message: "Cleanup returned empty text for speech.")
          }
        }
      }
      try Task.checkCancellation()
      guard !stopped else { throw CancellationError() }
      stopped = true
      dispose()
      return DictationResult(
        raw: original, text: final,
        timing: DictationTiming(
          recognition: recognitionSeconds, cleanup: cleanupSeconds,
          total: ProcessInfo.processInfo.systemUptime - started))
    } catch {
      stopped = true
      worker?.cancel()
      if let worker { await worker.value }
      dispose()
      throw error
    }
  }
  public func cancel() async {
    stopped = true
    worker?.cancel()
    if let worker { await worker.value }
    dispose()
    await disposal?.value
  }
}
