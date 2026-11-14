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
  private let formatting: VoiceFormatting.Options?
  private var cleanupBoundaries: [Int] = []
  private var cleanedRawUTF16 = 0
  private let language: String
  private let preserveRecognitionWhitespace: Bool
  private let cumulativeCleanup: Bool
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
  private var firstEvent: TimeInterval?
  private var firstDraft: TimeInterval?

  public init(
    incremental: Bool, needsCleanup: Bool, formatting: VoiceFormatting.Options? = nil,
    cumulativeCleanup: Bool = false, language: String = "",
    preserveRecognitionWhitespace: Bool = false,
    recognize: @escaping Recognize,
    clean: @escaping Clean, close: @escaping @Sendable () async -> Void = {},
    observe: @escaping Observe = { _ in }
  ) {
    self.language = language
    self.preserveRecognitionWhitespace = preserveRecognitionWhitespace
    self.cumulativeCleanup = cumulativeCleanup
    self.formatting = formatting
    self.incremental = incremental && formatting?.pressEnter != true
    self.needsCleanup = needsCleanup
    self.recognize = recognize
    self.clean = clean
    self.close = close
    self.observe = observe
  }
  public static func open(
    client: AntflyClient, preset: Preset, dictionary: [String],
    plan: ProcessingPlan, formatting: VoiceFormatting.Options? = nil,
    cleanupPreparation: Task<Void, Never>? = nil, observe: @escaping Observe
  ) async throws -> LiveDictation {
    let id = try await client.createSession(preset: preset, dictionary: dictionary)
    if Task.isCancelled {
      await client.closeSession(id)
      throw CancellationError()
    }
    let context = CleanupContext()
    let cleanupFormatting = formatting ?? .init(commands: false, pressEnter: false)
    return LiveDictation(
      incremental: plan.incrementalCleanup, needsCleanup: preset.runsCleanup,
      formatting: formatting, cumulativeCleanup: true, language: preset.language,
      recognize: {
        try await client.appendSessionEvents(id, audio: $0, sampleRate: 16000, commit: $1)
      },
      clean: {
        await cleanupPreparation?.value
        return try await client.cleanupText(
          $0, preset: preset, dictionary: dictionary, formatting: cleanupFormatting,
          context: context)
      },
      close: { await client.closeSession(id) }, observe: observe)
  }
  private func joined(_ parts: [String]) -> String {
    TranscriptAssembly.join(parts, language: language)
  }
  private var raw: String { joined(segments.map(\.raw)) }
  private var draft: String { joined(segments.compactMap(\.cleaned)) }
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
        raw: joined([raw, partial]), draft: draft))
    startCleanup()
  }
  private func accept(_ events: [RecognitionEvent]) throws {
    for event in events {
      if firstEvent == nil { firstEvent = ProcessInfo.processInfo.systemUptime - started }
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
      let text =
        preserveRecognitionWhitespace
        ? event.text : event.text.trimmingCharacters(in: .whitespacesAndNewlines)
      if let existing = segments.first(where: {
        $0.start == event.start_ms && $0.end == event.end_ms
      }) {
        guard existing.raw == text else {
          throw DictationError.invalid("Recognition revised an already final segment.")
        }
        continue
      }
      if text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { continue }
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
      let first = cumulativeCleanup ? 0 : index
      let input = joined(segments[first..<end].map(\.raw))
      do {
        let began = ProcessInfo.processInfo.systemUptime
        let output = try await clean(input)
        if firstDraft == nil {
          firstDraft =
            began - started
            + (output.timing?.firstDraft ?? (ProcessInfo.processInfo.systemUptime - began))
        }
        cleanupSeconds += ProcessInfo.processInfo.systemUptime - began
        try Task.checkCancellation()
        guard !stopped else { return }
        guard output.raw == input,
          !(formatting == nil
            ? output.text.trimmingCharacters(in: .whitespacesAndNewlines) : output.text).isEmpty
        else {
          throw DictationError.invalid("Cleanup returned empty text for speech.")
        }
        if cumulativeCleanup {
          cleanedRawUTF16 = input.utf16.count
        } else {
          cleanedRawUTF16 = joined(segments[..<end].map(\.raw)).utf16.count
        }
        cleanupBoundaries.append(cleanedRawUTF16)
        segments[first].cleaned = output.text
        if first + 1 < end { for offset in (first + 1)..<end { segments[offset].cleaned = "" } }
        await observe(
          DictationProgress(
            phase: committed ? .cleaning : .listening,
            raw: joined([raw, partial]), draft: draft))
      } catch {
        if !stopped && !Task.isCancelled { failure = error.localizedDescription }
        return
      }
    }
  }
  public struct RecognitionFailure: Error, LocalizedError, Sendable {
    public let detail: String
    public var errorDescription: String? { detail }
  }
  public func finishWithRecognitionFallback(
    _ tail: Data, allowFallback: Bool,
    fallback: @Sendable () async throws -> DictationResult
  ) async throws -> DictationResult {
    try await finishWithRecognitionFallback(
      tail: { tail }, allowFallback: allowFallback, fallback: fallback)
  }
  /// Acquire the final live tail inside recovery. Only a live-only overflow is
  /// recoverable here; capture failures and invalid full recordings stay errors.
  public func finishWithRecognitionFallback(
    tail: @Sendable () async throws -> Data, allowFallback: Bool,
    fallback: @Sendable () async throws -> DictationResult
  ) async throws -> DictationResult {
    try Task.checkCancellation()
    guard !stopped else { throw CancellationError() }
    do {
      let audio: Data
      do { audio = try await tail() } catch let failure as LiveAudioOverflow {
        throw RecognitionFailure(detail: failure.localizedDescription)
      }
      return try await finish(audio)
    } catch let failure as RecognitionFailure {
      await cancel()
      try Task.checkCancellation()
      guard allowFallback else { throw failure }
      return try await fallback()
    } catch {
      await cancel()
      try Task.checkCancellation()
      throw error
    }
  }
  public func finish(_ tail: Data) async throws -> DictationResult {
    try Task.checkCancellation()
    guard !stopped else { throw CancellationError() }
    do {
      do { try await append(tail, commit: true) } catch is CancellationError {
        throw CancellationError()
      } catch {
        try Task.checkCancellation()
        throw RecognitionFailure(detail: error.localizedDescription)
      }
      if needsCleanup && !raw.isEmpty {
        await observe(DictationProgress(phase: .cleaning, raw: raw, draft: draft))
      }
      if let pending = worker {
        await withTaskCancellationHandler {
          await pending.value
        } onCancel: {
          pending.cancel()
        }
      }
      try Task.checkCancellation()
      guard !stopped else { throw CancellationError() }
      let original = raw
      var final = original
      if !original.isEmpty && needsCleanup {
        await observe(DictationProgress(phase: .cleaning, raw: original, draft: draft))
        if let failure { throw DictationError.cleanupFailed(raw: original, message: failure) }
        let crosses =
          formatting.map { VoiceFormatting.Plan(original, options: $0).crosses(cleanupBoundaries) }
          ?? false
        if incremental && !crosses {
          guard segments.allSatisfy({ $0.cleaned != nil }) else {
            throw DictationError.cleanupFailed(
              raw: original, message: "Cleanup did not finish every final segment.")
          }
          final = draft
        } else {
          let began = ProcessInfo.processInfo.systemUptime
          do {
            let response = try await clean(original)
            final = response.text
            if firstDraft == nil {
              firstDraft =
                began - started
                + (response.timing?.firstDraft ?? (ProcessInfo.processInfo.systemUptime - began))
            }
          } catch is CancellationError {
            throw CancellationError()
          } catch {
            throw DictationError.cleanupFailed(raw: original, message: error.localizedDescription)
          }
          cleanupSeconds += ProcessInfo.processInfo.systemUptime - began
          guard
            !(formatting == nil ? final.trimmingCharacters(in: .whitespacesAndNewlines) : final)
              .isEmpty
              || formatting.map({ !VoiceFormatting.Plan(original, options: $0).hasCleanupText })
                == true
          else {
            throw DictationError.cleanupFailed(
              raw: original, message: "Cleanup returned empty text for speech.")
          }
        }
      }
      try Task.checkCancellation()
      guard !stopped else { throw CancellationError() }
      stopped = true
      dispose()
      var result = DictationResult(
        raw: original, text: final,
        timing: DictationTiming(
          recognition: recognitionSeconds, cleanup: cleanupSeconds,
          total: ProcessInfo.processInfo.systemUptime - started, firstEvent: firstEvent,
          firstDraft: firstDraft))
      if let formatting {
        let plan = VoiceFormatting.Plan(original, options: formatting)
        if !needsCleanup { result.text = plan.render().text }
        result.formatting = VoiceFormatting.Output(
          text: result.text, pressEnter: plan.render().pressEnter && !result.text.isEmpty)
      }
      return result
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
    dispose()
    if let worker { await worker.value }
    await disposal?.value
  }
}
