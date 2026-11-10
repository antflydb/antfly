// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private actor Signal {
  var ready = false
  var waiting: [CheckedContinuation<Void, Never>] = []
  func send() {
    ready = true
    let pending = waiting
    waiting = []
    pending.forEach { $0.resume() }
  }
  func wait() async { if !ready { await withCheckedContinuation { waiting.append($0) } } }
}
private actor Cleaner {
  var inputs: [String] = []
  let began = Signal()
  let release = Signal()
  var block: Bool
  init(block: Bool = false) { self.block = block }
  func clean(_ text: String) async -> DictationResult {
    inputs.append(text)
    await began.send()
    if block { await release.wait() }
    return DictationResult(raw: text, text: text.uppercased())
  }
}
private func final(_ sequence: Int, _ text: String, _ start: Int) -> RecognitionEvent {
  RecognitionEvent(type: "final", sequence: sequence, text: text, start: start, end: start + 100)
}
@Test func processingPlansRespectCapabilitiesAndMigrateExistingPresets() throws {
  var preset = Preset(id: "p", name: "Clean")
  preset.cleanupModel = "generators/cleanup"
  let catalog = try JSONDecoder().decode(
    ModelCatalog.self,
    from: Data(
      """
      {"data":[{"id":"transcribers/whisper","dictation":{"live_preview":true,"recognition_hints":true,"stable_segments":true,"text_cleanup":true}},
      {"id":"transcribers/apple","dictation":{"live_preview":false,"recognition_hints":false,"stable_segments":false,"text_cleanup":true}}]}
      """.utf8))
  let automatic = try ProcessingPlan(preset: preset, model: catalog.data[0])
  #expect(automatic.session && automatic.incrementalCleanup)
  let apple = try ProcessingPlan(preset: preset, model: catalog.data[1])
  #expect(!apple.session && !apple.incrementalCleanup)
  preset.processingStrategy = .incremental
  #expect(throws: DictationError.self) {
    try ProcessingPlan(preset: preset, model: catalog.data[1])
  }
  preset.outputMode = .rewrite
  #expect(throws: DictationError.self) {
    try ProcessingPlan(preset: preset, model: catalog.data[0])
  }
  preset.processingStrategy = .automatic
  let rewrite = try ProcessingPlan(preset: preset, model: catalog.data[0])
  #expect(rewrite.session && !rewrite.incrementalCleanup)
  preset.outputMode = .verbatim
  #expect(!preset.runsCleanup && preset.requestStyle == "verbatim")
  var legacy =
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(preset)) as! [String: Any]
  legacy.removeValue(forKey: "mode")
  legacy.removeValue(forKey: "processing")
  legacy["style"] = "formal"
  let migrated = try JSONDecoder().decode(
    Preset.self, from: JSONSerialization.data(withJSONObject: legacy))
  #expect(migrated.outputMode == .rewrite && migrated.processingStrategy == .automatic)
}

@Test(.timeLimit(.minutes(1))) func incrementalCleanupRunsBeforeReleaseAndOnlyFinishesTheTail()
  async throws
{
  let cleaner = Cleaner(block: true)
  let committed = Signal()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true,
    recognize: { audio, commit in
      if commit {
        await committed.send()
        return [final(4, "three", 200)]
      }
      if audio == Data([1]) {
        return [
          RecognitionEvent(
            type: "partial", sequence: 1, text: "wrong hypothesis", start: 0, end: 100),
          final(2, "one", 0),
        ]
      }
      return [final(3, "two", 100)]
    }, clean: { await cleaner.clean($0) })
  try await pipeline.append(Data([1]))
  await cleaner.began.wait()
  #expect(await cleaner.inputs == ["one"])
  try await pipeline.append(Data([2]))  // Recognition proceeds while generation is suspended.
  let finishing = Task { try await pipeline.finish(Data([3])) }
  await committed.wait()
  await cleaner.release.send()
  let result = try await finishing.value
  #expect(result.raw == "one two three")
  #expect(result.text == "ONE TWO THREE")
  #expect(await cleaner.inputs == ["one", "two three"])
}
@Test func wholeUtteranceCleanupWaitsForFinalRecognitionAndRunsOnce() async throws {
  let cleaner = Cleaner()
  let pipeline = LiveDictation(
    incremental: false, needsCleanup: true,
    recognize: { _, commit in commit ? [final(2, "two", 100)] : [final(1, "one", 0)] },
    clean: { await cleaner.clean($0) })
  try await pipeline.append(Data([1]))
  #expect(await cleaner.inputs.isEmpty)
  let result = try await pipeline.finish(Data())
  #expect(result.text == "ONE TWO")
  #expect(await cleaner.inputs == ["one two"])
}
@Test func provisionalStablePrefixesAreNeverCleanedAndFinalReplaysAreIdempotent() async throws {
  let cleaner = Cleaner()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true,
    recognize: { _, commit in
      commit
        ? [final(2, "right", 0), final(2, "right", 0)]
        : [RecognitionEvent(type: "partial", sequence: 1, text: "wrong", start: 0, end: 100)]
    }, clean: { await cleaner.clean($0) })
  try await pipeline.append(Data([1]))
  #expect(await cleaner.inputs.isEmpty)
  let result = try await pipeline.finish(Data())
  #expect(result.text == "RIGHT" && result.raw == "right")
  #expect(await cleaner.inputs == ["right"])
}
@Test func cleanupFailurePreservesAllFinalRecognitionIncludingLaterSegments() async throws {
  let failed = Signal()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true,
    recognize: { _, commit in commit ? [final(2, "two", 100)] : [final(1, "one", 0)] },
    clean: { _ in
      await failed.send()
      throw DictationError.invalid("model unavailable")
    })
  try await pipeline.append(Data([1]))
  await failed.wait()
  do {
    _ = try await pipeline.finish(Data())
    Issue.record("Failed cleanup became insertable")
  } catch let error as DictationError { #expect(error.rawTranscript == "one two") }
}
@Test func silenceSkipsCleanupAndVerbatimPreservesFinalText() async throws {
  let silent = LiveDictation(
    incremental: true, needsCleanup: true, recognize: { _, _ in [] },
    clean: { _ in
      Issue.record("Silence invoked cleanup")
      throw DictationError.invalid("unexpected")
    })
  #expect(try await silent.finish(Data()).text.isEmpty)
  let verbatim = LiveDictation(
    incremental: false, needsCleanup: false,
    recognize: { _, _ in [final(1, "um exactly this", 0)] },
    clean: { _ in
      Issue.record("Verbatim invoked cleanup")
      throw DictationError.invalid("unexpected")
    })
  #expect(try await verbatim.finish(Data()).text == "um exactly this")
}
@Test(.timeLimit(.minutes(1))) func cancellationCannotPublishACompletedResult() async throws {
  let began = Signal()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true,
    recognize: { _, commit in commit ? [] : [final(1, "one", 0)] },
    clean: { text in
      await began.send()
      try await Task.sleep(for: .seconds(60))
      return DictationResult(raw: text, text: "UNEXPECTED")
    })
  try await pipeline.append(Data([1]))
  await began.wait()
  let finishing = Task { try await pipeline.finish(Data()) }
  await pipeline.cancel()
  do {
    _ = try await finishing.value
    Issue.record("Canceled result became insertable")
  } catch { #expect(error is CancellationError || error is DictationError) }
}
@Test func conflictingFinalSegmentsFailInsteadOfChangingPreviouslyCleanedText() async throws {
  let pipeline = LiveDictation(
    incremental: false, needsCleanup: false,
    recognize: { _, commit in commit ? [final(2, "revised", 0)] : [final(1, "original", 0)] },
    clean: { DictationResult(raw: $0, text: $0) })
  try await pipeline.append(Data([1]))
  await #expect(throws: DictationError.self) { try await pipeline.finish(Data()) }
}

private final class RecoveryProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let cleanupOnly = request.value(forHTTPHeaderField: "Content-Type") == "application/json"
    let ending =
      cleanupOnly
      ? "data: {\"type\":\"dictation.completed\",\"text\":\"Clean.\"}\n\ndata: [DONE]\n\n"
      : "data: {\"type\":\"error\",\"message\":\"model failed\"}\n\ndata: [DONE]\n\n"
    let body =
      "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"raw\"}}\n\n" + ending
    let response = HTTPURLResponse(
      url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
      headerFields: ["Content-Type": "text/event-stream"])!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Data(body.utf8))
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}
@Test @MainActor func cleanupRecoveryKeepsRawSeparateAndRetriesTextWithoutAudio() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: directory) }
  let settings = URLSessionConfiguration.ephemeral
  settings.protocolClasses = [RecoveryProtocol.self]
  let client = try AntflyClient(
    server: "https://fixture.example", session: URLSession(configuration: settings))
  let store = Store(directory: directory, registerHotkey: false, clientFactory: { _ in client })
  store.config.autoPaste = false
  store.config.presets[0].cleanupModel = "generators/cleanup"
  store.submit(
    audio: Data([1]), preset: store.config.presets[0], snapshot: store.config, destination: nil)
  await store.requestTask?.value
  #expect(store.phase == .failed && store.canRecoverRaw)
  #expect(store.preview == "raw" && store.result.isEmpty)
  store.retryCleanup()
  await store.requestTask?.value
  #expect(store.phase == .ready && !store.canRecoverRaw)
  #expect(store.result == "Clean." && store.preview == "raw")
  #expect(store.timing != nil)
}

@Test func inferenceGateCapacityAndCanceledWaitersDoNotLeakSlots() async throws {
  let gate = InferenceGate(capacity: 2)
  try await gate.enter()
  try await gate.enter()
  let waiter = Task { try await gate.enter() }
  waiter.cancel()
  do {
    try await waiter.value
    await gate.leave()
  } catch { #expect(error is CancellationError) }
  await gate.leave()
  await gate.leave()
  try await gate.enter()
  try await gate.enter()
  await gate.leave()
  await gate.leave()
  var config = Configuration()
  config.concurrentRequests = 3
  #expect(throws: DictationError.self) { try config.validated() }
}

private actor PreviewProbe {
  var raw = ""
  let updated = Signal()
  func receive(_ event: DictationProgress) async {
    if !event.draft.isEmpty {
      raw = event.raw
      await updated.send()
    }
  }
}
@Test(.timeLimit(.minutes(1))) func cleanupCompletionDoesNotEraseTheCurrentProvisionalTail()
  async throws
{
  let cleaner = Cleaner(block: true)
  let preview = PreviewProbe()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true,
    recognize: { audio, commit in
      if commit { return [final(3, "tail", 100)] }
      return audio == Data([1])
        ? [final(1, "one", 0)]
        : [
          RecognitionEvent(
            type: "partial", sequence: 2, text: "tail provisional", start: 100, end: 200)
        ]
    }, clean: { await cleaner.clean($0) }, observe: { await preview.receive($0) })
  try await pipeline.append(Data([1]))
  await cleaner.began.wait()
  try await pipeline.append(Data([2]))
  await cleaner.release.send()
  await preview.updated.wait()
  #expect(await preview.raw == "one tail provisional")
  #expect(try await pipeline.finish(Data()).text == "ONE TAIL")
}

@Test func completedLiveResultDoesNotWaitForSessionDisposal() async throws {
  let began = Signal()
  let release = Signal()
  let disposed = Signal()
  let pipeline = LiveDictation(
    incremental: false, needsCleanup: false,
    recognize: { _, _ in [final(1, "ready", 0)] },
    clean: { DictationResult(raw: $0, text: $0) },
    close: {
      await began.send()
      await release.wait()
      await disposed.send()
    })
  let result = try await pipeline.finish(Data())
  #expect(result.text == "ready")
  await began.wait()
  #expect(await disposed.ready == false)
  await release.send()
  await pipeline.cancel()
  #expect(await disposed.ready)
}

private actor FormattingCleaner {
  var inputs: [String] = []
  let options: VoiceFormatting.Options
  init(pressEnter: Bool = false) { options = .init(pressEnter: pressEnter) }
  func clean(_ raw: String) -> DictationResult {
    inputs.append(raw)
    let rendered = VoiceFormatting.Plan(raw, options: options).render()
    var result = DictationResult(raw: raw, text: rendered.text)
    result.formatting = rendered
    return result
  }
}
@Test func streamingCleanupRepairsCommandsSplitAcrossFinalSegments() async throws {
  let cleaner = FormattingCleaner()
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true, formatting: .init(),
    recognize: { data, commit in
      if commit { return [] }
      return data.first == 1 ? [final(1, "First new", 0)] : [final(2, "paragraph second", 100)]
    }, clean: { await cleaner.clean($0) })
  try await pipeline.append(Data([1]))
  while await cleaner.inputs.isEmpty { await Task.yield() }
  try await pipeline.append(Data([2]))
  let result = try await pipeline.finish(Data())
  #expect(result.raw == "First new paragraph second" && result.text == "First\n\nsecond")
  #expect(await cleaner.inputs.last == result.raw)
}
@Test func terminalEnterIsDecidedAfterTheCompleteUtterance() async throws {
  let cleaner = FormattingCleaner(pressEnter: true)
  let pipeline = LiveDictation(
    incremental: true, needsCleanup: true, formatting: .init(pressEnter: true),
    recognize: { _, commit in
      commit ? [final(2, "enter", 100)] : [final(1, "Hello press", 0)]
    }, clean: { await cleaner.clean($0) })
  try await pipeline.append(Data([1]))
  #expect(await cleaner.inputs.isEmpty)
  let result = try await pipeline.finish(Data())
  #expect(result.text == "Hello" && result.formatting?.pressEnter == true)
  #expect(await cleaner.inputs == ["Hello press enter"])
}
