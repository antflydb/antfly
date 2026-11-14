// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

@preconcurrency import AVFoundation
import DictationCore
import Foundation
import Speech

/// Live, on-device recognition for the explicitly selected bundled Apple engine.
/// The recorded clip remains owned by Microphone for fallback; this worker never
/// downloads assets or handles audio destined for an external server.
@available(macOS 26, *)
actor NativeAppleSpeech {
  static func supportedLanguages() async -> [String] {
    await SpeechTranscriber.supportedLocales.map(\.identifier).sorted()
  }
  private let analyzer: SpeechAnalyzer
  private let transcriber: SpeechTranscriber
  private let format: AVAudioFormat
  private let source: AVAudioFormat
  private let converter: AVAudioConverter?
  private let input: AsyncStream<AnalyzerInput>.Continuation
  private var analysis: Task<Void, Error>?
  private var results: Task<Void, Error>?
  private var events: [RecognitionEvent] = []
  private var sequence = 0
  private var queuedBytes = 0
  private var failure: String?
  private var finished = false
  private var canceled = false
  private var cancellation: Task<Void, Never>?

  private init(
    analyzer: SpeechAnalyzer, transcriber: SpeechTranscriber,
    format: AVAudioFormat, input: AsyncStream<AnalyzerInput>.Continuation
  ) throws {
    self.analyzer = analyzer
    self.transcriber = transcriber
    self.format = format
    self.input = input
    source = AVAudioFormat(
      commonFormat: .pcmFormatFloat32, sampleRate: 16000, channels: 1, interleaved: false)!
    converter = source == format ? nil : AVAudioConverter(from: source, to: format)
    if source != format && converter == nil {
      throw DictationError.invalid("Cannot convert audio for Apple speech.")
    }
  }
  static func open(language: String, dictionary: [String], fasterResults: Bool = false) async throws
    -> NativeAppleSpeech
  {
    guard SpeechTranscriber.isAvailable,
      let locale = await SpeechTranscriber.supportedLocale(
        equivalentTo: Locale(identifier: language))
    else {
      throw DictationError.invalid(
        "This speech language is not supported on this Mac. Choose another language in Presets.")
    }
    var reporting: Set<SpeechTranscriber.ReportingOption> = [.volatileResults]
    if fasterResults { reporting.insert(.fastResults) }
    let transcriber = SpeechTranscriber(
      locale: locale, transcriptionOptions: [],
      reportingOptions: reporting, attributeOptions: [.audioTimeRange])
    if try await AssetInventory.assetInstallationRequest(supporting: [transcriber]) != nil {
      throw DictationError.invalid(
        "Apple speech assets are missing for " + language
          + ". Use Install / check speech in Settings → Speech & text.")
    }
    guard let format = await SpeechAnalyzer.bestAvailableAudioFormat(compatibleWith: [transcriber])
    else { throw DictationError.invalid("Apple speech has no compatible audio format.") }
    let analyzer = SpeechAnalyzer(modules: [transcriber])
    let context = AnalysisContext()
    context.contextualStrings[.general] = Array(Set(dictionary)).sorted()
    try await analyzer.setContext(context)
    try await analyzer.prepareToAnalyze(in: format)
    try Task.checkCancellation()
    let (stream, continuation) = AsyncStream<AnalyzerInput>.makeStream(
      bufferingPolicy: .bufferingOldest(8))
    let worker = try NativeAppleSpeech(
      analyzer: analyzer, transcriber: transcriber, format: format, input: continuation)
    await worker.start(stream)
    return worker
  }
  private func start(_ stream: AsyncStream<AnalyzerInput>) {
    results = Task { [self] in
      for try await result in transcriber.results {
        try Task.checkCancellation()
        accept(result)
      }
    }
    analysis = Task { [analyzer] in
      if let end = try await analyzer.analyzeSequence(stream) {
        try await analyzer.finalizeAndFinish(through: end)
      } else {
        await analyzer.cancelAndFinishNow()
      }
    }
  }
  private func accept(_ result: SpeechTranscriber.Result) {
    guard !canceled, failure == nil else { return }
    let text = String(result.text.characters)
    guard events.count < 2048, queuedBytes + text.utf8.count <= 65536,
      result.range.start.seconds.isFinite, result.range.duration.seconds.isFinite
    else {
      failure = "Apple speech exceeded its bounded result queue."
      return
    }
    let start = max(0, Int((result.range.start.seconds * 1000).rounded()))
    let end = max(
      start, Int(((result.range.start.seconds + result.range.duration.seconds) * 1000).rounded()))
    events.append(
      RecognitionEvent(
        type: result.isFinal ? "final" : "partial",
        sequence: sequence, text: text, start: start, end: end))
    sequence += 1
    queuedBytes += text.utf8.count
  }
  private func yield(_ buffer: AVAudioPCMBuffer) throws {
    guard buffer.frameLength > 0 else { return }
    switch input.yield(AnalyzerInput(buffer: buffer)) {
    case .enqueued: break
    case .dropped:
      throw DictationError.invalid(
        "Apple speech fell behind the audio stream. The recorded clip is preserved for fallback.")
    case .terminated:
      throw DictationError.invalid("Apple speech ended before the recording finished.")
    @unknown default: throw DictationError.invalid("Apple speech input failed.")
    }
  }
  func append(_ data: Data, commit: Bool) async throws -> [RecognitionEvent] {
    guard !finished, !canceled, data.count % 4 == 0, data.count <= 4 * 1024 * 1024
    else { throw DictationError.invalid("Apple speech received invalid or closed audio input.") }
    try Task.checkCancellation()
    if !data.isEmpty {
      guard
        let buffer = AVAudioPCMBuffer(
          pcmFormat: source, frameCapacity: AVAudioFrameCount(data.count / 4)),
        let channel = buffer.floatChannelData?[0]
      else { throw DictationError.invalid("Cannot allocate Apple speech audio.") }
      buffer.frameLength = buffer.frameCapacity
      data.withUnsafeBytes { bytes in
        channel.update(from: bytes.bindMemory(to: Float.self).baseAddress!, count: data.count / 4)
      }
      if let converter {
        let capacity =
          AVAudioFrameCount(
            ceil(Double(buffer.frameLength) * format.sampleRate / source.sampleRate)) + 256
        guard let output = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: capacity) else {
          throw DictationError.invalid("Cannot allocate converted speech audio.")
        }
        let provider = ConversionInput(buffer)
        var error: NSError?
        let status = converter.convert(to: output, error: &error) { _, state in provider.take(state)
        }
        if let error { throw error }
        guard status != .error else {
          throw DictationError.invalid("Apple speech audio conversion failed.")
        }
        try yield(output)
      } else {
        try yield(buffer)
      }
    }
    if commit {
      finished = true
      if let converter {
        while true {
          guard let output = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 1024) else {
            throw DictationError.invalid("Cannot flush speech audio.")
          }
          var error: NSError?
          let status = converter.convert(to: output, error: &error) { _, state in
            state.pointee = .endOfStream
            return nil
          }
          if let error { throw error }
          try yield(output)
          if status == .endOfStream || output.frameLength == 0 { break }
          guard status != .error else {
            throw DictationError.invalid("Apple speech audio flush failed.")
          }
        }
      }
      input.finish()
      do {
        try await withTaskCancellationHandler {
          try await analysis?.value
          try await results?.value
          try Task.checkCancellation()
        } onCancel: {
          Task { await self.cancel() }
        }
      } catch {
        await cancel()
        try Task.checkCancellation()
        throw error
      }
    }
    if let failure { throw DictationError.invalid(failure) }
    let pending = events
    events.removeAll(keepingCapacity: true)
    queuedBytes = 0
    return pending
  }
  func cancel() async {
    if let cancellation {
      await cancellation.value
      return
    }
    canceled = true
    input.finish()
    analysis?.cancel()
    results?.cancel()
    let runningAnalysis = analysis
    let runningResults = results
    let analyzer = analyzer
    let task = Task {
      await analyzer.cancelAndFinishNow()
      _ = try? await runningAnalysis?.value
      _ = try? await runningResults?.value
    }
    cancellation = task
    await task.value
    events.removeAll()
  }
}
