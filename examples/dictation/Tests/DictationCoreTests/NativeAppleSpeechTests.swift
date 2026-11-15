// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@Suite(.serialized) struct NativeAppleSpeechTests {
  @Test(
    .enabled(if: ProcessInfo.processInfo.environment["ANTFLY_RUN_NATIVE_SPEECH"] == "1"),
    .timeLimit(.minutes(1)))
  func nativeStreamingRecognizesFixtureWithoutDownloadingOrRecordingMicrophone() async throws {
    guard #available(macOS 26, *) else { return }
    var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    for _ in 0..<4 { root = root.deletingLastPathComponent() }
    let file = try AVAudioFile(
      forReading: root.appendingPathComponent("zig/lib/transcribing/src/testdata/apple-speech.wav"),
      commonFormat: .pcmFormatFloat32, interleaved: false)
    #expect(file.processingFormat.sampleRate == 16000)
    let buffer = try #require(
      AVAudioPCMBuffer(
        pcmFormat: file.processingFormat, frameCapacity: AVAudioFrameCount(file.length)))
    try file.read(into: buffer)
    let channel = try #require(buffer.floatChannelData?[0])
    let samples = Data(bytes: channel, count: Int(buffer.frameLength) * 4)
    let speech = try await NativeAppleSpeech.open(language: "en-US", dictionary: ["Antfly"])
    let began = ContinuousClock.now
    var events: [RecognitionEvent] = []
    for start in stride(from: 0, to: samples.count, by: 12800) {
      events += try await speech.append(
        samples.subdata(in: start..<min(samples.count, start + 12800)), commit: false)
      // Exercise an actual paced stream, rather than replaying one recorded clip.
      try await Task.sleep(for: .milliseconds(200))
    }
    events += try await speech.append(Data(), commit: true)
    await speech.cancel()
    let text = events.filter { $0.type == "final" }.map(\.text).joined(separator: " ").lowercased()
    #expect(text.contains("quick brown fox") && text.contains("lazy dog"))
    #expect(events.map(\.sequence) == events.map(\.sequence).sorted())
    print(
      "Native Apple speech fixture: " + String(describing: began.duration(to: .now)) + " total; "
        + String(events.filter { $0.type == "final" }.count) + " final segments")
  }

  @Test(
    .enabled(if: ProcessInfo.processInfo.environment["ANTFLY_RUN_NATIVE_SPEECH"] == "1"),
    .timeLimit(.minutes(1)))
  func cancelDrainsAnIdleNativeSession() async throws {
    guard #available(macOS 26, *) else { return }
    let speech = try await NativeAppleSpeech.open(language: "en-US", dictionary: [])
    await speech.cancel()
    await speech.cancel()
    await #expect(throws: (any Error).self) { try await speech.append(Data(), commit: true) }
  }
  @Test(
    .enabled(if: ProcessInfo.processInfo.environment["ANTFLY_RUN_NATIVE_SPEECH"] == "1"),
    .timeLimit(.minutes(1)))
  func cancelingFinalizationReleasesTheOwnedNativeWorkers() async throws {
    guard #available(macOS 26, *) else { return }
    let speech = try await NativeAppleSpeech.open(language: "en-US", dictionary: [])
    let work = Task { try await speech.append(Data(count: 16000 * 4), commit: true) }
    await Task.yield()
    work.cancel()
    do { _ = try await work.value } catch is CancellationError {}
    await speech.cancel()
    let next = try await NativeAppleSpeech.open(language: "en-US", dictionary: [])
    await next.cancel()
  }

  @Test(
    .enabled(if: ProcessInfo.processInfo.environment["ANTFLY_RUN_DICTATION_PIPELINE"] == "1"),
    .timeLimit(.minutes(1)))
  func nativeRecognitionAndReleasedCleanupPipelineLatencyProbe() async throws {
    guard #available(macOS 26, *) else { return }
    var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    for _ in 0..<4 { root = root.deletingLastPathComponent() }
    let fixture = root.appendingPathComponent("zig/lib/transcribing/src/testdata/apple-speech.wav")
    let password = ProcessInfo.processInfo.environment["ANTFLY_DICTATION_RUNTIME_PASSWORD"]
    var configuration = Configuration()
    if let server = ProcessInfo.processInfo.environment["ANTFLY_DICTATION_PROBE_SERVER"] {
      configuration.connectionMode = .external
      configuration.server = server
    }
    let connection = try AntflyClient(configuration: configuration, basicPassword: password)
    var preset = Preset(id: "probe", name: "Probe")
    preset.model = "transcribers/apple-speech"
    preset.language = "en-US"
    preset.outputMode = .clean
    preset.cleanupModel = "generators/cleanup"
    let selected = preset
    try await connection.prewarmCleanup(model: selected.cleanupModel)
    let file = try AVAudioFile(
      forReading: fixture, commonFormat: .pcmFormatFloat32, interleaved: false)
    let buffer = try #require(
      AVAudioPCMBuffer(
        pcmFormat: file.processingFormat, frameCapacity: AVAudioFrameCount(file.length)))
    try file.read(into: buffer)
    let channel = try #require(buffer.floatChannelData?[0])
    let samples = Data(bytes: channel, count: Int(buffer.frameLength) * 4)
    let speech = try await NativeAppleSpeech.open(language: "en-US", dictionary: [])
    let context = CleanupContext()
    let live = LiveDictation(
      incremental: true, needsCleanup: true,
      formatting: .init(commands: true, pressEnter: false), cumulativeCleanup: true,
      recognize: { try await speech.append($0, commit: $1) },
      clean: {
        try await connection.cleanupText(
          $0, preset: selected, dictionary: [],
          formatting: .init(commands: true, pressEnter: false), context: context)
      },
      close: { await speech.cancel() })
    for start in stride(from: 0, to: samples.count, by: 12800) {
      try await live.append(samples.subdata(in: start..<min(samples.count, start + 12800)))
      try await Task.sleep(for: .milliseconds(200))
    }
    let release = ContinuousClock.now
    let output = try await live.finish(Data())
    let nativeLatency = release.duration(to: .now)
    #expect(output.raw.lowercased().contains("quick brown fox"))
    #expect(
      output.text.lowercased().contains("quick brown fox")
        && output.text.lowercased().contains("lazy dog"))
    let recordedRelease = ContinuousClock.now
    let recorded = try await connection.dictate(
      audio: Data(contentsOf: fixture), preset: selected,
      dictionary: [], formatting: .init(commands: true, pressEnter: false), progress: { _ in })
    let recordedLatency = recordedRelease.duration(to: .now)
    #expect(recorded.text.lowercased().contains("quick brown fox"))
    print(
      "Synthetic pipeline release-to-result: native live " + String(describing: nativeLatency)
        + "; recorded clip " + String(describing: recordedLatency))
    await live.cancel()
  }

}
