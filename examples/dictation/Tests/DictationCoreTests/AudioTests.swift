// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@Test(arguments: [44100.0, 48000.0, 96000.0])
func microphoneConversionPreservesOneSecondOfStereoSpeech(rate: Double) throws {
  let sink = SpeechAudioBuffer()
  let format = AVAudioFormat(
    commonFormat: .pcmFormatFloat32, sampleRate: rate, channels: 2, interleaved: false)!
  let converter = try CaptureConverter(input: format, sink: sink)
  var offset = 0
  while offset < Int(rate) {
    let frames = min(4096, Int(rate) - offset)
    let block = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: AVAudioFrameCount(frames))!
    block.frameLength = AVAudioFrameCount(frames)
    for frame in 0..<frames {
      let value = Float(sin(Double(offset + frame) * 2 * .pi * 440 / rate) * 0.25)
      block.floatChannelData![0][frame] = value
      block.floatChannelData![1][frame] = value
    }
    converter.append(block)
    offset += frames
  }
  converter.finish()
  let bytes = try sink.all()
  #expect((15900...16100).contains(bytes.count / 4))
  let energy = bytes.withUnsafeBytes { raw in
    raw.bindMemory(to: Float.self).reduce(0.0) { $0 + Double($1 * $1) }
  }
  #expect(energy > 100 && energy < 1000)
}

@Test @MainActor func finalAudioUsesLittleEndianPCM16Wav() throws {
  let microphone = Microphone()
  let format = AVAudioFormat(
    commonFormat: .pcmFormatFloat32, sampleRate: 16000, channels: 1, interleaved: false)!
  let block = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 3)!
  block.frameLength = 3
  block.floatChannelData![0][0] = -1
  block.floatChannelData![0][1] = 0
  block.floatChannelData![0][2] = 1
  microphone.buffer.append(block)
  let wav = try microphone.wav()
  #expect(wav.count == 50)
  #expect(String(decoding: wav.prefix(4), as: UTF8.self) == "RIFF")
  #expect(wav[20] == 1 && wav[21] == 0)  // integer PCM
  #expect(wav[34] == 16 && wav[35] == 0)  // bits per sample
  #expect(Array(wav.suffix(6)) == [1, 128, 0, 0, 255, 127])
}

@Test func recordingEncodingIsCancellationAwareAndRejectsMalformedPCM() async throws {
  let malformed = DictationCore.PCMRecording(samples: Data([0, 0, 0]), sampleRate: 16000)
  #expect(throws: DictationCore.DictationError.self) { try malformed.wav() }
  let recording = DictationCore.PCMRecording(
    samples: Data(count: 16000 * 600 * 4), sampleRate: 16000)
  let worker = Task {
    withUnsafeCurrentTask { $0?.cancel() }
    return try await recording.encoded()
  }
  do {
    _ = try await worker.value
    Issue.record("Canceled recording encoded fully")
  } catch is CancellationError {}
}

@Test func audioArchiveCancellationLeavesNoPublishedOrTemporaryRecording() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }
  let worker = Task {
    withUnsafeCurrentTask { $0?.cancel() }
    return try await RecordingArchive.stage(Data(count: 1024), in: directory)
  }
  do {
    _ = try await worker.value
    Issue.record("Canceled audio archive was staged")
  } catch is CancellationError {}
  #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).isEmpty)
}

@Test func previewBackpressureDoesNotDestroyTheCompleteRecording() throws {
  let sink = SpeechAudioBuffer()
  let format = AVAudioFormat(
    commonFormat: .pcmFormatFloat32, sampleRate: 16000, channels: 1, interleaved: false)!
  let frames: AVAudioFrameCount = 2_100_000
  let block = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames)!
  block.frameLength = frames
  block.floatChannelData![0].initialize(repeating: 0, count: Int(frames))
  sink.append(block)
  #expect(throws: DictationError.self) { try sink.drain() }
  #expect(try sink.all().count == Int(frames) * 4)
  block.frameLength = 1
  sink.append(block)
  #expect(try sink.drain().count == 4)
  #expect(try sink.all().count == (Int(frames) + 1) * 4)
}

@Test func structuredServerFailureIncludesRetryAfterWithoutRetainingTheResponse() throws {
  let response = HTTPURLResponse(
    url: URL(string: "https://example.com")!, statusCode: 429,
    httpVersion: nil, headerFields: ["Retry-After": "5"])!
  let failure = ServerFailure(
    response: response,
    body: Data("{\"error\":{\"message\":\"No free slot\",\"code\":\"capacity\"}}".utf8))
  #expect(failure.code == "capacity" && failure.retryAfter == "5")
  #expect(failure.localizedDescription.contains("Retry-After: 5"))
}

@Test func crashRecoveryRemovesOnlyUncommittedAudioCandidates() throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }
  let candidate = directory.appendingPathComponent(".recording-" + UUID().uuidString + ".wav")
  try Data([1]).write(to: candidate)
  let retained = directory.appendingPathComponent("last-recording.wav")
  try Data([2]).write(to: retained)
  try RecordingArchive.removeStaleCandidates(in: directory)
  #expect(!FileManager.default.fileExists(atPath: candidate.path))
  #expect(try Data(contentsOf: retained) == Data([2]))
}

@Test @MainActor func microphoneTapCreatedOnMainActorAcceptsBackgroundAudio() async throws {
  let sink = SpeechAudioBuffer()
  let format = AVAudioFormat(
    commonFormat: .pcmFormatFloat32, sampleRate: 48000, channels: 1, interleaved: false)!
  let converter = try CaptureConverter(input: format, sink: sink)
  let tap = converter.makeTap()
  await Task.detached {
    let format = AVAudioFormat(
      commonFormat: .pcmFormatFloat32, sampleRate: 48000, channels: 1, interleaved: false)!
    let block = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 4800)!
    block.frameLength = 4800
    block.floatChannelData![0].initialize(repeating: 0.25, count: 4800)
    tap(block, AVAudioTime(sampleTime: 0, atRate: 48000))
    converter.finish()
  }.value
  let samples = try sink.all()
  #expect((1500...1700).contains(samples.count / MemoryLayout<Float>.size))
}
