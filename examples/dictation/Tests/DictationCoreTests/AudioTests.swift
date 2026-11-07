// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
import Foundation
import Testing

@testable import DictationApp

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
