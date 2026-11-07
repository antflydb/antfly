// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

final class FixtureProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var reply: (@Sendable (URLRequest) throws -> (Int, String, String))?
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    do {
      let (status, type, body) = try Self.reply!(request)
      let response = HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
        headerFields: ["Content-Type": type])!
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      client?.urlProtocol(self, didLoad: Data(body.utf8))
      client?.urlProtocolDidFinishLoading(self)
    } catch { client?.urlProtocol(self, didFailWithError: error) }
  }
  override func stopLoading() {}
}

@Suite(.serialized) struct ClientTests {
  func client() throws -> AntflyClient {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [FixtureProtocol.self]
    return try AntflyClient(
      server: "https://antfly.example", token: "test-token",
      session: URLSession(configuration: configuration))
  }
  @Test func jsonSettingsPersistenceUsesAuthenticatedExpectedEndpoint() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/db/v1/tables/dictation/batch")
      #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test-token")
      return (200, "application/json", "{}")
    }
    try await client().saveSettings(Configuration())
  }
  @Test func serverErrorsAreNotSilentlyAccepted() async throws {
    FixtureProtocol.reply = { _ in (403, "application/json", "{\"error\":\"denied\"}") }
    do {
      _ = try await client().models()
      Issue.record("Expected HTTP failure")
    } catch is DictationError {}
  }
  @Test func dictationConsumesTranscriptAndCompletedEvents() async throws {
    FixtureProtocol.reply = { request in
      #expect(request.url?.path == "/ai/v1/dictate")
      #expect(request.value(forHTTPHeaderField: "Content-Type") == AttachmentEnvelope.contentType)
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.delta\",\"delta\":\"Hello.\"}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Hello.\"}\n\ndata: [DONE]\n\n"
      )
    }
    let result = try await client().dictate(
      audio: Data([1]), preset: Configuration().presets[0], dictionary: []
    ) { _ in }
    #expect(result.raw == "um hello")
    #expect(result.text == "Hello.")
  }
  @Test func truncatedStreamCannotProduceInsertableResult() async throws {
    FixtureProtocol.reply = { _ in
      (200, "text/event-stream", "data: {\"type\":\"dictation.delta\",\"delta\":\"partial\"}\n\n")
    }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected incomplete stream failure")
    } catch is DictationError {}
  }
  @Test func sessionAppendAndDeleteUseTheSamePublicHandle() async throws {
    FixtureProtocol.reply = { request in
      switch request.url!.path {
      case "/ai/v1/transcription/sessions":
        return (200, "application/json", "{\"id\":\"session_123\"}")
      case "/ai/v1/transcription/sessions/session_123/audio":
        return (200, "application/json", "{\"data\":[{\"text\":\"hello\"}]}")
      default:
        #expect(request.httpMethod == "DELETE")
        #expect(request.url!.path == "/ai/v1/transcription/sessions/session_123")
        return (200, "application/json", "{}")
      }
    }
    let client = try client()
    let id = try await client.createSession(preset: Configuration().presets[0], dictionary: [])
    let text = try await client.appendSession(id, audio: Data(), sampleRate: 16000)
    #expect(text == "hello")
    await client.closeSession(id)
  }
}

@Test func binaryAudioEnvelopeHasExactLittleEndianLayout() throws {
  let metadata = Data("{\"audio\":\"attachment:0\"}".utf8)
  let audio = Data([0, 1, 2, 255])
  let frame = try AttachmentEnvelope.audio(metadata: metadata, recording: audio)
  #expect(String(decoding: frame.prefix(8), as: UTF8.self) == "AFATT001")
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian }
      == metadata.count)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 16, as: UInt32.self).littleEndian }
      == 1)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 24, as: UInt32.self).littleEndian }
      == 9)
  #expect(
    frame.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 32, as: UInt64.self).littleEndian }
      == 4)
  #expect(frame.subdata(in: 40..<(40 + metadata.count)) == metadata)
  #expect(frame.suffix(4) == audio)
}

@Test func tenMinuteClipRemainsBinaryWithSmallMetadata() throws {
  let audio = Data(repeating: 0, count: 16000 * 2 * 600 + 44)
  let metadata = Data("{\"model\":\"transcribers/whisper\",\"audio\":\"attachment:0\"}".utf8)
  let frame = try AttachmentEnvelope.audio(metadata: metadata, recording: audio)
  #expect(frame.count == audio.count + metadata.count + 49)
  #expect(frame.count - audio.count < 1024)
}
