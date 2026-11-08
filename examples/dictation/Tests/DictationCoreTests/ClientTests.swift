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
  @Test(arguments: [
    "data: {\"type\":\"dictation.delta\",\"delta\":\"late\"}\n\n",
    "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"replacement\"}}\n\n",
    "data: {\"type\":\"dictation.completed\",\"text\":\"duplicate\"}\n\n",
  ]) func completedTextCannotBeChangedByLaterEvents(late: String) async throws {
    FixtureProtocol.reply = { _ in
      (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"raw\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Clean.\"}\n\n"
          + late + "data: [DONE]\n\n"
      )
    }
    var preset = Configuration().presets[0]
    preset.cleanupModel = "generators/cleanup"
    do {
      _ = try await client().cleanupText("raw", preset: preset, dictionary: [])
      Issue.record("Accepted a post-completion event")
    } catch let error as DictationError {
      #expect(error.rawTranscript == "raw")
    }
  }

  @Test func serverErrorsAreNotSilentlyAccepted() async throws {
    FixtureProtocol.reply = { _ in (403, "application/json", "{\"error\":\"denied\"}") }
    do {
      _ = try await client().models()
      Issue.record("Expected HTTP failure")
    } catch let error as ServerFailure {
      #expect(error.status == 403 && error.message == "denied")
    }
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
        return (
          200, "application/json",
          "{\"data\":[{\"text\":\"hello\",\"type\":\"final\",\"sequence\":1,\"start_ms\":0,\"end_ms\":1000}]}"
        )
      default:
        #expect(request.httpMethod == "DELETE")
        #expect(request.url!.path == "/ai/v1/transcription/sessions/session_123")
        return (200, "application/json", "{}")
      }
    }
    let client = try client()
    let id = try await client.createSession(preset: Configuration().presets[0], dictionary: [])
    let text = try await client.appendSession(id, audio: Data(), sampleRate: 16000, commit: true)
    #expect(text == "hello")
    await client.closeSession(id)
  }
  @Test func presetModesControlCleanupAndInstructionsOnTheWire() async throws {
    FixtureProtocol.reply = { request in
      let bytes = try fixtureBody(request)
      let size = bytes.withUnsafeBytes {
        $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian
      }
      let body =
        try JSONSerialization.jsonObject(with: bytes.subdata(in: 40..<(40 + Int(size))))
        as! [String: Any]
      #expect(body["style"] as? String == "verbatim")
      #expect(body["instructions"] as? String == "")
      #expect(body["cleanup_model"] == nil)
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"um hello\"}\n\ndata: [DONE]\n\n"
      )
    }
    var preset = Preset(id: "v", name: "Verbatim", style: "formal")
    preset.outputMode = .verbatim
    preset.cleanupModel = "generators/cleanup"
    preset.instructions = "Rewrite everything"
    let result = try await client().dictate(
      audio: Data([1]), preset: preset, dictionary: [], progress: { _ in })
    #expect(result.text == "um hello")
  }
  @Test func textCleanupAndLiveAudioUseTheirDeclaredTransports() async throws {
    FixtureProtocol.reply = { request in
      let body = try JSONSerialization.jsonObject(with: fixtureBody(request)) as! [String: Any]
      #expect(body["audio"] as? String == "")
      #expect(body["transcript_text"] as? String == "um hello")
      #expect(body["style"] as? String == "clean")
      #expect(body["instructions"] as? String == "")
      return (
        200, "text/event-stream",
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"um hello\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Hello.\"}\n\ndata: [DONE]\n\n"
      )
    }
    var preset = Preset(id: "c", name: "Clean", style: "formal")
    preset.outputMode = .clean
    preset.instructions = "Custom rewrite"
    preset.cleanupModel = "generators/cleanup"
    let result = try await client().cleanupText("um hello", preset: preset, dictionary: [])
    #expect(result.text == "Hello.")
    FixtureProtocol.reply = { request in
      #expect(request.value(forHTTPHeaderField: "Content-Type") == AttachmentEnvelope.contentType)
      let bytes = try fixtureBody(request)
      #expect(String(decoding: bytes.prefix(8), as: UTF8.self) == "AFATT001")
      let size = bytes.withUnsafeBytes {
        $0.loadUnaligned(fromByteOffset: 8, as: UInt64.self).littleEndian
      }
      let body =
        try JSONSerialization.jsonObject(with: bytes.subdata(in: 40..<(40 + Int(size))))
        as! [String: Any]
      #expect(body["audio"] as? String == "attachment:0")
      #expect(body["format"] as? String == "pcm_f32")
      return (200, "application/json", "{\"data\":[]}")
    }
    _ = try await client().appendSessionEvents(
      "session", audio: Data([0, 0, 0, 0]), sampleRate: 16000)
  }
  @Test func dictationPreservesStructuredHTTPFailureAndRetryAdvice() async throws {
    FixtureProtocol.reply = { _ in
      (
        429, "application/json",
        "{\"error\":{\"code\":\"InferenceCapacityUnavailable\",\"message\":\"No free inference slot\"}}"
      )
    }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected overload")
    } catch let error as ServerFailure {
      #expect(error.status == 429 && error.code == "InferenceCapacityUnavailable")
      #expect(error.message == "No free inference slot")
      #expect(error.localizedDescription.contains("retry"))
    }
  }
  @Test func dictationBoundsUnstructuredHTTPErrorPages() async throws {
    FixtureProtocol.reply = { _ in (400, "text/html", String(repeating: "x", count: 100_000)) }
    do {
      _ = try await client().dictate(
        audio: Data([1]), preset: Configuration().presets[0], dictionary: []
      ) { _ in }
      Issue.record("Expected bad request")
    } catch let error as ServerFailure {
      #expect(error.status == 400 && error.message.count == 2048)
    }
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

private func fixtureBody(_ request: URLRequest) throws -> Data {
  if let body = request.httpBody { return body }
  guard let stream = request.httpBodyStream else {
    throw DictationError.invalid("Missing request body")
  }
  stream.open()
  defer { stream.close() }
  var body = Data()
  var buffer = [UInt8](repeating: 0, count: 4096)
  while true {
    let count = stream.read(&buffer, maxLength: buffer.count)
    if count < 0 { throw DictationError.invalid("Failed reading fixture body") }
    if count == 0 { return body }
    body.append(contentsOf: buffer.prefix(count))
  }
}
