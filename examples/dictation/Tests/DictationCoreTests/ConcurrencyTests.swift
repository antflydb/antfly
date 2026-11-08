// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

private final class RequestMetrics: @unchecked Sendable {
  private let lock = NSLock()
  private var active = 0
  private var peak = 0
  func begin() {
    lock.lock()
    defer { lock.unlock() }
    active += 1
    peak = max(peak, active)
  }
  func end() {
    lock.lock()
    defer { lock.unlock() }
    active -= 1
  }
  func maximum() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return peak
  }
}
private final class ConcurrentProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var metrics = RequestMetrics()
  private let lock = NSLock()
  private var running = false
  private var requestMetrics: RequestMetrics?
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    lock.lock()
    running = true
    requestMetrics = Self.metrics
    requestMetrics!.begin()
    lock.unlock()
    DispatchQueue.global().asyncAfter(deadline: .now() + 0.1) { [self] in
      lock.lock()
      guard running else {
        lock.unlock()
        return
      }
      running = false
      requestMetrics!.end()
      lock.unlock()
      let response = HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!
      let body =
        "data: {\"type\":\"dictation.transcript\",\"transcript\":{\"text\":\"Speech\"}}\n\ndata: {\"type\":\"dictation.completed\",\"text\":\"Speech\"}\n\ndata: [DONE]\n\n"
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      client?.urlProtocol(self, didLoad: Data(body.utf8))
      client?.urlProtocolDidFinishLoading(self)
    }
  }
  override func stopLoading() {
    lock.lock()
    defer { lock.unlock() }
    if running {
      running = false
      requestMetrics?.end()
    }
  }
}
@Suite(.serialized) struct ConfigurationConcurrencyTests {
  @Test(arguments: [1, 2]) func productionConfigurationControlsActualOverlappingRequests(
    capacity: Int
  ) async throws {
    ConcurrentProtocol.metrics = RequestMetrics()
    let options = URLSessionConfiguration.ephemeral
    options.protocolClasses = [ConcurrentProtocol.self]
    var config = Configuration()
    config.concurrentRequests = capacity
    let client = try AntflyClient(
      configuration: config, session: URLSession(configuration: options))
    let preset = config.presets[2]
    try await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<2 {
        group.addTask {
          _ = try await client.dictate(audio: Data([1]), preset: preset, dictionary: []) { _ in }
        }
      }
      try await group.waitForAll()
    }
    #expect(ConcurrentProtocol.metrics.maximum() == capacity)
  }
}
