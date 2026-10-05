// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import FoundationModels
import Speech
import AVFoundation

private enum BridgeError: Error { case status(Int32) }
private typealias Cancel = @convention(c) (UnsafeMutableRawPointer) -> Int32
private typealias Output = @convention(c) (UnsafeMutableRawPointer, UnsafePointer<UInt8>, Int) -> Int32

// The synchronous C boundary retains every borrowed pointer until the task has
// completed. Only the worker writes status; the semaphore publishes its write.
private final class Invocation: @unchecked Sendable {
    let context: UnsafeMutableRawPointer
    let cancel: Cancel
    let output: Output
    let limit: Int
    let finished = DispatchSemaphore(value: 0)
    var status: Int32 = 13
    init(_ context: UnsafeMutableRawPointer, _ cancel: @escaping Cancel, _ output: @escaping Output, _ limit: Int) {
        self.context = context; self.cancel = cancel; self.output = output; self.limit = limit
    }
    func check() throws {
        if Task.isCancelled || cancel(context) != 0 { throw BridgeError.status(7) }
    }
    func emit(_ data: Data) throws {
        try check()
        guard data.count <= limit else { throw BridgeError.status(9) }
        var empty: UInt8 = 0
        let result: Int32
        if data.isEmpty {
            result = withUnsafePointer(to: &empty) { output(context, $0, 0) }
        } else {
            result = data.withUnsafeBytes { bytes in
                output(context, bytes.bindMemory(to: UInt8.self).baseAddress!, data.count)
            }
        }
        if result != 0 { throw BridgeError.status(9) }
    }
}
private let gate = NSLock()

@available(macOS 26.0, *)
private func modelStatus() -> Int32 {
    switch SystemLanguageModel.default.availability {
    case .available: return 0
    case .unavailable(.appleIntelligenceNotEnabled): return 3
    case .unavailable(.modelNotReady): return 4
    case .unavailable: return 2
    }
}

@available(macOS 26.0, *)
private func generate(_ request: [String: Any], _ invocation: Invocation) async throws {
    let status = modelStatus()
    guard status == 0 else { throw BridgeError.status(status) }
    guard let messages = request["messages"] as? [[String: String]],
          let last = messages.last, last["role"] == "user",
          let prompt = last["text"], !prompt.isEmpty else { throw BridgeError.status(1) }
    var entries: [Transcript.Entry] = []
    var instructions: [String] = []
    var history: [[String: String]] = []
    for message in messages.dropLast() {
        guard let role = message["role"], let text = message["text"] else { throw BridgeError.status(1) }
        if role == "system" {
            guard history.isEmpty else { throw BridgeError.status(1) }
            instructions.append(text)
        } else { history.append(message) }
    }
    if !instructions.isEmpty {
        entries.append(.instructions(.init(segments: [.text(.init(content: instructions.joined(separator: "\n")))], toolDefinitions: [])))
    }
    for (index, message) in history.enumerated() {
        let role = message["role"]!
        guard role == (index % 2 == 0 ? "user" : "assistant") else { throw BridgeError.status(1) }
        let segments: [Transcript.Segment] = [.text(.init(content: message["text"]!))]
        if role == "user" { entries.append(.prompt(.init(segments: segments))) }
        else { entries.append(.response(.init(assetIDs: [], segments: segments))) }
    }
    guard history.count % 2 == 0 else { throw BridgeError.status(1) }
    let session = LanguageModelSession(model: .default, transcript: Transcript(entries: entries))
    let options = GenerationOptions(temperature: request["temperature"] as? Double,
                                    maximumResponseTokens: request["max_tokens"] as? Int)
    var content = ""
    // Bound snapshots before retaining them, and propagate task cancellation.
    for try await snapshot in session.streamResponse(to: prompt, options: options) {
        try invocation.check()
        guard snapshot.content.utf8.count <= invocation.limit else { throw BridgeError.status(9) }
        content = snapshot.content
    }
    try invocation.emit(Data(content.utf8))
}

@available(macOS 26.0, *)
private func transcribe(_ request: [String: Any], _ audio: Data, _ invocation: Invocation) async throws {
    guard SpeechTranscriber.isAvailable else { throw BridgeError.status(2) }
    let language = request["language"] as? String ?? "en-US"
    guard let locale = await SpeechTranscriber.supportedLocale(equivalentTo: Locale(identifier: language)) else {
        throw BridgeError.status(5)
    }
    let timestamps = request["timestamps"] as? Bool ?? true
    let transcriber = SpeechTranscriber(locale: locale, transcriptionOptions: [], reportingOptions: [],
                                       attributeOptions: timestamps ? [.audioTimeRange] : [])
    if let installation = try await AssetInventory.assetInstallationRequest(supporting: [transcriber]) {
        guard request["download_assets"] as? Bool == true else { throw BridgeError.status(6) }
        try await installation.downloadAndInstall()
    }
    try invocation.check()
    let path = FileManager.default.temporaryDirectory.appendingPathComponent("antfly-speech-\(UUID().uuidString).audio")
    try audio.write(to: path, options: [.atomic, .completeFileProtectionUnlessOpen])
    defer { try? FileManager.default.removeItem(at: path) }
    let file = try AVAudioFile(forReading: path)
    let duration = Double(file.length) / file.processingFormat.sampleRate
    guard duration.isFinite, duration >= 0, duration <= 3600 else { throw BridgeError.status(14) }
    let analyzer = SpeechAnalyzer(modules: [transcriber])
    let analysis = Task { try await analyzer.start(inputAudioFile: file, finishAfterFile: true) }
    do {
        var segments: [[String: Any]] = []
        var texts: [String] = []
        var charged = 1024
        for try await result in transcriber.results {
            try invocation.check()
            let text = String(result.text.characters)
            charged += text.utf8.count * 12 + 256
            guard charged <= invocation.limit, segments.count < 16384 else { throw BridgeError.status(9) }
            var segment: [String: Any] = ["text": text,
                "start_ms": Int64((result.range.start.seconds * 1000).rounded()),
                "end_ms": Int64((CMTimeRangeGetEnd(result.range).seconds * 1000).rounded())]
            if timestamps {
                var words: [[String: Any]] = []
                for run in result.text.runs {
                    guard let range = run.audioTimeRange else { continue }
                    let word = String(result.text[run.range].characters)
                    charged += word.utf8.count * 6 + 128
                    guard charged <= invocation.limit else { throw BridgeError.status(9) }
                    words.append(["word": word,
                        "start_ms": Int64((range.start.seconds * 1000).rounded()),
                        "end_ms": Int64((CMTimeRangeGetEnd(range).seconds * 1000).rounded())])
                }
                segment["words"] = words
            }
            texts.append(text); segments.append(segment)
        }
        try await analysis.value
        let response: [String: Any] = ["text": texts.joined(separator: " "), "language": locale.identifier,
            "duration_ms": Int64((duration * 1000).rounded()), "segments": timestamps ? segments : []]
        try invocation.emit(JSONSerialization.data(withJSONObject: response))
    } catch {
        analysis.cancel()
        await analyzer.cancelAndFinishNow()
        _ = try? await analysis.value
        throw error
    }
}

@_cdecl("antfly_apple_invoke")
public func invoke(_ operation: Int32, _ json: UnsafePointer<UInt8>, _ jsonLength: Int,
                   _ audio: UnsafePointer<UInt8>?, _ audioLength: Int, _ limit: Int,
                   _ context: UnsafeMutableRawPointer, _ cancel: @escaping @convention(c) (UnsafeMutableRawPointer) -> Int32,
                   _ output: @escaping @convention(c) (UnsafeMutableRawPointer, UnsafePointer<UInt8>, Int) -> Int32) -> Int32 {
    guard #available(macOS 26.0, *) else { return 2 }
    guard gate.try() else { return 8 }
    defer { gate.unlock() }
    guard cancel(context) == 0 else { return 7 }
    guard jsonLength > 0, jsonLength <= 1024 * 1024, audioLength <= 128 * 1024 * 1024,
          let request = try? JSONSerialization.jsonObject(with: Data(bytes: json, count: jsonLength)) as? [String: Any]
    else { return 1 }
    let audioData = audio.map { Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: $0), count: audioLength, deallocator: .none) } ?? Data()
    let invocation = Invocation(context, cancel, output, limit)
    let task = Task.detached {
        defer { invocation.finished.signal() }
        do {
            switch operation {
            case 1: try await generate(request, invocation)
            case 2: try await transcribe(request, audioData, invocation)
            case 3:
                try invocation.emit(JSONSerialization.data(withJSONObject: ["generation_status": modelStatus(),
                    "speech_available": SpeechTranscriber.isAvailable,
                    "speech_installed_locales": await SpeechTranscriber.installedLocales.map { $0.identifier }]))
            default: throw BridgeError.status(1)
            }
            invocation.status = 0
        } catch BridgeError.status(let status) { invocation.status = status }
        catch is CancellationError { invocation.status = 7 }
        catch {
            if #available(macOS 27.0, *), let modelError = error as? LanguageModelError {
                switch modelError {
                case .contextSizeExceeded: invocation.status = 10
                case .guardrailViolation, .refusal: invocation.status = 11
                default: invocation.status = 13
                }
            } else if let modelError = error as? LanguageModelSession.GenerationError {
                switch modelError {
                case .exceededContextWindowSize: invocation.status = 10
                case .guardrailViolation, .refusal: invocation.status = 11
                case .assetsUnavailable: invocation.status = 4
                default: invocation.status = 13
                }
            } else { invocation.status = 13 }
        }
    }
    while invocation.finished.wait(timeout: .now() + .milliseconds(50)) == .timedOut {
        if cancel(context) != 0 { task.cancel() }
    }
    return invocation.status
}
