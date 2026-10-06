// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Research probe, not a production provider. No model downloads or network requests.
// Build: xcrun swiftc -module-cache-path /tmp/antfly-apple-module-cache \
//   scripts/apple-provider-probe.swift -o /tmp/antfly-apple-provider-probe
// Run with no arguments for a synthetic OCR check, or pass an image file path.
import Foundation
import CoreGraphics
import CoreText
import ImageIO
import Vision

enum ProbeError: Error {
    case invalidArguments, imageCreationFailed, imageLoadFailed, unexpectedOCR
}

func syntheticImage() throws -> CGImage {
    guard let context = CGContext(
        data: nil, width: 1200, height: 360, bitsPerComponent: 8,
        bytesPerRow: 0, space: CGColorSpaceCreateDeviceRGB(),
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else { throw ProbeError.imageCreationFailed }
    context.setFillColor(CGColor(gray: 1, alpha: 1))
    context.fill(CGRect(x: 0, y: 0, width: 1200, height: 360))
    let font = CTFontCreateWithName("Helvetica" as CFString, 44, nil)
    for (index, text) in ["Antfly local OCR", "Invoice 12345", "Total USD 42.00"].enumerated() {
        let line = CTLineCreateWithAttributedString(NSAttributedString(
            string: text,
            attributes: [
                NSAttributedString.Key(kCTFontAttributeName as String): font,
                NSAttributedString.Key(kCTForegroundColorAttributeName as String): CGColor(gray: 0, alpha: 1)
            ]
        ))
        context.textPosition = CGPoint(x: 60, y: 270 - index * 90)
        CTLineDraw(line, context)
    }
    guard let image = context.makeImage() else { throw ProbeError.imageCreationFailed }
    return image
}

func probe() throws {
    if CommandLine.arguments.count == 3 && CommandLine.arguments[1] == "--write-fixture" {
        let url = URL(fileURLWithPath: CommandLine.arguments[2])
        guard let destination = CGImageDestinationCreateWithURL(url as CFURL, "public.png" as CFString, 1, nil)
        else { throw ProbeError.imageCreationFailed }
        CGImageDestinationAddImage(destination, try syntheticImage(), nil)
        guard CGImageDestinationFinalize(destination) else { throw ProbeError.imageCreationFailed }
        return
    }
    guard CommandLine.arguments.count <= 2 else { throw ProbeError.invalidArguments }
    let synthetic = CommandLine.arguments.count == 1
    let image: CGImage
    if synthetic {
        image = try syntheticImage()
    } else {
        let url = URL(fileURLWithPath: CommandLine.arguments[1])
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let loaded = CGImageSourceCreateImageAtIndex(source, 0, nil)
        else { throw ProbeError.imageLoadFailed }
        image = loaded
    }
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.recognitionLanguages = ["en-US"]
    request.usesLanguageCorrection = false
    try VNImageRequestHandler(cgImage: image, orientation: .up).perform([request])
    // Suitable for this single-column fixture; production needs layout-aware ordering.
    let observations = (request.results ?? []).sorted {
        $0.boundingBox.midY > $1.boundingBox.midY
    }
    let lines: [[String: Any]] = observations.compactMap { observation in
        guard let candidate = observation.topCandidates(1).first else { return nil }
        let rect = observation.boundingBox
        return [
            "text": candidate.string,
            "confidence": candidate.confidence,
            // Vision has a bottom-left origin; Antfly reader boxes use pixels.
            "bbox_top_left_pixels": [
                rect.minX * Double(image.width), (1 - rect.maxY) * Double(image.height),
                rect.maxX * Double(image.width), (1 - rect.minY) * Double(image.height)
            ]
        ]
    }
    var report: [String: Any] = [
        "os": ProcessInfo.processInfo.operatingSystemVersionString,
        "fixture": synthetic ? "synthetic" : "file",
        "vision_revision": request.revision,
        "supported_ocr_languages": try request.supportedRecognitionLanguages(),
        "lines": lines,
        "generation_runtime_tested": false,
        "transcription_runtime_tested": false
    ]
    #if canImport(FoundationModels)
    report["foundation_models_sdk_present"] = true
    #else
    report["foundation_models_sdk_present"] = false
    #endif
    let bytes = try JSONSerialization.data(withJSONObject: report, options: [.prettyPrinted, .sortedKeys])
    FileHandle.standardOutput.write(bytes)
    FileHandle.standardOutput.write(Data("\n".utf8))
    if synthetic {
        let actual = lines.compactMap { $0["text"] as? String }
        guard actual == ["Antfly local OCR", "Invoice 12345", "Total USD 42.00"]
        else { throw ProbeError.unexpectedOCR }
    }
}

do {
    try probe()
} catch {
    FileHandle.standardError.write(Data("Apple provider probe failed: \(error)\n".utf8))
    exit(1)
}
