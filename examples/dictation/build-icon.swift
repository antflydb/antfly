// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import Foundation

// Render the original vector outline and native emoji. Intermediates stay local.
let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
let assets = root.appendingPathComponent("Assets")
let iconset = root.appendingPathComponent(".build/AppIcon.iconset")
try FileManager.default.createDirectory(at: iconset, withIntermediateDirectories: true)
guard let outline = NSImage(contentsOf: assets.appendingPathComponent("AntflyOutline.svg")),
  let font = NSFont(name: "Apple Color Emoji", size: 440)
else { fatalError("Native SVG and Apple Color Emoji rendering are required.") }

func render(size: Int) throws -> Data {
  guard
    let bitmap = NSBitmapImageRep(
      bitmapDataPlanes: nil, pixelsWide: size, pixelsHigh: size, bitsPerSample: 8,
      samplesPerPixel: 4, hasAlpha: true, isPlanar: false, colorSpaceName: .deviceRGB,
      bytesPerRow: 0, bitsPerPixel: 0), let context = NSGraphicsContext(bitmapImageRep: bitmap)
  else { fatalError("Cannot allocate icon bitmap.") }
  NSGraphicsContext.saveGraphicsState()
  defer { NSGraphicsContext.restoreGraphicsState() }
  NSGraphicsContext.current = context
  context.imageInterpolation = .high
  context.cgContext.scaleBy(x: CGFloat(size) / 1024, y: CGFloat(size) / 1024)
  NSColor(calibratedRed: 0.97, green: 0.98, blue: 0.99, alpha: 1).setFill()
  NSBezierPath(
    roundedRect: NSRect(x: 44, y: 44, width: 936, height: 936), xRadius: 208, yRadius: 208
  ).fill()
  outline.draw(in: NSRect(x: 132, y: 132, width: 760, height: 760))
  let whisper = NSAttributedString(string: "🤫", attributes: [.font: font])
  let bounds = whisper.size()
  whisper.draw(at: NSPoint(x: (1024 - bounds.width) / 2, y: (1024 - bounds.height) / 2))
  guard let png = bitmap.representation(using: .png, properties: [:]) else {
    fatalError("Cannot encode icon.")
  }
  return png
}
let variants: [(String, Int)] = [
  ("icon_16x16.png", 16), ("icon_16x16@2x.png", 32),
  ("icon_32x32.png", 32), ("icon_32x32@2x.png", 64),
  ("icon_128x128.png", 128), ("icon_128x128@2x.png", 256),
  ("icon_256x256.png", 256), ("icon_256x256@2x.png", 512),
  ("icon_512x512.png", 512), ("icon_512x512@2x.png", 1024),
]
for (name, size) in variants {
  try render(size: size).write(to: iconset.appendingPathComponent(name), options: .atomic)
}
try render(size: 1024).write(to: assets.appendingPathComponent("AppIcon.png"), options: .atomic)
let convert = Process()
convert.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
convert.arguments = [
  "-c", "icns", "-o", assets.appendingPathComponent("AppIcon.icns").path, iconset.path,
]
try convert.run()
convert.waitUntilExit()
guard convert.terminationStatus == 0 else { fatalError("iconutil failed.") }
print("Generated Assets/AppIcon.png and Assets/AppIcon.icns")
