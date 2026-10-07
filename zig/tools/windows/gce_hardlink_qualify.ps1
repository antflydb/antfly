# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# Focused startup runner for a disposable private Windows qualification VM.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = 'C:\antfly-hardlink-test'
$metadata = 'http://metadata.google.internal/computeMetadata/v1/instance'
$headers = @{'Metadata-Flavor' = 'Google'}
function Publish-Status($value) {
    Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/hardlinks" `
        -Headers $headers -Body ($value | ConvertTo-Json -Depth 8 -Compress) | Out-Null
}
try {
    New-Item -ItemType Directory -Force -Path $root | Out-Null
    Publish-Status @{stage = 'download'}
    $bucket = Invoke-RestMethod -Uri "$metadata/attributes/antfly-artifact-bucket" -Headers $headers
    $token = Invoke-RestMethod -Uri "$metadata/service-accounts/default/token" -Headers $headers
    Invoke-WebRequest -UseBasicParsing -Uri "https://storage.googleapis.com/storage/v1/b/$bucket/o/tests.zip?alt=media" `
        -Headers @{'Authorization' = "Bearer $($token.access_token)"} -OutFile "$root\tests.zip" -TimeoutSec 60
    Expand-Archive -Path "$root\tests.zip" -DestinationPath $root -Force
    $manifest = Get-Content "$root\manifest.json" -Raw | ConvertFrom-Json
    $volume = Get-Volume -FilePath $root
    if ($volume.FileSystem -ne 'NTFS') { throw "Expected NTFS, got $($volume.FileSystem)" }
    $results = @()
    foreach ($test in $manifest.executables) {
        $path = Join-Path $root $test.name
        $actual = (Get-FileHash $path -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne $test.sha256) { throw "Hash mismatch: $($test.name)" }
        Publish-Status @{stage = 'test'; current = $test.name}
        # Own the .NET process and drain both pipes while waiting. Windows
        # PowerShell Start-Process can lose ExitCode after its process exits.
        $start = New-Object System.Diagnostics.ProcessStartInfo
        $start.FileName = $path
        $start.WorkingDirectory = $root
        $start.UseShellExecute = $false
        $start.CreateNoWindow = $true
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $start
        try {
            if (!$process.Start()) { throw "Cannot start test: $($test.name)" }
            $stdout = $process.StandardOutput.ReadToEndAsync()
            $stderr = $process.StandardError.ReadToEndAsync()
            if (!$process.WaitForExit(120000)) {
                $process.Kill()
                $process.WaitForExit()
                throw "Test timed out: $($test.name)"
            }
            $process.WaitForExit()
            $results += @{
                name = $test.name; sha256 = $actual; exit_code = $process.ExitCode
                stdout = $stdout.GetAwaiter().GetResult()
                stderr = $stderr.GetAwaiter().GetResult()
            }
        } finally {
            $process.Dispose()
        }
    }
    $result = @{
        stage = 'complete'; filesystem = $volume.FileSystem
        windows_build = [Environment]::OSVersion.Version.ToString()
        tests = $results
    }
    $json = $result | ConvertTo-Json -Depth 8 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($json)
    $token = Invoke-RestMethod -Uri "$metadata/service-accounts/default/token" -Headers $headers
    Invoke-RestMethod -Method Post -Uri "https://storage.googleapis.com/upload/storage/v1/b/$bucket/o?uploadType=media&name=results.json" `
        -Headers @{'Authorization' = "Bearer $($token.access_token)"} -ContentType 'application/json' -Body $bytes | Out-Null
    Publish-Status @{stage = 'complete'; tests = @($results | ForEach-Object { @{name = $_.name; exit_code = $_.exit_code} })}
} catch {
    Publish-Status @{stage = 'failed'; error = $_.Exception.Message}
    throw
}
