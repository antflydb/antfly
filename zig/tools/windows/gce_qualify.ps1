# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

# Startup script for a disposable VM only. HTTP ports must be private and
# restricted to IAP; the fixed check endpoint terminates this test server.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = 'C:\antfly-test'
New-Item -ItemType Directory -Force -Path $root | Out-Null
Start-Transcript -Path "$root\startup.log" -Append
$metadata = 'http://metadata.google.internal/computeMetadata/v1/instance'
$headers = @{'Metadata-Flavor' = 'Google'}
try {
    $bucket = Invoke-RestMethod -Uri "$metadata/attributes/antfly-artifact-bucket" -Headers $headers
    $expectedHash = Invoke-RestMethod -Uri "$metadata/attributes/antfly-artifact-sha256" -Headers $headers
    if (!(Test-Path "$root\artifacts-ready") -or
        (Get-FileHash "$root\antfly.exe" -Algorithm SHA256).Hash -ne $expectedHash) {
        Write-Host 'Downloading qualification artifact'
        Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/status" `
            -Headers $headers -Body '{"stage":"download"}' | Out-Null
        $downloaded = $false
        for ($attempt = 0; $attempt -lt 120; $attempt++) {
            try {
                $token = Invoke-RestMethod -Uri "$metadata/service-accounts/default/token" -Headers $headers
                Invoke-WebRequest -UseBasicParsing -Uri "https://storage.googleapis.com/storage/v1/b/$bucket/o/tests.zip?alt=media" `
                    -Headers @{'Authorization' = "Bearer $($token.access_token)"} -OutFile "$root\tests.zip" -TimeoutSec 60
                $downloaded = $true
                break
            } catch {
                Write-Host "Artifact download attempt failed: $($_.Exception.Message)"
                Start-Sleep -Seconds 10
            }
        }
        if (!$downloaded) { throw 'Test artifact download failed' }
        Write-Host 'Extracting qualification artifact'
        Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/status" `
            -Headers $headers -Body '{"stage":"extract"}' | Out-Null
        Expand-Archive -Path "$root\tests.zip" -DestinationPath $root -Force
        if ((Get-FileHash "$root\antfly.exe" -Algorithm SHA256).Hash -ne $expectedHash) {
            throw 'Qualification binary hash does not match instance metadata'
        }
        New-Item -ItemType File -Path "$root\artifacts-ready" -Force | Out-Null
    }
    $volume = Get-Volume -FilePath $root
    if ($volume.FileSystem -ne 'NTFS') { throw "Expected NTFS, got $($volume.FileSystem)" }
    $mode = Invoke-RestMethod -Uri "$metadata/attributes/antfly-mode" -Headers $headers
    if ($mode -eq 'check') {
        $check = Start-Process -FilePath "$root\antfly.exe" -ArgumentList @('lite', 'check', "$root\data.aflite") `
            -Wait -PassThru -RedirectStandardOutput "$root\check-out.log" -RedirectStandardError "$root\check-err.log"
        $result = @{exit_code = $check.ExitCode; output = (Get-Content "$root\check-out.log", "$root\check-err.log" | Out-String)}
        Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/check" `
            -Headers $headers -Body ($result | ConvertTo-Json -Compress) | Out-Null
        return
    }
    if ($mode -ne 'serve') { throw "Unknown qualification mode: $mode" }
    $tests = @{}
    foreach ($name in @('compat-test.exe', 'staged-test.exe', 'storage-test.exe', 'object-durability-test.exe')) {
        Write-Host "Running $name"
        $test = Start-Process -FilePath "$root\$name" -Wait -PassThru `
            -RedirectStandardOutput "$root\$name-out.log" -RedirectStandardError "$root\$name-err.log"
        $tests[$name] = $test.ExitCode
        if ($test.ExitCode -ne 0) { throw "Test failed: $name; see its log" }
    }
    New-NetFirewallRule -DisplayName 'Antfly disposable IAP tests' -Direction Inbound `
        -Protocol TCP -LocalPort 8080,9090 -RemoteAddress 35.235.240.0/20 -Action Allow -ErrorAction SilentlyContinue | Out-Null
    $server = Start-Process -FilePath "$root\antfly.exe" -ArgumentList @(
        'standalone', '--host', '0.0.0.0', '--port', '8080', '--health', 'false',
        '--storage-engine', 'lite', '--storage-path', "$root\data.aflite",
        '--data-dir', "$root\runtime", '--fsync', 'true'
    ) -PassThru -RedirectStandardOutput "$root\server-out.log" -RedirectStandardError "$root\server-err.log"
    $status = @{
        boot = [guid]::NewGuid().ToString(); windows = [Environment]::OSVersion.VersionString
        filesystem = $volume.FileSystem; tests = $tests
        sha256 = (Get-FileHash "$root\antfly.exe" -Algorithm SHA256).Hash
        server_pid = $server.Id
    }
    Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/status" `
        -Headers $headers -Body ($status | ConvertTo-Json -Compress) | Out-Null
    $listener = New-Object System.Net.HttpListener
    $listener.Prefixes.Add('http://+:9090/')
    $listener.Start()
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $result = $status
        if ($context.Request.HttpMethod -eq 'GET' -and $context.Request.Url.AbsolutePath -eq '/ntdll') {
            $stream = [IO.File]::OpenRead("$env:SystemRoot\System32\ntdll.dll")
            try {
                $context.Response.ContentType = 'application/octet-stream'
                $context.Response.ContentLength64 = $stream.Length
                $stream.CopyTo($context.Response.OutputStream)
            } finally { $stream.Dispose(); $context.Response.Close() }
            continue
        }
        if ($context.Request.HttpMethod -eq 'GET' -and $context.Request.Url.AbsolutePath -eq '/diagnostics') {
            $target = [Diagnostics.Process]::GetProcessById($server.Id)
            $result = @{
                cpu_seconds = $target.TotalProcessorTime.TotalSeconds
                working_set = $target.WorkingSet64
                data_bytes = (Get-Item "$root\data.aflite").Length
            }
            $target.Dispose()
            $bytes = [Text.Encoding]::UTF8.GetBytes(($result | ConvertTo-Json -Depth 8 -Compress))
            $context.Response.ContentType = 'application/json'
            $context.Response.OutputStream.Write($bytes, 0, $bytes.Length)
            $context.Response.Close()
            continue
        }
        if ($context.Request.HttpMethod -eq 'POST' -and $context.Request.Url.AbsolutePath -eq '/dump') {
            if (!('AntflyTestDump' -as [type])) {
                Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class AntflyTestDump {
    [DllImport("dbghelp.dll", SetLastError = true)]
    public static extern bool MiniDumpWriteDump(IntPtr process, uint pid, IntPtr file,
        uint flags, IntPtr exception, IntPtr streams, IntPtr callback);
}
'@
            }
            $target = [Diagnostics.Process]::GetProcessById($server.Id)
            $dump = [IO.File]::Create("$root\hang.dmp")
            try {
                if (![AntflyTestDump]::MiniDumpWriteDump($target.Handle, $server.Id,
                    $dump.SafeFileHandle.DangerousGetHandle(), 0x1000, [IntPtr]::Zero, [IntPtr]::Zero, [IntPtr]::Zero)) {
                    throw (New-Object ComponentModel.Win32Exception([Runtime.InteropServices.Marshal]::GetLastWin32Error()))
                }
            } finally { $dump.Dispose(); $target.Dispose() }
            $stream = [IO.File]::OpenRead("$root\hang.dmp")
            try {
                $context.Response.ContentType = 'application/octet-stream'
                $context.Response.ContentLength64 = $stream.Length
                $stream.CopyTo($context.Response.OutputStream)
            } finally { $stream.Dispose(); $context.Response.Close() }
            continue
        }
        if ($context.Request.HttpMethod -eq 'POST' -and $context.Request.Url.AbsolutePath -eq '/check') {
            Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
            $server.WaitForExit()
            $check = Start-Process -FilePath "$root\antfly.exe" -ArgumentList @('lite', 'check', "$root\data.aflite") `
                -Wait -PassThru -RedirectStandardOutput "$root\check-out.log" -RedirectStandardError "$root\check-err.log"
            $output = (Get-Content "$root\check-out.log", "$root\check-err.log" | Out-String)
            $result = @{exit_code = $check.ExitCode; output = $output}
        } elseif ($context.Request.HttpMethod -ne 'GET' -or $context.Request.Url.AbsolutePath -ne '/status') {
            $context.Response.StatusCode = 404
            $result = @{error = 'Unknown endpoint'}
        }
        $bytes = [Text.Encoding]::UTF8.GetBytes(($result | ConvertTo-Json -Depth 8 -Compress))
        $context.Response.ContentType = 'application/json'
        $context.Response.OutputStream.Write($bytes, 0, $bytes.Length)
        $context.Response.Close()
    }
} catch {
    $failure = @{error = ($_ | Out-String)} | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Put -Uri "$metadata/guest-attributes/antfly/status" -Headers $headers -Body $failure | Out-Null
    throw
} finally { Stop-Transcript }
