[CmdletBinding()]
param(
    [string]$Exe = (Join-Path $PSScriptRoot '..\target\release\agent-world.exe'),
    [int]$WarmupSeconds = 300,
    [int]$SampleSeconds = 60
)

$ErrorActionPreference = 'Stop'
$exePath = (Resolve-Path -LiteralPath $Exe).Path
$tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$fixtureRoot = Join-Path $tempBase ("AgentWorldResource-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $fixtureRoot | Out-Null
$fixturePath = (Resolve-Path -LiteralPath $fixtureRoot).Path
if (!$fixturePath.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -or
    !(Split-Path -Leaf $fixturePath).StartsWith('AgentWorldResource-')) {
    throw "Refusing unsafe fixture path: $fixturePath"
}

function Get-OwnedProcesses([int]$RootId) {
    $all = Get-CimInstance Win32_Process
    $ids = [Collections.Generic.HashSet[int]]::new()
    [void]$ids.Add($RootId)
    do {
        $changed = $false
        foreach ($item in $all) {
            if ($ids.Contains([int]$item.ParentProcessId) -and $ids.Add([int]$item.ProcessId)) {
                $changed = $true
            }
        }
    } while ($changed)
    Get-Process -Id @($ids | ForEach-Object { $_ }) -ErrorAction SilentlyContinue
}

$process = $null
try {
    & $exePath --seed-resource-fixture --runtime-root $fixturePath
    if ($LASTEXITCODE -ne 0) { throw 'Fixture generation failed' }

    $startupClock = [Diagnostics.Stopwatch]::StartNew()
    $process = Start-Process -FilePath $exePath -ArgumentList @('--runtime-root', "`"$fixturePath`"") -PassThru
    $interactive = $false
    while ($startupClock.Elapsed.TotalSeconds -lt 15 -and !$process.HasExited) {
        $process.Refresh()
        if ($process.MainWindowHandle -ne 0) {
            $interactive = $true
            break
        }
        Start-Sleep -Milliseconds 50
    }
    $startupSeconds = [Math]::Round($startupClock.Elapsed.TotalSeconds, 3)
    if (!$interactive) { throw 'Interactive window was not observed within 15 seconds' }

    if ($WarmupSeconds -gt 0) { Start-Sleep -Seconds $WarmupSeconds }
    $ownedAtStart = @(Get-OwnedProcesses $process.Id)
    $cpuStart = ($ownedAtStart | Measure-Object CPU -Sum).Sum
    $privateSamples = [Collections.Generic.List[double]]::new()
    $nodeSeen = $false
    $sampleClock = [Diagnostics.Stopwatch]::StartNew()
    while ($sampleClock.Elapsed.TotalSeconds -lt $SampleSeconds) {
        $owned = @(Get-OwnedProcesses $process.Id)
        $privateSamples.Add((($owned | Measure-Object PrivateMemorySize64 -Sum).Sum / 1MB))
        $nodeSeen = $nodeSeen -or [bool]($owned | Where-Object ProcessName -eq 'node')
        Start-Sleep -Milliseconds 500
    }
    $ownedAtEnd = @(Get-OwnedProcesses $process.Id)
    $cpuEnd = ($ownedAtEnd | Measure-Object CPU -Sum).Sum
    $elapsed = $sampleClock.Elapsed.TotalSeconds
    $maxPrivateMb = [Math]::Round(($privateSamples | Measure-Object -Maximum).Maximum, 2)
    $averageCpuPercent = [Math]::Round((($cpuEnd - $cpuStart) / $elapsed) * 100, 3)
    $result = [ordered]@{
        fixture = [ordered]@{ projects = 5; visible_threads = 50; persisted_messages = 20000 }
        startup_seconds = $startupSeconds
        max_private_mb = $maxPrivateMb
        average_idle_cpu_percent = $averageCpuPercent
        owned_processes = @($ownedAtEnd | Select-Object Id, ProcessName, PrivateMemorySize64)
        node_at_idle = $nodeSeen
        pass = [ordered]@{
            startup = $startupSeconds -le 3
            private_memory = $maxPrivateMb -le 250
            idle_cpu = $averageCpuPercent -le 0.5
            no_node = !$nodeSeen
            no_process_per_character = $ownedAtEnd.Count -lt 50
        }
    }
    $result | ConvertTo-Json -Depth 5
    if ($result.pass.Values -contains $false) { exit 1 }
}
finally {
    if ($process -and !$process.HasExited) {
        [void]$process.CloseMainWindow()
        if (!$process.WaitForExit(3000)) { Stop-Process -Id $process.Id -Force }
    }
    if (Test-Path -LiteralPath $fixturePath) {
        for ($attempt = 0; $attempt -lt 20; $attempt++) {
            try {
                Remove-Item -LiteralPath $fixturePath -Recurse -Force
                break
            }
            catch {
                if ($attempt -eq 19) { throw }
                Start-Sleep -Milliseconds 250
            }
        }
    }
}
