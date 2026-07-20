# Backend load test - BN Autotrade
$ErrorActionPreference = "Continue"

$BASE = "http://127.0.0.1:8020"

Write-Host "=== Wait 3s for autotrade loop to settle ==="
Start-Sleep -Seconds 3

Write-Host ""
Write-Host "=== Prime cache: 3 sequential /autotrade/status ==="
1..3 | ForEach-Object {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $r = Invoke-RestMethod -Method GET "$BASE/autotrade/status" -TimeoutSec 15
        $sw.Stop()
        Write-Host "  call $_ : $($sw.ElapsedMilliseconds) ms | running=$($r.running) | winsToday=$($r.liveStats.winsToday)"
    } catch {
        $sw.Stop()
        Write-Host "  call $_ : $($sw.ElapsedMilliseconds) ms TIMEOUT: $_"
    }
}

Write-Host ""
Write-Host "=== Concurrent /autotrade/status x 8 (background jobs, throttle 8) ==="
$jobs = 1..8 | ForEach-Object {
    Start-Job -ScriptBlock {
        param($i, $base)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $r = Invoke-RestMethod -Method GET "$base/autotrade/status" -TimeoutSec 15
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $true; Status = $r.running }
        } catch {
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $false; Err = $_.ToString() }
        }
    } -ArgumentList $_, $BASE
}
$jobs | Wait-Job -Timeout 30 | Out-Null
$results = $jobs | Receive-Job
$jobs | Remove-Job -Force
$results | Sort-Object Id | Format-Table -AutoSize
$ok = ($results | Where-Object { $_.Ok }).Count
if ($ok -gt 0) {
    $avg = [math]::Round((($results | Where-Object { $_.Ok } | Measure-Object Ms -Average).Average), 0)
    $max = ($results | Where-Object { $_.Ok } | Measure-Object Ms -Maximum).Maximum
} else {
    $avg = 0
    $max = 0
}
Write-Host "OK: $ok/8 | Avg: $avg ms | Max: $max ms"

Write-Host ""
Write-Host "=== Hammer /autotrade/status-lite x 16 (dashboard's hot path) ==="
$jobs2 = 1..16 | ForEach-Object {
    Start-Job -ScriptBlock {
        param($i, $base)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $r = Invoke-RestMethod -Method GET "$base/autotrade/status-lite" -TimeoutSec 10
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $true }
        } catch {
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $false; Err = $_.ToString() }
        }
    } -ArgumentList $_, $BASE
}
$jobs2 | Wait-Job -Timeout 30 | Out-Null
$results2 = $jobs2 | Receive-Job
$jobs2 | Remove-Job -Force
$results2 | Sort-Object Id | Format-Table -AutoSize
$ok2 = ($results2 | Where-Object { $_.Ok }).Count
if ($ok2 -gt 0) {
    $avg2 = [math]::Round((($results2 | Where-Object { $_.Ok } | Measure-Object Ms -Average).Average), 0)
    $max2 = ($results2 | Where-Object { $_.Ok } | Measure-Object Ms -Maximum).Maximum
} else {
    $avg2 = 0
    $max2 = 0
}
Write-Host "OK: $ok2/16 | Avg: $avg2 ms | Max: $max2 ms"

Write-Host ""
Write-Host "=== /health latency under load ==="
$jobs3 = 1..5 | ForEach-Object {
    Start-Job -ScriptBlock {
        param($i, $base)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $r = Invoke-RestMethod -Method GET "$base/health" -TimeoutSec 5
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $true; WS = $r.uptimeSec }
        } catch {
            $sw.Stop()
            return [PSCustomObject]@{ Id = $i; Ms = $sw.ElapsedMilliseconds; Ok = $false }
        }
    } -ArgumentList $_, $BASE
}
$jobs3 | Wait-Job -Timeout 15 | Out-Null
$results3 = $jobs3 | Receive-Job
$jobs3 | Remove-Job -Force
$results3 | Sort-Object Id | Format-Table -AutoSize
