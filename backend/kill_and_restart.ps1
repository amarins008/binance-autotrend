#Requires -RunAsAdministrator
<#
Kill-and-restart script for the Binance autotrend backend.
Kills ports 8020/8021/8040, starts run_backend.py, waits for /health.
#>

$ErrorActionPreference = "Stop"

$ROOT = "E:\My Project\Binance autotrend"
$BACKEND = Join-Path $ROOT "backend"
Set-Location $BACKEND

Write-Host "=== Binance autotrend — fresh backend restart ==="
Write-Host "Step 1/4: stopping anything on 8020/8021/8040"

foreach ($port in 8020, 8021, 8040) {
    $matches = netstat -ano |
        Select-String ":$port " |
        Select-String "LISTENING"
    foreach ($line in $matches) {
        $procId = ($line -split "\s+")[-1].Trim()
        if ($procId -match "^\d+$") {
            Write-Host "  Killing PID $procId (port $port)"
            try { Stop-Process -Id $procId -Force -ErrorAction Stop } catch {}
            try { taskkill /F /PID $procId 2>&1 | Out-Null } catch {}
        }
    }
}

Write-Host "Step 2/4: giving Windows 5s to release the sockets"
Start-Sleep -Seconds 5

$still = netstat -ano |
    Select-String ":8020 |:8040 " |
    Select-String "LISTENING"
if ($still) {
    Write-Host "  Ports still busy, retrying..."
    foreach ($line in $still) {
        $procId = ($line -split "\s+")[-1].Trim()
        if ($procId -match "^\d+$") {
            try { taskkill /F /PID $procId 2>&1 | Out-Null } catch {}
        }
    }
    Start-Sleep -Seconds 5
    $still = netstat -ano |
        Select-String ":8020 |:8040 " |
        Select-String "LISTENING"
    if ($still) {
        Write-Host "  Ports remain busy after retry. Restart aborted; run this script from an elevated Administrator console."
        exit 1
    }
}

Write-Host "Step 3/4: starting run_backend.py"
$venvPy = Join-Path $BACKEND ".venv\Scripts\python.exe"
if (Test-Path $venvPy) { $py = $venvPy } else { $py = "python" }
$runner = Join-Path $BACKEND "run_backend.py"
if (-not (Test-Path $runner)) {
    Write-Host "  run_backend.py not found at $runner"
    exit 1
}
$proc = Start-Process -FilePath $py -ArgumentList "`"$runner`"" -WorkingDirectory $BACKEND `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput "uvicorn.out.log" -RedirectStandardError "uvicorn.err.log" `
    -Environment @{ BACKEND_HOST = "0.0.0.0" }
Write-Host "  Started PID $($proc.Id)"

Write-Host "Step 4/4: polling /health (max 30s)"
$ok = $false
$deadline = (Get-Date).AddSeconds(30)
while ((Get-Date) -lt $deadline) {
    try {
        $r = Invoke-WebRequest "http://127.0.0.1:8020/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        if ($r.StatusCode -eq 200) {
            $owner = (netstat -ano |
                Select-String ":8020 " |
                Select-String "LISTENING" |
                ForEach-Object { ($_ -split "\s+")[-1].Trim() } |
                Select-Object -First 1)
            if ($owner -eq [string]$proc.Id) {
                Write-Host "  /health = 200 OK (PID $owner)"
                $ok = $true
                break
            }
            Write-Host "  /health came from existing PID $owner; waiting for new PID $($proc.Id)"
        }
    } catch {}
    Start-Sleep -Seconds 2
}
if (-not $ok) {
    Write-Host "  /health still not 200 after 30s — check uvicorn.out.log / uvicorn.err.log"
    exit 1
}
Write-Host "=== Done ==="
