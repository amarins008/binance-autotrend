#Requires -RunAsAdministrator
#Requires -Version 5.1
<#
Restart the Binance Autotrend backend with Tailscale-friendly settings.

What it does (all in one shot):
  1. Stops any existing python processes on ports 8020/8021 (backend + launcher).
  2. Adds the Windows Firewall rule that allows port 8020 inbound only from
     the Tailscale CGNAT range (100.64.0.0/10). Idempotent.
  3. Starts run_backend.py in a new detached window with BACKEND_HOST=0.0.0.0.
  4. Polls /health (over 127.0.0.1) until ready or 30s timeout.
  5. Reports the final listen address so you can confirm 0.0.0.0:8020.

After this finishes:
  - On this machine:  http://127.0.0.1:8020/dashboard/
  - From your phone:   http://100.89.42.68:8020/dashboard/
    (phone must be on the same Tailscale network)

Re-run safely at any time — it's idempotent.
#>

$ErrorActionPreference = 'Stop'

$ROOT    = "E:\My Project\Binance autotrend"
$BACKEND = Join-Path $ROOT "backend"
$RUNNER  = Join-Path $BACKEND "run_backend.py"
$VENV_PY = Join-Path $BACKEND ".venv\Scripts\python.exe"
$PORT    = 8020
$FW_NAME = "Binance Autotrend Dashboard (Tailscale)"
$TAILSCALE_SUBNET = "100.64.0.0/10"

Write-Host "=== Binance Autotrend — restart with Tailscale support ===" -ForegroundColor Cyan
Write-Host ""

# --- 1) Stop existing services ------------------------------------------------
Write-Host "[1/5] Stopping existing services on 8020 / 8021..."
$ports = @($PORT, 8021)
foreach ($p in $ports) {
    $matches = netstat -ano | Select-String ":$p " | Select-String "LISTENING"
    foreach ($line in $matches) {
        $parts = ($line -split "\s+") | Where-Object { $_ -ne "" }
        $procId = $parts[-1]
        if ($procId -match "^\d+$") {
            Write-Host "  killing pid $procId on :$p"
            & taskkill /F /PID $procId 2>&1 | Out-Null
        }
    }
}
Start-Sleep -Seconds 3

# --- 2) Firewall rule --------------------------------------------------------
Write-Host ""
Write-Host "[2/5] Ensuring firewall rule '$FW_NAME' (Tailscale subnet only)..."
$existing = Get-NetFirewallRule -DisplayName $FW_NAME -ErrorAction SilentlyContinue
if ($existing) { Remove-NetFirewallRule -DisplayName $FW_NAME -ErrorAction SilentlyContinue }

New-NetFirewallRule `
    -DisplayName $FW_NAME `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalPort $PORT `
    -RemoteAddress $TAILSCALE_SUBNET `
    -Profile Any `
    -Description "Allow Binance Autotrend dashboard (port $PORT) from Tailscale subnet only." `
    -ErrorAction Stop | Out-Null
Write-Host "  rule created (TCP $PORT from $TAILSCALE_SUBNET)" -ForegroundColor Green

# --- 3) Start backend --------------------------------------------------------
Write-Host ""
Write-Host "[3/5] Starting run_backend.py with BACKEND_HOST=0.0.0.0..."
if (-not (Test-Path $RUNNER)) { throw "run_backend.py not found: $RUNNER" }
$py = if (Test-Path $VENV_PY) { $VENV_PY } else { "python" }
$log = Join-Path $BACKEND "uvicorn.out.log"
$err = Join-Path $BACKEND "uvicorn.err.log"

$proc = Start-Process -FilePath $py `
    -ArgumentList "`"$RUNNER`"" `
    -WorkingDirectory $BACKEND `
    -WindowStyle Normal `
    -PassThru `
    -RedirectStandardOutput $log `
    -RedirectStandardError $err `
    -Environment @{ BACKEND_HOST = "0.0.0.0" }
Write-Host "  started pid $($proc.Id), logs: $log"

# --- 4) Wait for /health -----------------------------------------------------
Write-Host ""
Write-Host "[4/5] Polling http://127.0.0.1:$PORT/health (max 30s)..."
$ok = $false
$deadline = (Get-Date).AddSeconds(30)
while ((Get-Date) -lt $deadline) {
    try {
        $r = Invoke-WebRequest "http://127.0.0.1:$PORT/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($r.StatusCode -eq 200) { $ok = $true; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $ok) { throw "Backend did not respond on /health after 30s. Check $err" }
Write-Host "  /health = 200 OK" -ForegroundColor Green

# --- 5) Report final listen address -----------------------------------------
Write-Host ""
Write-Host "[5/5] Final listen state on port ${PORT}:"
Get-NetTCPConnection -LocalPort $PORT -State Listen -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, OwningProcess |
    Format-Table -AutoSize | Out-String | Write-Host

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Cyan
Write-Host "Local  : http://127.0.0.1:${PORT}/dashboard/"
Write-Host "Phone  : http://100.89.42.68:${PORT}/dashboard/   (Tailscale)"
Write-Host ""
Write-Host "If LocalAddress above is 0.0.0.0, the server is exposed on all interfaces."
Write-Host "The firewall rule restricts inbound traffic to the Tailscale subnet only."
