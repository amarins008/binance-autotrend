#Requires -RunAsAdministrator
#Requires -Version 5.1
<#
Add Windows Firewall rule to allow the Binance Autotrend dashboard
(port 8020) to be reached from the Tailscale subnet only.

Run this in an elevated PowerShell prompt (right-click PowerShell ->
"Run as administrator"), or by using "Run as administrator" on the .ps1
file via right-click.

What it does:
  - Removes any pre-existing rule with the same display name
  - Allows inbound TCP 8020 only from 100.64.0.0/10 (Tailscale CGNAT range)

Verify after running:
  Get-NetFirewallRule -DisplayName "Binance Autotrend Dashboard (Tailscale)"
#>

$ErrorActionPreference = 'Stop'

$ruleName = "Binance Autotrend Dashboard (Tailscale)"
$port     = 8020
$subnet   = "100.64.0.0/10"

# Idempotent: remove old rule if present
$existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if ($existing) {
    Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    Write-Host "Removed existing rule '$ruleName'."
}

$rule = New-NetFirewallRule `
    -DisplayName $ruleName `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalPort $port `
    -RemoteAddress $subnet `
    -Profile Any `
    -Description "Allow Binance Autotrend dashboard (port $port) from Tailscale subnet only." `
    -ErrorAction Stop

Write-Host ""
Write-Host "Firewall rule created:"
$rule | Format-List DisplayName, Direction, Action, LocalPort, RemoteAddress, Enabled, Profile
Write-Host ""
Write-Host "Test from your phone (must be on the same Tailscale network):"
Write-Host "    http://100.89.42.68:8020/dashboard/"
