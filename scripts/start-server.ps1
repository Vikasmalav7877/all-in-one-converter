$ErrorActionPreference = "Stop"

$port = 3000
$projectRoot = Split-Path -Parent $PSScriptRoot

function Get-ListeningPids([int]$targetPort) {
  $lines = netstat -ano | Select-String (":{0}" -f $targetPort)
  $pids = @()
  foreach ($line in $lines) {
    $parts = ($line.ToString() -split "\s+") | Where-Object { $_ -ne "" }
    if ($parts.Length -ge 5 -and $parts[3] -eq "LISTENING") {
      $pids += [int]$parts[-1]
    }
  }
  return $pids | Sort-Object -Unique
}

$existingPids = Get-ListeningPids -targetPort $port
$existing = $existingPids | Select-Object -First 1

if ($existing) {
  $existingProcId = $existing
  $proc = Get-Process -Id $existingProcId -ErrorAction SilentlyContinue
  if ($proc) {
    Write-Output ("Server already running on port {0} (PID {1}, {2})." -f $port, $existingProcId, $proc.ProcessName)
    exit 0
  }
}

$proc = Start-Process -FilePath "node" `
  -ArgumentList "server.js" `
  -WorkingDirectory $projectRoot `
  -WindowStyle Hidden `
  -PassThru

$healthOk = $false
for ($i = 0; $i -lt 20; $i++) {
  Start-Sleep -Milliseconds 500
  try {
    $response = Invoke-WebRequest -UseBasicParsing ("http://localhost:{0}/health" -f $port) -TimeoutSec 3
    if ($response.StatusCode -eq 200) {
      $healthOk = $true
      break
    }
  } catch {
    $healthOk = $false
  }
}

if ($healthOk) {
  Write-Output ("Server started successfully on http://localhost:{0} (PID {1})." -f $port, $proc.Id)
  exit 0
}

Write-Output ("Server process started (PID {0}) but health check failed. Check terminal logs." -f $proc.Id)
exit 1
