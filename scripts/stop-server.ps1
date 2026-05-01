$ErrorActionPreference = "Stop"

$port = 3000
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

$pids = Get-ListeningPids -targetPort $port

if (-not $pids) {
  Write-Output ("No server is listening on port {0}." -f $port)
  exit 0
}

foreach ($procId in $pids) {
  try {
    Stop-Process -Id $procId -Force -ErrorAction Stop
    Write-Output ("Stopped process PID {0} on port {1}." -f $procId, $port)
  } catch {
    Write-Output ("Could not stop PID {0}: {1}" -f $procId, $_.Exception.Message)
  }
}

exit 0
