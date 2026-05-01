$ErrorActionPreference = "Stop"

$port = 3000
function Get-ListeningPids([int]$targetPort) {
  if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
    try {
      return Get-NetTCPConnection -LocalPort $targetPort -State Listen -ErrorAction Stop |
        Select-Object -ExpandProperty OwningProcess -Unique
    } catch {
      # Fall back to netstat parsing
    }
  }

  $lines = netstat -ano -p tcp | Select-String "LISTENING"
  $pids = @()
  foreach ($line in $lines) {
    $parts = ($line.ToString() -split "\s+") | Where-Object { $_ -ne "" }
    if ($parts.Length -ge 5) {
      $localAddress = $parts[1]
      $state = $parts[3]
      $ownerPid = $parts[-1]
      if ($state -eq "LISTENING" -and $localAddress -match (":{0}$" -f [regex]::Escape([string]$targetPort))) {
        $pids += [int]$ownerPid
      }
    }
  }
  return $pids | Sort-Object -Unique
}

$pids = Get-ListeningPids -targetPort $port
if (-not $pids) {
  Write-Output ("Server is not running on port {0}." -f $port)
  exit 1
}

$procId = $pids | Select-Object -First 1
$proc = Get-Process -Id $procId -ErrorAction SilentlyContinue

if ($proc) {
  Write-Output ("Server is running on port {0} (PID {1}, {2})." -f $port, $procId, $proc.ProcessName)
} else {
  Write-Output ("Port {0} is listening (PID {1}), process details unavailable." -f $port, $procId)
}

try {
  $response = Invoke-WebRequest -UseBasicParsing ("http://localhost:{0}/health" -f $port) -TimeoutSec 5
  Write-Output ("Health: {0}" -f $response.Content)
  exit 0
} catch {
  Write-Output ("Health check failed: {0}" -f $_.Exception.Message)
  exit 1
}
