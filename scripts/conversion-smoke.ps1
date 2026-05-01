$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$tmpDir = Join-Path $projectRoot (".tmp-smoke-" + [string][DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
$apiBase = "http://localhost:3000"
$downloadCandidates = @(
  (Join-Path $projectRoot ".runtime-generated"),
  (Join-Path $projectRoot "downloads")
)

New-Item -ItemType Directory -Path $tmpDir | Out-Null

function New-TestPng {
  param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
  )
  $nodeScript = @'
const fs = require('fs');
const sharp = require('sharp');
const out = process.argv[2];
const svg = `<svg width="900" height="220" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="white"/>
  <text x="30" y="120" font-size="72" font-family="Arial" fill="black">HELLO OCR 123</text>
</svg>`;
sharp(Buffer.from(svg)).png().toFile(out).then(() => process.exit(0)).catch((e) => {
  console.error(e.message);
  process.exit(1);
});
'@
  $scriptPath = Join-Path $tmpDir "make-png.js"
  Set-Content -LiteralPath $scriptPath -Value $nodeScript -Encoding UTF8
  node $scriptPath $OutputPath | Out-Null
  if ($LASTEXITCODE -ne 0 -or -not (Test-Path $OutputPath)) {
    throw "Failed to generate PNG test fixture at $OutputPath"
  }
}

function Invoke-Conversion {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Name,
    [Parameter(Mandatory = $true)]
    [string]$FilePath,
    [Parameter(Mandatory = $true)]
    [string]$Format
  )

  $result = [ordered]@{
    test = $Name
    input = [System.IO.Path]::GetFileName($FilePath)
    target = $Format
    success = $false
    message = ""
    output = ""
  }

  try {
    $respRaw = curl.exe -sS -X POST -F "file=@$FilePath" -F "format=$Format" "$apiBase/convert"
    if ($LASTEXITCODE -ne 0) {
      $result.message = "curl failed with exit code $LASTEXITCODE"
      return [pscustomobject]$result
    }
    if (-not $respRaw) {
      $result.message = "Empty response from /convert"
      return [pscustomobject]$result
    }
    $json = $respRaw | ConvertFrom-Json
    if ($json.success -eq $true -and $json.status -eq "queued" -and $json.jobId) {
      $jobId = [string]$json.jobId
      for ($i = 0; $i -lt 120; $i++) {
        Start-Sleep -Milliseconds 500
        $jobRaw = curl.exe -sS "$apiBase/jobs/$jobId"
        if (-not $jobRaw) { continue }
        $job = $jobRaw | ConvertFrom-Json
        if ($job.status -eq "completed" -and $job.result -and $job.result.filename) {
          $result.success = $true
          $result.message = "OK"
          $result.output = [string]$job.result.filename
          return [pscustomobject]$result
        }
        if ($job.status -eq "failed") {
          $result.message = [string]($job.error)
          return [pscustomobject]$result
        }
      }
      $result.message = "Timed out waiting for queued conversion"
      return [pscustomobject]$result
    }
    if ($json.success -eq $true -and $json.url) {
      $result.success = $true
      $result.message = "OK"
      $result.output = "$($json.filename)"
      return [pscustomobject]$result
    }
    $result.message = [string]($json.message)
    return [pscustomobject]$result
  } catch {
    $result.message = $_.Exception.Message
    return [pscustomobject]$result
  }
}

function Resolve-GeneratedFilePath {
  param(
    [Parameter(Mandatory = $true)]
    [string]$FileName
  )
  foreach ($dir in $downloadCandidates) {
    $candidate = Join-Path $dir $FileName
    if (Test-Path $candidate) {
      return $candidate
    }
  }
  return $null
}

try {
  $health = Invoke-WebRequest -UseBasicParsing "$apiBase/health" -TimeoutSec 5
  if ($health.StatusCode -ne 200) {
    throw "Health check failed with HTTP $($health.StatusCode)"
  }

  $txtPath = Join-Path $tmpDir "sample.txt"
  $csvPath = Join-Path $tmpDir "sample.csv"
  $pngPath = Join-Path $tmpDir "ocr-sample.png"

  Set-Content -LiteralPath $txtPath -Value "Hello from smoke test" -Encoding UTF8
  Set-Content -LiteralPath $csvPath -Value "name,age`nAsha,25`nVikas,31" -Encoding UTF8
  New-TestPng -OutputPath $pngPath

  $results = @()
  $results += Invoke-Conversion -Name "TXT->PDF" -FilePath $txtPath -Format "pdf"
  $results += Invoke-Conversion -Name "TXT->DOCX" -FilePath $txtPath -Format "docx"
  $results += Invoke-Conversion -Name "CSV->XLSX" -FilePath $csvPath -Format "xlsx"
  $results += Invoke-Conversion -Name "PNG->JPG" -FilePath $pngPath -Format "jpg"
  $results += Invoke-Conversion -Name "PNG->TXT (OCR)" -FilePath $pngPath -Format "txt"

  $xlsx = $results | Where-Object { $_.test -eq "CSV->XLSX" -and $_.success } | Select-Object -First 1
  if ($xlsx) {
    $xlsxPath = Resolve-GeneratedFilePath -FileName $xlsx.output
    if (Test-Path $xlsxPath) {
      $results += Invoke-Conversion -Name "XLSX->CSV" -FilePath $xlsxPath -Format "csv"
    } else {
      $results += [pscustomobject]@{
        test = "XLSX->CSV"; input = $xlsx.output; target = "csv"; success = $false; message = "Generated XLSX not found in runtime output directories"; output = ""
      }
    }
  }

  $pdf = $results | Where-Object { $_.test -eq "TXT->PDF" -and $_.success } | Select-Object -First 1
  if ($pdf) {
    $pdfPath = Resolve-GeneratedFilePath -FileName $pdf.output
    if (Test-Path $pdfPath) {
      $results += Invoke-Conversion -Name "PDF->TXT (OCR)" -FilePath $pdfPath -Format "txt"
    } else {
      $results += [pscustomobject]@{
        test = "PDF->TXT (OCR)"; input = $pdf.output; target = "txt"; success = $false; message = "Generated PDF not found in runtime output directories"; output = ""
      }
    }
  }

  $results | Format-Table -AutoSize

  $failed = @($results | Where-Object { -not $_.success })
  if ($failed.Count -gt 0) {
    Write-Output ""
    Write-Output "Failed tests:"
    foreach ($f in $failed) {
      Write-Output ("- {0}: {1}" -f $f.test, $f.message)
    }
    exit 1
  }

  Write-Output ""
  Write-Output "All smoke tests passed."
  exit 0
} finally {
  if (Test-Path $tmpDir) {
    $removed = $false
    for ($i = 0; $i -lt 5; $i++) {
      try {
        Remove-Item -Recurse -Force $tmpDir -ErrorAction Stop
        $removed = $true
        break
      } catch {
        Start-Sleep -Milliseconds 250
      }
    }
    if (-not $removed) {
      # Non-fatal on Windows if an external process briefly holds a temp file lock.
    }
  }
}
