<# restore_chainlink.ps1
   Usage:
     # restore newest backup (folder or zip)
     .\restore_chainlink.ps1

     # OR restore a specific backup path (folder or .zip)
     .\restore_chainlink.ps1 -BackupPath "E:\Development\chainlink_core_backups\chainlink_core_20251106_204858.zip"

     # Optional flags
     #   -SkipPreSnapshot   : don't save a safety copy of current project
     #   -CleanCaches       : delete __pycache__ and recompile after restore
#>

param(
  [string]$BackupsRoot = "E:\Development\chainlink_core_backups",
  [string]$BackupPath,                 # optional: explicit folder or .zip
  [switch]$SkipPreSnapshot,
  [switch]$CleanCaches
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot ".")
$ProjectName = Split-Path -Leaf $ProjectRoot
$Stamp       = Get-Date -Format 'yyyyMMdd_HHmmss'

function Get-LatestBackup {
  param([string]$root,[string]$name)

  $folders = Get-ChildItem -Path $root -Directory -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "${name}_*" }

  $zips = Get-ChildItem -Path $root -Filter "${name}_*.zip" -File -ErrorAction SilentlyContinue

  ($folders + $zips) | Sort-Object Name -Descending | Select-Object -First 1
}

if (-not $BackupPath) {
  $latest = Get-LatestBackup -root $BackupsRoot -name $ProjectName
  if (-not $latest) { throw "No backups found in '$BackupsRoot' for project '$ProjectName'." }
  $BackupPath = $latest.FullName
}

Write-Host "Restoring from: $BackupPath" -ForegroundColor Cyan
Write-Host "Target project : $ProjectRoot" -ForegroundColor Cyan

# 1) Safety snapshot (unless skipped)
if (-not $SkipPreSnapshot) {
  $pre = Join-Path $BackupsRoot "pre_restore_$Stamp"
  Write-Host "Creating safety snapshot of current project -> $pre" -ForegroundColor Yellow
  robocopy $ProjectRoot $pre /E /R:1 /W:1 /COPY:DAT /DCOPY:DAT /XJ /NFL /NDL `
    /XD (Join-Path $ProjectRoot '.git') `
        (Join-Path $ProjectRoot 'chainlink_venv') `
        (Join-Path $ProjectRoot '__pycache__') `
        (Join-Path $ProjectRoot '.streamlit\__pycache__') `
        (Join-Path $ProjectRoot '.streamlit\logs') `
        (Join-Path $ProjectRoot '.streamlit\cache') `
        (Join-Path $ProjectRoot '.mypy_cache') `
        (Join-Path $ProjectRoot '.pytest_cache') `
    /XF '*.pyc' '*.log' 'Thumbs.db' 'desktop.ini' | Out-Null
  Write-Host "Safety snapshot complete." -ForegroundColor Green
}

# 2) If zip, extract to temp
$restoreSource = $BackupPath
$tempFolder = $null
if ($BackupPath.ToLower().EndsWith(".zip")) {
  $tempFolder = Join-Path $env:TEMP "$ProjectName`_restore_$Stamp"
  if (Test-Path $tempFolder) { Remove-Item -Recurse -Force $tempFolder }
  New-Item -ItemType Directory -Path $tempFolder | Out-Null
  Write-Host "Extracting zip to temp: $tempFolder" -ForegroundColor Yellow
  Expand-Archive -Path $BackupPath -DestinationPath $tempFolder -Force
  # Handle case where zip contains the folder or just contents
  $inner = Get-ChildItem $tempFolder | Select-Object -First 1
  if ($inner -and $inner.PSIsContainer -and $inner.Name -like "${ProjectName}_*") {
    $restoreSource = $inner.FullName
  } else {
    $restoreSource = $tempFolder
  }
}

# 3) Restore (robocopy into project root)
Write-Host "Copying backup into project..." -ForegroundColor Yellow
robocopy $restoreSource $ProjectRoot /E /R:1 /W:1 /COPY:DAT /DCOPY:DAT /XJ /NFL /NDL | Out-Null
$code = $LASTEXITCODE
if     ($code -ge 16) { throw "robocopy fatal error ($code)" }
elseif ($code -ge  8) { Write-Warning "robocopy reported copy failures ($code). Most files restored successfully." }
else                  { Write-Host "Files restored (code $code)" -ForegroundColor Green }

# 4) Optional cleanup/compile
if ($CleanCaches) {
  Write-Host "Cleaning __pycache__ and compiling..." -ForegroundColor Yellow
  Get-ChildItem -Recurse -Directory -Filter __pycache__ -Path $ProjectRoot | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
  python -m compileall $ProjectRoot  | Out-Null
  Write-Host "Cache cleanup + compile complete." -ForegroundColor Green
}

# 5) Cleanup temp if used
if ($tempFolder -and (Test-Path $tempFolder)) { Remove-Item -Recurse -Force $tempFolder }

Write-Host "✅ Restore finished." -ForegroundColor Green
