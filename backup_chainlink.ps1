<# backup_chainlink.ps1
   Usage:
     pwsh -File .\backup_chainlink.ps1
     pwsh -File .\backup_chainlink.ps1 -Zip
     pwsh -File .\backup_chainlink.ps1 -Prune -Keep 12
#>

param(
  [string]$DestRoot = "E:\Development\chainlink_core_backups",
  [switch]$Zip,
  [switch]$Prune,
  [int]$Keep = 10
)

$ErrorActionPreference = 'Stop'

# Project paths
$ProjectRoot = Resolve-Path $PSScriptRoot
$ProjectName = Split-Path -Leaf $ProjectRoot
$Stamp       = Get-Date -Format 'yyyyMMdd_HHmmss'

# Ensure destination root exists
if (!(Test-Path $DestRoot)) { New-Item -ItemType Directory -Path $DestRoot | Out-Null }

$BackupDir = Join-Path $DestRoot "${ProjectName}_$Stamp"
$ZipPath   = "${BackupDir}.zip"

# Exclusions
$ExcludeDirs = @(
  '.git', 'chainlink_venv', '__pycache__',
  '.streamlit\__pycache__', '.mypy_cache', '.pytest_cache'
)
$ExcludeFiles = @('*.pyc','*.log','Thumbs.db','desktop.ini')

Write-Host "Backing up '$ProjectName' -> '$BackupDir' ..." -ForegroundColor Cyan

# Use robocopy (fast & reliable)
$xd = @()
foreach ($d in $ExcludeDirs) { $xd += @('/XD', (Join-Path $ProjectRoot $d)) }
$xf = @()
foreach ($f in $ExcludeFiles) { $xf += @('/XF', $f) }

$rc = robocopy $ProjectRoot $BackupDir /E /R:1 /W:1 /COPY:DAT /DCOPY:DAT /NFL /NDL @xd @xf
if ($LASTEXITCODE -gt 7) { throw "robocopy failed with exit code $LASTEXITCODE" }

Write-Host "✅ Backup created: $BackupDir" -ForegroundColor Green

if ($Zip) {
  Write-Host "Creating zip: $ZipPath ..." -ForegroundColor Cyan
  Compress-Archive -Path $BackupDir -DestinationPath $ZipPath -Force
  Write-Host "✅ Zip ready: $ZipPath" -ForegroundColor Green
}

if ($Prune) {
  Write-Host "Pruning old backups (keeping newest $Keep) in $DestRoot ..." -ForegroundColor Cyan
  Get-ChildItem -Path $DestRoot -Directory |
    Where-Object { $_.Name -like "${ProjectName}_*" } |
    Sort-Object Name -Descending |
    Select-Object -Skip $Keep |
    ForEach-Object { Remove-Item -Recurse -Force $_.FullName }
  Get-ChildItem -Path $DestRoot -Filter "${ProjectName}_*.zip" -File |
    Sort-Object Name -Descending |
    Select-Object -Skip $Keep |
    ForEach-Object { Remove-Item -Force $_.FullName }
  Write-Host "🧹 Prune complete." -ForegroundColor Green
}
