<#
.SYNOPSIS
    Run the full Phase-2 pipeline end to end.
.DESCRIPTION
    Steps: prepare_raw -> ods_load -> dwd_clean -> dwd_derived -> dim_build
           -> dws_snapshot -> ads_metrics -> dq_check
    Any non-optional step failing aborts the run.

.NOTES
    IMPORTANT: this file deliberately contains ASCII-only text.
    Windows PowerShell 5.1 reads .ps1 as ANSI/GBK unless a UTF-8 BOM is present.
    If Chinese characters are written here without a BOM, they get mangled and
    can even break string terminators, producing:
        ParserError: The string is missing the terminator: ".
    Keeping this file ASCII-only makes it immune to that problem.
    Chinese log output comes from the Python scripts themselves.

    Run from anywhere; the script resolves the project root on its own.
#>
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$PY = Join-Path $ROOT '.venv\Scripts\python.exe'

if (-not (Test-Path $PY)) {
    Write-Host "ERROR: virtualenv python not found: $PY" -ForegroundColor Red
    Write-Host "Create it first:  python -m venv .venv" -ForegroundColor Yellow
    exit 1
}

Set-Location $ROOT

# MySQL must be up before ods_load.
#
# NOTE: `mysqladmin ping -p<pw>` prints a password warning on STDERR.
# With $ErrorActionPreference='Stop', a native command writing to stderr can be
# promoted to a terminating error, which made this check report
# "MySQL is not running" even when the server was healthy.
# Fix: wrap the call so stderr never reaches PowerShell's error stream, and
# temporarily relax ErrorActionPreference around the probe.
function Test-MySqlReady {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & docker exec credit-dwh-mysql mysqladmin ping -uroot -proot123456 2>&1
        $code = $LASTEXITCODE
        $text = ($out | Out-String)
        return ($code -eq 0 -and $text -match 'alive')
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $prev
    }
}

$dbUp = Test-MySqlReady

if (-not $dbUp) {
    Write-Host "MySQL is not ready. Starting container..." -ForegroundColor Yellow
    & docker start credit-dwh-mysql 2>&1 | Out-Null
    $ok = $false
    for ($i = 1; $i -le 30; $i++) {
        Start-Sleep -Seconds 3
        if (Test-MySqlReady) { $ok = $true; break }
        Write-Host ("  waiting for MySQL... attempt " + $i) -ForegroundColor DarkGray
    }
    if (-not $ok) {
        Write-Host "ERROR: MySQL did not become ready in 90s." -ForegroundColor Red
        Write-Host "Check: docker ps -a ; docker logs credit-dwh-mysql --tail 30" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "MySQL is ready." -ForegroundColor Green
}

$steps = @(
    @{ Name = '01 prepare_raw  (strip footer)'; Mod = 'src.prepare_raw';  Optional = $false },
    @{ Name = '02 ods_load     (land raw)';     Mod = 'src.ods_load';     Optional = $false },
    @{ Name = '03 dwd_clean    (clean)';        Mod = 'src.dwd_clean';    Optional = $false },
    @{ Name = '04 dwd_derived  (perf facts)';   Mod = 'src.dwd_derived';  Optional = $false },
    @{ Name = '05 dim_build    (dimensions)';   Mod = 'src.dim_build';    Optional = $false },
    @{ Name = '06 dws_snapshot (monthly snap)'; Mod = 'src.dws_snapshot'; Optional = $true  },
    @{ Name = '07 ads_metrics  (ads layer)';    Mod = 'src.ads_metrics';  Optional = $false },
    @{ Name = '08 dq_check     (quality)';      Mod = 'src.dq_check';     Optional = $false }
)

$t0 = Get-Date
$failed = @()

foreach ($s in $steps) {
    $t = Get-Date
    Write-Host ('=' * 70) -ForegroundColor Cyan
    Write-Host (">> " + $s.Name) -ForegroundColor Cyan
    Write-Host ('=' * 70) -ForegroundColor Cyan

    & $PY -m $s.Mod
    $rc = $LASTEXITCODE
    $el = [math]::Round(((Get-Date) - $t).TotalSeconds, 1)

    if ($rc -ne 0) {
        if ($s.Optional) {
            Write-Host ("[WARN] " + $s.Name + " failed rc=$rc  (${el}s) - continuing") -ForegroundColor Yellow
            $failed += ($s.Name + " (optional)")
        } else {
            Write-Host ("[FAIL] " + $s.Name + " failed rc=$rc  (${el}s) - aborting") -ForegroundColor Red
            $failed += $s.Name
            break
        }
    } else {
        Write-Host ("[OK]   " + $s.Name + "  (${el}s)") -ForegroundColor Green
    }
}

$total = [math]::Round(((Get-Date) - $t0).TotalMinutes, 1)
Write-Host ('=' * 70) -ForegroundColor Cyan
if ($failed.Count -eq 0) {
    Write-Host ("Pipeline finished successfully in ${total} min") -ForegroundColor Green
    exit 0
} else {
    Write-Host ("Pipeline had failures in ${total} min:") -ForegroundColor Red
    $failed | ForEach-Object { Write-Host ("  - " + $_) -ForegroundColor Red }
    exit 1
}
