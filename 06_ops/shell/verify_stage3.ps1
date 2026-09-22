<#
.SYNOPSIS
    Stage-3 acceptance check: one command, one PASS/FAIL line per item.
.DESCRIPTION
    Quick mode (default) - no Docker, no Spark, no heavy SQL:
        * deliverable inventory (from a UTF-8 side-car list file)
        * no unfilled "__" placeholder left in report tables
        * pytest: metric unit tests all green
        * ruff: static check clean
        * lineage: docs/gen_lineage.py --check passes
        * E1-E5 result files exist and look sane
        * dual-engine / row-diff result files all matched
        * resume material (README.md / JianLi.md) sanity
    -Full mode adds the parts that need a running environment:
        * docker containers up (mysql / metastore / spark)
        * the real Spark dual-engine consistency run exits 0
.USAGE
    .\06_ops\shell\verify_stage3.ps1
    .\06_ops\shell\verify_stage3.ps1 -Full
.NOTES
    ASCII-ONLY ON PURPOSE.
    Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI/GBK. A Chinese
    character in this file gets mangled and can even break string
    terminators, producing:
        ParserError: The string is missing the terminator: ".
    So every Chinese path / pattern lives in a UTF-8 side-car file read
    with -Encoding UTF8:
        06_ops/verify_stage3_files.txt
    Run it from anywhere; the script resolves the repo root itself.
#>
param([switch]$Full)

$ErrorActionPreference = 'Continue'

# ---------- resolve repo root (this file lives at 06_ops/shell/) ----------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT = Split-Path -Parent (Split-Path -Parent $ScriptDir)
Set-Location $ROOT

$PY = Join-Path $ROOT '.venv\Scripts\python.exe'
$RUFF = Join-Path $ROOT '.venv\Scripts\ruff.exe'

$script:pass = 0
$script:fail = 0

# NOTE: the parameters are named $title / $body on purpose. PowerShell script
#       blocks invoked with & are DYNAMICALLY scoped: a variable inside the
#       block resolves to the *invoking function's* parameter if the names
#       collide. Naming a parameter $name silently shadowed the caller's
#       container-name loop variable, so docker inspect received the check
#       label instead of a container name. Keep these names unique.
function Check([string]$title, [scriptblock]$body) {
    try {
        if (& $body) {
            Write-Host ("[ OK ] " + $title) -ForegroundColor Green
            $script:pass++
        } else {
            Write-Host ("[FAIL] " + $title) -ForegroundColor Red
            $script:fail++
        }
    } catch {
        Write-Host ("[FAIL] " + $title + "  -> " + $_.Exception.Message) -ForegroundColor Red
        $script:fail++
    }
}

function Get-RepoFile([string]$rel) { Join-Path $ROOT $rel }

function Read-Utf8Lines([string]$rel) {
    return @(Get-Content (Get-RepoFile $rel) -Encoding UTF8 |
             Where-Object { $_.Trim() -ne "" -and -not $_.Trim().StartsWith("#") } |
             ForEach-Object { $_.Trim() })
}

Write-Host "=== Stage 3 verification ===" -ForegroundColor Cyan
Write-Host ("repo: " + $ROOT)
Write-Host ("mode: " + $(if ($Full) { "FULL (docker + spark)" } else { "quick" }))
Write-Host ""

# ---------------------------------------------------------------- inventory
$listFile = "06_ops/verify_stage3_files.txt"
Check ("deliverable list exists: " + $listFile) { Test-Path (Get-RepoFile $listFile) }

$missing = @()
if (Test-Path (Get-RepoFile $listFile)) {
    $wanted = Read-Utf8Lines $listFile
    $missing = @($wanted | Where-Object { -not (Test-Path (Get-RepoFile $_)) })
    Check ("all stage-3 deliverables present (" + $wanted.Count + " paths)") { $missing.Count -eq 0 }
    if ($missing.Count -gt 0) {
        $missing | ForEach-Object { Write-Host ("        missing: " + $_) -ForegroundColor Yellow }
    }
}

Check "virtualenv python exists (.venv/Scripts/python.exe)" { Test-Path $PY }

# --------------------------------------------------- no unfilled blanks left
# NOTE: only *table rows* are checked. Prose may legitimately quote a bare
#       placeholder while explaining a documentation bug (one of the E-reports
#       does exactly that), and a naive whole-file grep would false-FAIL.
Check "no unfilled placeholders in report tables" {
    $targets = @()
    $targets += @(Get-ChildItem (Get-RepoFile "08_benchmark") -Filter *.md -File)
    foreach ($n in @("README.md", "JianLi.md")) {
        $p = Get-RepoFile $n
        if (Test-Path $p) { $targets += (Get-Item $p) }
    }
    if ($targets.Count -lt 9) { return $false }   # guard: did we actually scan anything?
    $bad = @()
    foreach ($t in $targets) {
        $bad += @(Select-String -Path $t.FullName -Pattern '`__`|\| __ \|' -ErrorAction SilentlyContinue |
                  Where-Object { $_.Line.TrimStart().StartsWith("|") })
    }
    if ($bad.Count -gt 0) {
        $bad | Select-Object -First 5 | ForEach-Object {
            Write-Host ("        blank at " + $_.Filename + ":" + $_.LineNumber) -ForegroundColor Yellow
        }
    }
    $bad.Count -eq 0
}

# --------------------------------------------------------------- unit tests
Check "pytest: metric tests all green (>= 30)" {
    # -o addopts="" because pytest.ini already carries -q; a second -q makes it
    # -qq, which suppresses the "N passed" summary line this check parses.
    $out = & $PY -m pytest tests -o addopts="" -q 2>&1 | Out-String
    $code = $LASTEXITCODE
    $m = [regex]::Match($out, '(\d+) passed')
    if (($code -ne 0) -or (-not $m.Success)) {
        Write-Host ($out.Trim()) -ForegroundColor Yellow
        return $false
    }
    # pytest exit code: 0 = all passed, 5 = nothing collected, 1 = failures
    ($code -eq 0) -and ([int]$m.Groups[1].Value -ge 30)
}

Check "ruff: no lint errors" {
    if (-not (Test-Path $RUFF)) { return $false }
    & $RUFF check src/ tests/ 07_bigdata/ 08_benchmark/ docs/gen_lineage.py *> $null
    $LASTEXITCODE -eq 0
}

Check "lineage: yaml/doc consistency check passes" {
    & $PY docs/gen_lineage.py --check *> $null
    $LASTEXITCODE -eq 0
}

# ------------------------------------------------------- result-file sanity
Check "E1-E5 harness results exist and are non-empty" {
    $names = @("E1_pruning", "E2_small_files", "E3_formats",
               "E4_baseline", "E4_salted", "E4_skew_quant", "E4_aqe",
               "E5_baseline", "E5_ladder")
    $bad = @($names | Where-Object {
        $p = Get-RepoFile ("08_benchmark/_results/" + $_ + ".csv")
        (-not (Test-Path $p)) -or ((Get-Item $p).Length -le 0)
    })
    $bad.Count -eq 0
}

Check "task stats parsed from spark event log" {
    $p = Get-RepoFile "08_benchmark/_results/spark_task_stats.csv"
    (Test-Path $p) -and ((Get-Item $p).Length -gt 100)
}

Check "file stats measured from filesystem" {
    $p = Get-RepoFile "08_benchmark/_results/file_stats.csv"
    (Test-Path $p) -and ((Get-Item $p).Length -gt 50)
}

Check "dual-engine consistency: every metric matched" {
    $csv = Get-RepoFile "08_benchmark/consistency_result.csv"
    if (-not (Test-Path $csv)) { return $false }
    $rows = @(Import-Csv $csv -Encoding UTF8)
    ($rows.Count -ge 7) -and (@($rows | Where-Object { $_.matched -ne "True" }).Count -eq 0)
}

Check "ADS row-by-row comparison: nothing over tolerance" {
    $csv = Get-RepoFile "08_benchmark/ads_row_diff.csv"
    if (-not (Test-Path $csv)) { return $false }
    $rows = @(Import-Csv $csv -Encoding UTF8)
    ($rows.Count -gt 0) -and (@($rows | Where-Object { [double]$_.over_tol -gt 0 }).Count -eq 0)
}

# ------------------------------------------------------------ resume assets
Check "README.md documents the stage-3 layer" {
    $p = Get-RepoFile "README.md"
    if (-not (Test-Path $p)) { return $false }
    $txt = Get-Content $p -Raw -Encoding UTF8
    ($txt -match 'Hive') -and ($txt -match 'Spark') -and ($txt -match 'E5')
}

Check "JianLi.md carries stage-3 measured numbers" {
    $p = Get-RepoFile "JianLi.md"
    if (-not (Test-Path $p)) { return $false }
    $txt = Get-Content $p -Raw -Encoding UTF8
    ($txt -match '47\.63') -and ($txt -match '3\.94') -and ($txt -match '5e-5')
}

Check "JianLi.md / README.md free of stale stage-3 claims" {
    # The forbidden Chinese phrases live in a UTF-8 side-car file (see header).
    $fp = Get-RepoFile "06_ops/verify_stage3_forbidden.txt"
    if (-not (Test-Path $fp)) { return $false }
    $patterns = Read-Utf8Lines "06_ops/verify_stage3_forbidden.txt"
    if ($patterns.Count -eq 0) { return $false }
    $hits = @()
    foreach ($rel in @("JianLi.md", "README.md")) {
        $p = Get-RepoFile $rel
        if (-not (Test-Path $p)) { continue }
        $lines = @(Get-Content $p -Encoding UTF8)
        foreach ($pat in $patterns) {
            $found = @($lines | Select-String -Pattern ([regex]::Escape($pat)) -SimpleMatch)
            foreach ($f in $found) {
                Write-Host ("        stale claim at " + $rel + ":" + $f.LineNumber) -ForegroundColor Yellow
                $hits += $f
            }
        }
    }
    $hits.Count -eq 0
}

# ------------------------------------------------------------- full mode
if ($Full) {
    Write-Host ""
    Write-Host "--- full mode: needs a running environment ---" -ForegroundColor Cyan

    Check "docker daemon reachable" {
        docker version --format '{{.Server.Version}}' *> $null
        $LASTEXITCODE -eq 0
    }

    foreach ($cname in @("credit-dwh-mysql", "bd-metastore", "bd-spark")) {
        Check ("container running: " + $cname) {
            ((docker inspect -f '{{.State.Running}}' $cname) -eq "true")
        }
    }

    Check "Spark dual-engine consistency run exits 0" {
        docker exec bd-spark /opt/spark/bin/spark-submit --master local[4] `
            /workspace/07_bigdata/check_consistency.py *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "        hint: MySQL must be up; free memory with 'docker stop credit-dwh-mysql' only when NOT running this check" -ForegroundColor Yellow
        }
        $LASTEXITCODE -eq 0
    }
}

# ------------------------------------------------------------------ summary
Write-Host ""
Write-Host ("PASS=" + $script:pass + "  FAIL=" + $script:fail) -ForegroundColor Cyan
if ($script:fail -gt 0) {
    Write-Host "STAGE 3 NOT VERIFIED" -ForegroundColor Red
    exit 1
}
Write-Host "STAGE 3 VERIFIED" -ForegroundColor Green
exit 0
