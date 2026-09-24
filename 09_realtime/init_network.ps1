# ============================================================
# Create the shared network for stage 4 and attach the stage-2 MySQL.
# Idempotent: safe to run repeatedly.
#
# IMPORTANT - ASCII-ONLY ON PURPOSE (appendix B-52).
#    Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI/GBK, so a single
#    Chinese character mangles output at best and can truncate a string
#    terminator (ParserError) at worst. Chinese belongs in UTF-8 side-car
#    files, never in a .ps1. This script got that wrong on first write -
#    messages came out as mojibake - hence the rule.
#
# Why this step exists: compose declares credit-dwh-net as `external`, so the
# network must already exist. And the stage-2 MySQL was started by a DIFFERENT
# compose file, so both stacks must join the same network before Flink can
# reach the CDC source by container name.
# ============================================================

$ErrorActionPreference = 'Stop'
$Network = if ($env:SHARED_NETWORK) { $env:SHARED_NETWORK } else { 'credit-dwh-net' }
$MysqlContainer = 'credit-dwh-mysql'

Write-Host "=== 1. network: $Network ==="
$existing = docker network ls --filter "name=^$Network$" --format '{{.Name}}'
if ($existing -eq $Network) {
    Write-Host "  already exists, skip"
} else {
    docker network create $Network | Out-Null
    Write-Host "  created"
}

Write-Host "=== 2. attach $MysqlContainer ==="
$running = docker ps --filter "name=^$MysqlContainer$" --format '{{.Names}}'
if ($running -ne $MysqlContainer) {
    Write-Host "  FAIL: $MysqlContainer is not running; run: docker start $MysqlContainer"
    exit 1
}
$attached = docker network inspect $Network --format '{{range .Containers}}{{.Name}} {{end}}'
if ($attached -split '\s+' -contains $MysqlContainer) {
    Write-Host "  already attached, skip"
} else {
    docker network connect $Network $MysqlContainer
    Write-Host "  attached"
}

Write-Host "=== 3. members ==="
docker network inspect $Network --format '{{range .Containers}}  {{.Name}} = {{.IPv4Address}}{{println}}{{end}}'

Write-Host "=== 4. DNS self-check (Flink reaches the CDC source by this short name) ==="
docker exec $MysqlContainer getent hosts $MysqlContainer
