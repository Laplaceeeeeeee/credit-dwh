# ============================================================
# Stage 4 init: give the Flink runtime user ownership of its volumes.
#
# ASCII-ONLY ON PURPOSE (appendix B-52).
#
# WHY THIS IS NEEDED (the M1 blocker, diagnosed 2026-09-24):
#   `docker top rt-taskmanager` shows the JobManager/TaskManager daemons run as
#   uid 9999 (user `flink`, set by the official image's USER directive).
#   But `docker exec` defaults to **root**. So every `sql-client.sh -f ...`
#   invocation created directories such as
#       /warehouse/rt.db/rt_loan_fact_latest/
#   as root:root with mode 755. The TaskManager (flink) then could not create
#   anything inside them and failed with:
#       java.io.IOException: Mkdirs failed to create
#       file:/warehouse/rt.db/rt_loan_fact_latest/bucket-0
#   ...which looks like a Hadoop bug or a filesystem bug but is neither.
#   Same root cause broke the checkpoint dir:
#       Failed to create directory for shared state:
#       file:/opt/flink/checkpoints/<jobid>/shared
#
#   Reproduction (before the fix):
#       docker exec -u flink rt-taskmanager \
#         touch /warehouse/rt.db/rt_loan_fact_latest/bucket-0/probe.txt
#       -> Permission denied
#
# FIX: chown the volume mount points to the runtime user, and always run
#      sql-client as that same user (`docker exec -u flink`), so DDL and the
#      daemons agree on ownership.
#
# Idempotent: safe to run repeatedly.
# ============================================================

$ErrorActionPreference = 'Stop'
$FlinkUser = 'flink'
$Containers = @('rt-jobmanager', 'rt-taskmanager')
$Paths = @('/warehouse', '/opt/flink/checkpoints')

foreach ($c in $Containers) {
    $running = docker ps --filter "name=^$c$" --format '{{.Names}}'
    if ($running -ne $c) {
        Write-Host "SKIP: $c is not running"
        continue
    }
    Write-Host "=== $c ==="
    foreach ($p in $Paths) {
        docker exec -u root $c chown -R "${FlinkUser}:${FlinkUser}" $p
        $owner = docker exec $c stat -c '%U:%G %a' $p
        Write-Host "  $p -> $owner"
    }
    Write-Host "  runtime uid: $(docker exec $c id -u)"
}

Write-Host ""
Write-Host "=== verify the runtime user can now write ==="
docker exec -u $FlinkUser rt-taskmanager sh -c "touch /warehouse/_ownership_probe && echo '  flink can write /warehouse' && rm -f /warehouse/_ownership_probe"
docker exec -u $FlinkUser rt-jobmanager sh -c "touch /opt/flink/checkpoints/_ownership_probe && echo '  flink can write /opt/flink/checkpoints' && rm -f /opt/flink/checkpoints/_ownership_probe"
