# ============================================================
# Stage 4 init: give each runtime its own user ownership of its volumes.
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
#   The Kafka image has the same pattern with a different user: it runs as
#   uid 1000 (appuser), so its named volume needs chown to 1000:1000.
#
# FIX: chown each volume mount point to the user that actually runs the
#      service, and always run sql-client as that same user
#      (`docker exec -u flink`), so DDL and the daemons agree on ownership.
#
# Idempotent: safe to run repeatedly.
# ============================================================

$ErrorActionPreference = 'Stop'

# container -> @{ user = owner to chown to; paths = volume mount points }
$targets = @(
    @{ container = 'rt-jobmanager';  user = 'flink';    uid = 9999; paths = @('/warehouse', '/opt/flink/checkpoints', '/results') },
    @{ container = 'rt-taskmanager'; user = 'flink';    uid = 9999; paths = @('/warehouse', '/opt/flink/checkpoints', '/results') },
    @{ container = 'rt-kafka';       user = 'appuser';  uid = 1000; paths = @('/var/lib/kafka/data') }
)

foreach ($t in $targets) {
    $c = $t.container
    $running = docker ps --filter "name=^$c$" --format '{{.Names}}'
    if ($running -ne $c) {
        Write-Host "SKIP: $c is not running"
        continue
    }
    Write-Host "=== $c (runtime uid should be $($t.uid)) ==="
    foreach ($p in $t.paths) {
        docker exec -u root $c chown -R "$($t.uid):$($t.uid)" $p
        $owner = docker exec $c stat -c '%u:%g %a' $p
        Write-Host "  $p -> $owner"
    }
}

Write-Host ""
Write-Host "=== verify each runtime user can write its volume ==="
docker exec -u flink rt-taskmanager sh -c "touch /warehouse/_ownership_probe && echo '  flink can write /warehouse' && rm -f /warehouse/_ownership_probe"
docker exec -u flink rt-jobmanager sh -c "touch /opt/flink/checkpoints/_ownership_probe && echo '  flink can write /opt/flink/checkpoints' && rm -f /opt/flink/checkpoints/_ownership_probe"
docker exec -u appuser rt-kafka sh -c "touch /var/lib/kafka/data/_ownership_probe && echo '  appuser can write kafka-data' && rm -f /var/lib/kafka/data/_ownership_probe"
