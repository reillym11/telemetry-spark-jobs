#!/usr/bin/env bash
# Usage: scripts/run-job.sh <ota-status-tracker|charging-session-builder|battery-health-aggregator|diagnostics-dtc-router>
set -euo pipefail
job="${1:?usage: $0 <job>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

case "$job" in
  ota-status-tracker)        in=vehicle.ota.status.v1;      out=ota.status.latest.v1;  cleanup=compact; project=otastatustracker;        class=telemetry.jobs.ota.OtaStatusTracker ;;
  charging-session-builder)  in=vehicle.charging.events.v1; out=charging.sessions.v1;  cleanup=delete;  project=chargingsessionbuilder;  class=telemetry.jobs.charging.ChargingSessionBuilder ;;
  battery-health-aggregator) in=vehicle.battery.health.v1;  out=s3a://telemetry-lake/battery_health_hourly; cleanup=; project=batteryhealthaggregator; class=telemetry.jobs.battery.BatteryHealthAggregator ;;
  diagnostics-dtc-router)    in=vehicle.diagnostics.dtc.v1; out=service.dtc.alerts.v1; cleanup=delete;  project=diagnosticsdtcrouter;    class=telemetry.jobs.dtc.DiagnosticsDtcRouter ;;
  *) echo "unknown job: $job" >&2; exit 2 ;;
esac

dc() { docker compose "$@"; }
kafka() { dc exec -T kafka /opt/kafka/bin/"$@"; }
bs=kafka:9092

${SBT:-sbt} -batch "$project/assembly"
jar="jobs/$job/target/scala-2.12/$job.jar"

dc up -d kafka minio
until kafka kafka-topics.sh --bootstrap-server $bs --list >/dev/null 2>&1; do sleep 1; done

topics=("$in")
[[ -n "$cleanup" ]] && topics+=("$out")
for t in "${topics[@]}"; do
  kafka kafka-topics.sh --bootstrap-server $bs --delete --if-exists --topic "$t"
done
for t in "${topics[@]}"; do
  policy=delete
  [[ "$t" == "$out" ]] && policy="$cleanup"
  until kafka kafka-topics.sh --bootstrap-server $bs --create --if-not-exists --topic "$t" --partitions 6 \
      --replication-factor 1 --config cleanup.policy="$policy" >/dev/null 2>&1; do sleep 1; done
done

fixture="contracts/fixtures/sample/$in.jsonl"
sed -E 's/^(.*"vin":"([^"]+)".*)$/\2\t\1/' "$fixture" |
  kafka kafka-console-producer.sh --bootstrap-server $bs --topic "$in" \
    --property parse.key=true --property key.separator=$'\t'

count() {
  kafka kafka-get-offsets.sh --bootstrap-server $bs --topic "$1" --time latest |
    awk -F: '{ s += $3 } END { print s + 0 }'
}

extra=()
if [[ -z "$cleanup" ]]; then
  until dc exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; do sleep 1; done
  dc exec -T minio mc mb --ignore-existing local/telemetry-lake >/dev/null
  dc exec -T minio mc rm --recursive --force local/telemetry-lake/battery_health_hourly >/dev/null 2>&1 || true
  extra=(--s3-endpoint http://minio:9000)
fi

checkpoint="/checkpoints/$job-$(date +%s)"
submit=(/opt/spark/bin/spark-submit --master 'local[2]'
  --conf spark.sql.session.timeZone=UTC
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC
  --conf spark.jars.ivy=/tmp/.ivy2)
mkdir -p out
dc run --rm -T spark "${submit[@]}" --class "$class" "/work/$jar" \
  --bootstrap $bs --checkpoint "$checkpoint" --output "$out" "${extra[@]}" > "out/$job.log" 2>&1 ||
  { tail -50 "out/$job.log"; exit 1; }

echo "input  $in: $(count "$in") records"
if [[ -n "$cleanup" ]]; then
  kafka kafka-console-consumer.sh --bootstrap-server $bs --topic "$out" --from-beginning --timeout-ms 5000 \
    2>/dev/null > "out/$out.jsonl" || true
  echo "output $out: $(count "$out") records (copied to out/$out.jsonl)"
else
  dc run --rm -T -w /tmp spark /opt/spark/bin/spark-sql --master 'local[1]' --jars "/work/$jar" \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    -S -e "SELECT 'output $out: ' || count(*) || ' rows' FROM parquet.\`$out\`" 2>/dev/null
fi
