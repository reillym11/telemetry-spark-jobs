# telemetry-spark-jobs

Spark Structured Streaming jobs for vehicle telemetry. Mock demo project, synthetic data only.

- Build and test: `sbt test` (Java 17), and `cd downstream-consumers && pytest`
- Run a job locally: `scripts/run-job.sh <job>` (needs Docker; jobs are under `jobs/`)
- Update the vendored contracts: `scripts/sync-contracts.sh <data-contracts sha>`
