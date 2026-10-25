"""Turns DTC alerts into service scheduling requests."""
import json

from common import Deduper, parse_args, records


def unique_alerts(stream):
    # The Spark Kafka sink is at-least-once and can write duplicates on task retry, so we dedupe here.
    dedupe = Deduper("alert_id")
    for r in stream:
        if dedupe.accept(r):
            yield r


def main():
    args = parse_args(__doc__, "service.dtc.alerts.v1")
    n = 0
    for a in unique_alerts(records(args, "service-scheduling")):
        n += 1
        print(json.dumps({"alert_id": a["alert_id"], "vin": a["vin"], "dtc_code": a["dtc_code"],
                          "severity": a["severity"], "lat": a["lat"], "lon": a["lon"]}))
    print(f"alerts={n}")


if __name__ == "__main__":
    main()
