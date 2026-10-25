"""Feeds charging sessions to the service backend."""
import json

from common import Deduper, parse_args, records


def unique_sessions(stream):
    # The Spark Kafka sink is at-least-once and can write duplicates on task retry, so we dedupe here.
    dedupe = Deduper("session_id")
    for r in stream:
        if dedupe.accept(r):
            yield r


def main():
    args = parse_args(__doc__, "charging.sessions.v1")
    n = 0
    for s in unique_sessions(records(args, "service-charging")):
        n += 1
        print(json.dumps({"session_id": s["session_id"], "vin": s["vin"], "close_reason": s["close_reason"],
                          "energy_added_kwh": s["energy_added_kwh"]}))
    print(f"sessions={n}")


if __name__ == "__main__":
    main()
