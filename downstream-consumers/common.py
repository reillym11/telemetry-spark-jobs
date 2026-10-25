import argparse
import json
import sys


def parse_args(description, topic):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default=topic)
    p.add_argument("--group", default=None)
    p.add_argument("--from-file", help="read JSONL records from a file instead of Kafka")
    return p.parse_args()


def records_from_file(path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def records_from_kafka(bootstrap, topic, group):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    idle = 0
    try:
        while idle < 5:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                print(msg.error(), file=sys.stderr)
                continue
            idle = 0
            yield json.loads(msg.value())
    finally:
        consumer.close()


def records(args, default_group):
    if args.from_file:
        return records_from_file(args.from_file)
    return records_from_kafka(args.bootstrap, args.topic, args.group or default_group)


class Deduper:
    def __init__(self, key):
        self.key = key
        self.seen = set()
        self.duplicates = 0

    def accept(self, record):
        k = record[self.key]
        if k in self.seen:
            self.duplicates += 1
            return False
        self.seen.add(k)
        return True
