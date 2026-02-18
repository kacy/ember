#!/usr/bin/env python3
"""
event consumer for a specific tenant.

subscribes to all events matching events:{tenant}:* and prints each
incoming event. on startup, checks for any cached "last known" events
so you see recent context even if the producer ran before you connected.

usage: python consumer.py <tenant>
       python consumer.py acme
"""

import json
import sys

import redis


def main() -> None:
    tenant = sys.argv[1] if len(sys.argv) > 1 else "acme"
    pattern = f"events:{{{tenant}}}:*"

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # show any cached last-known events before subscribing
    last_keys = r.keys(f"last:events:{{{tenant}}}:*")
    if last_keys:
        print(f"last known events for {tenant}:")
        for key in sorted(last_keys):
            raw = r.get(key)
            if raw:
                data = json.loads(raw)
                event_type = key.split(":")[-1]
                print(f"  {event_type:35s}  id={data.get('id', '?')}")
        print()

    pubsub = r.pubsub()
    pubsub.psubscribe(pattern)

    print(f"subscribed to {pattern!r}. waiting for events...\n")

    for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue

        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError):
            continue

        event_type = message["channel"].split(":")[-1]
        print(f"  {event_type:35s}  id={data.get('id', '?')}  ts={data['timestamp']:.3f}")


if __name__ == "__main__":
    main()
