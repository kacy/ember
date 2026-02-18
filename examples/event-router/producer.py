#!/usr/bin/env python3
"""
event producer for a multi-tenant SaaS platform.

publishes domain events to tenant-namespaced channels. after each publish,
stores the payload as the "last known" event on that channel so late-joining
consumers can catch up without waiting for the next real event.

uses redis-py unmodified — ember accepts standard RESP3 connections on port 6379.
"""

import json
import random
import time

import redis

TENANTS = ["acme", "globex", "initech"]

EVENT_TYPES = {
    "order": ["order.placed", "order.shipped", "order.delivered", "order.cancelled"],
    "payment": ["payment.captured", "payment.failed", "payment.refunded"],
    "user": ["user.created", "user.updated", "user.deleted"],
}

LAST_EVENT_TTL = 300  # 5 minutes


def channel(tenant: str, event_type: str) -> str:
    return f"events:{{{tenant}}}:{event_type}"


def publish_event(r: redis.Redis, tenant: str, event_type: str) -> None:
    ch = channel(tenant, event_type)
    payload = json.dumps(
        {
            "tenant": tenant,
            "event": event_type,
            "timestamp": time.time(),
            "id": f"{tenant[:3]}-{random.randint(1000, 9999)}",
        }
    )

    r.publish(ch, payload)
    r.setex(f"last:{ch}", LAST_EVENT_TTL, payload)
    print(f"  → {ch}")


def main() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    print("publishing events across three tenants. ctrl+c to stop.\n")
    try:
        while True:
            tenant = random.choice(TENANTS)
            category = random.choice(list(EVENT_TYPES))
            event_type = random.choice(EVENT_TYPES[category])
            publish_event(r, tenant, event_type)
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("\nproducer stopped")


if __name__ == "__main__":
    main()
