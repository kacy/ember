# event router

a multi-tenant SaaS event bus. each tenant's services publish domain events (`order.placed`, `payment.failed`, `user.created`) and subscribers receive only their tenant's events using pattern subscriptions.

this example uses **redis-py unchanged** over the RESP3 port — no gRPC client, no ember-specific code. it demonstrates that any redis-compatible client works with ember out of the box.

## how it works

producers publish to channels named `events:{acme}:order.placed`. the hash-tag notation (`{acme}`) is conventional; ember routes based on the full channel name.

subscribers call `PSUBSCRIBE events:{acme}:*` and receive only acme events, regardless of how many other tenants are active. when a consumer starts late, it fetches the most recent event on each channel from a short-lived string key (`last:events:{tenant}:...`) that the producer sets with a 5-minute TTL.

`PUBSUB CHANNELS` and `PUBSUB NUMSUB` let you inspect which channels are active and how many subscribers each has at any point.

## run it

in three terminals:

```sh
# terminal 1 — publish events for all three tenants
cd examples/event-router
pip install -r requirements.txt
python producer.py

# terminal 2 — consume only acme events
python consumer.py acme

# terminal 3 — consume only globex events
python consumer.py globex
```

## what to look for

each consumer sees only its own tenant's events even though the producer is publishing to all three simultaneously. swap consumer.py against a real redis instance (just change `host` to your redis address) and the output is identical — ember is a drop-in replacement at the protocol level.

to inspect active channels while the producer is running:

```sh
redis-cli -p 6379 PUBSUB CHANNELS 'events:*'
redis-cli -p 6379 PUBSUB NUMSUB events:{acme}:order.placed
```
