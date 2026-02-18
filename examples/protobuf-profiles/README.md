# protobuf profiles

a user profile service that stores structured data as protobuf messages and exposes field-level reads and updates without deserializing the full blob on every access.

## the pattern

two keys per user:

- **`profile:{id}`** — the full serialized `UserProfile` protobuf blob, with a TTL
- **`profile:{id}:idx`** — a hash of the frequently-accessed fields (name, email, subscription_tier, last_seen)

reading `subscription_tier` for an authorization check costs a single **HGET** on the index — no deserialization, no schema coupling between the reading service and the writing service. updating a field patches the index immediately, then re-serializes the full blob once.

this gives you the compactness and schema enforcement of protobuf without the read-amplification of always fetching and deserializing the full message.

## run it

compile the proto bindings once:

```sh
cd examples/protobuf-profiles
pip install -r requirements.txt
python generate.py
python main.py
```

## what to look for

the field-level reads show subscription tier for each user without touching the full blob. the upgrade from `free` to `pro` is reflected in both the field index and the full deserialized message:

```
stored Marisol Vega (u1001)
stored Tomas Andersson (u1002)
stored Priya Nair (u1003)

u1001  name=Marisol Vega         tier=pro
u1002  name=Tomas Andersson      tier=free
u1003  name=Priya Nair           tier=enterprise

upgrading u1002 to pro...
  full profile: Tomas Andersson — tier=pro
  field index:  tier=pro

u1001  ttl=3598s remaining
u1002  ttl=3598s remaining
u1003  ttl=3598s remaining
```
