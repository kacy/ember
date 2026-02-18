#!/usr/bin/env python3
"""
user profile service backed by ember.

demonstrates field-level access without round-tripping the full blob:
  - profile:{id}     — full serialized UserProfile (protobuf)
  - profile:{id}:idx — hash of frequently-accessed fields

reading subscription_tier for an auth check costs a single HGET on the
index, no deserialization. updating a field patches the index immediately,
then re-serializes the blob once. both keys share the same TTL.
"""

from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from ember import EmberClient
from user_profile_pb2 import UserProfile

PROFILE_TTL = 3600  # 1 hour


def now_ts() -> Timestamp:
    t = Timestamp()
    t.FromDatetime(datetime.now(timezone.utc))
    return t


def profile_key(user_id: str) -> str:
    return f"profile:{user_id}"


def index_key(user_id: str) -> str:
    return f"profile:{user_id}:idx"


def store_profile(client: EmberClient, profile: UserProfile) -> None:
    """write the full serialized profile and a parallel field index."""
    pk = profile_key(profile.id)
    ik = index_key(profile.id)

    client.set(pk, profile.SerializeToString(), ex=PROFILE_TTL)
    client.hset(
        ik,
        {
            "name": profile.name.encode(),
            "email": profile.email.encode(),
            "subscription_tier": profile.subscription_tier.encode(),
            "last_seen": str(profile.last_seen.seconds).encode(),
        },
    )
    client.expire(ik, PROFILE_TTL)


def get_profile(client: EmberClient, user_id: str) -> UserProfile | None:
    """fetch and deserialize the full profile."""
    blob = client.get(profile_key(user_id))
    if blob is None:
        return None
    p = UserProfile()
    p.ParseFromString(blob)
    return p


def get_field(client: EmberClient, user_id: str, field: str) -> str | None:
    """read one indexed field — no blob deserialization."""
    value = client.hget(index_key(user_id), field)
    return value.decode() if value else None


def set_field(client: EmberClient, user_id: str, field: str, value: str) -> None:
    """update one field: patch the index immediately, re-serialize the blob once."""
    client.hset(index_key(user_id), {field: value.encode()})

    blob = client.get(profile_key(user_id))
    if blob is None:
        return

    p = UserProfile()
    p.ParseFromString(blob)

    if field == "subscription_tier":
        p.subscription_tier = value
    elif field == "name":
        p.name = value
    elif field == "email":
        p.email = value

    remaining_ttl = client.ttl(profile_key(user_id))
    client.set(profile_key(user_id), p.SerializeToString(), ex=max(remaining_ttl, 1))


def main() -> None:
    with EmberClient("localhost:6380") as client:
        client.flushdb()

        profiles = [
            UserProfile(
                id="u1001",
                name="Marisol Vega",
                email="marisol@example.com",
                last_seen=now_ts(),
                subscription_tier="pro",
                preferences={"theme": "dark", "locale": "en-US"},
            ),
            UserProfile(
                id="u1002",
                name="Tomas Andersson",
                email="tomas@example.com",
                last_seen=now_ts(),
                subscription_tier="free",
                preferences={"theme": "light", "locale": "sv-SE"},
            ),
            UserProfile(
                id="u1003",
                name="Priya Nair",
                email="priya@example.com",
                last_seen=now_ts(),
                subscription_tier="enterprise",
                preferences={"locale": "en-IN", "notifications": "digest"},
            ),
        ]

        for p in profiles:
            store_profile(client, p)
            print(f"stored {p.name} ({p.id})")

        print()

        # field-level reads — HGET on index, no blob deserialization
        for user_id in ["u1001", "u1002", "u1003"]:
            name = get_field(client, user_id, "name")
            tier = get_field(client, user_id, "subscription_tier")
            print(f"{user_id}  name={name!s:22s}  tier={tier}")

        print()

        # field-level update — index patched first, blob re-serialized once
        print("upgrading u1002 to pro...")
        set_field(client, "u1002", "subscription_tier", "pro")

        p = get_profile(client, "u1002")
        if p:
            print(f"  full profile:  {p.name} — tier={p.subscription_tier}")

        tier = get_field(client, "u1002", "subscription_tier")
        print(f"  field index:   tier={tier}")

        print()

        for user_id in ["u1001", "u1002", "u1003"]:
            ttl = client.ttl(profile_key(user_id))
            print(f"{user_id}  ttl={ttl}s remaining")


if __name__ == "__main__":
    main()
