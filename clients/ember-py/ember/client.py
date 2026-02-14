"""High-level ember gRPC client for Python."""

from __future__ import annotations

from typing import Optional

import grpc

from ember.proto.ember.v1 import ember_pb2, ember_pb2_grpc


class EmberClient:
    """A gRPC client for the ember cache server.

    Usage::

        client = EmberClient("localhost:6380")
        client.set("key", b"value")
        value = client.get("key")
        client.close()

    Or as a context manager::

        with EmberClient("localhost:6380") as client:
            client.set("key", b"value")
    """

    def __init__(self, addr: str = "localhost:6380", password: str | None = None):
        self._channel = grpc.insecure_channel(addr)
        self._stub = ember_pb2_grpc.EmberCacheStub(self._channel)
        self._password = password

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        """Close the underlying gRPC channel."""
        self._channel.close()

    def _metadata(self) -> list[tuple[str, str]]:
        if self._password:
            return [("authorization", self._password)]
        return []

    # --- strings ---

    def get(self, key: str) -> bytes | None:
        """Get the value for a key, or None if it doesn't exist."""
        resp = self._stub.Get(
            ember_pb2.GetRequest(key=key),
            metadata=self._metadata(),
        )
        if resp.HasField("value"):
            return resp.value
        return None

    def set(
        self,
        key: str,
        value: bytes,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set a key-value pair. Returns True if the key was set."""
        req = ember_pb2.SetRequest(key=key, value=value, nx=nx, xx=xx)
        if px is not None:
            req.expire_millis = px
        elif ex is not None:
            req.expire_seconds = ex
        resp = self._stub.Set(req, metadata=self._metadata())
        return resp.ok

    def delete(self, *keys: str) -> int:
        """Delete keys. Returns the number of keys removed."""
        resp = self._stub.Del(
            ember_pb2.DelRequest(keys=list(keys)),
            metadata=self._metadata(),
        )
        return resp.deleted

    def exists(self, *keys: str) -> int:
        """Returns the number of keys that exist."""
        resp = self._stub.Exists(
            ember_pb2.ExistsRequest(keys=list(keys)),
            metadata=self._metadata(),
        )
        return resp.value

    def incr(self, key: str) -> int:
        """Increment a key by 1. Returns the new value."""
        resp = self._stub.Incr(
            ember_pb2.IncrRequest(key=key),
            metadata=self._metadata(),
        )
        return resp.value

    def incr_by(self, key: str, delta: int) -> int:
        """Increment a key by delta. Returns the new value."""
        resp = self._stub.IncrBy(
            ember_pb2.IncrByRequest(key=key, delta=delta),
            metadata=self._metadata(),
        )
        return resp.value

    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on a key. Returns True if the timeout was set."""
        resp = self._stub.Expire(
            ember_pb2.ExpireRequest(key=key, seconds=seconds),
            metadata=self._metadata(),
        )
        return resp.value

    def ttl(self, key: str) -> int:
        """Returns remaining TTL in seconds. -1 = no expiry, -2 = not found."""
        resp = self._stub.Ttl(
            ember_pb2.TtlRequest(key=key),
            metadata=self._metadata(),
        )
        return resp.value

    # --- lists ---

    def lpush(self, key: str, *values: bytes) -> int:
        """Prepend values to a list. Returns the new length."""
        resp = self._stub.LPush(
            ember_pb2.LPushRequest(key=key, values=list(values)),
            metadata=self._metadata(),
        )
        return resp.value

    def rpush(self, key: str, *values: bytes) -> int:
        """Append values to a list. Returns the new length."""
        resp = self._stub.RPush(
            ember_pb2.RPushRequest(key=key, values=list(values)),
            metadata=self._metadata(),
        )
        return resp.value

    def lpop(self, key: str) -> bytes | None:
        """Remove and return the first element, or None if empty."""
        resp = self._stub.LPop(
            ember_pb2.LPopRequest(key=key),
            metadata=self._metadata(),
        )
        if resp.HasField("value"):
            return resp.value
        return None

    def rpop(self, key: str) -> bytes | None:
        """Remove and return the last element, or None if empty."""
        resp = self._stub.RPop(
            ember_pb2.RPopRequest(key=key),
            metadata=self._metadata(),
        )
        if resp.HasField("value"):
            return resp.value
        return None

    def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        """Return elements in the given range."""
        resp = self._stub.LRange(
            ember_pb2.LRangeRequest(key=key, start=start, stop=stop),
            metadata=self._metadata(),
        )
        return list(resp.values)

    def llen(self, key: str) -> int:
        """Return the length of a list."""
        resp = self._stub.LLen(
            ember_pb2.LLenRequest(key=key),
            metadata=self._metadata(),
        )
        return resp.value

    # --- hashes ---

    def hset(self, key: str, fields: dict[str, bytes]) -> int:
        """Set fields in a hash. Returns the number of new fields."""
        fvs = [
            ember_pb2.FieldValue(field=f, value=v)
            for f, v in fields.items()
        ]
        resp = self._stub.HSet(
            ember_pb2.HSetRequest(key=key, fields=fvs),
            metadata=self._metadata(),
        )
        return resp.value

    def hget(self, key: str, field: str) -> bytes | None:
        """Get a field from a hash, or None if it doesn't exist."""
        resp = self._stub.HGet(
            ember_pb2.HGetRequest(key=key, field=field),
            metadata=self._metadata(),
        )
        if resp.HasField("value"):
            return resp.value
        return None

    def hgetall(self, key: str) -> dict[str, bytes]:
        """Return all fields and values in a hash."""
        resp = self._stub.HGetAll(
            ember_pb2.HGetAllRequest(key=key),
            metadata=self._metadata(),
        )
        return {fv.field: fv.value for fv in resp.fields}

    def hdel(self, key: str, *fields: str) -> int:
        """Remove fields from a hash. Returns the number removed."""
        resp = self._stub.HDel(
            ember_pb2.HDelRequest(key=key, fields=list(fields)),
            metadata=self._metadata(),
        )
        return resp.value

    # --- sets ---

    def sadd(self, key: str, *members: str) -> int:
        """Add members to a set. Returns the number of new members."""
        resp = self._stub.SAdd(
            ember_pb2.SAddRequest(key=key, members=list(members)),
            metadata=self._metadata(),
        )
        return resp.value

    def smembers(self, key: str) -> set[str]:
        """Return all members of a set."""
        resp = self._stub.SMembers(
            ember_pb2.SMembersRequest(key=key),
            metadata=self._metadata(),
        )
        return set(resp.keys)

    def scard(self, key: str) -> int:
        """Return the number of members in a set."""
        resp = self._stub.SCard(
            ember_pb2.SCardRequest(key=key),
            metadata=self._metadata(),
        )
        return resp.value

    # --- sorted sets ---

    def zadd(self, key: str, members: dict[str, float]) -> int:
        """Add members with scores to a sorted set. Returns the number added."""
        sm = [
            ember_pb2.ScoreMember(score=score, member=member)
            for member, score in members.items()
        ]
        resp = self._stub.ZAdd(
            ember_pb2.ZAddRequest(key=key, members=sm),
            metadata=self._metadata(),
        )
        return resp.value

    def zrange(
        self, key: str, start: int, stop: int, with_scores: bool = False
    ) -> list[tuple[str, float]]:
        """Return members in a sorted set within the given rank range."""
        resp = self._stub.ZRange(
            ember_pb2.ZRangeRequest(
                key=key, start=start, stop=stop, with_scores=with_scores,
            ),
            metadata=self._metadata(),
        )
        return [(m.member, m.score) for m in resp.members]

    # --- vectors ---

    def vadd(
        self,
        key: str,
        element: str,
        vector: list[float],
        metric: str = "cosine",
        m: int = 16,
        ef: int = 64,
    ) -> bool:
        """Add a vector to a vector set. Returns True if newly added.

        The vector is sent as packed IEEE 754 floats — no string parsing.
        """
        metric_map = {
            "cosine": ember_pb2.VECTOR_METRIC_COSINE,
            "euclidean": ember_pb2.VECTOR_METRIC_EUCLIDEAN,
            "ip": ember_pb2.VECTOR_METRIC_INNER_PRODUCT,
        }
        resp = self._stub.VAdd(
            ember_pb2.VAddRequest(
                key=key,
                element=element,
                vector=vector,
                metric=metric_map.get(metric, ember_pb2.VECTOR_METRIC_COSINE),
                connectivity=m,
                ef_construction=ef,
            ),
            metadata=self._metadata(),
        )
        return resp.value

    def vsim(
        self,
        key: str,
        query: list[float],
        count: int = 10,
        ef_search: int | None = None,
    ) -> list[tuple[str, float]]:
        """Search for nearest neighbors. Returns (element, distance) pairs."""
        req = ember_pb2.VSimRequest(key=key, query=query, count=count)
        if ef_search is not None:
            req.ef_search = ef_search
        resp = self._stub.VSim(req, metadata=self._metadata())
        return [(r.element, r.distance) for r in resp.results]

    # --- server ---

    def ping(self) -> str:
        """Send PING, returns 'PONG' (or echo message)."""
        resp = self._stub.Ping(
            ember_pb2.PingRequest(),
            metadata=self._metadata(),
        )
        return resp.message

    def flushdb(self, async_mode: bool = False) -> None:
        """Remove all keys."""
        # field name in proto is `async` which is a reserved word in python,
        # so protobuf generates it as `async_` or we use the kwargs approach
        self._stub.FlushDb(
            ember_pb2.FlushDbRequest(**{"async": async_mode}),
            metadata=self._metadata(),
        )

    def dbsize(self) -> int:
        """Return the total number of keys."""
        resp = self._stub.DbSize(
            ember_pb2.DbSizeRequest(),
            metadata=self._metadata(),
        )
        return resp.value
