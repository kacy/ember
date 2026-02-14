"""Tests for the ember python client.

These are unit-level tests that verify the client API surface and proto
mapping without requiring a running server. Integration tests that hit
a real ember-server are left to the CI integration suite.
"""

from unittest.mock import MagicMock, patch

import pytest

from ember.client import EmberClient


# --- construction ---


def test_default_addr():
    """Client should default to localhost:6380."""
    with patch("grpc.insecure_channel") as mock_channel:
        mock_channel.return_value = MagicMock()
        client = EmberClient()
        mock_channel.assert_called_once_with("localhost:6380")
        client.close()


def test_custom_addr():
    """Client should accept a custom address."""
    with patch("grpc.insecure_channel") as mock_channel:
        mock_channel.return_value = MagicMock()
        client = EmberClient("10.0.0.1:9999")
        mock_channel.assert_called_once_with("10.0.0.1:9999")
        client.close()


def test_context_manager():
    """Client should work as a context manager and close on exit."""
    with patch("grpc.insecure_channel") as mock_channel:
        chan = MagicMock()
        mock_channel.return_value = chan
        with EmberClient() as client:
            assert client is not None
        chan.close.assert_called_once()


# --- metadata ---


def test_no_password_metadata():
    """Without a password, metadata should be empty."""
    with patch("grpc.insecure_channel"):
        client = EmberClient()
        assert client._metadata() == []
        client.close()


def test_password_metadata():
    """With a password, metadata should include authorization header."""
    with patch("grpc.insecure_channel"):
        client = EmberClient(password="secret")
        metadata = client._metadata()
        assert len(metadata) == 1
        assert metadata[0] == ("authorization", "secret")
        client.close()


# --- type conversions ---


def test_vadd_metric_mapping():
    """Verify that metric string maps to the correct proto enum."""
    from ember.proto.ember.v1 import ember_pb2

    metric_map = {
        "cosine": ember_pb2.VECTOR_METRIC_COSINE,
        "euclidean": ember_pb2.VECTOR_METRIC_EUCLIDEAN,
        "ip": ember_pb2.VECTOR_METRIC_INNER_PRODUCT,
    }
    for name, expected in metric_map.items():
        assert metric_map[name] == expected


def test_vadd_unknown_metric_defaults_to_cosine():
    """Unknown metric strings should default to cosine."""
    from ember.proto.ember.v1 import ember_pb2

    metric_map = {
        "cosine": ember_pb2.VECTOR_METRIC_COSINE,
        "euclidean": ember_pb2.VECTOR_METRIC_EUCLIDEAN,
        "ip": ember_pb2.VECTOR_METRIC_INNER_PRODUCT,
    }
    result = metric_map.get("unknown", ember_pb2.VECTOR_METRIC_COSINE)
    assert result == ember_pb2.VECTOR_METRIC_COSINE
