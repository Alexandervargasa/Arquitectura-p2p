"""Tests de parseo/validacion de configuracion (common/config.py)."""

import pytest

from common.config import chunk_size_bytes, peer_weight, ConfigError, MB


def test_chunk_size_default_8mb():
    assert chunk_size_bytes(env={}) == 8 * MB


def test_chunk_size_from_env():
    assert chunk_size_bytes(env={"CHUNK_SIZE_MB": "16"}) == 16 * MB


def test_chunk_size_empty_is_default():
    assert chunk_size_bytes(env={"CHUNK_SIZE_MB": ""}) == 8 * MB


@pytest.mark.parametrize("bad", ["0", "-3", "abc", "3.5"])
def test_chunk_size_invalid_raises(bad):
    with pytest.raises(ConfigError):
        chunk_size_bytes(env={"CHUNK_SIZE_MB": bad})


def test_peer_weight_default_1():
    assert peer_weight(env={}) == 1


def test_peer_weight_from_env():
    assert peer_weight(env={"PEER_WEIGHT": "3"}) == 3


@pytest.mark.parametrize("bad", ["0", "-1", "x", "1.2"])
def test_peer_weight_invalid_raises(bad):
    with pytest.raises(ConfigError):
        peer_weight(env={"PEER_WEIGHT": bad})
