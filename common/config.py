"""
Parseo y validacion de configuracion por variables de entorno,
compartido entre peer y cliente.
"""

import os

MB = 1024 * 1024
DEFAULT_CHUNK_SIZE_MB = 8
DEFAULT_PEER_WEIGHT = 1


class ConfigError(ValueError):
    """Valor de configuracion invalido en una variable de entorno."""


def _parse_positive_int(raw: str, var_name: str) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ConfigError(
            f"{var_name} debe ser un entero positivo, se recibio: {raw!r}"
        )
    if value < 1:
        raise ConfigError(
            f"{var_name} debe ser un entero positivo (>= 1), se recibio: {value}"
        )
    return value


def chunk_size_bytes(env=os.environ) -> int:
    """Tamaño de chunk en bytes, derivado de CHUNK_SIZE_MB (default 8 MB).
    Lanza ConfigError si el valor no es un entero positivo."""
    raw = env.get("CHUNK_SIZE_MB")
    if raw is None or raw == "":
        return DEFAULT_CHUNK_SIZE_MB * MB
    return _parse_positive_int(raw, "CHUNK_SIZE_MB") * MB


def peer_weight(env=os.environ) -> int:
    """Peso de capacidad del peer, derivado de PEER_WEIGHT (default 1).
    Lanza ConfigError si el valor no es un entero positivo."""
    raw = env.get("PEER_WEIGHT")
    if raw is None or raw == "":
        return DEFAULT_PEER_WEIGHT
    return _parse_positive_int(raw, "PEER_WEIGHT")
