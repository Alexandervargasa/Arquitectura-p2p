"""
Hashing consistente para el anillo DFSha.

Cada peer tiene un id (entero, espacio SHA-1 de 160 bits) calculado a
partir de su address ("host:puerto"). Cada clave (chunk de archivo o
metadato) tambien se mapea a un entero en ese mismo espacio.

El dueño primario de una clave es el primer peer, recorriendo el anillo
en sentido horario desde la posicion de la clave, cuyo id sea >= key_id
(con wraparound). Los siguientes (replication_factor - 1) peers en el
anillo (sucesores) guardan las replicas.

Esto es un anillo de hashing consistente "de vista completa": cada peer
conoce a todos los peers vivos y calcula el/los dueños localmente en un
solo paso (sin enrutamiento multi-salto tipo Chord con finger tables).

Nodos virtuales ponderados por capacidad
-----------------------------------------
Cada peer aporta `peso * BASE_VIRTUAL_NODES` nodos virtuales al anillo.
Con pesos iguales (default 1) el reparto es uniforme (como antes). Con
pesos distintos, un peer de peso 3 aporta 3x nodos virtuales y por tanto
recibe estadisticamente ~3x claves: asi una maquina mas grande guarda
proporcionalmente mas chunks. El calculo sigue siendo determinista (solo
depende de {address: peso}), sin indice central.
"""

import hashlib
from typing import Dict, List, Mapping, Union


def ring_id(s: str) -> int:
    """Hashea un string (address de un peer, o una key logica) a un
    entero en el espacio del anillo (160 bits, SHA-1)."""
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)


class Ring:
    # Nodos virtuales base que aporta un peer de peso 1. Un peer de peso
    # w aporta w * BASE_VIRTUAL_NODES. Mejora mucho el balanceo de carga
    # del hashing consistente (tecnica estandar, ej. Dynamo/Cassandra).
    BASE_VIRTUAL_NODES = 100

    def __init__(self, peers: Union[List[str], Mapping[str, int]]):
        """`peers` puede ser:
          - una lista de addresses (retrocompatible): cada peer tiene peso 1.
          - un dict {address: peso}: cada peer aporta peso*BASE_VIRTUAL_NODES
            nodos virtuales.
        Un peso <= 0 o no entero se trata como 1 (defensivo; la validacion
        dura del valor configurado vive en el arranque del peer).
        """
        weights = self._normalize_weights(peers)

        points = []
        for addr in sorted(weights):
            w = weights[addr]
            vnodes = w * self.BASE_VIRTUAL_NODES
            for v in range(vnodes):
                points.append((ring_id(f"{addr}#vn{v}"), addr))
        self._members = sorted(points, key=lambda t: t[0])
        self._physical = sorted(weights)
        self._weights = weights

    @staticmethod
    def _normalize_weights(peers: Union[List[str], Mapping[str, int]]) -> Dict[str, int]:
        weights: Dict[str, int] = {}
        if isinstance(peers, Mapping):
            items = peers.items()
        else:
            items = ((addr, 1) for addr in peers)
        for addr, w in items:
            try:
                wi = int(w)
            except (TypeError, ValueError):
                wi = 1
            if wi < 1:
                wi = 1
            weights[addr] = wi
        return weights

    def is_empty(self) -> bool:
        return len(self._physical) == 0

    def members(self) -> List[str]:
        return list(self._physical)

    def weight_of(self, address: str) -> int:
        return self._weights.get(address, 1)

    def owners_for(self, key: str, replication_factor: int) -> List[str]:
        """Devuelve hasta `replication_factor` addresses de peers FISICOS
        distintos, empezando por el dueño primario de `key` y siguiendo
        el anillo (de nodos virtuales) en sentido horario, saltando
        nodos virtuales que ya hayan aportado un peer repetido."""
        if not self._members:
            return []
        key_id = ring_id(key)
        n = len(self._members)

        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if self._members[mid][0] < key_id:
                lo = mid + 1
            else:
                hi = mid
        start = lo % n

        owners = []
        seen = set()
        target = min(replication_factor, len(self._physical))
        for i in range(n):
            addr = self._members[(start + i) % n][1]
            if addr not in seen:
                seen.add(addr)
                owners.append(addr)
            if len(owners) == target:
                break
        return owners

    def primary_owner(self, key: str) -> str:
        owners = self.owners_for(key, 1)
        return owners[0] if owners else None
