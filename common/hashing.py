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
"""

import hashlib
from typing import List


def ring_id(s: str) -> int:
    """Hashea un string (address de un peer, o una key logica) a un
    entero en el espacio del anillo (160 bits, SHA-1)."""
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)


class Ring:
    # Nodos virtuales por peer fisico: mejora mucho el balanceo de carga
    # del hashing consistente (tecnica estandar, ej. usada en Dynamo/Cassandra).
    VIRTUAL_NODES = 100

    def __init__(self, peer_addresses: List[str]):
        # lista ordenada por id de anillo: [(id, address), ...]
        # cada peer fisico aporta VIRTUAL_NODES puntos al anillo.
        uniq = sorted(set(peer_addresses))
        points = []
        for addr in uniq:
            for v in range(self.VIRTUAL_NODES):
                points.append((ring_id(f"{addr}#vn{v}"), addr))
        self._members = sorted(points, key=lambda t: t[0])
        self._physical = uniq

    def is_empty(self) -> bool:
        return len(self._physical) == 0

    def members(self) -> List[str]:
        return list(self._physical)

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
        for i in range(n):
            addr = self._members[(start + i) % n][1]
            if addr not in seen:
                seen.add(addr)
                owners.append(addr)
            if len(owners) == min(replication_factor, len(self._physical)):
                break
        return owners

    def primary_owner(self, key: str) -> str:
        owners = self.owners_for(key, 1)
        return owners[0] if owners else None

