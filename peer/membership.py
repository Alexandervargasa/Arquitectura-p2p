"""
Membresia dinamica del anillo.

Cada peer arranca con al menos un "peer semilla" (o su propia direccion
si es el primero). Cada cierto intervalo hace HealthCheck a todos los
peers que conoce; en cada respuesta intercambia listas de peers
conocidos (gossip/anti-entropia). Peers que dejan de responder se
sacan de la vista "viva" del anillo (aunque se mantienen en la lista de
conocidos por si vuelven a aparecer).

Esto permite:
  - Que un peer nuevo entre al cluster con solo conocer una semilla.
  - Que un peer caido deje de recibir trafico (se excluye del ring
    activo), sin que los demas tengan que reiniciarse.
"""

import logging
import threading
import time
from typing import List, Set

import grpc

import ring_pb2
import ring_pb2_grpc
from common.hashing import Ring, ring_id

log = logging.getLogger("dfsha.membership")


class Membership:
    def __init__(self, self_address: str, seed_addresses: List[str], interval_sec: float = 3.0):
        self.self_address = self_address
        self._known: Set[str] = set(seed_addresses) | {self_address}
        self._alive: Set[str] = {self_address}
        self._interval = interval_sec
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def current_ring(self) -> Ring:
        with self._lock:
            alive = set(self._alive)
        return Ring(sorted(alive))

    def known_peers_proto(self) -> List[ring_pb2.PeerInfo]:
        with self._lock:
            known = set(self._known)
        return [ring_pb2.PeerInfo(address=a, peer_id_hex=hex(ring_id(a))) for a in known]

    def merge_known(self, addresses: List[str]):
        with self._lock:
            self._known |= set(addresses)

    def start(self):
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.is_set():
            self._probe_round()
            time.sleep(self._interval)

    def _probe_round(self):
        with self._lock:
            targets = [a for a in self._known if a != self.self_address]
        newly_alive = {self.self_address}
        for addr in targets:
            try:
                channel = grpc.insecure_channel(addr)
                stub = ring_pb2_grpc.RingServiceStub(channel)
                req = ring_pb2.HealthRequest(
                    caller=ring_pb2.PeerInfo(address=self.self_address, peer_id_hex=hex(ring_id(self.self_address))),
                    known_peers=self.known_peers_proto(),
                )
                resp = stub.HealthCheck(req, timeout=1.5)
                if resp.alive:
                    newly_alive.add(addr)
                self.merge_known([p.address for p in resp.known_peers])
            except grpc.RpcError:
                log.debug("peer no responde: %s", addr)
            except Exception:
                log.exception("error de health-check contra %s", addr)

        with self._lock:
            self._alive = newly_alive
        log.info("ring activo: %s", sorted(newly_alive))
