"""
Membresia dinamica del anillo.

Cada peer arranca con al menos un "peer semilla" (o su propia direccion
si es el primero). Cada cierto intervalo hace HealthCheck a todos los
peers que conoce; en cada respuesta intercambia listas de peers
conocidos (gossip/anti-entropia). Peers que dejan de responder se
sacan de la vista "viva" del anillo (aunque se mantienen en la lista de
conocidos por si vuelven a aparecer).

Ademas del address, el gossip propaga el PESO (capacidad) de cada peer,
para construir el anillo con nodos virtuales ponderados: un peer de
peso w aporta w*BASE nodos virtuales y recibe ~w veces mas claves.

Esto permite:
  - Que un peer nuevo entre al cluster con solo conocer una semilla.
  - Que un peer caido deje de recibir trafico (se excluye del ring
    activo), sin que los demas tengan que reiniciarse.
"""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Set

import grpc

from common.proto import ring_pb2
from common.proto import ring_pb2_grpc
from common.hashing import Ring, ring_id

log = logging.getLogger("dfsha.membership")


class Membership:
    def __init__(
        self,
        self_address: str,
        seed_addresses: List[str],
        self_weight: int = 1,
        interval_sec: float = 3.0,
    ):
        self.self_address = self_address
        self.self_weight = self_weight
        self._known: Set[str] = set(seed_addresses) | {self_address}
        self._alive: Set[str] = {self_address}
        # peso conocido de cada peer (por gossip); default 1 si se desconoce.
        self._weights: Dict[str, int] = {self_address: self_weight}
        self._interval = interval_sec
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # callback opcional que se dispara cuando cambia el conjunto vivo
        # (lo usa el coordinador para rebalancear).
        self._on_change: Optional[Callable[[Set[str]], None]] = None

    def set_on_membership_change(self, callback: Callable[[Set[str]], None]):
        self._on_change = callback

    def current_ring(self) -> Ring:
        with self._lock:
            alive = set(self._alive)
            weights = {a: self._weights.get(a, 1) for a in alive}
        return Ring(weights)

    def alive_peers(self) -> Set[str]:
        with self._lock:
            return set(self._alive)

    def known_peers_proto(self) -> List[ring_pb2.PeerInfo]:
        with self._lock:
            known = set(self._known)
            weights = dict(self._weights)
        return [
            ring_pb2.PeerInfo(
                address=a,
                peer_id_hex=hex(ring_id(a)),
                weight=weights.get(a, 1),
            )
            for a in known
        ]

    def merge_known(self, peers: List[ring_pb2.PeerInfo]):
        """Mezcla una lista de PeerInfo (address + weight) en la vista
        de conocidos. Acepta tambien addresses sueltos (str) por
        compatibilidad."""
        with self._lock:
            for p in peers:
                if isinstance(p, str):
                    self._known.add(p)
                    self._weights.setdefault(p, 1)
                else:
                    self._known.add(p.address)
                    # un weight 0 significa "no informado"; conserva lo
                    # que ya supieramos, o 1 por defecto.
                    if p.weight and p.weight > 0:
                        self._weights[p.address] = p.weight
                    else:
                        self._weights.setdefault(p.address, 1)

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
                    caller=ring_pb2.PeerInfo(
                        address=self.self_address,
                        peer_id_hex=hex(ring_id(self.self_address)),
                        weight=self.self_weight,
                    ),
                    known_peers=self.known_peers_proto(),
                )
                resp = stub.HealthCheck(req, timeout=1.5)
                if resp.alive:
                    newly_alive.add(addr)
                self.merge_known(list(resp.known_peers))
            except grpc.RpcError:
                log.debug("peer no responde: %s", addr)
            except Exception:
                log.exception("error de health-check contra %s", addr)

        with self._lock:
            changed = newly_alive != self._alive
            self._alive = newly_alive
        if changed:
            log.info("ring activo cambio: %s", sorted(newly_alive))
            if self._on_change is not None:
                try:
                    self._on_change(set(newly_alive))
                except Exception:
                    log.exception("callback de cambio de membresia fallo")
        else:
            log.debug("ring activo: %s", sorted(newly_alive))
