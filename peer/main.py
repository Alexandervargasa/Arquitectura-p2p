"""
Punto de entrada de un peer del anillo DFSha.

Variables de entorno:
  SELF_ADDRESS         host:puerto por el que este peer es alcanzable (obligatorio)
  LISTEN_PORT          puerto en el que escucha (default: el de SELF_ADDRESS)
  SEED_PEERS           lista separada por comas de peers semilla para el gossip
                        (puede estar vacia si es el primer peer del cluster)
  REPLICATION_FACTOR   cuantas copias de cada chunk/metadato (default 2)
  DATA_DIR             carpeta donde vive el archivo sqlite de este peer (default ./data)
"""

import logging
import os
import time
from concurrent import futures

import grpc

import dfsha_pb2_grpc
import ring_pb2_grpc
from peer.membership import Membership
from peer.storage import BlobStore
from peer.coordinator import Coordinator
from peer.service import DFSServiceServicer, RingServiceServicer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("dfsha.peer")


def main():
    self_address = os.environ["SELF_ADDRESS"]
    listen_port = int(os.environ.get("LISTEN_PORT", self_address.split(":")[-1]))
    seed_peers = [s for s in os.environ.get("SEED_PEERS", "").split(",") if s]
    replication_factor = int(os.environ.get("REPLICATION_FACTOR", "2"))
    data_dir = os.environ.get("DATA_DIR", "./data")

    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "peer.sqlite3")

    log.info(
        "arrancando peer self=%s puerto=%d seeds=%s rf=%d db=%s",
        self_address, listen_port, seed_peers, replication_factor, db_path,
    )

    local_store = BlobStore(db_path)
    membership = Membership(self_address, seed_peers)
    membership.start()

    # dar tiempo a que el primer round de gossip corra antes de aceptar trafico
    time.sleep(1.0)

    coordinator = Coordinator(membership, local_store, self_address, replication_factor)
    coordinator.ensure_root()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    dfsha_pb2_grpc.add_DFSServiceServicer_to_server(DFSServiceServicer(coordinator), server)
    ring_pb2_grpc.add_RingServiceServicer_to_server(RingServiceServicer(membership, local_store), server)
    server.add_insecure_port(f"[::]:{listen_port}")
    server.start()
    log.info("peer listo, escuchando en el puerto %d", listen_port)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
