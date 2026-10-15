"""
Tests del coordinador: streaming de archivos grandes (Req 1),
particionamiento con chunk_size configurable (Req 2), round-trip con
integridad (Req 1.6 / Req 4 checksum) y rebalanceo ante cambios de
membresia (Req 4).

Usamos un cluster SIMULADO en memoria: cada peer tiene un dict como
almacen local y el coordinador habla con los demas peers a traves de
un stub falso que escribe/lee directamente en esos dicts. Asi probamos
toda la logica de coordinacion y rebalanceo sin levantar gRPC real.
"""

import hashlib

from common.hashing import Ring
from peer.coordinator import Coordinator, chunk_key, file_meta_key


# ---------- dobles de prueba ----------

class DictStore:
    """Almacen local en memoria (equivalente a BlobStore sobre SQLite)."""
    def __init__(self):
        self.d = {}

    def put(self, key, data):
        self.d[key] = bytes(data)

    def get(self, key):
        return self.d.get(key)

    def delete(self, key):
        return self.d.pop(key, None) is not None

    def keys(self):
        return list(self.d.keys())


class FakeStatus:
    def __init__(self, ok):
        self.ok = ok
        self.message = "ok" if ok else "err"


class FakeBlob:
    def __init__(self, found, data=b""):
        self.found = found
        self.data = data


class FakeStub:
    """Stub RingService falso: opera directo sobre el store del peer destino."""
    def __init__(self, store):
        self.store = store

    def StoreBlob(self, request, timeout=None):
        self.store.put(request.key, request.data)
        return FakeStatus(True)

    def GetBlob(self, request, timeout=None):
        data = self.store.get(request.key)
        if data is None:
            return FakeBlob(False)
        return FakeBlob(True, data)

    def DeleteBlob(self, request, timeout=None):
        self.store.delete(request.key)
        return FakeStatus(True)


class FakeMembership:
    def __init__(self, cluster, weights=None):
        # cluster: {address: DictStore}
        self.cluster = cluster
        self.weights = weights or {a: 1 for a in cluster}

    def current_ring(self):
        return Ring({a: self.weights[a] for a in self.cluster})

    def set_alive(self, addresses):
        self.cluster = {a: self.cluster[a] for a in addresses}
        self.weights = {a: self.weights.get(a, 1) for a in addresses}


def make_coordinator(address, cluster, membership, chunk_size):
    coord = Coordinator(membership, cluster[address], address, replication_factor=2, chunk_size=chunk_size)
    # parchear _stub_for para hablar con los stores del cluster
    coord._stub_for = lambda addr: FakeStub(cluster[addr])
    return coord


def build_cluster(addresses, chunk_size, weights=None):
    cluster = {a: DictStore() for a in addresses}
    membership = FakeMembership(cluster, weights)
    coords = {a: make_coordinator(a, cluster, membership, chunk_size) for a in addresses}
    # inicializar la raiz una sola vez (cualquier coordinador sirve)
    coords[addresses[0]].ensure_root()
    return cluster, membership, coords


# ---------- Req 1 / 2: streaming y chunk_size ----------

def test_store_file_stream_particiona_en_chunk_size():
    chunk_size = 4  # 4 bytes por chunk para la prueba
    cluster, membership, coords = build_cluster(["p1:1", "p2:1", "p3:1"], chunk_size)
    coord = coords["p1:1"]

    data = b"abcdefghij"  # 10 bytes -> ceil(10/4) = 3 chunks (4,4,2)
    pieces = [data[i:i + 3] for i in range(0, len(data), 3)]  # trozos irregulares de 3
    n_chunks, total = coord.store_file_stream("/f.bin", iter(pieces))

    assert total == 10
    assert n_chunks == 3  # reagrupado a chunk_size=4, no a los trozos de entrada


def test_round_trip_identico_y_checksum():
    chunk_size = 8
    cluster, membership, coords = build_cluster(["p1:1", "p2:1", "p3:1"], chunk_size)
    coord = coords["p1:1"]

    original = bytes(range(256)) * 10  # 2560 bytes
    coord.store_file_stream("/big.bin", iter([original[i:i+100] for i in range(0, len(original), 100)]))

    got = b"".join(coord.read_file("/big.bin"))
    assert got == original
    assert coord.file_checksum("/big.bin") == hashlib.sha256(original).hexdigest()


def test_archivo_vacio():
    cluster, membership, coords = build_cluster(["p1:1", "p2:1"], chunk_size=8)
    coord = coords["p1:1"]
    n_chunks, total = coord.store_file_stream("/empty.bin", iter([]))
    assert total == 0
    assert n_chunks == 1  # un unico chunk vacio
    assert b"".join(coord.read_file("/empty.bin")) == b""


def test_memoria_acotada_no_crece_con_el_archivo():
    # verificamos que el coordinador NUNCA materializa mas de ~2 chunks:
    # alimentamos un generador "grande" y comprobamos que el buffer interno
    # no retiene todo. Lo hacemos indirecto: contamos que put_blob se llama
    # por chunk y no una sola vez con todo el archivo.
    chunk_size = 16
    cluster, membership, coords = build_cluster(["p1:1", "p2:1"], chunk_size)
    coord = coords["p1:1"]

    sizes = []
    orig_put = coord.put_blob

    def spy(key, data):
        if key.startswith("chunk::"):
            sizes.append(len(data))
        return orig_put(key, data)

    coord.put_blob = spy

    total_size = chunk_size * 50  # 50 chunks
    coord.store_file_stream("/g.bin", iter([b"x" * chunk_size for _ in range(50)]))

    # 50 chunks escritos, cada uno <= chunk_size (ningun put con el archivo entero)
    assert len(sizes) == 50
    assert all(s <= chunk_size for s in sizes)


# ---------- Req 4: rebalanceo ----------

def test_rebalanceo_migra_claves_y_no_pierde_datos():
    chunk_size = 8
    # arrancamos con 2 peers
    cluster, membership, coords = build_cluster(["p1:1", "p2:1"], chunk_size)
    coord = coords["p1:1"]

    original = b"hola mundo distribuido " * 20
    coord.store_file_stream("/f.bin", iter([original]))

    # el archivo debe leerse bien con 2 peers
    assert b"".join(coord.read_file("/f.bin")) == original

    # agregamos 2 peers nuevos y rebalanceamos TODOS los peers existentes
    cluster["p3:1"] = DictStore()
    cluster["p4:1"] = DictStore()
    membership.set_alive(["p1:1", "p2:1", "p3:1", "p4:1"])
    # recrear coordinadores con el cluster ampliado y rebalancear
    coords = {a: make_coordinator(a, cluster, membership, chunk_size) for a in cluster}
    for c in coords.values():
        c._rebalance()

    # tras el rebalanceo, el archivo sigue siendo legible desde cualquier peer
    for a in cluster:
        got = b"".join(coords[a].read_file("/f.bin"))
        assert got == original, f"peer {a} no reconstruye el archivo"


def test_rebalanceo_conserva_si_sigue_siendo_dueno():
    chunk_size = 8
    cluster, membership, coords = build_cluster(["p1:1", "p2:1", "p3:1"], chunk_size)
    coord = coords["p1:1"]
    coord.store_file_stream("/f.bin", iter([b"contenido de prueba"]))

    # sin cambios de membresia, rebalancear no debe borrar nada que siga siendo propio
    before = {a: set(cluster[a].keys()) for a in cluster}
    for c in coords.values():
        c._rebalance()
    after = {a: set(cluster[a].keys()) for a in cluster}
    assert before == after
