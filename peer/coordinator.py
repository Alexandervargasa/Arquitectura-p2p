"""
Logica de coordinacion: cualquier peer que reciba una solicitud de un
cliente actua como "coordinador" para esa operacion. No guarda estado
de sesion del cliente (servidor stateless): toda la informacion que
necesita la deriva del path recibido y de la vista actual del anillo.

Responsabilidades:
  - Particionar un archivo en chunks de tamaño configurable y decidir,
    via hashing consistente, que peers son duenos de cada chunk (RF4).
    El particionamiento es por STREAMING: nunca se mantiene el archivo
    completo en memoria (RNF1), solo unos pocos chunks a la vez.
  - Replicar cada chunk/metadato a `replication_factor` peers (RNF2/RNF3).
  - Mantener un namespace de directorios (para ls/mkdir/rmdir) tratando
    cada indice de directorio como un blob mas, replicado igual que un
    chunk.
  - Rebalancear: cuando cambia la membresia del anillo, empujar a sus
    nuevos duenos las claves locales que ya no corresponden a este peer
    y borrarlas localmente solo si el empuje tuvo exito (Req 4).
"""

import hashlib
import json
import logging
import posixpath
import threading

import grpc

from common.proto import ring_pb2
from common.proto import ring_pb2_grpc

log = logging.getLogger("dfsha.coordinator")

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def chunk_key(path: str, index: int) -> str:
    return f"chunk::{path}::{index}"


def file_meta_key(path: str) -> str:
    return f"file::{path}"


def dir_key(path: str) -> str:
    return f"dir::{path}"


class Coordinator:
    def __init__(
        self,
        membership,
        local_store,
        self_address: str,
        replication_factor: int,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.membership = membership
        self.local_store = local_store
        self.self_address = self_address
        self.rf = replication_factor
        self.chunk_size = chunk_size
        self._stubs = {}
        self._rebalance_lock = threading.Lock()

    # ---------- primitivas de blob distribuido (chunks, metadatos y dirs) ----------

    def _stub_for(self, address: str):
        if address not in self._stubs:
            # permitir mensajes mayores al default de 4 MB (chunks grandes
            # viajan peer->peer en StoreBlob/GetBlob)
            max_msg = 64 * 1024 * 1024
            channel = grpc.insecure_channel(
                address,
                options=[
                    ("grpc.max_send_message_length", max_msg),
                    ("grpc.max_receive_message_length", max_msg),
                ],
            )
            self._stubs[address] = ring_pb2_grpc.RingServiceStub(channel)
        return self._stubs[address]

    def put_blob(self, key: str, data: bytes) -> int:
        """Escribe en los `rf` peers duenos de `key`. Devuelve cuantas
        escrituras tuvieron exito (para poder reportar quorum)."""
        ring = self.membership.current_ring()
        owners = ring.owners_for(key, self.rf)
        ok_count = 0
        for addr in owners:
            if addr == self.self_address:
                self.local_store.put(key, data)
                ok_count += 1
            else:
                try:
                    resp = self._stub_for(addr).StoreBlob(
                        ring_pb2.BlobRequest(key=key, data=data), timeout=30.0
                    )
                    if resp.ok:
                        ok_count += 1
                except grpc.RpcError:
                    log.warning("no se pudo replicar %s en %s", key, addr)
        return ok_count

    def get_blob(self, key: str):
        """Lee de cualquiera de los duenos vivos de `key` (el primero
        que responda con exito)."""
        ring = self.membership.current_ring()
        owners = ring.owners_for(key, self.rf)
        for addr in owners:
            if addr == self.self_address:
                data = self.local_store.get(key)
                if data is not None:
                    return data
            else:
                try:
                    resp = self._stub_for(addr).GetBlob(ring_pb2.BlobKeyRequest(key=key), timeout=30.0)
                    if resp.found:
                        return resp.data
                except grpc.RpcError:
                    continue
        return None

    def delete_blob(self, key: str):
        ring = self.membership.current_ring()
        owners = ring.owners_for(key, self.rf)
        for addr in owners:
            if addr == self.self_address:
                self.local_store.delete(key)
            else:
                try:
                    self._stub_for(addr).DeleteBlob(ring_pb2.BlobKeyRequest(key=key), timeout=10.0)
                except grpc.RpcError:
                    log.warning("no se pudo borrar %s en %s", key, addr)

    # ---------- namespace de directorios ----------

    def _get_dir(self, path: str):
        raw = self.get_blob(dir_key(path))
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def _put_dir(self, path: str, entries: dict):
        self.put_blob(dir_key(path), json.dumps({"entries": entries}).encode("utf-8"))

    def ensure_root(self):
        if self._get_dir("/") is None:
            self._put_dir("/", {})

    def ls(self, path: str):
        d = self._get_dir(path)
        if d is None:
            return None
        out = []
        for name, kind in d["entries"].items():
            if kind == "dir":
                out.append((name, True, 0))
            else:
                meta = self._get_file_meta(posixpath.join(path, name))
                size = meta["size"] if meta else 0
                out.append((name, False, size))
        return sorted(out)

    def mkdir(self, path: str):
        if path == "/":
            return False, "la raiz ya existe"
        if self._get_dir(path) is not None:
            return False, "ya existe"
        parent = posixpath.dirname(path) or "/"
        parent_dir = self._get_dir(parent)
        if parent_dir is None:
            return False, f"el directorio padre no existe: {parent}"
        name = posixpath.basename(path)
        parent_dir["entries"][name] = "dir"
        self._put_dir(parent, parent_dir["entries"])
        self._put_dir(path, {})
        return True, "directorio creado"

    def rmdir(self, path: str):
        d = self._get_dir(path)
        if d is None:
            return False, "no existe"
        if d["entries"]:
            return False, "el directorio no esta vacio"
        parent = posixpath.dirname(path) or "/"
        parent_dir = self._get_dir(parent)
        name = posixpath.basename(path)
        if parent_dir and name in parent_dir["entries"]:
            del parent_dir["entries"][name]
            self._put_dir(parent, parent_dir["entries"])
        self.delete_blob(dir_key(path))
        return True, "directorio eliminado"

    # ---------- metadatos y chunks de archivo ----------

    def _get_file_meta(self, path: str):
        raw = self.get_blob(file_meta_key(path))
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def rm(self, path: str):
        meta = self._get_file_meta(path)
        if meta is None:
            return False, "el archivo no existe"
        for i in range(meta["chunk_count"]):
            self.delete_blob(chunk_key(path, i))
        self.delete_blob(file_meta_key(path))
        parent = posixpath.dirname(path) or "/"
        parent_dir = self._get_dir(parent)
        name = posixpath.basename(path)
        if parent_dir and name in parent_dir["entries"]:
            del parent_dir["entries"][name]
            self._put_dir(parent, parent_dir["entries"])
        return True, "archivo eliminado"

    def store_file_stream(self, path: str, byte_iter):
        """Particiona y almacena un archivo por STREAMING.

        `byte_iter` es un iterable de trozos de bytes de tamaño
        arbitrario (los que lleguen por gRPC desde el cliente). Los
        reagrupamos a `chunk_size` y ubicamos/replicamos cada chunk sin
        acumular el archivo completo en memoria (RNF1). Mantiene a lo
        sumo ~1-2 chunks residentes. Calcula un hash SHA-256 del
        contenido completo (de forma incremental) para verificar
        integridad en la lectura (RNF3, barato).

        Devuelve (chunk_count, total_bytes).
        """
        parent = posixpath.dirname(path) or "/"
        parent_dir = self._get_dir(parent)
        if parent_dir is None:
            raise RuntimeError(f"el directorio padre no existe: {parent}")

        hasher = hashlib.sha256()
        buffer = bytearray()
        idx = 0
        total = 0

        def flush_chunk(data: bytes):
            nonlocal idx
            written = self.put_blob(chunk_key(path, idx), data)
            if written == 0:
                raise RuntimeError(f"no se pudo escribir el chunk {idx} en ningun peer")
            idx += 1

        for piece in byte_iter:
            if not piece:
                continue
            hasher.update(piece)
            total += len(piece)
            buffer.extend(piece)
            # emitimos tantos chunks completos como haya en el buffer
            while len(buffer) >= self.chunk_size:
                flush_chunk(bytes(buffer[:self.chunk_size]))
                del buffer[:self.chunk_size]

        # ultimo chunk (residuo), o un unico chunk vacio si el archivo era vacio
        if buffer or idx == 0:
            flush_chunk(bytes(buffer))

        meta = {
            "size": total,
            "chunk_count": idx,
            "chunk_size": self.chunk_size,
            "sha256": hasher.hexdigest(),
        }
        if self.put_blob(file_meta_key(path), json.dumps(meta).encode("utf-8")) == 0:
            raise RuntimeError("no se pudieron escribir los metadatos del archivo")

        name = posixpath.basename(path)
        parent_dir["entries"][name] = "file"
        self._put_dir(parent, parent_dir["entries"])
        return idx, total

    def read_file(self, path: str):
        """Generador de los chunks de un archivo, en orden. No carga el
        archivo completo en memoria (RNF1): produce un chunk a la vez."""
        meta = self._get_file_meta(path)
        if meta is None:
            return None
        return self._read_chunks(path, meta)

    def _read_chunks(self, path: str, meta: dict):
        for idx in range(meta["chunk_count"]):
            data = self.get_blob(chunk_key(path, idx))
            if data is None:
                raise RuntimeError(f"chunk {idx} de {path} no disponible (peers caidos)")
            yield data

    def file_checksum(self, path: str):
        """Devuelve el sha256 esperado de un archivo (de sus metadatos),
        o None si no existe / no tiene checksum."""
        meta = self._get_file_meta(path)
        if meta is None:
            return None
        return meta.get("sha256")

    # ---------- rebalanceo ante cambios de membresia (Req 4) ----------

    def on_membership_change(self, alive_peers):
        """Callback que dispara membership cuando cambia el conjunto de
        peers vivos. Corre el rebalanceo en un hilo de fondo para no
        bloquear el gossip."""
        t = threading.Thread(target=self._rebalance, daemon=True)
        t.start()

    def _rebalance(self):
        # un solo rebalanceo a la vez
        if not self._rebalance_lock.acquire(blocking=False):
            log.debug("rebalanceo ya en curso, se omite este disparo")
            return
        try:
            ring = self.membership.current_ring()
            if ring.is_empty():
                return
            migrated = 0
            for key in list(self.local_store.keys()):
                owners = ring.owners_for(key, self.rf)
                if self.self_address in owners:
                    continue  # seguimos siendo duenos: conservar (Req 4.6)
                data = self.local_store.get(key)
                if data is None:
                    continue
                # empujar a todos los nuevos duenos que falten
                if self._push_key_to_owners(key, data, owners):
                    self.local_store.delete(key)  # borrar local solo si exito (Req 4.3/4.4)
                    migrated += 1
                else:
                    log.warning("rebalanceo: no se pudo migrar %s, se conserva local", key)
            if migrated:
                log.info("rebalanceo: %d claves migradas", migrated)
        except Exception:
            log.exception("error durante el rebalanceo")
        finally:
            self._rebalance_lock.release()

    def _push_key_to_owners(self, key: str, data: bytes, owners) -> bool:
        """Empuja `key` a cada owner (que no sea este peer). Devuelve
        True solo si TODOS los owners destino confirmaron la escritura."""
        all_ok = True
        for addr in owners:
            if addr == self.self_address:
                continue
            try:
                resp = self._stub_for(addr).StoreBlob(
                    ring_pb2.BlobRequest(key=key, data=data), timeout=30.0
                )
                if not resp.ok:
                    all_ok = False
            except grpc.RpcError:
                all_ok = False
        return all_ok
