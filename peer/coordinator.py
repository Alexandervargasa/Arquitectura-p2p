"""
Logica de coordinacion: cualquier peer que reciba una solicitud de un
cliente actua como "coordinador" para esa operacion. No guarda estado
de sesion del cliente (servidor stateless): toda la informacion que
necesita la deriva del path recibido y de la vista actual del anillo.

Responsabilidades:
  - Particionar un archivo en chunks fijos y decidir, via hashing
    consistente, que peers son duenos de cada chunk (RF4).
  - Replicar cada chunk/metadato a `replication_factor` peers (RNF2/RNF3).
  - Mantener un namespace de directorios (para ls/mkdir/rmdir) tratando
    cada indice de directorio como un blob mas, replicado igual que un
    chunk.
"""

import json
import logging
import posixpath

import grpc

import ring_pb2
import ring_pb2_grpc

log = logging.getLogger("dfsha.coordinator")

CHUNK_SIZE = 1024 * 1024  # 1 MB


def chunk_key(path: str, index: int) -> str:
    return f"chunk::{path}::{index}"


def file_meta_key(path: str) -> str:
    return f"file::{path}"


def dir_key(path: str) -> str:
    return f"dir::{path}"


class Coordinator:
    def __init__(self, membership, local_store, self_address: str, replication_factor: int):
        self.membership = membership
        self.local_store = local_store
        self.self_address = self_address
        self.rf = replication_factor
        self._stubs = {}

    # ---------- primitivas de blob distribuido (usadas por chunks, metadatos y dirs) ----------

    def _stub_for(self, address: str):
        if address not in self._stubs:
            channel = grpc.insecure_channel(address)
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
                        ring_pb2.BlobRequest(key=key, data=data), timeout=5.0
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
                    resp = self._stub_for(addr).GetBlob(ring_pb2.BlobKeyRequest(key=key), timeout=5.0)
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
                    self._stub_for(addr).DeleteBlob(ring_pb2.BlobKeyRequest(key=key), timeout=5.0)
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

    def store_file(self, path: str, data: bytes):
        chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)] or [b""]
        for idx, chunk in enumerate(chunks):
            written = self.put_blob(chunk_key(path, idx), chunk)
            if written == 0:
                raise RuntimeError(f"no se pudo escribir el chunk {idx} en ningun peer")
        meta = {"size": len(data), "chunk_count": len(chunks)}
        self.put_blob(file_meta_key(path), json.dumps(meta).encode("utf-8"))

        parent = posixpath.dirname(path) or "/"
        parent_dir = self._get_dir(parent)
        if parent_dir is None:
            raise RuntimeError(f"el directorio padre no existe: {parent}")
        name = posixpath.basename(path)
        parent_dir["entries"][name] = "file"
        self._put_dir(parent, parent_dir["entries"])
        return len(chunks)

    def read_file(self, path: str):
        meta = self._get_file_meta(path)
        if meta is None:
            return None
        for idx in range(meta["chunk_count"]):
            data = self.get_blob(chunk_key(path, idx))
            if data is None:
                raise RuntimeError(f"chunk {idx} de {path} no disponible (peers caidos)")
            yield data
