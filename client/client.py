"""
DFSha - Cliente CLI (arquitectura de anillo)

Se conecta a UN peer cualquiera del anillo (ese peer actua de
coordinador para cada request). El cliente no sabe nada de chunks,
particionamiento ni replicacion: para el, sigue siendo un DFS normal.

Comandos:
  ls [path]
  cd <path>
  pwd
  mkdir <path>
  rmdir <path>
  rm <path>
  send <archivo_local> [ruta_remota]
  recv <ruta_remota> [archivo_local]
  exit
"""

import hashlib
import os
import posixpath
import argparse
import sys

import grpc

from common.proto import dfsha_pb2
from common.proto import dfsha_pb2_grpc
from common.config import chunk_size_bytes, ConfigError

# Permitir mensajes gRPC mayores al default de 4 MB (chunks de 8-16 MB).
_MAX_MSG = 64 * 1024 * 1024
_GRPC_OPTIONS = [
    ("grpc.max_send_message_length", _MAX_MSG),
    ("grpc.max_receive_message_length", _MAX_MSG),
]


class DFSClient:
    def __init__(self, host: str, port: int, chunk_size: int):
        self.channel = grpc.insecure_channel(f"{host}:{port}", options=_GRPC_OPTIONS)
        self.stub = dfsha_pb2_grpc.DFSServiceStub(self.channel)
        self.cwd = "/"
        self.chunk_size = chunk_size

    def resolve(self, path: str) -> str:
        if not path:
            return self.cwd
        candidate = path if path.startswith("/") else posixpath.join(self.cwd, path)
        return posixpath.normpath(candidate)

    def cmd_cd(self, path):
        target = self.resolve(path)
        resp = self.stub.Ls(dfsha_pb2.LsRequest(path=target))
        if not resp.ok:
            print(f"cd: {resp.message}")
            return
        self.cwd = target

    def cmd_pwd(self):
        print(self.cwd)

    def cmd_ls(self, path):
        target = self.resolve(path)
        resp = self.stub.Ls(dfsha_pb2.LsRequest(path=target))
        if not resp.ok:
            print(f"ls: {resp.message}")
            return
        for e in resp.entries:
            tag = "d" if e.is_dir else "f"
            print(f"{tag}  {e.size_bytes:>10}  {e.name}")

    def cmd_mkdir(self, path):
        resp = self.stub.Mkdir(dfsha_pb2.PathRequest(path=self.resolve(path)))
        print(resp.message if resp.ok else f"mkdir: {resp.message}")

    def cmd_rmdir(self, path):
        resp = self.stub.Rmdir(dfsha_pb2.PathRequest(path=self.resolve(path)))
        print(resp.message if resp.ok else f"rmdir: {resp.message}")

    def cmd_rm(self, path):
        resp = self.stub.Rm(dfsha_pb2.PathRequest(path=self.resolve(path)))
        print(resp.message if resp.ok else f"rm: {resp.message}")

    def cmd_send(self, local_path, remote_path=None):
        if not os.path.isfile(local_path):
            print(f"send: archivo local no existe: {local_path}")
            return
        remote_path = self.resolve(remote_path or posixpath.basename(local_path))

        def chunk_generator():
            # STREAMING: leemos el archivo en trozos de chunk_size y los
            # mandamos uno a uno, sin cargar el archivo completo en RAM.
            with open(local_path, "rb") as f:
                while True:
                    data = f.read(self.chunk_size)
                    if not data:
                        break
                    yield dfsha_pb2.FileChunk(path=remote_path, data=data)

        resp = self.stub.Send(chunk_generator())
        print(resp.message if resp.ok else f"send: {resp.message}")

    def cmd_recv(self, remote_path, local_path=None):
        remote_path = self.resolve(remote_path)
        local_path = local_path or posixpath.basename(remote_path)
        try:
            # STREAMING: escribimos cada chunk a disco a medida que llega
            # y vamos calculando el sha256 de forma incremental.
            hasher = hashlib.sha256()
            with open(local_path, "wb") as f:
                total = 0
                for chunk in self.stub.Receive(dfsha_pb2.PathRequest(path=remote_path)):
                    f.write(chunk.data)
                    hasher.update(chunk.data)
                    total += len(chunk.data)
            print(f"recv: {total} bytes guardados en {local_path} (sha256={hasher.hexdigest()[:12]}...)")
        except grpc.RpcError as e:
            print(f"recv: {e.details()}")

    def repl(self):
        print("DFSha client (ring). 'help' para comandos, 'exit' para salir.")
        while True:
            try:
                line = input(f"dfsha:{self.cwd}$ ").strip()
            except EOFError:
                break
            if not line:
                continue
            parts = line.split()
            cmd, args = parts[0], parts[1:]
            if cmd in ("exit", "quit"):
                break
            elif cmd == "help":
                print(__doc__)
            elif cmd == "ls":
                self.cmd_ls(args[0] if args else "")
            elif cmd == "cd":
                self.cmd_cd(args[0] if args else "/")
            elif cmd == "pwd":
                self.cmd_pwd()
            elif cmd == "mkdir":
                self.cmd_mkdir(args[0])
            elif cmd == "rmdir":
                self.cmd_rmdir(args[0])
            elif cmd == "rm":
                self.cmd_rm(args[0])
            elif cmd == "send":
                self.cmd_send(*args)
            elif cmd == "recv":
                self.cmd_recv(*args)
            else:
                print(f"comando desconocido: {cmd}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()
    try:
        chunk_size = chunk_size_bytes()
    except ConfigError as e:
        print(f"config invalida: {e}", file=sys.stderr)
        sys.exit(2)
    DFSClient(args.host, args.port, chunk_size).repl()
