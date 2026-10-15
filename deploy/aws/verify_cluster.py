#!/usr/bin/env python3
"""
Verificacion post-despliegue de un cluster DFSha en AWS.

Comprueba, contra un peer cualquiera (el coordinador que elijas):
  1. ls / responde (el anillo esta vivo y la raiz existe).
  2. send de un archivo de prueba de tamaño configurable.
  3. recv del mismo archivo y verificacion de integridad por sha256.

Uso (desde una maquina con acceso al puerto 50051 del peer):
  PYTHONPATH=<raiz_repo> python deploy/aws/verify_cluster.py \
      --host <IP_PUBLICA_PEER> --port 50051 --size-mb 50
"""

import argparse
import hashlib
import os
import sys
import tempfile

from client.client import DFSClient
from common.config import chunk_size_bytes


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--size-mb", type=int, default=50)
    args = ap.parse_args()

    client = DFSClient(args.host, args.port, chunk_size_bytes())

    print(f"[1/3] ls / contra {args.host}:{args.port}")
    client.cmd_ls("/")

    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, "verify.bin")
        dst = os.path.join(tmp, "verify_out.bin")
        with open(src, "wb") as f:
            f.write(os.urandom(args.size_mb * 1024 * 1024))
        src_sha = sha256_of(src)

        print(f"[2/3] send de {args.size_mb} MB -> /verify.bin")
        client.cmd_send(src, "/verify.bin")

        print("[3/3] recv /verify.bin y verificacion de integridad")
        client.cmd_recv("/verify.bin", dst)
        dst_sha = sha256_of(dst)

        if src_sha == dst_sha:
            print(f"OK: integridad verificada (sha256={src_sha[:16]}...)")
            # limpieza del archivo de prueba en el cluster
            client.cmd_rm("/verify.bin")
            return 0
        print(f"FALLO: sha256 no coincide\n  enviado:  {src_sha}\n  recibido: {dst_sha}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
