import logging
import posixpath

import grpc

import dfsha_pb2
import dfsha_pb2_grpc
import ring_pb2
import ring_pb2_grpc

log = logging.getLogger("dfsha.service")


def normalize(path: str) -> str:
    if not path:
        return "/"
    p = posixpath.normpath("/" + path.lstrip("/"))
    return p


class RingServiceServicer(ring_pb2_grpc.RingServiceServicer):
    """Cara interna: lo que un peer expone a los demas peers."""

    def __init__(self, membership, local_store):
        self.membership = membership
        self.local_store = local_store

    def HealthCheck(self, request, context):
        self.membership.merge_known([p.address for p in request.known_peers])
        self.membership.merge_known([request.caller.address])
        return ring_pb2.HealthResponse(alive=True, known_peers=self.membership.known_peers_proto())

    def StoreBlob(self, request, context):
        try:
            self.local_store.put(request.key, request.data)
            return ring_pb2.StatusResponse(ok=True, message="ok")
        except Exception as e:
            log.exception("StoreBlob error")
            return ring_pb2.StatusResponse(ok=False, message=str(e))

    def GetBlob(self, request, context):
        data = self.local_store.get(request.key)
        if data is None:
            return ring_pb2.BlobResponse(found=False)
        return ring_pb2.BlobResponse(found=True, data=data)

    def DeleteBlob(self, request, context):
        self.local_store.delete(request.key)
        return ring_pb2.StatusResponse(ok=True, message="ok")


class DFSServiceServicer(dfsha_pb2_grpc.DFSServiceServicer):
    """Cara externa: lo que un peer expone a los clientes. Stateless:
    no guarda nada del cliente entre llamadas; cualquier peer del
    anillo puede atender cualquier request de cualquier cliente."""

    def __init__(self, coordinator):
        self.coord = coordinator

    def Ls(self, request, context):
        path = normalize(request.path)
        entries = self.coord.ls(path)
        if entries is None:
            return dfsha_pb2.LsResponse(ok=False, message="no es un directorio o no existe")
        proto_entries = [
            dfsha_pb2.DirEntry(name=n, is_dir=d, size_bytes=s) for n, d, s in entries
        ]
        return dfsha_pb2.LsResponse(ok=True, message="OK", entries=proto_entries)

    def Mkdir(self, request, context):
        ok, msg = self.coord.mkdir(normalize(request.path))
        return dfsha_pb2.StatusResponse(ok=ok, message=msg)

    def Rmdir(self, request, context):
        ok, msg = self.coord.rmdir(normalize(request.path))
        return dfsha_pb2.StatusResponse(ok=ok, message=msg)

    def Rm(self, request, context):
        ok, msg = self.coord.rm(normalize(request.path))
        return dfsha_pb2.StatusResponse(ok=ok, message=msg)

    def Send(self, request_iterator, context):
        path = None
        buf = bytearray()
        try:
            for chunk in request_iterator:
                if path is None:
                    path = normalize(chunk.path)
                buf.extend(chunk.data)
            if path is None:
                return dfsha_pb2.StatusResponse(ok=False, message="no se recibio ningun chunk")
            n_chunks = self.coord.store_file(path, bytes(buf))
            return dfsha_pb2.StatusResponse(
                ok=True, message=f"{len(buf)} bytes recibidos, particionados en {n_chunks} chunk(s)"
            )
        except Exception as e:
            log.exception("Send error")
            return dfsha_pb2.StatusResponse(ok=False, message=str(e))

    def Receive(self, request, context):
        path = normalize(request.path)
        try:
            gen = self.coord.read_file(path)
            if gen is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("archivo no encontrado")
                return
            for data in gen:
                yield dfsha_pb2.FileChunk(path=path, data=data)
        except Exception as e:
            log.exception("Receive error")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
