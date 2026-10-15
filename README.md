# DFSha — Hito 2: arquitectura de anillo (hashing consistente, replicación, particionamiento)

**SI3007 Sistemas Distribuidos — Proyecto 1 — Opción 2 (P2P)**

Se desmontó la versión monolítica del hito 1 y se reestructuró el
proyecto por componentes: ahora el servicio lo presta un **anillo de
peers**, cada uno con su propio almacenamiento local en **SQLite**. No
hay un servidor central: cualquier peer puede atender a cualquier
cliente (servidor **stateless**), y cada archivo se **particiona en
chunks** y se **replica** entre varios peers usando **hashing
consistente**.

---

## 1. Arquitectura

### 1.1 Topología: anillo de hashing consistente

Cada peer tiene un id (hash SHA-1 de su dirección `host:puerto`) que lo
ubica en un anillo de 160 bits. Cada clave lógica (un chunk de archivo,
los metadatos de un archivo, o el índice de un directorio) también se
hashea a ese mismo espacio. El **dueño primario** de una clave es el
primer peer encontrado avanzando en sentido horario desde la posición
de la clave; los siguientes `N-1` peers en el anillo guardan las
réplicas (factor de replicación configurable).

Para balancear bien la carga entre peers se usan **nodos virtuales**
(100 por peer físico): en vez de que cada peer aporte un solo punto al
anillo, aporta 100, lo que reparte las claves de forma mucho más
uniforme (técnica estándar de sistemas tipo Dynamo/Cassandra).

```
        anillo de hashing consistente (con nodos virtuales)

              peer1 ●───● peer3
                   ╱     ╲
                  ●       ●
                 ╱         ╲
              peer4 ●─────● peer2

   chunk::/docs/a.bin::0  -> owners_for() -> [peer3, peer4]  (rf=2)
   chunk::/docs/a.bin::1  -> owners_for() -> [peer1, peer2]  (rf=2)
   file::/docs/a.bin      -> owners_for() -> [peer2, peer3]  (rf=2)
```

### 1.2 Membresía dinámica (gossip)

No hay un registro central de peers. Cada peer arranca con al menos un
**peer semilla** conocido (o ninguno, si es el primero del cluster).
Cada pocos segundos hace un `HealthCheck` a todos los peers que conoce;
en cada respuesta intercambia listas de peers conocidos
(anti-entropía). Así:

- Un peer nuevo solo necesita conocer **una** semilla para integrarse.
- Un peer caído deja de responder al `HealthCheck` y se excluye del
  anillo activo (sin tener que reiniciar a los demás).

### 1.3 Componentes del proyecto

```
common/
  hashing.py         # anillo de hashing consistente + nodos virtuales PONDERADOS
  config.py          # parseo/validacion de env vars (CHUNK_SIZE_MB, PEER_WEIGHT)
  proto/
    dfsha.proto      # API cliente <-> peer (Ls, Mkdir, Rmdir, Rm, Send, Receive)
    ring.proto       # API peer <-> peer (HealthCheck, StoreBlob, GetBlob, DeleteBlob)
    *_pb2*.py        # stubs gRPC generados (imports de estilo paquete)
peer/
  main.py            # entrypoint: arranca membresía + servidor gRPC (ambas caras)
  membership.py      # gossip / health-check / vista del anillo (propaga PEER_WEIGHT)
  storage.py         # almacenamiento local respaldado en SQLite (clave -> blob)
  coordinator.py     # particionamiento STREAMING, replicación, rebalanceo, namespace
  service.py         # implementación de los servicios gRPC (DFSService y RingService)
  Dockerfile         # imagen del peer (junto a su código)
client/
  client.py          # CLI (misma interfaz que el hito 1), se conecta a cualquier peer
  Dockerfile         # imagen del cliente (junto a su código)
tests/               # pytest: hashing ponderado, config, streaming, rebalanceo
scripts/
  gen_proto.sh       # regenera los stubs gRPC en common/proto/
deploy/aws/          # despliegue en AWS (una EC2 por peer): guía, compose, user-data
docker-compose.yml   # 4 peers + 1 cliente, en red Docker interna
```

| Componente | Responsabilidad |
|---|---|
| `common/hashing.py` | Calcula qué peer(s) son dueños de una clave (particionamiento + replicación). |
| `peer/membership.py` | Mantiene la vista de "quién está vivo" en el anillo (gossip). |
| `peer/storage.py` | Persistencia local de cada peer, en SQLite. |
| `peer/coordinator.py` | Lógica de negocio: parte archivos en chunks, decide dónde van, arma/desarma directorios. Cualquier peer la ejecuta al recibir una request — de ahí que el servidor sea *stateless*: no hay afinidad de sesión cliente-peer. |
| `peer/service.py` | Traduce gRPC <-> llamadas al coordinador / storage. |
| `client/client.py` | Shell interactivo; no sabe nada de chunks ni replicación, para el cliente sigue siendo un DFS normal. |

### 1.4 Cómo se resuelve cada operación

- **`mkdir` / `rmdir` / `rm` / `ls`**: cada directorio tiene un "índice"
  (`dir::<path>`) con sus entradas (nombre + tipo), tratado como un blob
  más del anillo (se ubica y replica igual que un chunk).
- **`send <archivo>`**: el peer que recibe la llamada actúa de
  coordinador: parte el archivo en chunks de 1 MB (`chunk::<path>::<i>`),
  calcula sus dueños con hashing consistente, los replica, guarda los
  metadatos (`file::<path>`) y actualiza el índice del directorio padre.
- **`recv <archivo>`**: el coordinador lee los metadatos, y para cada
  chunk intenta con todos sus dueños hasta encontrar uno vivo — por eso
  el sistema sigue funcionando aunque un peer con una réplica esté
  caído.

---

## 2. Cómo ejecutar

### 2.1 Con Docker Compose (recomendado)

Requiere Docker Desktop corriendo.

```bash
docker compose up --build -d      # levanta 4 peers (peer1..peer4) en red interna
docker compose run --rm client    # shell interactivo, conectado a peer1
```

- `./local-files` (junto al `docker-compose.yml`) se monta como `/work`
  dentro del cliente: ahí van los archivos para `send`/`recv`.
- El factor de replicación se configura por variable de entorno
  `REPLICATION_FACTOR` en `docker-compose.yml` (default 2).
- Puedes apuntar el cliente a cualquier otro peer para comprobar que da
  igual a cuál te conectes: `docker compose run --rm client --host peer3 --port 50051`.
- Para simular la caída de un nodo: `docker stop dfsha-peer3` (y
  comprobar que `ls`/`recv` siguen funcionando contra los demás).

### 2.2 Local (sin Docker), para desarrollo

```bash
pip install -r requirements.txt

export PYTHONPATH=.
SELF_ADDRESS=localhost:60001 SEED_PEERS= REPLICATION_FACTOR=2 CHUNK_SIZE_MB=8 PEER_WEIGHT=1 DATA_DIR=./data1 python -m peer.main
# en otra terminal
SELF_ADDRESS=localhost:60002 SEED_PEERS=localhost:60001 REPLICATION_FACTOR=2 DATA_DIR=./data2 python -m peer.main
# en otra terminal
SELF_ADDRESS=localhost:60003 SEED_PEERS=localhost:60001 REPLICATION_FACTOR=2 DATA_DIR=./data3 python -m peer.main
# cliente, apuntando a cualquiera de los tres
python client/client.py --host localhost --port 60001
```

Variables de entorno nuevas en este hito:

- `CHUNK_SIZE_MB` (default 8): tamaño de chunk en MB, en peer **y** cliente.
  Útil para benchmarking (prueba 8, 10, 16...). El coordinador NO carga el
  archivo completo en RAM: lo procesa chunk por chunk (streaming).
- `PEER_WEIGHT` (default 1): peso de capacidad del peer. Un peer de peso 3
  aporta 3× nodos virtuales y recibe ~3× chunks (nodos virtuales ponderados).

### 2.3 Tests

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. pytest
```

Cubre: hashing ponderado y determinista, parseo de config, particionamiento
por streaming con round-trip íntegro, y rebalanceo sin pérdida de datos.

### 2.4 Regenerar stubs gRPC

Si editas los `.proto` (en `common/proto/`), regenera los stubs con:

```bash
pip install -r requirements-dev.txt
./scripts/gen_proto.sh
```

### 2.5 Despliegue en AWS

Una instancia EC2 por peer, escalando de 4 a 10 nodos. Ver la guía completa
en [`deploy/aws/README.md`](deploy/aws/README.md).

---

## 3. Comandos del cliente

```
ls [path]
cd <path>
pwd
mkdir <path>
rmdir <path>
rm <path>
send <archivo_local> [ruta_remota]
recv <ruta_remota> [archivo_local]
exit
```

---

## 4. Estado de los requerimientos cubiertos en este hito

- ✅ **Archivos grandes por streaming** (RNF1): `send`/`recv` procesan el
  archivo chunk por chunk; el coordinador nunca mantiene el archivo
  completo en memoria. Verificado end-to-end con un archivo de 50 MB
  (round-trip con sha256 idéntico).
- ✅ **Tamaño de chunk configurable** (`CHUNK_SIZE_MB`, default 8 MB) en
  peer y cliente, para benchmarking.
- ✅ **Particionamiento** (RNF4): cada archivo se parte en chunks
  distribuidos entre distintos peers vía hashing consistente.
- ✅ **Nodos virtuales ponderados** (`PEER_WEIGHT`): una máquina más grande
  recibe proporcionalmente más chunks, de forma determinista y sin índice
  central.
- ✅ **Rebalanceo ante cambios de membresía**: al unirse/salir peers, cada
  peer empuja a sus nuevos dueños las claves que ya no le corresponden
  (estilo *hinted handoff*) y las borra localmente solo si el empuje tuvo
  éxito. Verificado escalando un cluster de 3 → 5 peers sin pérdida de
  datos (archivo recuperable incluso desde un peer nuevo).
- ✅ **Replicación**: cada chunk/metadato/índice de directorio se
  guarda en `REPLICATION_FACTOR` peers.
- ✅ **Servidor stateless**: no hay afinidad cliente-peer; cualquier peer
  atiende cualquier request usando solo lo que viene en la request más
  la vista actual del anillo.
- ✅ **Reestructuración por componentes**: stubs gRPC en el paquete
  `common/proto/`, cada Dockerfile junto a su componente, suite de tests.
- ✅ **Despliegue en AWS** (una EC2 por peer): guía, compose de un solo
  peer, user-data y script de verificación en `deploy/aws/`.
- ⚠️ **Diferido al hito 3**: consistencia fuerte con quórum/versionado,
  *read repair*, y seguridad (cifrado/TLS, autenticación, ACL — RNF6).

> **Nota de diseño (para el informe):** a diferencia del ejemplo HDFS del
> enunciado (NameNode central), DFSha **no tiene nodo central de
> metadatos**: los metadatos de archivo, los índices de directorio y los
> chunks se distribuyen y replican por el anillo igual que cualquier blob.
> Es una decisión deliberada de la arquitectura P2P (opción 2).
