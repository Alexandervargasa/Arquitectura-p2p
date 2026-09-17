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
proto/
  dfsha.proto        # API cliente <-> peer (Ls, Mkdir, Rmdir, Rm, Send, Receive)
  ring.proto         # API peer <-> peer (HealthCheck, StoreBlob, GetBlob, DeleteBlob)
common/
  hashing.py         # anillo de hashing consistente + nodos virtuales
peer/
  main.py            # entrypoint: arranca membresía + servidor gRPC (ambas caras)
  membership.py      # gossip / health-check / vista del anillo
  storage.py         # almacenamiento local respaldado en SQLite (clave -> blob)
  coordinator.py      # particionamiento en chunks, replicación, namespace (ls/mkdir/rm)
  service.py         # implementación de los servicios gRPC (DFSService y RingService)
client/
  client.py          # CLI (misma interfaz que el hito 1), se conecta a cualquier peer
docker/
  Dockerfile.peer
  Dockerfile.client
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
SELF_ADDRESS=localhost:60001 SEED_PEERS= REPLICATION_FACTOR=2 DATA_DIR=./data1 python -m peer.main
# en otra terminal
SELF_ADDRESS=localhost:60002 SEED_PEERS=localhost:60001 REPLICATION_FACTOR=2 DATA_DIR=./data2 python -m peer.main
# en otra terminal
SELF_ADDRESS=localhost:60003 SEED_PEERS=localhost:60001 REPLICATION_FACTOR=2 DATA_DIR=./data3 python -m peer.main
# cliente, apuntando a cualquiera de los tres
python client/client.py --host localhost --port 60001
```

Si editas los `.proto`, regenera los stubs con:

```bash
pip install -r requirements-dev.txt
python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dfsha.proto proto/ring.proto
```

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

- ✅ **Particionamiento** (RNF4): cada archivo se parte en chunks de 1 MB
  distribuidos entre distintos peers vía hashing consistente.
- ✅ **Replicación**: cada chunk/metadato/índice de directorio se
  guarda en `REPLICATION_FACTOR` peers.
- ✅ **Servidor stateless**: no hay afinidad cliente-peer; cualquier peer
  atiende cualquier request usando solo lo que viene en la request más
  la vista actual del anillo.
- ✅ **Reestructuración por componentes**: hashing, membresía, storage,
  coordinación y transporte gRPC están en módulos separados.
- ⚠️ **Pendiente / fuera de alcance todavía**: rebalanceo automático de
  datos cuando un peer se cae o se une (*hinted handoff* / *read
  repair*), consistencia fuerte entre réplicas (ahora mismo las
  escrituras son best-effort a los `rf` dueños, sin quorum ni
  versionado), autenticación/ACL/cifrado (RNF6).
