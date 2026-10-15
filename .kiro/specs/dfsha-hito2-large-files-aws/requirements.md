# Requirements Document

## Introduction

Este documento define los requerimientos del **Hito 2** del proyecto DFSha
(SI3007 Sistemas Distribuidos — Proyecto 1 — Opción 2 P2P): un sistema de
archivos distribuido sobre un **anillo de peers** con hashing consistente,
escrito en Python con gRPC y SQLite, desplegado con Docker.

El hito 1 ya entregó la arquitectura de anillo con particionamiento,
replicación y servidor stateless. Este hito agrega: (1) soporte real de
archivos grandes mediante *streaming* sin cargarlos completos en memoria,
(2) tamaño de chunk configurable por variable de entorno, (3) nodos
virtuales ponderados por capacidad del peer, (4) rebalanceo básico ante
cambios de membresía (estilo *hinted-handoff*), (5) reorganización limpia
del repositorio, y (6) despliegue en AWS con una instancia EC2 por peer y
membresía por gossip que escala de 4 a 10 nodos sobre direcciones reales.

El sistema conserva la decisión de diseño del hito 1: **no existe un nodo
central de metadatos** (a diferencia del ejemplo HDFS/NameNode del
enunciado). Los metadatos, los índices de directorio y los chunks se
distribuyen y replican en el anillo. Esta elección se justifica en el
informe.

### Trazabilidad con el enunciado

Este hito respeta y refina los requerimientos del enunciado del curso:

- **RF1** — gestión del sistema de archivos (ls, cd, mkdir, rmdir, rm).
- **RF2** — transferencia de archivos (send/receive).
- **RNF1** — escalabilidad en número de usuarios, archivos y **tamaño de archivo**.
- **RNF2** — alta disponibilidad / redundancia.
- **RNF3** — consistencia de datos.
- **RNF4** — particionamiento de archivos entre múltiples nodos.
- **RNF5** — rendimiento / concurrencia.
- **RNF6** — seguridad (cifrado + control de acceso): **diferido al hito 3, fuera de alcance** (ver Requerimiento 8).
- **RNF7** — transparencia de acceso / ubicación.
- **Infra** — ejecución en AWS (IaaS, VMs), cada nodo en Docker, nodos en máquinas distintas comunicándose por Internet.

## Glossary

- **DFSha**: el sistema de archivos distribuido P2P objeto de este proyecto.
- **Peer**: proceso nodo del anillo; expone una cara interna (RingService) y una externa (DFSService).
- **Coordinador**: el peer que recibe una solicitud de cliente y la ejecuta; cualquier peer puede actuar de coordinador (servidor stateless).
- **Cliente**: proceso CLI que se conecta a cualquier peer; no conoce chunks ni replicación.
- **Chunk**: fragmento de tamaño fijo en que se parte un archivo para distribuirlo.
- **Chunk_Size**: tamaño configurado de cada chunk, en bytes, derivado de la variable de entorno `CHUNK_SIZE_MB`.
- **Blob**: unidad de almacenamiento genérica del anillo (un chunk, un metadato de archivo o un índice de directorio), identificada por una clave lógica (`key`).
- **Key**: clave lógica de un blob (p. ej. `chunk::<path>::<i>`, `file::<path>`, `dir::<path>`).
- **Ring**: anillo de hashing consistente de 160 bits (espacio SHA-1); mapea peers y keys a un entero y determina dueños.
- **Virtual_Node**: punto que un peer físico aporta al anillo para balancear carga; un peer aporta varios nodos virtuales.
- **Peer_Weight**: peso de capacidad de un peer, derivado de la variable de entorno `PEER_WEIGHT`; determina cuántos nodos virtuales aporta.
- **Replication_Factor**: número de peers distintos que guardan copia de cada blob, derivado de la variable de entorno `REPLICATION_FACTOR`.
- **Owners**: lista de peers dueños de una key, calculada por el Ring (dueño primario más sucesores hasta `Replication_Factor`).
- **Membership**: componente que mantiene la vista de peers vivos del anillo mediante gossip / health-check.
- **Rebalanceo**: proceso por el cual, tras un cambio de membresía, cada peer transfiere a los nuevos dueños las keys locales que ya no le corresponden y luego las elimina localmente.
- **Gossip**: intercambio periódico entre peers de listas de peers conocidos y de metadatos de membresía (incluido `Peer_Weight`).
- **EC2_Instance**: máquina virtual de AWS que hospeda exactamente un peer.
- **Enunciado**: documento del curso que define RF1–RF2 y RNF1–RNF7.

## Requirements

### Requirement 1: Soporte de archivos grandes mediante streaming

**User Story:** Como estudiante que opera DFSha, quiero enviar y recibir archivos grandes sin que el peer coordinador los cargue completos en memoria, para que el sistema escale en tamaño de archivo (RNF1) sin agotar la RAM del nodo.

#### Acceptance Criteria

1. WHEN un cliente ejecuta `send` de un archivo, THE Coordinador SHALL procesar el archivo en chunks de tamaño `Chunk_Size` de forma secuencial, sin acumular en memoria el contenido completo del archivo.
2. WHEN un cliente ejecuta `recv` de un archivo, THE Coordinador SHALL transmitir el archivo en chunks de tamaño `Chunk_Size` de forma secuencial, sin acumular en memoria el contenido completo del archivo.
3. WHILE una operación `send` o `recv` está en curso, THE Coordinador SHALL mantener residente en memoria a lo sumo 2 chunks simultáneamente (2 × Chunk_Size), de forma independiente del tamaño total del archivo.
4. WHEN un cliente ejecuta `send` de un archivo, THE Cliente SHALL leer el archivo local y transmitirlo en chunks de tamaño `Chunk_Size` de forma secuencial, manteniendo residente en memoria a lo sumo 1 chunk a la vez.
5. WHEN un cliente ejecuta `recv` de un archivo, THE Cliente SHALL escribir el archivo local chunk por chunk a medida que recibe cada chunk, manteniendo residente en memoria a lo sumo 1 chunk a la vez.
6. WHEN un archivo de N bytes se envía y luego se recibe en una ruta destino, THE DFSha SHALL producir un archivo recibido de exactamente N bytes cuya secuencia de bytes es idéntica, posición por posición, a la del archivo original.
7. IF un chunk no puede escribirse en ningún peer dueño vivo durante `send`, THEN THE Coordinador SHALL abortar la operación, no registrar los metadatos del archivo como completos, y devolver una respuesta de error que indica el índice del chunk que falló.
8. IF un chunk solicitado no está disponible en ningún peer dueño vivo durante `recv`, THEN THE Coordinador SHALL terminar la transmisión antes de completarla y devolver un error que indica el índice del chunk no disponible.

### Requirement 2: Tamaño de chunk configurable por variable de entorno

**User Story:** Como estudiante que mide rendimiento, quiero configurar el tamaño de chunk por variable de entorno en peer y cliente, para que pueda comparar distintos tamaños rápidamente sin recompilar.

#### Acceptance Criteria

1. WHEN un Peer arranca, THE Peer SHALL derivar `Chunk_Size` de la variable de entorno `CHUNK_SIZE_MB` interpretada como megabytes (1 MB = 1.048.576 bytes).
2. WHEN un Cliente arranca, THE Cliente SHALL derivar `Chunk_Size` de la variable de entorno `CHUNK_SIZE_MB` interpretada como megabytes.
3. IF la variable de entorno `CHUNK_SIZE_MB` no está definida, THEN THE Peer SHALL usar un `Chunk_Size` por defecto de 8 megabytes.
4. IF la variable de entorno `CHUNK_SIZE_MB` no está definida, THEN THE Cliente SHALL usar un `Chunk_Size` por defecto de 8 megabytes.
5. WHEN un Peer parte un archivo durante `send`, THE Peer SHALL usar `Chunk_Size` como tamaño de cada chunk salvo el último, que contiene el residuo.
6. IF la variable de entorno `CHUNK_SIZE_MB` tiene un valor que no es un entero positivo, THEN THE Peer SHALL rechazar el arranque con un mensaje de error que identifica la variable inválida.
7. IF la variable de entorno `CHUNK_SIZE_MB` tiene un valor que no es un entero positivo, THEN THE Cliente SHALL rechazar el arranque con un mensaje de error que identifica la variable inválida.

### Requirement 3: Nodos virtuales ponderados por capacidad

**User Story:** Como operador de un cluster heterogéneo, quiero que cada peer aporte nodos virtuales en proporción a su capacidad configurada, para que las máquinas más grandes reciban proporcionalmente más chunks sin introducir un índice central.

#### Acceptance Criteria

1. WHEN un Peer arranca, THE Peer SHALL derivar `Peer_Weight` de la variable de entorno `PEER_WEIGHT`, interpretándola como un entero positivo.
2. IF la variable de entorno `PEER_WEIGHT` no está definida, THEN THE Peer SHALL usar un `Peer_Weight` por defecto de 1.
3. IF la variable de entorno `PEER_WEIGHT` está definida pero no es un entero positivo, THEN THE Peer SHALL rechazar el arranque y registrar un mensaje de error que indique el valor inválido de `PEER_WEIGHT`.
4. WHEN se construye el Ring, THE Ring SHALL asignar a cada Peer un número de Virtual_Node igual a los nodos virtuales base multiplicados por el `Peer_Weight` de ese Peer.
5. WHEN dos Peer tienen el mismo conjunto de miembros vivos y los mismos `Peer_Weight`, THE Ring SHALL calcular los mismos Owners para una misma Key en todos los Peer, de forma determinista y sin consultar ningún índice central.
6. THE Membership SHALL propagar el `Peer_Weight` de cada Peer a los demás Peer mediante Gossip en cada ronda de HealthCheck.
7. WHERE los Peer tienen distinto `Peer_Weight`, THE Ring SHALL asignar a cada Peer una fracción de las Key que, medida sobre una muestra de al menos 10000 Key, no se desvíe en más de 5 puntos porcentuales de la fracción esperada (el `Peer_Weight` del Peer dividido por la suma de los `Peer_Weight` de todos los Peer vivos).
8. IF un Peer no recibe por Gossip el `Peer_Weight` de otro Peer, THEN THE Ring SHALL asumir para ese Peer el `Peer_Weight` por defecto de 1.

### Requirement 4: Rebalanceo básico ante cambios de membresía

**User Story:** Como operador que escala el cluster, quiero que al unirse o salir peers el sistema redistribuya los datos a sus nuevos dueños sin perder información, para que escalar de 4 a 10 peers funcione sin pérdida de datos.

#### Acceptance Criteria

1. WHEN la vista de miembros vivos de un Peer cambia, THE Peer SHALL recalcular los Owners de cada Key almacenada localmente.
2. WHILE existan Key locales cuyos Owners recalculados ya no incluyen al Peer, THE Peer SHALL transferir cada una de esas Key a los Owners que aún no la poseen.
3. WHEN un Peer transfiere una Key a sus nuevos Owners con éxito, THE Peer SHALL eliminar esa Key de su almacenamiento local.
4. IF la transferencia de una Key a un nuevo Owner falla, THEN THE Peer SHALL conservar esa Key en su almacenamiento local y no eliminarla.
5. WHEN el cluster escala de 4 a 10 Peer, THE DFSha SHALL conservar todos los archivos y directorios previamente almacenados sin pérdida de datos.
6. WHEN un Peer sigue siendo Owner de una Key tras el cambio de membresía, THE Peer SHALL conservar esa Key en su almacenamiento local.
7. WHILE un rebalanceo está en curso, THE Coordinador SHALL seguir atendiendo solicitudes de cliente de lectura y escritura.

### Requirement 5: Reorganización limpia del repositorio

**User Story:** Como mantenedor del proyecto, quiero los stubs gRPC generados en un paquete propio y cada Dockerfile junto a su componente, para que el despliegue y los imports sean más claros.

#### Acceptance Criteria

1. THE DFSha SHALL ubicar los stubs gRPC generados dentro del paquete `common/proto/`.
2. WHEN un módulo del proyecto importa los stubs gRPC, THE módulo SHALL importarlos mediante imports de estilo paquete desde `common/proto/`.
3. THE DFSha SHALL ubicar el Dockerfile del peer en `peer/Dockerfile`.
4. THE DFSha SHALL ubicar el Dockerfile del cliente en `client/Dockerfile`.
5. WHEN se construyen las imágenes del peer y del cliente tras la reorganización, THE DFSha SHALL producir imágenes que arrancan peer y cliente sin errores de importación.
6. WHEN se ejecuta la suite de operaciones de cliente (ls, mkdir, rmdir, rm, send, recv) tras la reorganización, THE DFSha SHALL completar cada operación con el mismo resultado observable que antes de la reorganización.

### Requirement 6: Despliegue en AWS con una instancia por peer

**User Story:** Como estudiante con una cuenta personal de capa gratuita de AWS, quiero desplegar un peer por instancia EC2 con membresía por gossip sobre direcciones reales, para que el cluster escale de 4 a 10 nodos en máquinas distintas comunicándose por Internet.

#### Acceptance Criteria

1. THE DFSha SHALL hospedar exactamente un Peer por EC2_Instance.
2. WHEN un Peer arranca en una EC2_Instance, THE Peer SHALL identificarse ante los demás Peer con una dirección de la forma IP:puerto en vez de un nombre de servicio de Docker.
3. WHEN un Peer nuevo arranca conociendo al menos un Peer semilla alcanzable, THE Membership SHALL integrar el Peer nuevo a la vista del anillo mediante Gossip, sin reiniciar los demás Peer.
4. IF el Peer semilla configurado no es alcanzable al arrancar, THEN THE Peer SHALL registrar el error, reintentar periódicamente, y no bloquear a los demás Peer del cluster.
5. WHEN el cluster escala de 4 a 10 EC2_Instance, THE Membership SHALL reflejar los 10 Peer vivos en la vista del anillo de cada Peer tras converger el Gossip.
6. WHEN un Peer deja de responder a los health-check, THE Membership SHALL retirarlo de la vista del anillo activo de los demás Peer.
7. WHERE una acción de despliegue crea o modifica recursos en la nube de AWS, THE DFSha SHALL requerir confirmación explícita del usuario y no ejecutar la acción hasta recibirla.
8. THE DFSha SHALL proveer scripts o archivos de composición para arrancar un único Peer por EC2_Instance.
9. THE DFSha SHALL proveer una guía de despliegue en AWS que describe los pasos para levantar el cluster de 4 a 10 Peer.
10. WHEN un Peer corre en una EC2_Instance, THE Peer SHALL ejecutarse dentro de un contenedor Docker.

### Requirement 7: Transparencia de acceso y ubicación (RNF7)

**User Story:** Como cliente de DFSha, quiero conectarme a cualquier peer y operar sobre el sistema de archivos sin conocer dónde reside cada chunk, para que el acceso sea transparente a la ubicación.

#### Acceptance Criteria

1. WHEN un Cliente se conecta a cualquier Peer vivo del anillo, THE DFSha SHALL atender las operaciones del Cliente (ls, cd, mkdir, rmdir, rm, send, recv) con el mismo resultado observable independientemente del Peer elegido.
2. THE Cliente SHALL operar sobre rutas lógicas sin referenciar la ubicación física de los chunks ni de las réplicas.
3. WHEN un Peer dueño de una réplica de un chunk está caído durante `recv`, THE Coordinador SHALL servir el chunk desde otro Owner vivo sin intervención del Cliente.

### Requirement 8: Alcance diferido al hito 3 (fuera de alcance)

**User Story:** Como responsable del proyecto, quiero registrar explícitamente qué capacidades quedan fuera de este hito, para que el alcance del hito 2 sea inequívoco y no se evalúen funciones no comprometidas.

#### Acceptance Criteria

1. THE DFSha SHALL documentar como diferida al hito 3 la consistencia fuerte con quórum y versionado de réplicas.
2. THE DFSha SHALL documentar como diferida al hito 3 la reparación en lectura (*read-repair*).
3. THE DFSha SHALL documentar como diferido al hito 3 el cifrado de datos y el transporte seguro TLS.
4. THE DFSha SHALL documentar como diferido al hito 3 el control de acceso y la autenticación (RNF6).
5. THE DFSha SHALL documentar en el informe la justificación de la ausencia de un nodo central de metadatos frente al ejemplo HDFS/NameNode del Enunciado.
