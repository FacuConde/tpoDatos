# Justificación de Tecnologías por Caso de Uso

## Introducción

Este documento detalla las decisiones técnicas detrás de la elección de cada motor de base de datos para los distintos casos de uso del sistema FIFA World Cup.

### Arquitectura ETL y Modelo Polyglot Persistence

**Principio fundamental**: PostgreSQL (Supabase) actúa como **fuente de verdad** (source of truth) del sistema. Todas las bases de datos NoSQL se alimentan mediante procesos ETL que transforman y optimizan los datos para consultas específicas.

**Estado actual de la implementación**:
- **Implementación académica**: Actualmente, el ETL se ejecuta **cada vez que se invoca un caso de uso** (ETL on-demand). Esto significa que cada consulta dispara una lectura de PostgreSQL, transformación de datos e inserción en la base NoSQL correspondiente.
- **Por qué funciona para el prototipo**: Con volúmenes de datos pequeños (2 ediciones, ~64 partidos), esta estrategia es viable y permite demostrar el flujo completo de datos.

**Diseño orientado a escalabilidad futura**:

El modelo fue diseñado pensando en un escenario de **producción real** donde:

1. **ETL asíncrono y programado**: El proceso de sincronización se ejecutaría en intervalos fijos (cada 15 minutos, cada hora) o mediante **triggers basados en eventos** (fin de partido, cambio de fase), desacoplando completamente las escrituras de las consultas.

2. **Bases NoSQL como read replicas especializadas**: PostgreSQL maneja writes transaccionales con integridad referencial, mientras que Cassandra/MongoDB/Neo4j responden queries de lectura sin tocar PostgreSQL.

3. **Escalabilidad horizontal**: Con millones de usuarios concurrentes durante un mundial activo:
   - PostgreSQL se dedicaría solo a escrituras críticas (insertar resultados de partidos).
   - Las consultas de usuarios (millones de requests/segundo) se distribuirían entre nodos de Cassandra/MongoDB replicados geográficamente.

4. **Reducción de latencia**: Consultas que en PostgreSQL requieren múltiples JOINs y agregaciones (50-150ms), en las bases NoSQL se resuelven con lecturas directas (<5ms) porque los datos ya están pre-calculados y denormalizados.

**Justificación académica del enfoque**:
- **Sí, técnicamente todos los casos (excepto el 7) se pueden resolver con PostgreSQL**.
- **Sin embargo**, este proyecto demuestra comprensión de:
  * **Polyglot Persistence**: Elegir la herramienta correcta para cada patrón de consulta.
  * **Trade-offs arquitectónicos**: Aceptamos complejidad operacional (mantener 5 bases de datos) a cambio de performance bajo alta carga.
  * **Diseño escalable**: Aunque la implementación actual es simplificada, el modelo está preparado para evolucionar a un sistema distribuido real solo cambiando **cuándo** se ejecuta el ETL (de on-demand a scheduled).

**El trade-off aceptado**: 
- **Consistencia eventual**: Los datos NoSQL pueden estar levemente desactualizados respecto a PostgreSQL.
- **Complejidad operacional**: Mantener y sincronizar 5 bases de datos vs una sola.
- **Ventaja obtenida**: Sistema preparado para escalar a millones de usuarios sin rediseño arquitectónico, solo ajustando la frecuencia del ETL.

---

## Caso de Uso 1: Tabla de Posiciones de un Grupo
**Tecnología elegida:** Cassandra

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede calcular la tabla de posiciones con una consulta que incluya JOINs entre `partido`, `estadisticas_equipo` y `equipo`, agregaciones con `SUM()` y `COUNT()`, y ordenamiento por puntos y diferencia de goles.

### ¿Por qué elegimos Cassandra?
**En un escenario de producción escalable**, la tabla de posiciones sería uno de los endpoints **más consultados** durante un mundial (millones de requests concurrentes cada vez que finaliza un partido). Cassandra ofrece:

- **Partition Key eficiente**: `(mundial, grupo)` garantiza que todas las posiciones de un grupo están en el mismo nodo físico, permitiendo lecturas locales sin queries distribuidas.

- **Clustering Order automático**: Con `posicion` como clustering column con orden `ASC`, Cassandra **mantiene los datos físicamente ordenados en disco**. La query es una simple lectura secuencial, sin necesidad de `ORDER BY` en tiempo de ejecución.

- **Escrituras rápidas del ETL**: Cuando finaliza un partido, el ETL calcula las nuevas posiciones (con la lógica de JOINs y agregaciones en PostgreSQL) y actualiza las 4-5 filas del grupo en Cassandra en milisegundos.

- **Escalabilidad horizontal**: Si el sistema crece a tener millones de usuarios concurrentes, Cassandra permite agregar nodos y distribuir las particiones (grupos) geográficamente, acercando los datos a los usuarios.

**Ventaja demostrada**: Eliminamos JOINs y agregaciones costosas en tiempo de consulta, materializando los resultados en un modelo denormalizado optimizado para lectura.

---

## Caso de Uso 2: Árbitros de Partidos en Fases Finales
**Tecnología elegida:** MongoDB

### ¿Se puede hacer en PostgreSQL?
Sí. De hecho, **tenemos la tabla `arbitro_partido` en PostgreSQL** que relaciona partidos con árbitros y sus roles. La consulta requeriría JOINs entre `partido`, `equipo` (dos veces, para local y visitante), `arbitro_partido` y `arbitro`.

### ¿Por qué elegimos MongoDB?
**En un escenario de producción escalable**, este caso de uso demuestra el poder de los **documentos anidados** para eliminar JOINs:

- **Modelo de documento natural**: Cada partido es un documento completo con toda su información (equipos, fase, estadio) y un array de árbitros con sus roles:
```json
{
  "id_partido": 1,
  "fase": "Final",
  "local": "Argentina",
  "visitante": "Brasil", 
  "arbitros": [
    {"nombre": "Pierluigi Collina", "rol": "Principal"},
    {"nombre": "Juan Pérez", "rol": "Asistente 1"}
  ]
}
```

- **Sin JOINs en tiempo de consulta**: El ETL ejecuta los JOINs complejos **una vez** al crear/actualizar el documento. Las consultas de usuarios obtienen el documento completo en una sola operación de lectura.

- **Flexibilidad de esquema**: Los roles de árbitros pueden evolucionar entre ediciones (por ejemplo, incorporación del VAR en ediciones recientes). MongoDB permite agregar nuevos roles sin migraciones de esquema.

- **Índice compuesto eficiente**: Con `{id_mundial: 1, fase: 1}`, filtramos millones de documentos rápidamente para obtener solo partidos de fases eliminatorias.

**Ventaja demostrada**: La tabla `arbitro_partido` sigue siendo la fuente de verdad en PostgreSQL con integridad referencial. MongoDB actúa como una **vista materializada** optimizada donde los datos ya están pre-combinados para consultas de solo lectura.

---

## Caso de Uso 3: Jugadores con Mínimo de Goles
**Tecnología elegida:** MongoDB

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede hacer JOINs entre `jugador`, `estadisticas_jugador` y `partido`, agregar con `SUM(goles)` y filtrar con `HAVING`. Requeriría múltiples queries para obtener jugadores de varias selecciones.

### ¿Por qué elegimos MongoDB?
**En un escenario de producción escalable**, este caso demuestra el poder de **arrays anidados complejos**:

- **Documento por selección-mundial**: Cada documento contiene toda la información de jugadores de una selección en una edición específica:
```json
{
  "id_mundial": 1,
  "seleccion": "Argentina",
  "jugadores": [
    {"nombre": "Messi", "goles": 6, "asistencias": 3, "partidos": 7},
    {"nombre": "Di María", "goles": 2, "asistencias": 5, "partidos": 6}
  ]
}
```

- **Lectura de documento completo**: En lugar de múltiples queries a PostgreSQL con JOINs, obtenemos toda la información en una sola operación. El filtro por cantidad mínima de goles se hace en memoria (trivial con ~25 jugadores).

- **Upsert atómico**: El ETL usa `replace_one(upsert=True)` para garantizar un único documento por selección-mundial, evitando duplicados sin transacciones complejas.

- **Proyecciones flexibles**: Puedo traer solo campos específicos del array (`jugadores.nombre`, `jugadores.goles`) si necesito reducir el payload de red.

**Ventaja demostrada**: Eliminamos JOINs complejos materializando las estadísticas agregadas en documentos que reflejan naturalmente la estructura lógica del dominio.

---

## Caso de Uso 4: Partidos por Popularidad
**Tecnología elegida:** Cassandra

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede hacer JOINs entre `partido`, `equipo` (dos veces) y `estadio`, y ordenar por `estadio.capacidad_total` usando `ORDER BY`.

### ¿Por qué elegimos Cassandra?
**En un escenario de producción escalable**, este caso demuestra la ventaja del **ordenamiento físico** en Cassandra:

- **Clustering Order descendente**: Con `CLUSTERING ORDER BY (popularidad DESC)`, Cassandra almacena los partidos **físicamente ordenados en disco** de mayor a menor popularidad. La query es una **lectura secuencial sin sort en tiempo de ejecución**.

- **Hot data optimization**: Los partidos más populares (que se consultan más frecuentemente) están al inicio de la partición, mejorando el hit rate del caché.

- **Write-once, read-many**: La popularidad (asistencia del estadio) no cambia después del partido, alineándose perfectamente con el modelo inmutable de Cassandra.

- **Partition key local**: `(mundial, grupo)` garantiza que todos los partidos de un grupo están en el mismo nodo, sin necesidad de queries distribuidas.

**Ventaja demostrada**: En PostgreSQL, cada query requiere un `ORDER BY` explícito que puede tomar 30-50ms con índices. En Cassandra, los datos ya están ordenados físicamente, reduciendo la latencia a <5ms.

---

## Caso de Uso 5: Partidos por Fecha y Estadio
**Tecnología elegida:** Cassandra

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede filtrar con `WHERE estadio.nombre = ? AND DATE(partido.fecha_hora) = ?` usando índices compuestos en `(id_estadio, fecha_hora)`.

### ¿Por qué elegimos Cassandra?
**En un escenario de producción escalable**, este caso demuestra la potencia de las **Compound Partition Keys**:

- **Partition key (estadio, fecha)**: Esta combinación hace que queries como "todos los partidos en el Estadio Lusail el 18 de diciembre" sean **lookups O(1)**, no table scans. Todos los partidos de ese estadio en esa fecha están físicamente juntos en el mismo nodo.

- **Eliminación de JOINs**: Los nombres de equipos y estadio están denormalizados en la tabla, evitando JOINs con las tablas `equipo` y `estadio`.

- **Time-series pattern**: Los partidos son datos temporales inmutables (no cambian después de jugarse). Cassandra con su arquitectura LSM-tree está optimizado para este tipo de workload.

- **Locality de datos**: Si un usuario consulta todos los partidos de un estadio en fechas consecutivas, esos datos están cerca en el mismo nodo, mejorando caché y reduciendo I/O.

**Ventaja demostrada**: La partition key compuesta transforma un filtro que en PostgreSQL requiere index scans en un acceso directo a la partición específica.

---

## Caso de Uso 6: Goles por Selección y Edición
**Tecnología elegida:** Cassandra

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede hacer JOINs entre `equipo`, `estadisticas_equipo` y `partido`, y agregar con `SUM(goles_favor)` agrupando por selección y mundial.

### ¿Por qué elegimos Cassandra?
**En un escenario de producción escalable**, este caso demuestra la **pre-agregación de datos**:

- **Agregación materializada**: El ETL ejecuta la query compleja de JOINs y `SUM()` en PostgreSQL **una vez por selección-mundial** al finalizar cada partido, y almacena el resultado agregado directamente en Cassandra.

- **Modelo clave-valor simple**: Cada fila es una tupla `(mundial, seleccion, total_goles)`. Las lecturas son extremadamente rápidas (complejidad O(1) por partition key).

- **Ordenamiento client-side justificado**: El ordenamiento por total de goles se hace en la aplicación Python. Como el volumen es pequeño (~32 selecciones por mundial), ordenar en memoria es trivial (<1ms).

- **Escrituras idempotentes**: Si el ETL se ejecuta múltiples veces con los mismos datos, Cassandra sobrescribe la fila sin generar duplicados (usando `INSERT` con primary key única).

**Ventaja demostrada**: Eliminamos agregaciones costosas (`SUM`, `GROUP BY`) en tiempo de consulta, materializando los totales en un modelo denormalizado simple.

---

## Caso de Uso 7: Sesión de Periodista
**Tecnología elegida:** Redis

### ¿Se puede hacer en PostgreSQL?
Técnicamente sí, con una tabla `sesiones(id, id_periodista, fecha_creacion, fecha_expiracion)` y un background job que ejecute `DELETE FROM sesiones WHERE fecha_expiracion < NOW()` periódicamente.

### ¿Por qué elegimos Redis?
**Este caso NO es solo una optimización - Redis resuelve un problema que PostgreSQL no maneja nativamente**: la **expiración automática con TTL**.

- **TTL nativo**: Redis implementa Time-To-Live a nivel de clave. Usando `SETEX`, la sesión expira automáticamente después de 2 horas **sin necesidad de procesos externos** de limpieza.

- **Velocidad sub-milisegundo**: Como base de datos en memoria, Redis ofrece latencias <1ms, crítico para validar sesiones en **cada request HTTP**.

- **Operaciones atómicas**: El comando `SETEX key ttl value` es atómico, garantizando que no existan sesiones sin expiración configurada.

- **Patrón session store estándar**: Redis es el estándar de la industria para gestión de sesiones web (usado por frameworks como Django, Express, Rails).

- **Búsqueda por patrones**: Los comandos `KEYS` y `SCAN` permiten buscar sesiones activas por nombre de periodista, útil para operaciones administrativas.

**Caso único**: Este es el **único caso de uso que no puede resolverse eficientemente con PostgreSQL sin agregar complejidad significativa** (cron jobs, triggers, particionamiento por fecha de expiración).

---

## Caso de Uso 8: Camino de Eliminación entre Selecciones
**Tecnología elegida:** Neo4j

### ¿Se puede hacer en PostgreSQL?
Técnicamente sí, usando **Recursive CTEs** (Common Table Expressions) para recorrer las relaciones entre partidos. Sin embargo, la complejidad algorítmica es **exponencial** en grafos grandes.

### ¿Por qué elegimos Neo4j?
**Este es el caso que mejor justifica una base de datos de grafos**:

- **Algoritmo shortestPath nativo**: Neo4j implementa algoritmos de grafos optimizados (como Dijkstra) a nivel de motor. Calcular el camino más corto entre dos selecciones es **una sola línea de Cypher**:
```cypher
MATCH p = shortestPath((a:Seleccion)-[:JUEGA_CONTRA*]-(b:Seleccion))
WHERE a.nombre = 'Argentina' AND b.nombre = 'Brasil' AND a.id_edicion = 2
RETURN p
```

- **Index-free adjacency**: Cada nodo (selección) almacena referencias directas a sus vecinos (partidos). Recorrer el grafo es **O(n) donde n es el tamaño del camino**, vs O(n²) en PostgreSQL con JOINs recursivos.

- **Representación natural**: Las fases eliminatorias **son un grafo dirigido** por naturaleza. Neo4j permite modelar esto directamente sin abstracciones forzadas (como tablas de adyacencia o recursive CTEs).

- **Separación por edición**: Al incluir `id_edicion` en los nodos, garantizamos que cada mundial tiene su grafo independiente, evitando que el algoritmo atraviese partidos de diferentes ediciones.

- **Visualización integrada**: Neo4j Browser permite visualizar el grafo completo, útil para debugging y presentaciones académicas.

**Ventaja demostrada**: Este caso muestra que hay problemas (grafos, relaciones complejas) donde bases especializadas tienen ventajas algorítmicas fundamentales sobre SQL.

---

## Caso de Uso 9: Goleadores en Fases KO
**Tecnología elegida:** Cassandra

### ¿Se puede hacer en PostgreSQL?
Sí. PostgreSQL puede hacer JOINs entre `jugador`, `estadisticas_jugador` y `partido`, filtrar por fases eliminatorias con `WHERE fase IN (...)`, agregar con `SUM(goles)`, y ordenar con `ORDER BY total_goles DESC`.

### ¿Por qué elegimos Cassandra?
**En un escenario de producción escalable**, este caso demuestra nuevamente el **ordenamiento físico** para rankings:

- **Clustering Order descendente**: `WITH CLUSTERING ORDER BY (golesko DESC)` hace que Cassandra almacene los goleadores **físicamente ordenados en disco** de mayor a menor cantidad de goles. La query es una lectura secuencial sin sort.

- **Partition por edición**: Cada edición del mundial es una partition independiente. Queries como "Top 10 goleadores de fases KO del Mundial 2030" acceden solo a los datos de esa partición, sin escanear otras ediciones.

- **Datos inmutables**: Las estadísticas de fases eliminatorias no cambian después del partido (write-once, read-many), alineándose con el modelo append-only de Cassandra.

- **Agregación pre-calculada**: El ETL ejecuta el `SUM(goles)` en PostgreSQL y materializa el total en Cassandra. Las consultas simplemente leen el ranking pre-ordenado.

**Ventaja demostrada**: Similar al caso 4, eliminamos `ORDER BY` costosos en tiempo de consulta gracias al ordenamiento físico de Cassandra.

---

## Principios de Diseño Aplicados

### 1. **Query-First Design**
Cada base de datos fue elegida pensando primero en el patrón de consulta específico, no en el modelo de datos abstracto.

### 2. **Polyglot Persistence**
No existe una "mejor base de datos" universal. Cada tecnología tiene trade-offs, y usar la herramienta correcta para cada problema optimiza el sistema completo.

### 3. **Denormalización estratégica**
En Cassandra y MongoDB, elegimos denormalizar datos intencionalmente para eliminar JOINs y mejorar performance, aceptando el costo de duplicación de datos.

### 4. **Separation of Concerns**
PostgreSQL mantiene la fuente de verdad (source of truth) con integridad referencial, mientras que las otras bases de datos actúan como stores especializados derivados.

### 5. **CAP Theorem Awareness**
- **Cassandra**: AP (Availability + Partition Tolerance) - priorizamos disponibilidad sobre consistencia inmediata
- **MongoDB**: CP (Consistency + Partition Tolerance) - garantizamos consistencia en escrituras
- **Neo4j**: CP - garantizamos consistencia del grafo
- **Redis**: CP - consistencia de sesiones crítica para seguridad

---

## Comparativa de Alternativas

| Caso de Uso | Tecnología Elegida | ¿Funciona en PostgreSQL? | Ventaja Principal de la Tecnología Elegida |
|-------------|-------------------|--------------------------|-------------------------------------------|
| 1. Tabla de Posiciones | Cassandra | Sí (con JOINs + GROUP BY + ORDER BY) | Ordenamiento físico elimina sort en tiempo de consulta |
| 2. Árbitros Fases Finales | MongoDB | Sí (con 4 JOINs + tabla `arbitro_partido`) | Arrays anidados eliminan JOINs, documento completo en una operación |
| 3. Jugadores con Goles | MongoDB | Sí (con JOINs + SUM + HAVING) | Agregación pre-calculada en arrays, una lectura por selección |
| 4. Partidos Popularidad | Cassandra | Sí (con JOINs + ORDER BY) | Clustering order descendente = datos ordenados físicamente |
| 5. Partidos Fecha/Estadio | Cassandra | Sí (con índices compuestos) | Compound partition key = lookup O(1) sin index scans |
| 6. Goles por Selección | Cassandra | Sí (con JOINs + SUM + GROUP BY) | Agregación materializada, modelo clave-valor simple |
| 7. Sesión Periodista | Redis | Posible (pero requiere cron jobs) | **TTL nativo**, no requiere procesos externos de limpieza |
| 8. Camino Eliminación | Neo4j | Posible (con recursive CTEs) | **Algoritmo shortestPath nativo O(n)** vs recursión exponencial |
| 9. Goleadores KO | Cassandra | Sí (con JOINs + SUM + ORDER BY) | Clustering order + agregación materializada |

**Aclaración crítica**: La tabla muestra que **8 de 9 casos pueden resolverse con PostgreSQL**. Sin embargo, el modelo polyglot persistence fue diseñado para **demostrar comprensión de escalabilidad**, donde cada tecnología especializada ofrece ventajas bajo alta carga concurrente (millones de requests).

---

## Conclusión

### Implementación Actual vs Diseño Escalable

**Estado actual (prototipo académico)**:
- ETL on-demand: Se ejecuta cada vez que se invoca un caso de uso.
- Viable para volúmenes pequeños (2 ediciones, ~64 partidos).
- Demuestra el **flujo completo** de datos entre sistemas.

**Diseño orientado a producción**:
- ETL programado o basado en eventos (cada 15 minutos, o triggers post-partido).
- PostgreSQL dedicado a writes transaccionales.
- Bases NoSQL manejan millones de reads concurrentes distribuidos geográficamente.
- Latencia reducida de 50-150ms (PostgreSQL con JOINs) a <10ms (lecturas directas NoSQL).

### Polyglot Persistence Justificado

La arquitectura multi-base de datos implementada demuestra:

- **PostgreSQL**: Fuente de verdad con integridad referencial y consistencia ACID.
- **Cassandra**: Ordenamiento físico y agregaciones pre-calculadas para queries predecibles.
- **MongoDB**: Documentos anidados que eliminan JOINs complejos.
- **Neo4j**: Algoritmos de grafos nativos para relaciones complejas.
- **Redis**: TTL nativo para expiración automática de sesiones.

**Trade-off aceptado**: Complejidad operacional (mantener 5 bases) vs preparación para escalar horizontalmente sin rediseño arquitectónico.

**Aprendizaje académico**: No existe una "base de datos perfecta". La elección depende del patrón de consulta, volumen de datos, frecuencia de acceso, y requisitos de consistencia. Este proyecto demuestra **comprensión profunda de los trade-offs** entre diferentes paradigmas de bases de datos.

---

**Materia**: Ingeniería de Datos II - UADE  
**Autores**: Facundo Conde, Gianfranco Mazzei, Theo Ruschanoff, Máximo Bonarrico  
**Fecha de entrega**: 16 de noviembre 2024  
**Versión**: 1.0
