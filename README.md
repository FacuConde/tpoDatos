# TPO Ingeniería de Datos II - FIFA World Cup

Sistema de consultas multi-base de datos para análisis de datos históricos de la Copa del Mundo FIFA. Proyecto desarrollado para la materia Ingeniería de Datos II - UADE.

## 📋 Descripción

Sistema ETL (Extract, Transform, Load) que integra múltiples tecnologías de bases de datos para resolver casos de uso específicos sobre datos del Mundial de Fútbol. El sistema extrae datos desde PostgreSQL y los distribuye en bases de datos especializadas según el tipo de consulta.

## 🏗️ Arquitectura

```
PostgreSQL (Supabase)     →    ETL Manager    →    Cassandra / MongoDB / Neo4j
   [Datos maestros]              [Transformación]     [Bases especializadas]
```

### Bases de Datos Utilizadas

- **PostgreSQL (Supabase)**: Base de datos maestra con información completa del torneo
- **Cassandra**: Consultas de alto rendimiento con modelos denormalizados
- **MongoDB**: Documentos con estructuras anidadas y arrays
- **Neo4j**: Grafos para análisis de relaciones entre selecciones
- **Redis**: Cache y gestión de sesiones con TTL automático

## 🚀 Casos de Uso Implementados

### 1. Tabla de Posiciones de un Grupo (PostgreSQL → Cassandra)
Consulta la tabla de posiciones de un grupo específico en una edición del mundial.
- **Base destino**: Cassandra
- **Tabla**: `tabla_posiciones`
- **Ordenamiento**: Por posición ascendente

### 2. Árbitros de Fases Finales (PostgreSQL → MongoDB)
Lista todos los partidos de fases finales con sus árbitros agrupados por partido.
- **Base destino**: MongoDB
- **Colección**: `arbitros_fases_finales`
- **Estructura**: Documentos con array de árbitros por partido

### 3. Jugadores con Mínimo de Goles (PostgreSQL → MongoDB)
Consulta jugadores de un país que superaron un umbral de goles en una edición.
- **Base destino**: MongoDB
- **Colección**: `jugadores_goleadores`
- **Filtrado**: Por país, edición y mínimo de goles

### 4. Partidos por Popularidad (PostgreSQL → Cassandra)
Partidos de un grupo ordenados por popularidad (asistencia).
- **Base destino**: Cassandra
- **Tabla**: `partidos_populares`
- **Ordenamiento**: Clustering por popularidad descendente

### 5. Partidos por Fecha y Estadio (PostgreSQL → Cassandra)
Consulta partidos jugados en un estadio específico en un año determinado.
- **Base destino**: Cassandra
- **Tabla**: `partidos_fecha_estadio`
- **Clave primaria**: (estadio, fecha)

### 6. Goles por Selección (PostgreSQL → Cassandra)
Ranking de goles anotados por cada selección en una edición.
- **Base destino**: Cassandra
- **Tabla**: `goles_seleccion_edicion`
- **Ordenamiento**: Por cantidad de goles (descendente)

### 7. Sesión de Periodista (PostgreSQL → Redis)
Gestión de sesiones de usuarios periodistas con TTL (Time To Live) de 2 horas.
- **Base destino**: Redis
- **TTL**: 7200 segundos (2 horas)
- **Funcionalidades**:
  - Crear nueva sesión para un periodista
  - Buscar sesiones activas por nombre
  - Renovar TTL de sesiones existentes
  - Eliminar sesiones manualmente
  - Contador de sesiones totales activas
- **Estructura de clave**: `sesion:periodista:{nombre_normalizado}_token`
- **Payload**: JSON con información del periodista

### 8. Camino de Eliminación (PostgreSQL → Neo4j)
Encuentra el camino más corto entre dos selecciones en fase eliminatoria usando teoría de grafos.
- **Base destino**: Neo4j
- **Algoritmo**: `shortestPath`
- **Nodos**: Selecciones (con id_edicion)
- **Relaciones**: JUEGA_CONTRA (con fase, id_partido, id_edicion)

### 9. Goleadores en Fases KO (PostgreSQL → Cassandra)
Lista de goleadores en fases de eliminación directa de una edición.
- **Base destino**: Cassandra
- **Tabla**: `goleadores_ko_edicion`
- **Ordenamiento**: Por goles descendente

## 📦 Requisitos

### Dependencias de Python
```
psycopg2-binary==2.9.9
cassandra-driver==3.29.1
pymongo==4.6.1
neo4j==5.16.0
redis==5.0.1
python-dotenv==1.0.0
```

### Bases de Datos Externas

1. **PostgreSQL (Supabase)**
   - Host: aws-1-us-east-2.pooler.supabase.com
   - Puerto: 5432
   - Tipo: Connection Pooler

2. **Cassandra**
   - Host: localhost
   - Puerto: 9042
   - Keyspace: fifa_db
   - Despliegue: Docker

3. **MongoDB Atlas**
   - Tipo: Cluster en la nube
   - Database: fifa_db

4. **Neo4j Aura**
   - Tipo: Instancia gestionada en la nube
   - Database: neo4j

## 🔧 Instalación

### 1. Clonar el repositorio
```bash
git clone <repository-url>
cd "TPO DATOS (FIFA)"
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Configurar Cassandra (Docker)
```bash
docker run --name cassandra-fifa -p 9042:9042 -d cassandra:latest
```

### 4. Configurar variables de entorno
Crear archivo `.env` en la raíz del proyecto (usar `.env.example` como plantilla):

```env
# PostgreSQL (Supabase)
DATABASE_URL=postgresql://postgres.xxxxx:password@aws-1-us-east-2.pooler.supabase.com:5432/postgres

# Cassandra
CASSANDRA_HOST=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=fifa_db

# MongoDB Atlas
MONGODB_URI=mongodb+srv://user:password@cluster.mongodb.net/

# Neo4j Aura
NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j

# Redis (Pendiente)
REDIS_HOST=localhost
REDIS_PORT=6379
```

### 5. Ejecutar el sistema
```bash
python main.py
```

## 📁 Estructura del Proyecto

```
TPO DATOS (FIFA)/
├── main.py                      # Interfaz principal del sistema (menú interactivo)
├── db_manager.py                # Gestor de conexiones a bases de datos
├── etl_manager.py               # Lógica ETL y transformaciones
├── .env                         # Variables de entorno (NO INCLUIR EN GIT)
├── .env.example                 # Plantilla de variables de entorno
├── requirements.txt             # Dependencias de Python
├── README.md                    # Este archivo
└── recrear_tabla_partidos.py    # Utilidad para recrear tablas Cassandra
```

## 🏛️ Modelos de Datos

### Cassandra - tabla_posiciones
```cql
CREATE TABLE tabla_posiciones (
    mundial text,
    grupo text,
    posicion int,
    pais text,
    puntos int,
    goles_favor int,
    goles_contra int,
    diferencia_gol int,
    PRIMARY KEY ((mundial, grupo), posicion)
);
```

### Cassandra - partidos_populares
```cql
CREATE TABLE partidos_populares (
    mundial text,
    grupo text,
    popularidad int,
    id_partido int,
    fecha_hora timestamp,
    estadio text,
    seleccion_local text,
    seleccion_visitante text,
    PRIMARY KEY ((mundial, grupo), popularidad, id_partido)
) WITH CLUSTERING ORDER BY (popularidad DESC);
```

### MongoDB - arbitros_fases_finales
```json
{
    "_id": ObjectId,
    "edicion": "Mundial 2030",
    "idPartido": 11,
    "fase": "semifinal",
    "local": "Argentina",
    "visitante": "Brasil",
    "arbitros": [
        {
            "nombre": "Nombre Arbitro",
            "apellido": "Apellido",
            "rol": "Principal"
        }
    ]
}
```

### Neo4j - Grafo de Eliminación
```cypher
// Nodos
(:Seleccion {
    id_seleccion: 1,
    nombre: "Argentina",
    id_edicion: 2
})

// Relaciones
-[:JUEGA_CONTRA {
    id_edicion: 2,
    id_partido: 11,
    fase: "semifinal"
}]->
```

## 🎯 Patrones de Diseño Utilizados

### 1. Singleton Pattern
- **Clase**: `DatabaseManager`
- **Propósito**: Única instancia para gestionar todas las conexiones

### 2. ETL Pattern
- **Extract**: Consultas a PostgreSQL usando funciones almacenadas
- **Transform**: Agregaciones y transformaciones en Python (usando `defaultdict` para agrupamiento)
- **Load**: Inserción en bases de datos especializadas con MERGE/UPSERT

### 3. Factory Pattern
- **Función**: Creación dinámica de tablas Cassandra según el caso de uso

## 🔍 Decisiones Técnicas

### ¿Por qué múltiples bases de datos?

1. **Cassandra**: 
   - Consultas de lectura masiva con baja latencia
   - Modelo denormalizado optimizado por query
   - Clustering para ordenamiento eficiente sin procesamiento adicional

2. **MongoDB**:
   - Datos semi-estructurados con arrays anidados
   - Consultas flexibles con filtrado complejo
   - Agregaciones y proyecciones dinámicas

3. **Neo4j**:
   - Análisis de relaciones y caminos entre entidades
   - Algoritmos de grafos (`shortestPath`)
   - Visualización de conexiones entre selecciones en fase eliminatoria

### Manejo de Ediciones en Neo4j

**Problema inicial**: Nodos compartidos entre ediciones causaban mezcla de datos (Argentina 2026 conectada con Argentina 2030).

**Solución implementada**:
- Cada nodo `Seleccion` incluye `id_edicion` como propiedad clave única
- Limpieza previa usando `DETACH DELETE` filtrado por `id_edicion` numérico
- Queries de búsqueda filtran tanto nodos como relaciones por edición específica
- Conversión de nombre de edición ("Mundial 2030") a ID numérico (2) antes de operar

### Connection Pooling

Se usa el **connection pooler de Supabase** (`aws-1-us-east-2.pooler.supabase.com`) en lugar de conexión directa para:
- Mayor estabilidad en conexiones
- Manejo automático de reconexiones
- Mejor performance bajo carga

### Prepared Statements en Cassandra

Todas las queries parametrizadas usan `session.prepare()` para:
- Evitar SQL injection
- Mejor performance (query compilado una vez)
- Manejo correcto de tipos de datos

## 🐛 Troubleshooting

### Error: "relation does not exist"
- Verificar que las tablas Cassandra estén creadas
- Ejecutar `recrear_tabla_partidos.py` si es necesario

### Error: "Database objects do not implement truth value testing"
- Usar `if mongodb_db is None` en lugar de `if not mongodb_db`
- MongoDB Database objects son truthy pero no soportan operadores booleanos

### Error: Neo4j muestra caminos incorrectos entre ediciones
- Limpiar el grafo completamente: `MATCH (n) DETACH DELETE n`
- Volver a ejecutar el caso de uso 8
- El problema se debe a nodos duplicados sin `id_edicion`

### Cassandra: "not all arguments converted during string formatting"
- Usar `session.prepare()` para queries parametrizadas
- Nunca usar f-strings o format() directamente con valores

### PostgreSQL: "column does not exist"
- Verificar nombres de columnas en las vistas (ej: `id_edicion` vs `edicion`)
- Revisar la definición de la vista con: `SELECT definition FROM pg_views WHERE viewname = 'nombre_vista'`

## 📊 Vistas de PostgreSQL Utilizadas

- `vw_partidos_ko_edges`: Vista para obtener aristas del grafo de eliminación (fixture + partido + selecciones)
- Funciones almacenadas personalizadas para cada caso de uso (definidas en Supabase)

## 🔧 Comandos Útiles

### Ver contenedores Docker activos
```powershell
docker ps
```

### Iniciar Cassandra
```powershell
docker start cassandra-fifa
```

### Acceder a cqlsh (Cassandra shell)
```powershell
docker exec -it cassandra-fifa cqlsh
```

### Ver tablas en Cassandra
```cql
USE fifa_db;
DESCRIBE TABLES;
```

### Limpiar grafo Neo4j completo
```cypher
MATCH (n) DETACH DELETE n
```

## 👥 Autor

- **Materia**: Ingeniería de Datos II
- **Universidad**: UADE
- **Año**: 2024

## 📝 Notas Importantes

- El archivo `.env` contiene credenciales sensibles. **NO SUBIR A GIT**
- Todos los 9 casos de uso están implementados y funcionales
- Las tablas Cassandra se crean automáticamente al ejecutar cada caso de uso
- Neo4j limpia y recarga datos en cada ejecución del caso de uso 8
- MongoDB usa `replace_one` con `upsert=True` para evitar duplicados
- Redis gestiona sesiones con expiración automática (TTL)

## 🔮 Futuras Mejoras

- [ ] Agregar tests unitarios y de integración
- [ ] Implementar logging estructurado (JSON logs)
- [ ] Crear dashboard de visualización con Grafana
- [ ] Dockerizar toda la aplicación (docker-compose)
- [ ] Agregar CI/CD pipeline
- [ ] Implementar cache de consultas frecuentes en Redis
- [ ] Agregar métricas de performance
- [ ] Implementar autenticación JWT para sesiones de periodistas

## 📄 Licencia

Este proyecto es de uso académico para la materia Ingeniería de Datos II - UADE.
