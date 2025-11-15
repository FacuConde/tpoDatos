"""
Módulo para manejar conexiones a las bases de datos
"""

import os
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlparse
from cassandra.cluster import Cluster

load_dotenv()


class DatabaseManager:
    """Gestor de conexiones a las bases de datos"""
    
    def __init__(self):
        self.pg_conn = None
        self.cassandra_cluster = None
        self.cassandra_session = None
        self.mongodb_client = None
        self.mongodb_db = None
        self.neo4j_driver = None
    
    def connect_postgresql(self):
        """Conectar a PostgreSQL (Supabase)"""
        try:
            database_url = os.getenv('DATABASE_URL')
            result = urlparse(database_url)
            
            self.pg_conn = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
            return True
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return False
    
    def connect_cassandra(self):
        """Conectar a Cassandra"""
        try:
            host = os.getenv('CASSANDRA_HOST', 'localhost')
            port = int(os.getenv('CASSANDRA_PORT', 9042))
            keyspace = os.getenv('CASSANDRA_KEYSPACE', 'fifa_db')
            
            self.cassandra_cluster = Cluster([host], port=port)
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Crear keyspace si no existe
            self.cassandra_session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Usar el keyspace
            self.cassandra_session.set_keyspace(keyspace)
            
            # Crear tabla de posiciones si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS tabla_posiciones (
                    edicion TEXT,
                    grupo TEXT,
                    posicion INT,
                    pais TEXT,
                    puntos INT,
                    gf INT,
                    gc INT,
                    dg INT,
                    PRIMARY KEY ((edicion, grupo), posicion)
                ) WITH CLUSTERING ORDER BY (posicion ASC)
            """)
            
            # Crear tabla de partidos populares si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS partidos_populares (
                    edicion TEXT,
                    grupo TEXT,
                    popularidad INT,
                    id_partido INT,
                    fecha_hora TIMESTAMP,
                    estadio TEXT,
                    seleccionLocal TEXT,
                    seleccionVisitante TEXT,
                    PRIMARY KEY ((edicion, grupo), popularidad, id_partido)
                ) WITH CLUSTERING ORDER BY (popularidad DESC)
            """)
            
            # Crear tabla de goles por selección y edición si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS goles_seleccion_edicion (
                    edicion TEXT,
                    seleccion TEXT,
                    goles INT,
                    PRIMARY KEY (edicion, seleccion)
                )
            """)
            
            # Crear tabla de partidos por fecha y estadio si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS partidos_fecha_estadio (
                    id_partido INT,
                    fecha TIMESTAMP,
                    estadio TEXT,
                    seleccionLocal TEXT,
                    seleccionVisitante TEXT,
                    golesLocal INT,
                    golesVisitante INT,
                    PRIMARY KEY (estadio, fecha)
                ) WITH CLUSTERING ORDER BY (fecha ASC)
            """)
            
            # Crear tabla de goleadores en fases KO si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS goleadores_ko_edicion (
                    edicion TEXT,
                    golesko INT,
                    id_jugador INT,
                    apellidojugador TEXT,
                    nombrejugador TEXT,
                    seleccion TEXT,
                    PRIMARY KEY (edicion, golesko, id_jugador)
                ) WITH CLUSTERING ORDER BY (golesko DESC, id_jugador ASC)
            """)
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a Cassandra: {e}")
            return False
    
    def connect_mongodb(self):
        """Conectar a MongoDB"""
        try:
            from pymongo import MongoClient
            
            uri = os.getenv('MONGODB_URI')
            database_name = os.getenv('MONGODB_DATABASE', 'fifa_db')
            
            self.mongodb_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            
            # Probar la conexión
            self.mongodb_client.server_info()
            self.mongodb_db = self.mongodb_client[database_name]
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a MongoDB: {e}")
            return False
    
    def connect_neo4j(self):
        """Conectar a Neo4j"""
        try:
            from neo4j import GraphDatabase
            
            uri = os.getenv('NEO4J_URI')
            user = os.getenv('NEO4J_USER', 'neo4j')
            password = os.getenv('NEO4J_PASSWORD')
            
            self.neo4j_driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Verificar conexión
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a Neo4j: {e}")
            return False
        """Conectar a Cassandra"""
        try:
            host = os.getenv('CASSANDRA_HOST', 'localhost')
            port = int(os.getenv('CASSANDRA_PORT', 9042))
            keyspace = os.getenv('CASSANDRA_KEYSPACE', 'fifa_db')
            
            self.cassandra_cluster = Cluster([host], port=port)
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Crear keyspace si no existe
            self.cassandra_session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Usar el keyspace
            self.cassandra_session.set_keyspace(keyspace)
            
            # Crear tabla de posiciones si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS tabla_posiciones (
                    edicion TEXT,
                    grupo TEXT,
                    posicion INT,
                    pais TEXT,
                    puntos INT,
                    gf INT,
                    gc INT,
                    dg INT,
                    PRIMARY KEY ((edicion, grupo), posicion)
                ) WITH CLUSTERING ORDER BY (posicion ASC)
            """)
            
            # Crear tabla de partidos populares si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS partidos_populares (
                    edicion TEXT,
                    grupo TEXT,
                    popularidad INT,
                    id_partido INT,
                    fecha_hora TIMESTAMP,
                    estadio TEXT,
                    seleccionLocal TEXT,
                    seleccionVisitante TEXT,
                    PRIMARY KEY ((edicion, grupo), popularidad, id_partido)
                ) WITH CLUSTERING ORDER BY (popularidad DESC)
            """)
            
            # Crear tabla de goles por selección y edición si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS goles_seleccion_edicion (
                    edicion TEXT,
                    seleccion TEXT,
                    goles INT,
                    PRIMARY KEY (edicion, seleccion)
                )
            """)
            
            # Crear tabla de partidos por fecha y estadio si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS partidos_fecha_estadio (
                    id_partido INT,
                    fecha TIMESTAMP,
                    estadio TEXT,
                    seleccionLocal TEXT,
                    seleccionVisitante TEXT,
                    golesLocal INT,
                    golesVisitante INT,
                    PRIMARY KEY (estadio, fecha)
                ) WITH CLUSTERING ORDER BY (fecha ASC)
            """)
            
            # Crear tabla de goleadores en fases KO si no existe
            self.cassandra_session.execute("""
                CREATE TABLE IF NOT EXISTS goleadores_ko_edicion (
                    edicion TEXT,
                    golesko INT,
                    id_jugador INT,
                    apellidojugador TEXT,
                    nombrejugador TEXT,
                    seleccion TEXT,
                    PRIMARY KEY (edicion, golesko, id_jugador)
                ) WITH CLUSTERING ORDER BY (golesko DESC, id_jugador ASC)
            """)
            
            return True
        except Exception as e:
            print(f"❌ Error conectando a Cassandra: {e}")
            return False
    
    def get_postgresql_cursor(self):
        """Obtener cursor de PostgreSQL"""
        if not self.pg_conn or self.pg_conn.closed:
            self.connect_postgresql()
        return self.pg_conn.cursor()
    
    def get_cassandra_session(self):
        """Obtener sesión de Cassandra"""
        if not self.cassandra_session:
            self.connect_cassandra()
        return self.cassandra_session
    
    def get_mongodb_db(self):
        """Obtener base de datos de MongoDB"""
        if self.mongodb_db is None:
            self.connect_mongodb()
        return self.mongodb_db
    
    def get_neo4j_driver(self):
        """Obtener driver de Neo4j"""
        if self.neo4j_driver is None:
            self.connect_neo4j()
        return self.neo4j_driver
    
    def close_all(self):
        """Cerrar todas las conexiones"""
        try:
            if self.pg_conn:
                self.pg_conn.close()
            if self.cassandra_session:
                self.cassandra_session.shutdown()
            if self.cassandra_cluster:
                self.cassandra_cluster.shutdown()
            if self.mongodb_client:
                self.mongodb_client.close()
            if self.neo4j_driver:
                self.neo4j_driver.close()
        except Exception as e:
            print(f"⚠️  Error cerrando conexiones: {e}")


# Instancia global
db_manager = DatabaseManager()
