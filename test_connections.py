"""
Script para probar las conexiones a las diferentes bases de datos
Ingeniería de Datos II - TPO FIFA
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


class DatabaseConnections:
    """Clase para manejar las conexiones a todas las bases de datos"""
    
    def __init__(self):
        self.connections = {}
        
    def test_postgresql(self):
        """Probar conexión a PostgreSQL (Supabase)"""
        try:
            import psycopg2
            from urllib.parse import urlparse
            
            database_url = os.getenv('DATABASE_URL')
            result = urlparse(database_url)
            
            conn = psycopg2.connect(
                database=result.path[1:],
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            
            self.connections['postgresql'] = conn
            print("✓ PostgreSQL (Supabase) conectado exitosamente")
            print(f"  Versión: {version[0][:50]}...")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"✗ Error conectando a PostgreSQL: {e}")
            return False
    
    def test_cassandra(self):
        """Probar conexión a Cassandra"""
        try:
            from cassandra.cluster import Cluster
            
            host = os.getenv('CASSANDRA_HOST', 'localhost')
            port = int(os.getenv('CASSANDRA_PORT', 9042))
            
            cluster = Cluster([host], port=port)
            session = cluster.connect()
            
            # Obtener información del cluster
            rows = session.execute("SELECT cluster_name, release_version FROM system.local")
            for row in rows:
                cluster_name = row.cluster_name
                version = row.release_version
            
            self.connections['cassandra'] = {'cluster': cluster, 'session': session}
            print("✓ Cassandra conectado exitosamente")
            print(f"  Cluster: {cluster_name}")
            print(f"  Versión: {version}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error conectando a Cassandra: {e}")
            return False
    
    def test_mongodb(self):
        """Probar conexión a MongoDB"""
        try:
            from pymongo import MongoClient
            
            uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            database_name = os.getenv('MONGODB_DATABASE', 'fifa_db')
            
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            
            # Probar la conexión
            client.server_info()
            db = client[database_name]
            
            self.connections['mongodb'] = {'client': client, 'db': db}
            print("✓ MongoDB conectado exitosamente")
            print(f"  Base de datos: {database_name}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error conectando a MongoDB: {e}")
            return False
    
    def test_neo4j(self):
        """Probar conexión a Neo4j"""
        try:
            from neo4j import GraphDatabase
            
            uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
            user = os.getenv('NEO4J_USER', 'neo4j')
            password = os.getenv('NEO4J_PASSWORD', 'password')
            
            driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Verificar conexión
            with driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
            
            self.connections['neo4j'] = driver
            print("✓ Neo4j conectado exitosamente")
            print(f"  URI: {uri}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error conectando a Neo4j: {e}")
            return False
    
    def test_redis(self):
        """Probar conexión a Redis"""
        try:
            import redis
            
            host = os.getenv('REDIS_HOST', 'localhost')
            port = int(os.getenv('REDIS_PORT', 6379))
            password = os.getenv('REDIS_PASSWORD', None)
            db = int(os.getenv('REDIS_DB', 0))
            
            r = redis.Redis(
                host=host,
                port=port,
                password=password if password else None,
                db=db,
                decode_responses=True
            )
            
            # Probar la conexión
            r.ping()
            info = r.info()
            
            self.connections['redis'] = r
            print("✓ Redis conectado exitosamente")
            print(f"  Versión: {info['redis_version']}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error conectando a Redis: {e}")
            return False
    
    def test_all_connections(self):
        """Probar todas las conexiones"""
        print("="*60)
        print("Probando conexiones a las bases de datos...")
        print("="*60)
        print()
        
        results = {
            'PostgreSQL (Supabase)': self.test_postgresql(),
            'Cassandra': self.test_cassandra(),
            'MongoDB': self.test_mongodb(),
            'Neo4j': self.test_neo4j(),
            'Redis': self.test_redis()
        }
        
        print()
        print("="*60)
        print("Resumen de conexiones:")
        print("="*60)
        
        successful = sum(1 for v in results.values() if v)
        total = len(results)
        
        for db, status in results.items():
            status_text = "✓ Conectado" if status else "✗ Falló"
            print(f"{db:30} {status_text}")
        
        print()
        print(f"Total: {successful}/{total} bases de datos conectadas")
        
        return results
    
    def close_all_connections(self):
        """Cerrar todas las conexiones"""
        try:
            if 'postgresql' in self.connections:
                self.connections['postgresql'].close()
            
            if 'cassandra' in self.connections:
                self.connections['cassandra']['session'].shutdown()
                self.connections['cassandra']['cluster'].shutdown()
            
            if 'mongodb' in self.connections:
                self.connections['mongodb']['client'].close()
            
            if 'neo4j' in self.connections:
                self.connections['neo4j'].close()
            
            if 'redis' in self.connections:
                self.connections['redis'].close()
            
            print("\nTodas las conexiones cerradas correctamente")
            
        except Exception as e:
            print(f"\nError cerrando conexiones: {e}")


def main():
    """Función principal"""
    db_manager = DatabaseConnections()
    
    try:
        db_manager.test_all_connections()
    finally:
        db_manager.close_all_connections()


if __name__ == "__main__":
    main()
