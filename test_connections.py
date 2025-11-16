import os
from dotenv import load_dotenv

load_dotenv()

def short_error(e, max_len=120):
    s = f"{type(e).__name__}: {e}"
    return s if len(s) <= max_len else s[:max_len] + "..."

def test_postgresql():
    try:
        import psycopg2
        from urllib.parse import urlparse
        url = os.getenv("DATABASE_URL")
        r = urlparse(url)
        conn = psycopg2.connect(
            database=r.path[1:], user=r.username, password=r.password,
            host=r.hostname, port=r.port
        )
        conn.close()
        print("PostgreSQL: OK")
    except Exception as e:
        print("PostgreSQL: ERROR ->", short_error(e))

def test_cassandra():
    try:
        from cassandra.cluster import Cluster
        host = os.getenv("CASSANDRA_HOST", "localhost")
        port = int(os.getenv("CASSANDRA_PORT", 9042))
        cluster = Cluster([host], port=port)
        session = cluster.connect()
        session.shutdown()
        cluster.shutdown()
        print("Cassandra: OK")
    except Exception as e:
        print("Cassandra: ERROR ->", short_error(e))

def test_mongodb():
    try:
        from pymongo import MongoClient
        uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        client = MongoClient(uri, serverSelectionTimeoutMS=4000)
        client.server_info()
        client.close()
        print("MongoDB: OK")
    except Exception as e:
        print("MongoDB: ERROR ->", short_error(e))

def test_neo4j():
    try:
        from neo4j import GraphDatabase
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        pwd = os.getenv("NEO4J_PASSWORD", "password")
        driver = GraphDatabase.driver(uri, auth=(user, pwd))
        with driver.session() as s:
            s.run("RETURN 1").single()
        driver.close()
        print("Neo4j: OK")
    except Exception as e:
        print("Neo4j: ERROR ->", short_error(e))

def test_redis():
    try:
        import redis
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", 6379))
        pwd = os.getenv("REDIS_PASSWORD")
        db = int(os.getenv("REDIS_DB", 0))
        r = redis.Redis(host=host, port=port, password=pwd, db=db)
        r.ping()
        print("Redis: OK")
    except Exception as e:
        print("Redis: ERROR ->", short_error(e))

def main():
    print("=== Probando conexiones ===")
    test_postgresql()
    test_cassandra()
    test_mongodb()
    test_neo4j()
    test_redis()

if __name__ == "__main__":
    main()
