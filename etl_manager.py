"""
Módulo para operaciones ETL (Extract, Transform, Load)
"""

from db_manager import db_manager
from collections import defaultdict


class ETLManager:
    """Gestor de procesos ETL"""
    
    @staticmethod
    def etl_tabla_posiciones(edicion, grupo):
        """
        ETL: Extraer tabla de posiciones desde PostgreSQL y cargar en Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2030')
            grupo (str): Letra del grupo (ej: 'A')
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para {edicion} - Grupo {grupo}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            # Llamar a la función de PostgreSQL
            cursor.execute("SELECT * FROM get_tabla_posiciones_grupo(%s, %s)", (edicion, grupo))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron datos para {edicion} - Grupo {grupo}")
                return False
            
            print(f"✅ Extraídos {len(rows)} registros desde PostgreSQL")
            
            # TRANSFORM & LOAD: Insertar en Cassandra
            print("📤 Cargando datos en Cassandra...")
            session = db_manager.get_cassandra_session()
            
            # Limpiar datos anteriores del mismo grupo y edición
            delete_stmt = session.prepare("""
                DELETE FROM tabla_posiciones 
                WHERE edicion = ? AND grupo = ?
            """)
            session.execute(delete_stmt, (edicion, grupo))
            
            # Preparar statement de inserción
            insert_query = """
                INSERT INTO tabla_posiciones 
                (edicion, grupo, posicion, pais, puntos, gf, gc, dg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_query)
            
            # Insertar cada registro
            for row in rows:
                # Ajustar según la estructura que devuelve PostgreSQL
                # Puede ser que la función devuelva solo: (posicion, pais, puntos, gf, gc, dg)
                # O puede incluir: (edicion, grupo, posicion, pais, puntos, gf, gc, dg)
                
                if len(row) == 6:
                    # Si solo devuelve los datos sin edicion y grupo
                    session.execute(prepared, (
                        edicion,  # edicion (parámetro)
                        grupo,    # grupo (parámetro)
                        row[0],   # posicion
                        row[1],   # pais
                        row[2],   # puntos
                        row[3],   # gf
                        row[4],   # gc
                        row[5]    # dg
                    ))
                else:
                    # Si devuelve todos los campos incluyendo edicion y grupo
                    session.execute(prepared, (
                        row[0],   # edicion
                        row[1],   # grupo
                        row[2],   # posicion
                        row[3],   # pais
                        row[4],   # puntos
                        row[5],   # gf
                        row[6],   # gc
                        row[7]    # dg
                    ))
            
            print(f"✅ Cargados {len(rows)} registros en Cassandra")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_tabla_posiciones_cassandra(edicion, grupo):
        """
        Obtener tabla de posiciones desde Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial
            grupo (str): Letra del grupo
        
        Returns:
            list: Lista de tuplas con los datos de la tabla
        """
        try:
            session = db_manager.get_cassandra_session()
            
            query_stmt = session.prepare("""
                SELECT posicion, pais, puntos, gf, gc, dg
                FROM tabla_posiciones
                WHERE edicion = ? AND grupo = ?
                ORDER BY posicion ASC
            """)
            
            rows = session.execute(query_stmt, (edicion, grupo))
            return list(rows)
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de Cassandra: {e}")
            return []
    
    @staticmethod
    def etl_partidos_populares(edicion, grupo):
        """
        ETL: Extraer partidos por popularidad desde PostgreSQL y cargar en Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2030')
            grupo (str): Letra del grupo (ej: 'C')
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para partidos populares {edicion} - Grupo {grupo}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_partidos_grupo_por_popularidad(%s, %s)", (edicion, grupo))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron partidos para {edicion} - Grupo {grupo}")
                return False
            
            print(f"✅ Extraídos {len(rows)} partidos desde PostgreSQL")
            
            # TRANSFORM & LOAD: Insertar en Cassandra
            print("📤 Cargando datos en Cassandra...")
            session = db_manager.get_cassandra_session()
            
            # Limpiar datos anteriores del mismo grupo y edición
            delete_stmt = session.prepare("""
                DELETE FROM partidos_populares 
                WHERE edicion = ? AND grupo = ?
            """)
            session.execute(delete_stmt, (edicion, grupo))
            
            # Preparar statement de inserción
            insert_query = """
                INSERT INTO partidos_populares 
                (edicion, grupo, popularidad, id_partido, fecha_hora, estadio, seleccionLocal, seleccionVisitante)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_query)
            
            # Insertar cada registro
            for row in rows:
                # Asumiendo que PostgreSQL devuelve: (id_partido, fecha_hora, estadio, local, visitante, popularidad)
                session.execute(prepared, (
                    edicion,
                    grupo,
                    row[5],  # popularidad
                    row[0],  # id_partido
                    row[1],  # fecha_hora
                    row[2],  # estadio
                    row[3],  # seleccionLocal
                    row[4]   # seleccionVisitante
                ))
            
            print(f"✅ Cargados {len(rows)} partidos en Cassandra")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_partidos_populares_cassandra(edicion, grupo):
        """
        Obtener partidos ordenados por popularidad desde Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial
            grupo (str): Letra del grupo
        
        Returns:
            list: Lista de tuplas con los datos de los partidos
        """
        try:
            session = db_manager.get_cassandra_session()
            
            query_stmt = session.prepare("""
                SELECT id_partido, fecha_hora, estadio, seleccionLocal, seleccionVisitante, popularidad
                FROM partidos_populares
                WHERE edicion = ? AND grupo = ?
            """)
            
            rows = session.execute(query_stmt, (edicion, grupo))
            return list(rows)
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de Cassandra: {e}")
            return []
    
    @staticmethod
    def etl_goles_seleccion_edicion(edicion):
        """
        ETL: Extraer goles por selección desde PostgreSQL y cargar en Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2026')
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para goles por selección - {edicion}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_goles_por_seleccion_edicion(%s)", (edicion,))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron datos para {edicion}")
                return False
            
            print(f"✅ Extraídos {len(rows)} registros desde PostgreSQL")
            
            # TRANSFORM & LOAD: Insertar en Cassandra
            print("📤 Cargando datos en Cassandra...")
            session = db_manager.get_cassandra_session()
            
            # Preparar statement de inserción
            insert_query = """
                INSERT INTO goles_seleccion_edicion 
                (edicion, seleccion, goles)
                VALUES (?, ?, ?)
            """
            prepared = session.prepare(insert_query)
            
            # Insertar cada registro (reemplazará si ya existe por la PRIMARY KEY)
            for row in rows:
                # Asumiendo que PostgreSQL devuelve: (edicion, seleccion, goles_totales)
                session.execute(prepared, (
                    row[0],  # edicion
                    row[1],  # seleccion
                    row[2]   # goles
                ))
            
            print(f"✅ Cargados {len(rows)} registros en Cassandra")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_goles_seleccion_edicion_cassandra(edicion):
        """
        Obtener goles por selección ordenados descendentemente desde Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial
        
        Returns:
            list: Lista de tuplas con los datos ordenados por goles
        """
        try:
            session = db_manager.get_cassandra_session()
            
            query_stmt = session.prepare("""
                SELECT seleccion, goles
                FROM goles_seleccion_edicion
                WHERE edicion = ?
            """)
            
            rows = session.execute(query_stmt, (edicion,))
            
            # Ordenar por goles descendentemente (Cassandra no ordena por columna no-clave)
            rows_list = list(rows)
            rows_list.sort(key=lambda x: x.goles, reverse=True)
            
            return rows_list
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de Cassandra: {e}")
            return []
    
    @staticmethod
    def etl_partidos_fecha_estadio(anio, estadio):
        """
        ETL: Extraer partidos por año y estadio desde PostgreSQL y cargar en Cassandra
        
        Args:
            anio (int): Año del mundial (ej: 2030)
            estadio (str): Nombre del estadio
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para partidos en {estadio} - Año {anio}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_partidos_por_anio_estadio(%s, %s)", (anio, estadio))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron partidos para {estadio} en {anio}")
                return False
            
            print(f"✅ Extraídos {len(rows)} partidos desde PostgreSQL")
            
            # TRANSFORM & LOAD: Insertar en Cassandra
            print("📤 Cargando datos en Cassandra...")
            session = db_manager.get_cassandra_session()
            
            # Preparar statement de inserción
            insert_query = """
                INSERT INTO partidos_fecha_estadio 
                (id_partido, fecha, estadio, seleccionLocal, seleccionVisitante, golesLocal, golesVisitante)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_query)
            
            # Insertar cada registro
            for row in rows:
                # Asumiendo que PostgreSQL devuelve: (id_partido, fecha, estadio, local, visitante, goles_local, goles_visitante)
                session.execute(prepared, (
                    row[0],  # id_partido
                    row[1],  # fecha
                    row[2],  # estadio
                    row[3],  # seleccionLocal
                    row[4],  # seleccionVisitante
                    row[5],  # golesLocal
                    row[6]   # golesVisitante
                ))
            
            print(f"✅ Cargados {len(rows)} partidos en Cassandra")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_partidos_fecha_estadio_cassandra(estadio):
        """
        Obtener partidos de un estadio ordenados por fecha desde Cassandra
        
        Args:
            estadio (str): Nombre del estadio
        
        Returns:
            list: Lista de tuplas con los datos de los partidos
        """
        try:
            session = db_manager.get_cassandra_session()
            
            query_stmt = session.prepare("""
                SELECT id_partido, fecha, seleccionLocal, seleccionVisitante, golesLocal, golesVisitante
                FROM partidos_fecha_estadio
                WHERE estadio = ?
            """)
            
            rows = session.execute(query_stmt, (estadio,))
            return list(rows)
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de Cassandra: {e}")
            return []
    
    @staticmethod
    def etl_goleadores_ko_edicion(edicion):
        """
        ETL: Extraer goleadores de fases KO desde PostgreSQL y cargar en Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2026')
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para goleadores KO - {edicion}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_goleadores_fases_ko(%s)", (edicion,))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron goleadores para {edicion}")
                return False
            
            print(f"✅ Extraídos {len(rows)} goleadores desde PostgreSQL")
            
            # TRANSFORM & LOAD: Insertar en Cassandra
            print("📤 Cargando datos en Cassandra...")
            session = db_manager.get_cassandra_session()
            
            # Preparar statement de inserción
            insert_query = """
                INSERT INTO goleadores_ko_edicion 
                (edicion, golesko, id_jugador, apellidojugador, nombrejugador, seleccion)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_query)
            
            # Insertar cada registro
            for row in rows:
                # Asumiendo que PostgreSQL devuelve: (id_jugador, nombre, apellido, pais, goles_ko)
                session.execute(prepared, (
                    edicion,
                    row[4],  # golesko (goles_ko)
                    row[0],  # id_jugador
                    row[2],  # apellidojugador
                    row[1],  # nombrejugador
                    row[3]   # seleccion (pais)
                ))
            
            print(f"✅ Cargados {len(rows)} goleadores en Cassandra")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_goleadores_ko_edicion_cassandra(edicion):
        """
        Obtener goleadores de fases KO ordenados por goles desde Cassandra
        
        Args:
            edicion (str): Nombre de la edición del mundial
        
        Returns:
            list: Lista de tuplas con los datos de los goleadores
        """
        try:
            session = db_manager.get_cassandra_session()
            
            query_stmt = session.prepare("""
                SELECT id_jugador, nombrejugador, apellidojugador, seleccion, golesko
                FROM goleadores_ko_edicion
                WHERE edicion = ?
            """)
            
            rows = session.execute(query_stmt, (edicion,))
            return list(rows)
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de Cassandra: {e}")
            return []
    
    @staticmethod
    def etl_arbitros_fases_finales(edicion):
        """
        ETL: Extraer árbitros de fases finales desde PostgreSQL y cargar en MongoDB
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2030')
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para árbitros fases finales - {edicion}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_arbitros_fases_finales(%s)", (edicion,))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron árbitros para {edicion}")
                return False
            
            print(f"✅ Extraídos {len(rows)} registros desde PostgreSQL")
            
            # TRANSFORM: Agrupar por partido
            print("🔄 Transformando datos...")
            partidos = defaultdict(lambda: {
                'edicion': edicion,
                'fase': None,
                'idPartido': None,
                'local': None,
                'visitante': None,
                'arbitros': []
            })
            
            for row in rows:
                # row contiene: (edicion, fase, id_partido, local, visitante, arbitro, rol)
                id_partido = row[2]
                
                # Si es la primera vez que vemos este partido, llenar datos básicos
                if partidos[id_partido]['idPartido'] is None:
                    partidos[id_partido]['fase'] = row[1]
                    partidos[id_partido]['idPartido'] = id_partido
                    partidos[id_partido]['local'] = row[3]
                    partidos[id_partido]['visitante'] = row[4]
                
                # Agregar árbitro
                partidos[id_partido]['arbitros'].append({
                    'nombre': row[5],
                    'rol': row[6]
                })
            
            # LOAD: Insertar en MongoDB
            print("📤 Cargando datos en MongoDB...")
            db = db_manager.get_mongodb_db()
            collection = db['arbitros_fases_finales']
            
            # Limpiar datos anteriores de la misma edición
            collection.delete_many({'edicion': edicion})
            
            # Insertar documentos
            documentos = list(partidos.values())
            if documentos:
                collection.insert_many(documentos)
            
            print(f"✅ Cargados {len(documentos)} partidos en MongoDB")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_arbitros_fases_finales_mongodb(edicion):
        """
        Obtener árbitros de fases finales desde MongoDB
        
        Args:
            edicion (str): Nombre de la edición del mundial
        
        Returns:
            list: Lista de documentos con los datos
        """
        try:
            db = db_manager.get_mongodb_db()
            collection = db['arbitros_fases_finales']
            
            # Consultar documentos y ordenar por fase y id de partido
            documentos = list(collection.find(
                {'edicion': edicion},
                {'_id': 0}  # Excluir el _id de los resultados
            ).sort([('fase', 1), ('idPartido', 1)]))
            
            return documentos
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de MongoDB: {e}")
            return []
    
    @staticmethod
    def etl_jugadores_goles_pais(edicion, pais, min_goles):
        """
        ETL: Extraer jugadores con mínimo de goles desde PostgreSQL y cargar en MongoDB
        
        Args:
            edicion (str): Nombre de la edición del mundial (ej: 'Mundial 2026')
            pais (str): País de los jugadores
            min_goles (int): Mínimo de goles (usado solo para extracción inicial)
        
        Returns:
            bool: True si fue exitoso, False si hubo error
        """
        try:
            print(f"\n🔄 Iniciando ETL para jugadores de {pais} - {edicion}...")
            
            # EXTRACT: Obtener datos desde PostgreSQL (con mínimo de goles para extraer)
            print("📥 Extrayendo datos desde PostgreSQL...")
            cursor = db_manager.get_postgresql_cursor()
            
            cursor.execute("SELECT * FROM get_jugadores_pais_min_goles(%s, %s, %s)", (edicion, pais, min_goles))
            
            rows = cursor.fetchall()
            
            if not rows:
                print(f"⚠️  No se encontraron jugadores de {pais} con {min_goles}+ goles")
                return False
            
            print(f"✅ Extraídos {len(rows)} jugadores desde PostgreSQL")
            
            # TRANSFORM: Crear documento con array de jugadores
            print("🔄 Transformando datos...")
            from datetime import datetime
            
            jugadores = []
            for row in rows:
                # row contiene: (id_jugador, nombre, apellido, pais, goles_totales)
                jugadores.append({
                    'id_jugador': row[0],
                    'nombre': row[1],
                    'apellido': row[2],
                    'goles_totales': row[4]
                })
            
            documento = {
                'edicion': edicion,
                'pais': pais,
                'jugadores': jugadores,
                'actualizado_en': datetime.utcnow()
            }
            
            # LOAD: Insertar en MongoDB
            print("📤 Cargando datos en MongoDB...")
            db = db_manager.get_mongodb_db()
            collection = db['jugadores_goleadores']
            
            # Reemplazar documento existente de la misma edición y país
            collection.replace_one(
                {'edicion': edicion, 'pais': pais},
                documento,
                upsert=True
            )
            
            print(f"✅ Cargado documento con {len(jugadores)} jugadores en MongoDB")
            print("✨ ETL completado exitosamente\n")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error en ETL: {e}")
            return False
    
    @staticmethod
    def obtener_jugadores_goles_pais_mongodb(edicion, pais, min_goles):
        """
        Obtener jugadores con mínimo de goles desde MongoDB
        
        Args:
            edicion (str): Nombre de la edición del mundial
            pais (str): País de los jugadores
            min_goles (int): Mínimo de goles para filtrar
        
        Returns:
            list: Lista de jugadores que cumplen el criterio
        """
        try:
            db = db_manager.get_mongodb_db()
            collection = db['jugadores_goleadores']
            
            # Buscar documento del país y edición
            documento = collection.find_one(
                {'edicion': edicion, 'pais': pais},
                {'_id': 0}
            )
            
            if not documento:
                return []
            
            # Filtrar jugadores por mínimo de goles
            jugadores_filtrados = [
                j for j in documento['jugadores'] 
                if j['goles_totales'] >= min_goles
            ]
            
            # Ordenar por goles descendente
            jugadores_filtrados.sort(key=lambda x: x['goles_totales'], reverse=True)
            
            return jugadores_filtrados
            
        except Exception as e:
            print(f"❌ Error obteniendo datos de MongoDB: {e}")
            return []


def etl_partidos_ko_neo4j(db_manager, edicion):
    """
    ETL para cargar el grafo de partidos de eliminación directa en Neo4j
    
    Args:
        db_manager: Instancia del gestor de bases de datos
        edicion: Edición del mundial (nombre como "Mundial 2030")
        
    Returns:
        int: Número de relaciones creadas
    """
    try:
        # Obtener conexiones
        pg_conn = db_manager.pg_conn
        neo4j_driver = db_manager.get_neo4j_driver()
        
        # Primero obtener el id_edicion numérico desde el nombre
        cursor = pg_conn.cursor()
        cursor.execute("""
            SELECT DISTINCT id_edicion 
            FROM vw_partidos_ko_edges 
            WHERE edicion_nombre = %s
        """, (edicion,))
        
        result = cursor.fetchone()
        if not result:
            print(f"⚠️ No se encontró la edición '{edicion}'")
            return 0
        
        id_edicion = result[0]
        print(f"📌 ID de edición: {id_edicion}")
        
        # Extraer datos desde PostgreSQL
        print(f"🔄 Extrayendo datos de vw_partidos_ko_edges para edición {edicion}...")
        cursor.execute("""
            SELECT id_edicion, id_partido, fase, sel_a, pais_a, sel_b, pais_b
            FROM vw_partidos_ko_edges
            WHERE edicion_nombre = %s
        """, (edicion,))
        
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            print(f"⚠️ No se encontraron partidos KO para la edición {edicion}")
            return 0
        
        print(f"📊 {len(rows)} partidos encontrados")
        
        # Cargar en Neo4j
        print(f"🔄 Cargando grafo en Neo4j...")
        
        with neo4j_driver.session() as session:
            # Limpiar TODOS los datos anteriores de esta edición usando el ID numérico
            print(f"🧹 Limpiando datos anteriores de edición {id_edicion}...")
            session.run("""
                MATCH (s:Seleccion)
                WHERE s.id_edicion = $id_edicion
                DETACH DELETE s
            """, id_edicion=id_edicion)
            
            relaciones_creadas = 0
            
            for row in rows:
                id_edicion_val, id_partido, fase, sel_a, pais_a, sel_b, pais_b = row
                
                # MERGE de los nodos de selecciones CON id_edicion como parte de la clave única
                session.run("""
                    MERGE (a:Seleccion {id_seleccion: $sel_a, id_edicion: $id_edicion})
                    ON CREATE SET a.nombre = $pais_a
                    MERGE (b:Seleccion {id_seleccion: $sel_b, id_edicion: $id_edicion})
                    ON CREATE SET b.nombre = $pais_b
                """, sel_a=sel_a, pais_a=pais_a, sel_b=sel_b, pais_b=pais_b, id_edicion=id_edicion_val)
                
                # MERGE de la relación JUEGA_CONTRA (evita duplicados)
                session.run("""
                    MATCH (a:Seleccion {id_seleccion: $sel_a, id_edicion: $id_edicion})
                    MATCH (b:Seleccion {id_seleccion: $sel_b, id_edicion: $id_edicion})
                    MERGE (a)-[r:JUEGA_CONTRA {id_partido: $id_partido, id_edicion: $id_edicion}]->(b)
                    ON CREATE SET r.fase = $fase
                """, sel_a=sel_a, sel_b=sel_b, 
                     id_edicion=id_edicion_val, id_partido=id_partido, fase=fase)
                
                relaciones_creadas += 1
            
            print(f"✅ Grafo cargado: {relaciones_creadas} relaciones creadas")
            return relaciones_creadas
            
    except Exception as e:
        print(f"❌ Error en ETL de Neo4j: {e}")
        return 0


def buscar_camino_eliminacion_neo4j(db_manager, edicion, pais_a, pais_b):
    """
    Busca el camino más corto de eliminación entre dos selecciones en Neo4j
    
    Args:
        db_manager: Instancia del gestor de bases de datos
        edicion: Edición del mundial (nombre como "Mundial 2030")
        pais_a: País de origen
        pais_b: País de destino
        
    Returns:
        dict: Diccionario con 'camino_selecciones' y 'camino_partidos'
    """
    try:
        pg_conn = db_manager.pg_conn
        neo4j_driver = db_manager.get_neo4j_driver()
        
        # Obtener el id_edicion numérico desde PostgreSQL
        cursor = pg_conn.cursor()
        cursor.execute("""
            SELECT DISTINCT id_edicion 
            FROM vw_partidos_ko_edges 
            WHERE edicion_nombre = %s
        """, (edicion,))
        
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"⚠️ No se encontró la edición '{edicion}'")
            return None
        
        id_edicion = result[0]
        print(f"🔍 Buscando en edición ID: {id_edicion}")
        
        with neo4j_driver.session() as session:
            # Buscar camino usando el ID numérico de edición
            result = session.run("""
                MATCH (a:Seleccion {nombre: $pais_a, id_edicion: $id_edicion}), 
                      (b:Seleccion {nombre: $pais_b, id_edicion: $id_edicion}),
                      p = shortestPath((a)-[:JUEGA_CONTRA*]-(b))
                WHERE ALL(r IN relationships(p) WHERE r.id_edicion = $id_edicion)
                RETURN 
                    [n IN nodes(p) | n.nombre] AS camino_selecciones,
                    [r IN relationships(p) | {fase: r.fase, id_partido: r.id_partido}] AS camino_partidos
            """, pais_a=pais_a, pais_b=pais_b, id_edicion=id_edicion)
            
            record = result.single()
            
            if record:
                return {
                    'camino_selecciones': record['camino_selecciones'],
                    'camino_partidos': record['camino_partidos']
                }
            else:
                return None
                
    except Exception as e:
        print(f"❌ Error en búsqueda de camino: {e}")
        return None
