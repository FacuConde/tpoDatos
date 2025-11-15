"""
TPO Ingeniería de Datos II - FIFA
Sistema de consultas para bases de datos del Mundial
"""

import os
from dotenv import load_dotenv
from db_manager import db_manager
from etl_manager import ETLManager

# Cargar variables de entorno
load_dotenv()


class FIFAQuerySystem:
    """Sistema de consultas para datos del Mundial FIFA"""
    
    def __init__(self):
        self.running = True
        
    def mostrar_menu(self):
        """Mostrar el menú principal"""
        print("\n" + "="*70)
        print(" SISTEMA DE CONSULTAS FIFA - INGENIERÍA DE DATOS II")
        print("="*70)
        print("\n📊 OPCIONES DISPONIBLES:\n")
        print("  1. Tabla de posiciones de un grupo")
        print("  2. Árbitros de partidos en fases finales")
        print("  3. Jugadores con ≥3 goles en 2030 (por país)")
        print("  4. Partidos del grupo C ordenados por popularidad")
        print("  5. Partidos por fecha y estadio")
        print("  6. Goles por selección y edición (ranking interno)")
        print("  7. Sesión de periodista (2h)")
        print("  8. Camino corto de eliminación entre dos selecciones")
        print("  9. Goleadores en fases KO de 2030")
        print("\n  0. Salir")
        print("\n" + "="*70)
        
    def ejecutar_opcion(self, opcion):
        """Ejecutar la opción seleccionada"""
        if opcion == "1":
            self.tabla_posiciones_grupo()
        elif opcion == "2":
            self.arbitros_fases_finales()
        elif opcion == "3":
            self.jugadores_goles_2030()
        elif opcion == "4":
            self.partidos_grupo_c_popularidad()
        elif opcion == "5":
            self.partidos_por_fecha_estadio()
        elif opcion == "6":
            self.goles_ranking_interno()
        elif opcion == "7":
            self.sesion_periodista()
        elif opcion == "8":
            self.camino_eliminacion()
        elif opcion == "9":
            self.goleadores_ko_2030()
        elif opcion == "0":
            print("\n👋 Saliendo del sistema...")
            self.running = False
        else:
            print("\n❌ Opción inválida. Por favor, intente nuevamente.")
    
    # ==================================================================
    # CASOS DE USO
    # ==================================================================
    
    def tabla_posiciones_grupo(self):
        """Caso de uso 1: Tabla de posiciones de un grupo"""
        print("\n" + "="*70)
        print("📋 TABLA DE POSICIONES DE UN GRUPO")
        print("="*70)
        
        # Solicitar datos
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2030): ")
        grupo_input = input("🔤 Ingrese el grupo (A-H): ").strip().upper()
        
        # Extraer solo la letra si viene con "Grupo" o "GRUPO"
        if grupo_input.startswith("GRUPO "):
            grupo = grupo_input.replace("GRUPO ", "")
        else:
            grupo = grupo_input
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_cassandra():
                print("❌ No se pudo conectar a Cassandra")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_tabla_posiciones(mundial, grupo):
                # Obtener y mostrar datos desde Cassandra
                print("📊 TABLA DE POSICIONES:")
                print("-" * 70)
                print(f"{'Pos':<5} {'País':<20} {'Pts':<6} {'GF':<6} {'GC':<6} {'DG':<6}")
                print("-" * 70)
                
                rows = ETLManager.obtener_tabla_posiciones_cassandra(mundial, grupo)
                
                if rows:
                    for row in rows:
                        posicion, pais, puntos, gf, gc, dg = row
                        print(f"{posicion:<5} {pais:<20} {puntos:<6} {gf:<6} {gc:<6} {dg:<6}")
                    print("-" * 70)
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def arbitros_fases_finales(self):
        """Caso de uso 2: Árbitros de partidos en fases finales"""
        print("\n" + "="*70)
        print("👨‍⚖️ ÁRBITROS DE FASES FINALES")
        print("="*70)
        
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2030): ")
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            # Forzar reconexión
            db_manager.pg_conn = None
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_mongodb():
                print("❌ No se pudo conectar a MongoDB")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_arbitros_fases_finales(mundial):
                # Obtener y mostrar datos desde MongoDB
                print(f"📊 ÁRBITROS DE FASES FINALES - {mundial}:")
                print("=" * 100)
                
                documentos = ETLManager.obtener_arbitros_fases_finales_mongodb(mundial)
                
                if documentos:
                    for doc in documentos:
                        print(f"\n🏟️  Partido {doc['idPartido']} - {doc['fase'].upper()}")
                        print(f"   {doc['local']} vs {doc['visitante']}")
                        print(f"   Árbitros:")
                        for arbitro in doc['arbitros']:
                            print(f"      • {arbitro['nombre']} ({arbitro['rol']})")
                    
                    print("\n" + "=" * 100)
                    print(f"\nTotal: {len(documentos)} partidos")
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def jugadores_goles_2030(self):
        """Caso de uso 3: Jugadores con ≥3 goles en 2030"""
        print("\n" + "="*70)
        print("⚽ JUGADORES CON MÍNIMO DE GOLES")
        print("="*70)
        
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2026): ")
        pais = input("🌍 Ingrese el país: ")
        min_goles_str = input("⚽ Ingrese el mínimo de goles (ej: 3): ")
        
        try:
            min_goles = int(min_goles_str)
            
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            # Forzar reconexión
            db_manager.pg_conn = None
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_mongodb():
                print("❌ No se pudo conectar a MongoDB")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_jugadores_goles_pais(mundial, pais, min_goles):
                # Obtener y mostrar datos desde MongoDB
                print(f"📊 JUGADORES DE {pais.upper()} CON {min_goles}+ GOLES - {mundial}:")
                print("-" * 80)
                print(f"{'Pos':<6} {'Nombre':<20} {'Apellido':<20} {'⚽ Goles':<10}")
                print("-" * 80)
                
                jugadores = ETLManager.obtener_jugadores_goles_pais_mongodb(mundial, pais, min_goles)
                
                if jugadores:
                    for idx, jugador in enumerate(jugadores, 1):
                        print(f"{idx:<6} {jugador['nombre']:<20} {jugador['apellido']:<20} {jugador['goles_totales']:<10}")
                    print("-" * 80)
                    print(f"\nTotal: {len(jugadores)} jugadores")
                else:
                    print(f"No se encontraron jugadores de {pais} con {min_goles}+ goles")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except ValueError:
            print("\n❌ Error: El mínimo de goles debe ser un número válido")
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def partidos_grupo_c_popularidad(self):
        """Caso de uso 4: Partidos de un grupo ordenados por popularidad"""
        print("\n" + "="*70)
        print("📺 PARTIDOS POR POPULARIDAD")
        print("="*70)
        
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2030): ")
        grupo_input = input("🔤 Ingrese el grupo (A-H): ").strip().upper()
        
        # Extraer solo la letra si viene con "Grupo" o "GRUPO"
        if grupo_input.startswith("GRUPO "):
            grupo = grupo_input.replace("GRUPO ", "")
        else:
            grupo = grupo_input
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_cassandra():
                print("❌ No se pudo conectar a Cassandra")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_partidos_populares(mundial, grupo):
                # Obtener y mostrar datos desde Cassandra
                print(f"📊 PARTIDOS DEL GRUPO {grupo} (ORDENADOS POR POPULARIDAD):")
                print("-" * 100)
                print(f"{'ID':<6} {'Fecha/Hora':<20} {'Estadio':<22} {'Local':<15} {'vs':<4} {'Visitante':<15} {'👥 Pop.':<10}")
                print("-" * 100)
                
                rows = ETLManager.obtener_partidos_populares_cassandra(mundial, grupo)
                
                if rows:
                    for row in rows:
                        id_partido, fecha_hora, estadio, local, visitante, popularidad = row
                        fecha_str = fecha_hora.strftime('%Y-%m-%d %H:%M') if fecha_hora else 'N/A'
                        print(f"{id_partido:<6} {fecha_str:<20} {estadio:<22} {local:<15} vs  {visitante:<15} {popularidad:<10}")
                    print("-" * 100)
                    print(f"\nTotal: {len(rows)} partidos")
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def partidos_por_fecha_estadio(self):
        """Caso de uso 5: Partidos por fecha y estadio"""
        print("\n" + "="*70)
        print("📅 PARTIDOS POR FECHA Y ESTADIO")
        print("="*70)
        
        anio = input("\n📆 Ingrese el año (ej: 2030): ")
        estadio = input("🏟️  Ingrese el nombre del estadio: ")
        
        try:
            # Validar año
            anio_int = int(anio)
            
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            # Forzar reconexión
            db_manager.pg_conn = None
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_cassandra():
                print("❌ No se pudo conectar a Cassandra")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_partidos_fecha_estadio(anio_int, estadio):
                # Obtener y mostrar datos desde Cassandra
                print(f"📊 PARTIDOS EN {estadio} - AÑO {anio}:")
                print("-" * 110)
                print(f"{'ID':<6} {'Fecha/Hora':<20} {'Local':<20} {'vs':<4} {'Visitante':<20} {'Goles':<15}")
                print("-" * 110)
                
                rows = ETLManager.obtener_partidos_fecha_estadio_cassandra(estadio)
                
                if rows:
                    for row in rows:
                        id_partido, fecha, local, visitante, goles_local, goles_visitante = row
                        fecha_str = fecha.strftime('%Y-%m-%d %H:%M') if fecha else 'N/A'
                        goles_str = f"{goles_local} - {goles_visitante}"
                        print(f"{id_partido:<6} {fecha_str:<20} {local:<20} vs  {visitante:<20} {goles_str:<15}")
                    print("-" * 110)
                    print(f"\nTotal: {len(rows)} partidos")
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except ValueError:
            print("\n❌ Error: El año debe ser un número válido")
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def goles_ranking_interno(self):
        """Caso de uso 6: Goles por selección y edición para ranking interno"""
        print("\n" + "="*70)
        print("🎯 GOLES POR SELECCIÓN (RANKING INTERNO)")
        print("="*70)
        
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2026): ")
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_cassandra():
                print("❌ No se pudo conectar a Cassandra")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_goles_seleccion_edicion(mundial):
                # Obtener y mostrar datos desde Cassandra
                print(f"📊 RANKING DE GOLES POR SELECCIÓN - {mundial}:")
                print("-" * 70)
                print(f"{'Posición':<12} {'Selección':<30} {'⚽ Goles':<10}")
                print("-" * 70)
                
                rows = ETLManager.obtener_goles_seleccion_edicion_cassandra(mundial)
                
                if rows:
                    for idx, row in enumerate(rows, 1):
                        seleccion, goles = row
                        print(f"{idx:<12} {seleccion:<30} {goles:<10}")
                    print("-" * 70)
                    print(f"\nTotal: {len(rows)} selecciones")
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def sesion_periodista(self):
        """Caso de uso 7: Sesión de periodista (2h)"""
        print("\n" + "="*70)
        print("📰 SESIÓN DE PERIODISTA (2 HORAS)")
        print("="*70)
        
        periodista = input("\n👤 Ingrese el nombre del periodista: ")
        
        print(f"\n🔍 Iniciando sesión de 2 horas para {periodista}...")
        print("\n⏳ [Funcionalidad por implementar]")
        print("\n💡 Se registrará el acceso con TTL de 2 horas en Redis")
        print("💡 Se mostrarán estadísticas y datos relevantes en tiempo real")
        
        input("\n\nPresione ENTER para continuar...")
    
    def camino_eliminacion(self):
        """Caso de uso 8: Camino corto de eliminación entre dos selecciones"""
        print("\n" + "="*70)
        print("🔗 CAMINO CORTO DE ELIMINACIÓN")
        print("="*70)
        
        pais_a = input("\n🌍 Ingrese la primera selección: ")
        pais_b = input("🌍 Ingrese la segunda selección: ")
        edicion = input("🏆 Ingrese la edición del mundial (ej: 2022): ")
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            # Forzar reconexión
            db_manager.pg_conn = None
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_neo4j():
                print("❌ No se pudo conectar a Neo4j")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL para cargar el grafo
            print(f"\n📊 Cargando grafo de eliminación directa para {edicion}...")
            from etl_manager import etl_partidos_ko_neo4j, buscar_camino_eliminacion_neo4j
            
            relaciones = etl_partidos_ko_neo4j(db_manager, edicion)
            
            if relaciones > 0:
                # Buscar el camino
                print(f"\n🔍 Buscando camino entre {pais_a} y {pais_b}...")
                resultado = buscar_camino_eliminacion_neo4j(db_manager, edicion, pais_a, pais_b)
                
                if resultado and resultado['camino_selecciones']:
                    print(f"\n✅ CAMINO ENCONTRADO:")
                    print("=" * 70)
                    
                    selecciones = resultado['camino_selecciones']
                    partidos = resultado['camino_partidos']
                    
                    print(f"\n🏆 Longitud del camino: {len(partidos)} partido(s)")
                    print(f"\n🗺️  Recorrido:")
                    
                    for i, pais in enumerate(selecciones):
                        print(f"   {'→' if i > 0 else ' '} {pais}")
                        if i < len(partidos):
                            partido = partidos[i]
                            print(f"      📌 {partido['fase']} (Partido #{partido['id_partido']})")
                    
                else:
                    print(f"\n⚠️ No se encontró un camino entre {pais_a} y {pais_b} en {edicion}")
                    print("   Pueden estar en diferentes ramas del torneo o uno fue eliminado antes.")
            else:
                print(f"\n⚠️ No se encontraron datos de eliminación directa para {edicion}")
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    def goleadores_ko_2030(self):
        """Caso de uso 9: Goleadores en fases KO de 2030"""
        print("\n" + "="*70)
        print("👑 GOLEADORES EN FASES KNOCKOUT")
        print("="*70)
        
        mundial = input("\n🏆 Ingrese la edición del mundial (ej: Mundial 2026): ")
        
        try:
            # Conectar a las bases de datos
            print("\n🔌 Conectando a las bases de datos...")
            # Forzar reconexión
            db_manager.pg_conn = None
            if not db_manager.connect_postgresql():
                print("❌ No se pudo conectar a PostgreSQL")
                input("\nPresione ENTER para continuar...")
                return
            
            if not db_manager.connect_cassandra():
                print("❌ No se pudo conectar a Cassandra")
                input("\nPresione ENTER para continuar...")
                return
            
            # Ejecutar ETL
            if ETLManager.etl_goleadores_ko_edicion(mundial):
                # Obtener y mostrar datos desde Cassandra
                print(f"📊 GOLEADORES EN FASES ELIMINATORIAS - {mundial}:")
                print("-" * 90)
                print(f"{'Pos':<6} {'Nombre':<25} {'Apellido':<25} {'País':<20} {'⚽ Goles':<10}")
                print("-" * 90)
                
                rows = ETLManager.obtener_goleadores_ko_edicion_cassandra(mundial)
                
                if rows:
                    for idx, row in enumerate(rows, 1):
                        id_jugador, nombre, apellido, seleccion, goles = row
                        print(f"{idx:<6} {nombre:<25} {apellido:<25} {seleccion:<20} {goles:<10}")
                    print("-" * 90)
                    print(f"\nTotal: {len(rows)} goleadores")
                else:
                    print("No se encontraron datos")
            else:
                print("\n❌ Error ejecutando el ETL")
                
        except Exception as e:
            print(f"\n❌ Error: {e}")
        
        input("\n\nPresione ENTER para continuar...")
    
    # ==================================================================
    # MÉTODOS PRINCIPALES
    # ==================================================================
    
    def run(self):
        """Ejecutar el sistema"""
        print("\n🚀 Iniciando sistema de consultas FIFA...")
        
        try:
            while self.running:
                self.mostrar_menu()
                opcion = input("\n👉 Seleccione una opción: ").strip()
                self.ejecutar_opcion(opcion)
        finally:
            # Cerrar conexiones al salir
            print("\n🔌 Cerrando conexiones...")
            db_manager.close_all()
        
        print("\n✅ Sistema finalizado correctamente.\n")


def main():
    """Función principal"""
    sistema = FIFAQuerySystem()
    sistema.run()


if __name__ == "__main__":
    main()
