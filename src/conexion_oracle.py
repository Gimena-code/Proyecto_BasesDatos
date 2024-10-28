import os
import cx_Oracle
from tabulate import tabulate
from tqdm import tqdm
import time
import subprocess
from colorama import init, Fore, Back, Style

dsn_tns = cx_Oracle.makedsn("localhost", 1521, "XE")
connection = cx_Oracle.connect("SYS", "root", dsn_tns, mode=cx_Oracle.SYSDBA)

def imprimir_barra_progreso_lineal(accion):
    for i in tqdm(range(100), desc=accion, ncols=100, bar_format='{l_bar}{bar} |'):
        time.sleep(0.04)


init(autoreset=True)

def limpiar_pantalla():
    print('\033c', end='')


#FUNCIONES TABLESPACES
def crear_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_tablespace}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"CREATE TABLESPACE {nombre_tablespace} DATAFILE '{ruta_archivo_datos}' SIZE 10M REUSE AUTOEXTEND ON NEXT 10M MAXSIZE 200M")
        imprimir_barra_progreso_lineal("Creando tablespace...")
        print(f"El tablespace {nombre_tablespace} ha sido creado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el tablespace:", error.message)
    connection.commit()
    cursor.close()

def crear_tablespace_temporal():
    nombre_tablespace_temporal = input("Ingrese el nombre del tablespace temporal: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_tablespace_temporal}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"CREATE TEMPORARY TABLESPACE {nombre_tablespace_temporal} TEMPFILE '{ruta_archivo_datos}' SIZE 5M REUSE AUTOEXTEND ON NEXT 5M MAXSIZE 100M")
        imprimir_barra_progreso_lineal("Creando tablespace temporal...")
        print(f"El tablespace temporal {nombre_tablespace_temporal} ha sido creado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el tablespace temporal:", error.message)
    connection.commit()
    cursor.close()

def visualizar_tamaño_tablespace():
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute("SELECT tablespace_name, round((bytes / 1024 / 1024), 2) as size_mb FROM dba_data_files")
        headers = ["Tablespace_name", "Size_MB"]
        data = [[row[0], row[1]] for row in cursor]
        print(tabulate(data, headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar el tamaño del tablespace:", error.message)
    connection.commit()
    cursor.close()

def modificar_tamaño_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace que desea modificar: ")
    ruta_carpeta = input("Ingrese la ruta de la carpeta donde se encuentra el archivo: ")
    tamaño_nuevo = input("Ingrese el nuevo tamaño para el tablespace (por ejemplo, 100M): ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        ruta_completa = f"{ruta_carpeta}\\{nombre_tablespace}.dbf"
        cursor.execute(f"ALTER DATABASE DATAFILE '{ruta_completa}' RESIZE {tamaño_nuevo}")
        imprimir_barra_progreso_lineal("Modificando tablespace...")
        print(f"El tablespace {nombre_tablespace} ha sido modificado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar el tamaño del tablespace:", error.message)
    connection.commit()
    cursor.close()

def agregar_datafile_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    nombre_datafile = input("Ingrese el nombre del datafile: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_datafile}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"ALTER TABLESPACE {nombre_tablespace} ADD DATAFILE '{ruta_archivo_datos}' SIZE 10M REUSE AUTOEXTEND ON NEXT 10M MAXSIZE 200M")
        imprimir_barra_progreso_lineal("Modificando tablespace...")
        print(f"El tablespace {nombre_tablespace} ha sido modificado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar el tablespace:", error.message)
    connection.commit()
    cursor.close()

def eliminar_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DROP TABLESPACE {nombre_tablespace} INCLUDING CONTENTS AND DATAFILES")
        imprimir_barra_progreso_lineal("Eliminando tablespace...")
        print(f"El tablespace {nombre_tablespace} ha sido eliminado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el tablespace:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES USUARIOS 
def crear_usuario():
    try:
        usuario = input("Ingrese el nombre del nuevo usuario: ")
        contraseña = input("Ingrese la contraseña del nuevo usuario: ")
        cursor = connection.cursor()
        cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
        cursor.execute(f"CREATE USER {usuario} IDENTIFIED BY {contraseña}")
        connection.commit()
        cursor.close()
        imprimir_barra_progreso_lineal("Creando usuario...")
        print(f"Se ha creado el usuario {usuario} correctamente.")
        print("\n")
        return True
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error de Oracle:", error.message)
        return False

def borrar_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DROP USER {nombre_usuario} CASCADE")
        imprimir_barra_progreso_lineal("Eliminando usuario...")
        print(f"El usuario {nombre_usuario} ha sido eliminado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el usuario:", error.message)
    connection.commit()
    cursor.close()

def asignar_privilegio_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ").strip().upper()

    privilegios = {
        "1": "CREATE SESSION",
        "2": "SELECT ANY TABLE",
        "3": "INSERT ANY TABLE",
        "4": "UPDATE ANY TABLE",
        "5": "DELETE ANY TABLE",
        "6": "ALTER ANY TABLE",
        "7": "DROP ANY TABLE",
        "8": "CREATE ANY TABLE",
        "9": "CREATE ANY INDEX",
        "10": "CREATE ANY VIEW",
        "11": "CREATE ANY PROCEDURE",
        "12": "DROP USER",
        "13": "GRANT ANY PRIVILEGE"
    }

    seleccionados = []

    while True:
        print("Lista de privilegios disponibles:")
        for i, privilegio in privilegios.items():
            print(f"{i}. {privilegio}")

        opcion_privilegio = input("Ingrese el número correspondiente al privilegio que desea asignar o presione Enter para salir: ")

        if opcion_privilegio in privilegios:
            privilegio_elegido = privilegios[opcion_privilegio]
            seleccionados.append(privilegio_elegido)
            print(f"Privilegio '{privilegio_elegido}' agregado a la lista.")

            respuesta = input("¿Desea agregar otro privilegio? (S/N): ")
            if respuesta.upper() != "S":
                break
        elif opcion_privilegio == "":
            break
        else:
            print("Opción no válida. Por favor, ingrese el número correspondiente al privilegio.")

    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")

    try:
        for privilegio in privilegios:
            cursor.execute(f"GRANT {privilegio} TO {nombre_usuario}")
            imprimir_barra_progreso_lineal("Asignando privilegio...")
            print(f"El privilegio {privilegio} ha sido asignado al usuario {nombre_usuario} exitosamente.")
            print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al asignar el privilegio al usuario:", error.message)
    connection.commit()
    cursor.close()

def revocar_privilegio_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = '{nombre_usuario}'")
        rows = cursor.fetchall()
        if rows:
            print(f"Los privilegios del usuario {nombre_usuario} son:")
            for i, row in enumerate(rows, 1):
                print(f"{i}. {row[0]}")

            privilegio_seleccionado = int(input("Ingrese el número correspondiente al privilegio que desea revocar: "))

            if 1 <= privilegio_seleccionado <= len(rows):
                nombre_privilegio = rows[privilegio_seleccionado - 1][0]
                cursor.execute(f"REVOKE {nombre_privilegio} FROM {nombre_usuario}")
                imprimir_barra_progreso_lineal("Eliminando privilegio...")
                print(f"El privilegio {nombre_privilegio} ha sido revocado del usuario {nombre_usuario} exitosamente.")
                print("\n")
            else:
                print("La opción ingresada no es válida.")
        else:
            print(f"El usuario {nombre_usuario} no tiene ningún privilegio asignado.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al revocar el privilegio al usuario:", error)
    connection.commit()
    cursor.close()

def visualizar_privilegios_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = '{nombre_usuario}'")
        rows = cursor.fetchall()
        if rows:
            print(f"Los privilegios del usuario {nombre_usuario} son:")
            for row in rows:
                print(row[0])
        else:
            print(f"El usuario {nombre_usuario} no tiene ningún privilegio asignado.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar los privilegios del usuario:", error)
    connection.commit()
    cursor.close()

#FUNCIONES ROLES 
def crear_rol():
    try:
        rol = input("Ingrese el nombre del nuevo rol: ")
        cursor = connection.cursor()
        cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
        cursor.execute(f"CREATE ROLE {rol}")
        connection.commit()
        cursor.close()
        imprimir_barra_progreso_lineal("Creando rol...")
        print(f"Se ha creado el rol {rol} correctamente.")
        print("\n")

        return True

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error de Oracle:", error.message)
        return False

def borrar_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DROP ROLE {nombre_rol}")
        imprimir_barra_progreso_lineal("Eliminando rol...")
        print(f"El rol {nombre_rol} ha sido eliminado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el rol:", error.message)
    connection.commit()
    cursor.close()

def asignar_privilegio_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")

    lista_privilegios = [
        "CREATE SESSION",
        "SELECT ANY TABLE",
        "INSERT ANY TABLE",
        "UPDATE ANY TABLE",
        "DELETE ANY TABLE",
        "ALTER ANY TABLE",
        "DROP ANY TABLE",
        "CREATE ANY TABLE",
        "CREATE ANY INDEX",
        "CREATE ANY VIEW",
        "CREATE ANY PROCEDURE",
        "DROP USER",
        "GRANT ANY PRIVILEGE"
    ]
    privilegios = []
    while True:
        print("Lista de privilegios disponibles:")
        for i, privilegio in enumerate(lista_privilegios, 1):
            print(f"{i}. {privilegio}")

        opcion_privilegio = input("Ingrese el número correspondiente al privilegio que desea asignar o presione Enter para salir: ")

        if opcion_privilegio.isdigit() and 1 <= int(opcion_privilegio) <= len(lista_privilegios):
            privilegio_elegido = lista_privilegios[int(opcion_privilegio) - 1]
            privilegios.append(privilegio_elegido)

            respuesta = input("¿Desea agregar otro privilegio? (S/N): ")
            if respuesta.upper() != "S":
                break
        elif opcion_privilegio == "":
            break
        else:
            print("Opción no válida. Por favor, ingrese el número correspondiente al privilegio.")

    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        for privilegio in privilegios:
            cursor.execute(f"GRANT {privilegio} TO {nombre_rol}")
            imprimir_barra_progreso_lineal("Asignando privilegio...")
            print(f"El privilegio {privilegio} ha sido asignado al rol {nombre_rol} exitosamente.")
            print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al asignar el privilegio al rol:", error.message)
    connection.commit()
    cursor.close()

def revocar_privilegio_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = '{nombre_rol}'")
        rows = cursor.fetchall()
        if rows:
            print(f"Los privilegios del rol {nombre_rol} son:")
            for i, row in enumerate(rows, 1):
                print(f"{i}. {row[0]}")

            privilegio_seleccionado = int(input("Ingrese el número correspondiente al privilegio que desea revocar: "))

            if 1 <= privilegio_seleccionado <= len(rows):
                nombre_privilegio = rows[privilegio_seleccionado - 1][0]
                cursor.execute(f"REVOKE {nombre_privilegio} FROM {nombre_rol}")
                imprimir_barra_progreso_lineal("Eliminando privilegio...")
                print(f"El privilegio {nombre_privilegio} ha sido revocado del rol {nombre_rol} exitosamente.")
                print("\n")
            else:
                print("La opción ingresada no es válida.")
        else:
            print(f"El rol {nombre_rol} no tiene ningún privilegio asignado.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al revocar el privilegio al rol:", error)
    connection.commit()
    cursor.close()

def visualizar_privilegios_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = '{nombre_rol}'")
        rows = cursor.fetchall()
        if rows:
            print(f"Los privilegios del rol {nombre_rol} son:")
            for row in rows:
                print(row[0])
        else:
            print(f"El rol {nombre_rol} no tiene ningún privilegio asignado.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar los privilegios del rol:", error)
    connection.commit()
    cursor.close()

#FUNCIONES INDICES 
def crear_indice():
    nombre_indice = input("Ingrese el nombre del índice: ")
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"CREATE INDEX {nombre_indice} ON {nombre_tabla}({nombre_columna})")
        imprimir_barra_progreso_lineal("Creando índice...")
        print(f"El índice {nombre_indice} ha sido creado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el índice:", error.message)
    connection.commit()
    cursor.close()

def eliminar_indice():
    nombre_indice = input("Ingrese el nombre del índice: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DROP INDEX {nombre_indice}")
        imprimir_barra_progreso_lineal("Eliminando índice...")
        print(f"El índice {nombre_indice} ha sido eliminado exitosamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el índice:", error.message)
    connection.commit()
    cursor.close()

def monitorear_indices():
    cursor = connection.cursor()
    try:
        cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
        cursor.execute("SELECT index_name FROM user_indexes")
        cantidad = input("¿Cuantos indices quiere ver?(1887): ")
        try:
            cantidad = int(cantidad)  # Convertir a entero
        except ValueError:
            print("Por favor, ingrese un número válido.")
            return  # Salir de la función si la conversión falla

        print("│           Nombre del Índice        │")
        contador = 0
        
        # Iterar sobre los resultados y mostrarlos
        for row in cursor:
            if contador < cantidad:  # Limitar a los primeros 10 índices
                print(f"│    {row[0]:<25}       │")  # Formatear el nombre del índice
                contador += 1
            else:
                break  # Salir del bucle después de 10 índices

        print("╘═══════════════════════════════════╛")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al monitorear los índices:", error)
    connection.commit()
    cursor.close()

#PLAN EJECUCION
def plan_ejecucion():
    consulta_sql = input("Ingrese la consulta SQL para la cual desea generar el plan de ejecución: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"EXPLAIN PLAN FOR {consulta_sql}")
        imprimir_barra_progreso_lineal("Generando plan de ejecución...")
        print("Plan de ejecución generado correctamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al generar el plan de ejecución:", error)
    connection.commit()
    cursor.close()

def visualizar_plan_ejecucion():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)")
        
        print("\nPlan de ejecución:")
        print("----------------------------------------------------------------------------------")
        for row in cursor:
            print(row[0])
        print("----------------------------------------------------------------------------------")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar los planes de ejecución:", error.message)
    connection.commit()
    cursor.close()

def eliminar_planes_ejecucion():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute("DELETE PLAN_TABLE")
        print("Se han eliminado todos los planes de ejecución correctamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar los planes de ejecución:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES ESTADISTICAS
def recopilar_estadisticas_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => USER, tabname => '{nombre_tabla}'); END;")
        imprimir_barra_progreso_lineal("Recopilando estadísticas...")
        print(f"Se han recopilado las estadísticas para la tabla {nombre_tabla} correctamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al recopilar estadísticas para la tabla:", error.message)
    connection.commit()
    cursor.close()

def ver_estadisticas_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT table_name, num_rows, last_analyzed FROM USER_TABLES WHERE TABLE_NAME = '{nombre_tabla}'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al ver las estadísticas de la tabla:", error.message)
    connection.commit()
    cursor.close()

def eliminar_estadisticas_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"BEGIN DBMS_STATS.DELETE_TABLE_STATS(ownname => USER, tabname => '{nombre_tabla}'); END;")
        imprimir_barra_progreso_lineal("Eliminando estadísticas...")
        print(f"Se han eliminado las estadísticas para la tabla {nombre_tabla} correctamente.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar estadísticas para la tabla:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES PERFORMANCE BASE DE DATOS
def Vista_estadoBD():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM V$INSTANCE")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar estado de la base de datos:", error.message)
    connection.commit()
    cursor.close()

def Parametros_Generales():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM V$SYSTEM_PARAMETER")
        headers = ["Numero", "Nombre", "Tipo", "Valor"]
        data = [[row[0], row[1],row[2],row[3]] for row in cursor]
        print(tabulate(data, headers, tablefmt="grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error a la consulta de parametros generales:", error.message)
    cursor.close()

def Parametros_Generales_All():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM V$SYSTEM_PARAMETER")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error a la consulta de parametros generales:", error.message)
    cursor.close()


def Know_Version():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'compatible'")
        rows = [row for row in cursor]
        print(tabulate(rows, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar version:", error.message)
    connection.commit()
    cursor.close()

def Name_Spfile():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"select value from v$system_parameter where name = 'spfile'")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar ubicacion y nombre del spfile:", error.message)
    connection.commit()
    cursor.close()

def Name_ControlFiles():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'control_files'")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar ubicacion y nombre de los ficheros de control:", error.message)
    connection.commit()
    cursor.close()

def Name_DB():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'db_name'")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar nombre de la base de datos:", error.message)
    connection.commit()
    cursor.close()

def Actual_Cx():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT OSUSER, USERNAME, MACHINE, PROGRAM FROM V$SESSION ORDER BY OSUSER")
        rows = [row for row in cursor]
        headers = ["OSUSER", "USERNAME", "MACHINE", "PROGRAM"]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar las conexiones actuales:", error.message)
    connection.commit()
    cursor.close()

def Objects():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT OWNER, COUNT(OWNER) Numero FROM DBA_OBJECTS GROUP BY OWNER")
        rows = [row for row in cursor]
        headers = ["Owner", "Numero"]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los propietarios por objetos y numero de objetos:", error.message)
    connection.commit()
    cursor.close()

def Tables_ActualUser():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM USER_TABLES")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar las tablas del usuario actual:", error.message)
    connection.commit()
    cursor.close()

def User_Products():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM USER_CATALOG")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los productos del usuario:", error.message)
    connection.commit()
    cursor.close()

def Usser_Cx():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT USERNAME USUARIO_ORACLE, COUNT(USERNAME) NUMERO_SESIONES FROM V$SESSION GROUP BY USERNAME ORDER BY NUMERO_SESIONES DESC")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los usuarios conectados:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES AUDITORIA --FALTA

def Active_Cx():
    cursor = connection.cursor()
    #cambiar el estado de activada a desactivada
    #cursor.execute (f"ALTER SYSTEM SET audit_trail = 'NONE' SCOPE=SPFILE")
    #activa la auditoria
    cursor.execute (f"alter system set audit_trail = DB scope = spfile")
    try:
        print("Activando")
        cursor.execute (f"audit connect")
        connection.commit()
        Mostrar_Auditoría()
        #cursor.execute (f"select name , value from v$parameter where name like 'audit_trail'")
        #for row in cursor:
        #    print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al activar la auditoria:", error.message)
    connection.commit()
    cursor.close()


def Mostrar_Auditoría():
    cursor = connection.cursor()
    try:
        cursor.execute (f"select name , value from v$parameter where name like 'audit_trail'")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar la auditoria:", error.message)
    connection.commit()
    cursor.close()


def Vizualizar_Tables_Auditoria():
    cursor = connection.cursor()
    try:
        cursor.execute("select username , action_name , priv_used , returncode from dba_audit_trail")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
    # En caso de error en la conexión, muestra el error
        print("Error al vizualizar las tablas de auditoría :", e)
    connection.commit()
    cursor.close()


#FUNCIONES RESPALDOS CON EXPDP e IMPDP --FALTA PROBAR
def crear_directorio():
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    ruta_carpeta = input("Ingrese la ruta de la carpeta donde desea guardar el directorio: ")
    ruta_completa = f"{ruta_carpeta}\\{nombre_directorio}"
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"CREATE DIRECTORY {nombre_directorio} AS '{ruta_completa}'")
        cursor.execute(f"GRANT READ, WRITE ON DIRECTORY {nombre_directorio} TO SYSTEM")
        imprimir_barra_progreso_lineal("Creando directorio...")
        print(f"El directorio {nombre_directorio} ha sido creado exitosamente en la ruta {ruta_completa}.")
        print("\n")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el directorio:", error.message)
    connection.commit()
    cursor.close() 

def respaldo_esquema():
    nombre_esquema = input("Ingrese el nombre del esquema: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.check_call([
            "expdp",
            f"system/root@localhost:1521/XE",
            f"schemas={nombre_esquema}",
            f"directory={nombre_directorio}",
            f"dumpfile={nombre_respaldo}.dmp",
            f"logfile={nombre_respaldo}.log"])
        print(f"El respaldo {nombre_respaldo} ha sido creado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al crear el respaldo:", e)

def recuperar_respaldo_esquema():
    nombre_esquema = input("Ingrese el nombre del esquema: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.check_call([
            "impdp",
            f"system/root@localhost:1521/XE",
            f"schemas={nombre_esquema}",
            f"directory={nombre_directorio}",
            f"dumpfile={nombre_respaldo}.dmp",
            f"logfile={nombre_respaldo}.log"])
        print(f"El respaldo {nombre_respaldo} ha sido recuperado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al recuperar el respaldo:", e)

def respaldo_tabla():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.check_call([
            "expdp",
            f"system/root@localhost:1521/XE",
            f"tables='{nombre_usuario}.{nombre_tabla}'",
            f"directory='{nombre_directorio}'",
            f"dumpfile='{nombre_respaldo}.dmp'",
            f"logfile='{nombre_respaldo}.log'"])
        print(f"El respaldo {nombre_respaldo} ha sido creado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al crear el respaldo:", e)

def recuperar_respaldo_tabla():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.check_call([
            "impdp",
            f"system/root@localhost:1521/XE",
            f"tables='{nombre_usuario}.{nombre_tabla}'",
            f"directory='{nombre_directorio}'",
            f"dumpfile='{nombre_respaldo}.dmp'",
            f"logfile='{nombre_respaldo}.log'"])
        print(f"El respaldo {nombre_respaldo} ha sido recuperado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al recuperar el respaldo:", e)

#MENÚS
def mostrar_menu_principal():
    print(Fore.GREEN + Back.BLACK + "Bienvenido al sistema de administración de bases de datos Oracle.\n")
    data = [
        ["1", "Administración de tablespaces y seguridad"],
        ["2", "Tunning de consultas"],
        ["3", "Performance de la base de datos"],
        ["4", "Auditoría de la base de datos"],
        ["5", "Administración de archivos de respaldos y directorios"],
        ["0", "Salir"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_administracion_tablespaces():
    print(Fore.GREEN + Back.BLACK + "\nAdministración de tablespaces y seguridad:\n")
    data = [
        ["1", "Tablespaces"],
        ["2", "Usuarios"],
        ["3", "Roles"],
        ["0", "Volver al menú principal"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_tablespaces():
    print(Fore.GREEN + Back.BLACK + "\nTablespaces:\n")
    data = [
        ["1", "Crear nuevo tablespace"],
        ["2", "Crear nuevo tablespace temporal"],
        ["3", "Visualizar tamaño"],
        ["4", "Modificar el tamaño"],
        ["5", "Agregar un datafile"],
        ["6", "Borrar un tablespace"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_usuarios():
    print(Fore.GREEN + Back.BLACK + "\nUsuarios:\n")
    data = [
        ["1", "Crear nuevo usuario"],
        ["2", "Asignar privilegio"],
        ["3", "Visualizar privilegios"],
        ["4", "Quitar privilegios"],
        ["5", "Borrar usuario"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_roles():
    print(Fore.GREEN + Back.BLACK + "\nRoles:\n")
    data = [
        ["1", "Crear nuevo rol"],
        ["2", "Asignar privilegio"],
        ["3", "Visualizar privilegios"],
        ["4", "Quitar privilegios"],
        ["5", "Borrar rol"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_tunning_consultas():
    print(Fore.GREEN + Back.BLACK + "\nTunning de consultas:\n")
    data = [
        ["1", "Índices"],
        ["2", "Plan de ejecución"],
        ["3", "Estadísticas"],
        ["0", "Volver al menú principal"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_indices():
    print(Fore.GREEN + Back.BLACK + "\nÍndices:\n")
    data = [
        ["1", "Crear un índice"],
        ["2", "Monitorear índices"],
        ["3", "Borrar índices"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_ejecucion():
    print(Fore.GREEN + Back.BLACK + "\nPlan de Ejecución:\n")
    data = [
        ["1", "Crear un plan de ejecución"],
        ["2", "Visualizar los planes de ejecución generados"],
        ["3", "Eliminar todos los planes de ejecución"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_estadisticas():
    print(Fore.GREEN + Back.BLACK + "\nEstadísticas:\n")
    data = [
        ["1", "Recopilar estadísticas de una tabla"],
        ["2", "Ver estadísticas"],
        ["3", "Eliminar estadísticas"],
        ["0", "Volver"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_performance_bd():
    print(Fore.GREEN + Back.BLACK + "\nPerformance de la base de datos:\n")
    data = [
        ["1", "Estado de la base de datos"],
        ["2", "Parámetros generales en tabla"],
        ["3", "Versión"],
        ["4", "Ubicación y nombre de SPFILE"],
        ["5", "Ubicación y número de ficheros de control"],
        ["6", "Nombre de la base de datos"],
        ["7", "Conexiones actuales"],
        ["8", "Usuarios conectados y número de sesiones"],
        ["9", "Propietarios por objetos y número de objetos"],
        ["10", "Tablas sobre propiedad del usuario actual"],
        ["11", "Todos los productos del usuario"],
        ["12", "Parámetros generales desplegados"],
        ["0", "Volver al menú principal"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_auditoria():
    print(Fore.GREEN + Back.BLACK + "\nAuditoría de la base de datos:\n")
    data = [
        ["1", "Activar auditoría"],
        ["2", "Visualizar las tablas de auditoría"],
        ["0", "Volver al menú principal"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))

def mostrar_menu_respaldos():
    print(Fore.GREEN + Back.BLACK + "\nAdministración de archivos de respaldos y directorios:\n")
    data = [
        ["1", "Crear directorio"],
        ["2", "Crear respaldos de un esquema"],
        ["3", "Recuperar respaldo de un esquema"],
        ["4", "Crear respaldo de una tabla"],
        ["5", "Recuperar respaldo de una tabla"],
        ["0", "Volver al menú principal"]
    ]
    print(tabulate(data, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))


def mostrar_menu():
    while True:
        limpiar_pantalla()
        mostrar_menu_principal()
        opcion = input("Seleccione una opción: ")

        if opcion == "1":
            while True:
                input("Presione Enter para continuar...")
                limpiar_pantalla()
                mostrar_menu_administracion_tablespaces()
                opcion_administracion_tablespaces = input("Seleccione una opción: ")
                if opcion_administracion_tablespaces == "1":#tablespaces
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_tablespaces()
                        opcion_tablespaces = input("Seleccione una opción: ")
                        if opcion_tablespaces == "1":
                            crear_tablespace()
                        elif opcion_tablespaces == "2":
                            crear_tablespace_temporal()
                        elif opcion_tablespaces == "3":
                            visualizar_tamaño_tablespace()
                        elif opcion_tablespaces == "4":
                            modificar_tamaño_tablespace()
                        elif opcion_tablespaces == "5":
                            agregar_datafile_tablespace()
                        elif opcion_tablespaces == "6":
                            eliminar_tablespace()
                        elif opcion_tablespaces == "0":
                            break

                elif opcion_administracion_tablespaces == "2":#usuarios
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_usuarios()
                        opcion_usuarios = input("Seleccione una opción: ")
                        if opcion_usuarios == "1":
                            crear_usuario()
                        elif opcion_usuarios == "2":
                            asignar_privilegio_usuario()
                        elif opcion_usuarios == "3":
                            visualizar_privilegios_usuario()
                        elif opcion_usuarios == "4":
                            revocar_privilegio_usuario()
                        elif opcion_usuarios == "5":
                            borrar_usuario()
                        elif opcion_usuarios == "0":
                            break

                elif opcion_administracion_tablespaces == "3":#roles
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_roles()
                        opcion_roles = input("Seleccione una opción: ")
                        if opcion_roles == "1":
                            crear_rol()
                        elif opcion_roles == "2":
                            asignar_privilegio_rol()
                        elif opcion_roles == "3":
                            visualizar_privilegios_rol()
                        elif opcion_roles == "4":
                            revocar_privilegio_rol()
                        elif opcion_roles == "5":
                            borrar_rol()
                        elif opcion_roles == "0":
                            break
                elif opcion_administracion_tablespaces == "0":
                    break

        elif opcion == "2":
            while True:
                input("Presione Enter para continuar...")
                limpiar_pantalla()
                mostrar_menu_tunning_consultas()
                opcion_tunning = input("Seleccione una opción: ")
                if opcion_tunning == "1":#indices
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_indices()
                        opcion_indices = input("Seleccione una opción: ")
                        if opcion_indices == "1":
                            crear_indice()
                        elif opcion_indices == "2":
                            monitorear_indices()
                        elif opcion_indices == "3":
                            eliminar_indice()
                        elif opcion_indices == "0":
                            break

                elif opcion_tunning == "2":#Plan Ejecución
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_ejecucion()
                        opcion_ejecucion = input("Seleccione una opción: ")
                        if opcion_ejecucion == "1":
                            plan_ejecucion()
                        elif opcion_ejecucion == "2":
                            visualizar_plan_ejecucion()
                        elif opcion_ejecucion == "3":
                            eliminar_planes_ejecucion()
                        elif opcion_ejecucion == "0":
                            break
                elif opcion_tunning == "3":#Estadisticas
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_estadisticas()
                        opcion_estadisticas = input("Seleccione una opción: ")
                        if opcion_estadisticas == "1":
                            recopilar_estadisticas_tabla()
                        elif opcion_estadisticas == "2":
                            ver_estadisticas_tabla()
                        elif opcion_estadisticas == "3":
                            eliminar_estadisticas_tabla()
                        elif opcion_estadisticas == "0":
                            break
                elif opcion_tunning == "0":
                    break

        elif opcion == "3":
            while True:
                input("Presione Enter para continuar...")
                limpiar_pantalla()
                mostrar_menu_performance_bd()
                opcion_performance = input("Seleccione una opción: ")
                if opcion_performance == "1":
                    print("Vista del estado de la base de datos")
                    Vista_estadoBD()
                elif opcion_performance == "2":
                    print("Consulta Parametros generales")
                    Parametros_Generales()
                elif opcion_performance == "3":
                    print("Consulta la version")
                    Know_Version()
                elif opcion_performance == "4":
                    print("Consulta de ubicacion y nombre de spfile")
                    Name_Spfile()
                elif opcion_performance == "5":
                    print("Consulta ubicacion y numero de ficheros de control")
                    Name_ControlFiles()
                elif opcion_performance == "6":
                    print("Consulta el nombre de la base de datos")
                    Name_DB()
                elif opcion_performance == "7":
                    print("Consulta de las conexiones actuales")
                    Actual_Cx()
                elif opcion_performance == "8":
                    print("Consulta de usuarios conectados y número de sesiones")
                    Usser_Cx()
                elif opcion_performance == "9":
                    print("Propietarios por objetos y número de objetos")
                    Objects()
                elif opcion_performance == "10":
                    print("Consulta las tablas propiedad del usuario actual")
                    Tables_ActualUser()
                elif opcion_performance == "11":
                    print("Consulta todos los productos del usuario")
                    User_Products()
                elif opcion_performance == "12":
                    print("Consulta Parametros generales Desplegados")
                    Parametros_Generales_All()
                elif opcion_performance == "0":
                    break
        elif opcion == "4":
            while True:
                input("Presione Enter para continuar...")
                limpiar_pantalla()
                mostrar_menu_auditoria()
                opcion_auditoria = input("Seleccione una opción: ")
                if opcion_auditoria == "1":
                    print("ACTIVAR AUDITORIA")
                    Active_Cx()
                elif opcion_auditoria == "2":
                    print("VISUALIZAR LAS TABLAS DE AUDITORÍA")
                    Vizualizar_Tables_Auditoria()
                elif opcion_auditoria == "0":
                    break

        elif opcion == "5":
            while True:
                input("Presione Enter para continuar...")
                limpiar_pantalla()
                mostrar_menu_respaldos()
                opcion_respaldos = input("Seleccione una opción: ")
                if opcion_respaldos == "1":
                    crear_directorio()
                elif opcion_respaldos == "2":
                    respaldo_esquema()
                elif opcion_respaldos == "3":
                    recuperar_respaldo_esquema()
                elif opcion_respaldos == "4":
                    respaldo_tabla()
                elif opcion_respaldos == "5":
                    recuperar_respaldo_tabla()
                elif opcion_respaldos == "0":
                    break

        elif opcion == "0":
            print("Saliendo del programa...")
            break

        else:
            print("Opción no válida. Intente nuevamente.")

if __name__ == '__main__':
    mostrar_menu()

connection.close()