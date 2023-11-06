import os
import cx_Oracle
from tabulate import tabulate
from tqdm import tqdm
import time

dsn_tns = cx_Oracle.makedsn("localhost", 1521, "XE")
connection = cx_Oracle.connect("SYS", "root", dsn_tns, mode=cx_Oracle.SYSDBA)

def imprimir_barra_progreso_lineal(accion):
    for i in tqdm(range(100), desc=accion, ncols=100, bar_format='{l_bar}{bar} |'):
        time.sleep(0.04)


def limpiar_pantalla():
    if os.name == 'nt':
        _ = os.system('cls')
    else:
        _ = os.system('clear')

#FUNCIONES TABLESPACES --LISTO
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

#FUNCIONES USUARIOS --LISTO
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
    nombre_usuario = input("Ingrese el nombre del usuario: ")

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
        # Agrega aquí más privilegios según sea necesario
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

#FUNCIONES ROLES --LISTO
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

#FUNCIONES TABLAS --LISTO FALTA BARRA DE PROGRESO
def crear_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"CREATE TABLE {nombre_tabla} (id NUMBER PRIMARY KEY, nombre VARCHAR2(50), apellido VARCHAR2(50), edad NUMBER, correo VARCHAR2(50))")
        print(f"La tabla {nombre_tabla} ha sido creada exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear la tabla:", error.message)
    connection.commit()
    cursor.close()

def eliminar_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DROP TABLE {nombre_tabla}")
        print(f"La tabla {nombre_tabla} ha sido eliminada exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar la tabla:", error.message)
    connection.commit()
    cursor.close()

def insertar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    id = input("Ingrese el id: ")
    nombre = input("Ingrese el nombre: ")
    apellido = input("Ingrese el apellido: ")
    edad = input("Ingrese la edad: ")
    correo = input("Ingrese el correo: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"INSERT INTO {nombre_tabla} VALUES ({id}, '{nombre}', '{apellido}', {edad}, '{correo}')")
        print(f"Los datos han sido insertados exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al insertar los datos:", error.message)
    connection.commit()
    cursor.close()

def consultar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM {nombre_tabla}")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al consultar los datos:", error.message)
    connection.commit()
    cursor.close()

def modificar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    id = input("Ingrese el id: ")
    nombre = input("Ingrese el nombre: ")
    apellido = input("Ingrese el apellido: ")
    edad = input("Ingrese la edad: ")
    correo = input("Ingrese el correo: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"UPDATE {nombre_tabla} SET nombre = '{nombre}', apellido = '{apellido}', edad = {edad}, correo = '{correo}' WHERE id = {id}")
        print(f"Los datos han sido modificados exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar los datos:", error.message)
    connection.commit()
    cursor.close()

def eliminar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    id = input("Ingrese el id: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"DELETE FROM {nombre_tabla} WHERE id = {id}")
        print(f"Los datos han sido eliminados exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar los datos:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES INDICES --LISTO
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
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al monitorear los índices:", error)
    connection.commit()
    cursor.close()

#PLAN EJECUCION --LISTO
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
        for row in cursor:
            print(row)
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

#FUNCIONES ESTADISTICAS --LISTO
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

#FUNCIONES PERFORMANCE BASE DE DATOS --FALTA TABULAR LOS RESULTADOS DE CONSULTAS
def Vista_estadoBD():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM V$INSTANCE")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar estado de la base de datos:", error.message)
    connection.commit()
    cursor.close()

def Parametros_Generales():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT * FROM V$SYSTEM_PARAMETER")
        rows = [row for row in cursor]
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(rows, headers=headers, tablefmt="jira"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error a la consulta de parametros generales:", error)
    connection.commit()
    cursor.close()

def Know_Version():
    cursor = connection.cursor()
    cursor.execute("alter session set \"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'compatible'")
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
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
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los usuarios conectados:", error.message)
    connection.commit()
    cursor.close()

#FUNCIONES AUDITORIA --FALTA

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
        print(f"El directorio {nombre_directorio} ha sido creado exitosamente en la ruta {ruta_completa}.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el directorio:", error.message)
    connection.commit()
    cursor.close() 

def crear_respaldo_full():
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"expdp system/root@localhost:1521/XE full=y directory={nombre_directorio} dumpfile={nombre_respaldo}.dmp logfile={nombre_respaldo}.log")
        print(f"El respaldo {nombre_respaldo} ha sido creado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el respaldo:", error.message)
    connection.commit()
    cursor.close()

def recuperar_respaldo_full():
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"impdp system/root@localhost:1521/XE full=y directory={nombre_directorio} dumpfile={nombre_respaldo}.dmp logfile={nombre_respaldo}.log")
        print(f"El respaldo {nombre_respaldo} ha sido recuperado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al recuperar el respaldo:", error.message)
    connection.commit()
    cursor.close()

def respaldo_esquema():
    nombre_esquema = input("Ingrese el nombre del esquema: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"expdp system/root@localhost:1521/XE schemas={nombre_esquema} directory={nombre_directorio} dumpfile={nombre_respaldo}.dmp logfile={nombre_respaldo}.log")
        print(f"El respaldo {nombre_respaldo} ha sido creado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el respaldo:", error.message)
    connection.commit()
    cursor.close()

def recuperar_respaldo_esquema():
    nombre_esquema = input("Ingrese el nombre del esquema: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    cursor = connection.cursor()
    cursor.execute ("alter session set\"_ORACLE_SCRIPT\" = true")
    try:
        cursor.execute(f"impdp system/root@localhost:1521/XE schemas={nombre_esquema} directory={nombre_directorio} dumpfile={nombre_respaldo}.dmp logfile={nombre_respaldo}.log")
        print(f"El respaldo {nombre_respaldo} ha sido recuperado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al recuperar el respaldo:", error.message)
    connection.commit()
    cursor.close()

import subprocess

def respaldo_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.run([f"expdp system/root@localhost:1521/XE tables='{nombre_tabla}' directory='{nombre_directorio}' dumpfile='{nombre_respaldo}.dmp' logfile='{nombre_respaldo}.log'"], shell=True, check=True)
        print(f"El respaldo {nombre_respaldo} ha sido creado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al crear el respaldo:", e)

def recuperar_respaldo_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_respaldo = input("Ingrese el nombre del respaldo: ")
    nombre_directorio = input("Ingrese el nombre del directorio: ")
    try:
        subprocess.run([f"impdp system/root@localhost:1521/XE tables='{nombre_tabla}' directory='{nombre_directorio}' dumpfile='{nombre_respaldo}.dmp' logfile='{nombre_respaldo}.log'"], shell=True, check=True)
        print(f"El respaldo {nombre_respaldo} ha sido recuperado exitosamente.")
    except subprocess.CalledProcessError as e:
        print("Error al recuperar el respaldo:", e)





#MENUS
def mostrar_menu_principal():
    print("1) Administración de tablespaces y seguridad\n")
    print("2) Tunning de consultas\n")
    print("3) Performance de la base de datos\n")
    print("4) Auditoría de la base de datos\n")
    print("5) Administración de archivos de respaldos y directorios\n")
    print("0) Salir\n")

def mostrar_menu_administracion_tablespaces():
    print("1) Tablespaces\n")
    print("2) Usuarios\n")
    print("3) Roles\n")
    print("4) Tablas\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_tablespaces():
    print("1) Crear nuevo tablespace\n")
    print("2) Crear nuevo tablespace temporal\n")
    print("3) Visualizar tamaño\n")
    print("4) Modificar el tamaño\n")
    print("5) Agregar un datafile\n")
    print("6) Borrar un tablespace\n")
    print("0) Volver\n")

def mostrar_menu_usuarios():
    print("1) Crear nuevo usuario\n")
    print("2) Asignar privilegio\n")
    print("3) Visualizar privilegios\n")
    print("4) Quitar privilegios\n")
    print("5)  Borrar usuario\n")
    print("0) Volver\n")

def mostrar_menu_roles():
    print("1) Crear nuevo rol\n")
    print("2) Asignar privilegio\n")
    print("3) Visualizar privilegios\n")
    print("4) Quitar privilegios\n")
    print("5) Borrar rol\n")
    print("0) Volver\n")

def mostrar_menu_tablas():
    print("1) Crear una tabla\n")
    print("2) Eliminar una tabla\n")
    print("3) Insertar datos\n")
    print("4) Consultar datos\n")
    print("5) Modificar datos\n")
    print("6) Eliminar datos\n")
    print("0) Volver\n")

def mostrar_menu_tunning_consultas():
    print("1) Índices\n")
    print("2) Plan de ejecución\n")
    print("3) Estadísticas\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_indices():
    print("1) Crear un índice\n")
    print("2) Monitorear índices\n")
    print("3) Borrar índices\n")
    print("0) Volver\n")

def mostrar_menu_ejecucion():
    print("1) Crear un plan de ejecución\n")
    print("2) Visualizar los planes de ejecución generados\n")
    print("3) Eliminar todos los planes de ejecución\n")
    print("0) Volver\n")

def mostrar_menu_estadisticas():
    print("1) Recopilar estadísticas de una tabla\n")
    print("2) Ver estadísticas\n")
    print("3) Eliminar estadísticas\n")
    print("0) Volver\n")

def mostrar_menu_performance_bd():
    print("1) Estado de la base de datos\n")
    print("2) Parámetros generales\n")
    print("3) Versión\n")
    print("4) Ubicación y nombre de SPFILE\n")
    print("5) Ubicación y número de ficheros de control\n")
    print("6) Nombre de la base de datos\n")
    print("7) Conexiones actuales\n")
    print("8) Usuarios conectados y número de sesiones\n")
    print("9) Propietarios por objetos y número de objetos\n")
    print("10) Tablas sobre propiedad del usuario actual\n")
    print("11) Todos los productos del usuario\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_auditoria():
    print("1) Activar auditorí\n")
    print("2) Visualizarlas tablas de auditoría\n")
    print("3) Activar la auditoría sobre la modificación de tablas del usuario\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_respaldos():
    print("1) Crear directorio\n")
    print("2) Crear respaldo full\n")
    print("3) Recuperar respaldos full\n")
    print("4) Crear respaldos de un esquema\n")
    print("5) Recuperar respaldo de un esquema\n")
    print("6) Crear respaldo de una tabla\n")
    print("7) Recuperar respaldo de una tabla\n")
    print("0) Volver al menú principal\n")
    

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
                elif opcion_administracion_tablespaces == "4":#tablas
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_tablas()
                        opcion_tablas = input("Seleccione una opción: ")
                        if opcion_tablas == "1":
                            crear_tabla()
                        elif opcion_tablas == "2":
                            eliminar_tabla()
                        elif opcion_tablas == "3":
                            insertar_datos()
                        elif opcion_tablas == "4":
                            consultar_datos()
                        elif opcion_tablas == "5":
                            modificar_datos()
                        elif opcion_tablas == "6":
                            eliminar_datos()
                        elif opcion_tablas == "0":
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
                elif opcion_auditoria == "2":
                    print("VISUALIZAR LAS TABLAS DE AUDITORÍA")
                elif opcion_auditoria == "3":
                    print("ACTIVAR LA AUDITORIA SOBRE LA MODIFICACIÓN DE TABLAS DEL USUARIO")
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
                    crear_respaldo_full()
                elif opcion_respaldos == "3":
                    recuperar_respaldo_full()
                elif opcion_respaldos == "4":
                    respaldo_esquema()
                elif opcion_respaldos == "5":
                    recuperar_respaldo_esquema()
                elif opcion_respaldos == "6":
                    respaldo_tabla()
                elif opcion_respaldos == "7":
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