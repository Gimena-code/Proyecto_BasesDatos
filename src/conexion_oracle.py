import os
import cx_Oracle
from tabulate import tabulate

dsn_tns = cx_Oracle.makedsn("localhost", 1521, "XE")
connection = cx_Oracle.connect("SYS", "root", dsn_tns, mode=cx_Oracle.SYSDBA)

def limpiar_pantalla():
    if os.name == 'nt':
        _ = os.system('cls')
    else:
        _ = os.system('clear')

#FUNCIONES TABLESPACES
def crear_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_tablespace}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE TABLESPACE {nombre_tablespace} DATAFILE '{ruta_archivo_datos}' SIZE 10M REUSE AUTOEXTEND ON NEXT 10M MAXSIZE 200M")
        print(f"El tablespace {nombre_tablespace} ha sido creado exitosamente en la ruta {ruta_archivo_datos}.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el tablespace:", error.message)
    cursor.close()

def crear_tablespace_temporal():
    nombre_tablespace_temporal = input("Ingrese el nombre del tablespace temporal: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_tablespace_temporal}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE TEMPORARY TABLESPACE {nombre_tablespace_temporal} TEMPFILE '{ruta_archivo_datos}' SIZE 5M REUSE AUTOEXTEND ON NEXT 5M MAXSIZE 100M")
        print(f"El tablespace temporal {nombre_tablespace_temporal} ha sido creado exitosamente en la ruta {ruta_archivo_datos}.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el tablespace temporal:", error.message)
    cursor.close()

def visualizar_tamaño_tablespace():
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT tablespace_name, round((bytes / 1024 / 1024), 2) as size_mb FROM dba_data_files")
        headers = ["Tablespace_name", "Size_MB"]
        data = [[row[0], row[1]] for row in cursor]
        print(tabulate(data, headers, tablefmt="fancy_grid"))
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar el tamaño del tablespace:", error.message)
    cursor.close()

def modificar_tamaño_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace que desea modificar: ")
    ruta_carpeta = input("Ingrese la ruta de la carpeta donde se encuentra el archivo: ")
    tamaño_nuevo = input("Ingrese el nuevo tamaño para el tablespace (por ejemplo, 100M): ")
    cursor = connection.cursor()
    try:
        ruta_completa = f"{ruta_carpeta}\\{nombre_tablespace}.dbf"
        cursor.execute(f"ALTER DATABASE DATAFILE '{ruta_completa}' RESIZE {tamaño_nuevo}")
        print(f"El tablespace {nombre_tablespace} ha sido modificado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar el tamaño del tablespace:", error.message)
    cursor.close()

def agregar_datafile_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    nombre_datafile = input("Ingrese el nombre del datafile: ")
    carpeta_guardado = input("Ingrese la ruta de la carpeta donde desea guardar el archivo: ")
    nombre_archivo = f"{nombre_datafile}.dbf"
    ruta_archivo_datos = f"{carpeta_guardado}\\{nombre_archivo}"
    cursor = connection.cursor()
    try:
        cursor.execute(f"ALTER TABLESPACE {nombre_tablespace} ADD DATAFILE '{ruta_archivo_datos}' SIZE 10M REUSE AUTOEXTEND ON NEXT 10M MAXSIZE 200M")
        print(f"El tablespace {nombre_tablespace} ha sido modificado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar el tablespace:", error.message)
    cursor.close()

def eliminar_tablespace():
    nombre_tablespace = input("Ingrese el nombre del tablespace: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLESPACE {nombre_tablespace} INCLUDING CONTENTS AND DATAFILES")
        print(f"El tablespace {nombre_tablespace} ha sido eliminado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el tablespace:", error.message)
    cursor.close()

#FUNCIONES USUARIOS Y ROLES
def crear_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    password_usuario = input("Ingrese la contraseña del usuario: ")
    tablespace_usuario = input("Ingrese el nombre del tablespace para el usuario: ")
    tablespace_temporal = input("Ingrese el nombre del tablespace temporal para el usuario: ")
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"CREATE USER {nombre_usuario} IDENTIFIED BY {password_usuario} DEFAULT TABLESPACE {tablespace_usuario} TEMPORARY TABLESPACE {tablespace_temporal} QUOTA UNLIMITED ON {tablespace_usuario} QUOTA UNLIMITED ON {tablespace_temporal}"
        )
        cursor.execute(f"GRANT CREATE SESSION TO {nombre_usuario}")
        cursor.execute(f"GRANT ALL PRIVILEGES TO {nombre_usuario}")
        print(f"Se ha creado el usuario {nombre_usuario} correctamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el usuario:", error.message)
    cursor.close()



def eliminar_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP USER {nombre_usuario} CASCADE")
        print(f"El usuario {nombre_usuario} ha sido eliminado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el usuario:", error.message)
    cursor.close()



def crear_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE ROLE {nombre_rol}")
        print(f"El rol {nombre_rol} ha sido creado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el rol:", error.message)
    cursor.close()

def borrar_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP USER {nombre_usuario} CASCADE")
        print(f"El usuario {nombre_usuario} ha sido borrado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al borrar el usuario:", error.message)
    cursor.close()

def borrar_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP ROLE {nombre_rol}")
        print(f"El rol {nombre_rol} ha sido borrado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al borrar el rol:", error.message)
    cursor.close()

def asignar_privilegio_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    nombre_privilegio = input("Ingrese el nombre del privilegio: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"GRANT {nombre_privilegio} TO {nombre_usuario}")
        print(f"El privilegio {nombre_privilegio} ha sido asignado al usuario {nombre_usuario} exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al asignar el privilegio al usuario:", error.message)
    cursor.close()

def asignar_privilegio_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    nombre_privilegio = input("Ingrese el nombre del privilegio: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"GRANT {nombre_privilegio} TO {nombre_rol}")
        print(f"El privilegio {nombre_privilegio} ha sido asignado al rol {nombre_rol} exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al asignar el privilegio al rol:", error.message)
    cursor.close()

def revocar_privilegio_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    nombre_privilegio = input("Ingrese el nombre del privilegio: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"REVOKE {nombre_privilegio} FROM {nombre_usuario}")
        print(f"El privilegio {nombre_privilegio} ha sido revocado del usuario {nombre_usuario} exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al revocar el privilegio al usuario:", error.message)
    cursor.close()

def revocar_privilegio_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    nombre_privilegio = input("Ingrese el nombre del privilegio: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"REVOKE {nombre_privilegio} FROM {nombre_rol}")
        print(f"El privilegio {nombre_privilegio} ha sido revocado del rol {nombre_rol} exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al revocar el privilegio al rol:", error.message)
    cursor.close()

def visualizar_privilegios_usuario():
    nombre_usuario = input("Ingrese el nombre del usuario: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM USER_SYS_PRIVS WHERE GRANTEE = '{nombre_usuario}'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar los privilegios del usuario:", error.message)
    cursor.close()

def visualizar_privilegios_rol():
    nombre_rol = input("Ingrese el nombre del rol: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM USER_ROLE_PRIVS WHERE GRANTEE = '{nombre_rol}'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al visualizar los privilegios del rol:", error.message)
    cursor.close()

#FUNCIONES INDICES
def crear_indice():
    nombre_indice = input("Ingrese el nombre del índice: ")
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE INDEX {nombre_indice} ON {nombre_tabla}({nombre_columna})")
        print(f"El índice {nombre_indice} ha sido creado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear el índice:", error.message)
    cursor.close()

def eliminar_indice():
    nombre_indice = input("Ingrese el nombre del índice: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP INDEX {nombre_indice}")
        print(f"El índice {nombre_indice} ha sido eliminado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el índice:", error.message)
    cursor.close()

def monitorear_indices():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM USER_INDEXES")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al monitorear los índices:", error.message)
    cursor.close()


#FUNCIONES TABLAS
def crear_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    tipo_dato = input("Ingrese el tipo de dato: ")
    tamaño_dato = input("Ingrese el tamaño del dato: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE TABLE {nombre_tabla}({nombre_columna} {tipo_dato}({tamaño_dato}))")
        print(f"La tabla {nombre_tabla} ha sido creada exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al crear la tabla:", error.message)
    cursor.close()

def eliminar_tabla():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE {nombre_tabla}")
        print(f"La tabla {nombre_tabla} ha sido eliminada exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar la tabla:", error.message)
    cursor.close()    

def insertar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    valor = input("Ingrese el valor: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"INSERT INTO {nombre_tabla}({nombre_columna}) VALUES({valor})")
        print(f"El valor {valor} ha sido insertado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al insertar el valor:", error.message)
    cursor.close()        

def consultar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {nombre_tabla}")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al consultar los datos:", error.message)
    cursor.close()

def modificar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    valor = input("Ingrese el valor: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"UPDATE {nombre_tabla} SET {nombre_columna} = {valor}")
        print(f"El valor {valor} ha sido modificado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al modificar el valor:", error.message)
    cursor.close()

def eliminar_datos():
    nombre_tabla = input("Ingrese el nombre de la tabla: ")
    nombre_columna = input("Ingrese el nombre de la columna: ")
    valor = input("Ingrese el valor: ")
    cursor = connection.cursor()
    try:
        cursor.execute(f"DELETE FROM {nombre_tabla} WHERE {nombre_columna} = {valor}")
        print(f"El valor {valor} ha sido eliminado exitosamente.")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al eliminar el valor:", error.message)
    cursor.close()
                        

#PERFORMANCE BASE DE DATOS
def Vista_estadoBD():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM V$INSTANCE")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar estado de la base de datos:", error.message)
    cursor.close()

def Parametros_Generales():
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
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'compatible'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar version:", error.message)
    cursor.close()

def Name_Spfile():
    cursor = connection.cursor()
    try:
        cursor.execute(f"select value from v$system_parameter where name = 'spfile'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar ubicacion y nombre del spfile:", error.message)
    cursor.close()

def Name_ControlFiles():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'control_files'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar ubicacion y nombre de los ficheros de control:", error.message)
    cursor.close()

def Name_DB():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT VALUE FROM V$SYSTEM_PARAMETER WHERE NAME = 'db_name'")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar nombre de la base de datos:", error.message)
    cursor.close()

def Actual_Cx():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT OSUSER, USERNAME, MACHINE, PROGRAM FROM V$SESSION ORDER BY OSUSER")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar las conexiones actuales:", error.message)
    cursor.close()

def Objects():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT OWNER, COUNT(OWNER) Numero FROM DBA_OBJECTS GROUP BY OWNER")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los propietarios por objetos y numero de objetos:", error.message)
    cursor.close()

def Tables_ActualUser():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM USER_TABLES")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar las tablas del usuario actual:", error.message)
    cursor.close()

def User_Products():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM USER_CATALOG")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los productos del usuario:", error.message)
    cursor.close()


def Usser_Cx():
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT USERNAME USUARIO_ORACLE, COUNT(USERNAME) NUMERO_SESIONES FROM V$SESSION GROUP BY USERNAME ORDER BY NUMERO_SESIONES DESC")
        for row in cursor:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error al mostrar los usuarios conectados:", error.message)
    cursor.close()


#MENUS
def mostrar_menu_principal():
    print("1) ADMINISTRACION DE TABLESPACES\n")
    print("2) TUNNING DE CONSULTAS\n")
    print("3) PERFORMANCE DE LA BASE DE DATOS\n")
    print("4) AUDITORIA DE LA BD\n")
    print("5) ADMINISTRACIÓN DE ARCHIVOS DE RESPALDOS Y DIRECTORIOS\n")
    print("0) SALIR\n")

def mostrar_menu_administracion_tablespaces():
    print("1) TABLESPACES\n")
    print("2) USUARIOS\n")
    print("3) ROLES\n")
    print("4) TABLAS\n")
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
    print("1) INDICES\n")
    print("2) REALIZAR UN PLAN DE EJECUCIÓN\n")
    print("3) ESTADÍSTICAS\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_indices():
    print("1) Crear un índice\n")
    print("2) Monitorear índices\n")
    print("3) Borrar índices\n")
    print("0) Volver\n")

def mostrar_menu_estadisticas():
    print("1) Esquemas\n")
    print("2) Tablas\n")
    print("0) Volver\n")

def mostrar_menu_performance_bd():
    print("1) Vista que muestra el estado de la base de datos\n")
    print("2) Consulta de parámetros generales\n")
    print("3) Consulta para conocer la versión\n")
    print("4) Consulta ubicación y nombre de SPFILE\n")
    print("5) Consulta ubicación y número de ficheros de control\n")
    print("6) Consulta el nombre de la base de datos\n")
    print("7) Consulta de las conexiones actuales\n")
    print("8) Consulta de usuarios conectados y número de sesiones\n")
    print("9) Propietarios por objetos y número de objetos\n")
    print("10) Consulta las tablas propiedad del usuario actual\n")
    print("11) Consulta todos los productos del usuario\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_auditoria():
    print("1) ACTIVAR AUDITORIA\n")
    print("2) VISUALIZAR LAS TABLAS DE AUDITORÍA\n")
    print("3) ACTIVAR LA AUDITORIA SOBRE LA MODIFICACIÓN DE TABLAS DEL USUARIO\n")
    print("0) Volver al menú principal\n")

def mostrar_menu_respaldos():
    print("1) CREACIÓN DE RESPALDOS\n")
    print("2) RECUPERACIÓN DE RESPALDOS\n")
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
                            eliminar_indice()
                        elif opcion_indices == "3":
                            monitorear_indices()
                        elif opcion_indices == "0":
                            break

                elif opcion_tunning == "2":#Plan Ejecución
                    print("Función para crear plan ejecución")
                elif opcion_tunning == "3":#Estadisticas
                    while True:
                        input("Presione Enter para continuar...")
                        limpiar_pantalla()
                        mostrar_menu_estadisticas()
                        opcion_estadisticas = input("Seleccione una opción: ")
                        if opcion_estadisticas == "1":
                            print("Función para Esquemas")
                        elif opcion_estadisticas == "2":
                            print("Función para Tablas")
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
                    print("CREACIÓN DE RESPALDOS")
                elif opcion_respaldos == "2":
                    print("RECUPERACIÓN DE RESPALDOS")
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