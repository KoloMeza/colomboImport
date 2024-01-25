import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Configuración de la base de datos
db_config = {
    'host': '34.174.155.124',
    'user': 'uqphvntlhtkz3',
    'password': '0315Columbus.86',
    'database': 'dbwbzbw8uywpo9',
}

# Carpeta donde se encuentra el archivo CSV
carpeta_csv = '/home/meza/Descargas/'

# Lista de nombres de tablas en la base de datos
nombres_tablas = ['unoterceros', 'unoclientes', 'unoentidadestercero', 'unoentidadesclientes']

# Función para establecer la conexión a la base de datos
def conectar_a_mysql(db_config):
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para actualizar registros después de una importación exitosa
def actualizar_registros(engine, nombre_tabla):
    try:
        with engine.connect() as connection:
            # Consulta para actualizar registros con Estado_impor = 3 a Estado_impor = 0
            update_query = text(f"UPDATE `{nombre_tabla}` SET Estado_impor = 0 WHERE Estado_impor = :estado_impor")

            # Crear un diccionario de parámetros
            params = {'estado_impor': 3}

            # Ejecutar la consulta de actualización con el diccionario de parámetros
            result = connection.execute(update_query, params)

            # Confirmar la transacción
            connection.commit()

            # Verificar si los registros se actualizaron correctamente
            select_query = text(f"SELECT COUNT(*) FROM `{nombre_tabla}` WHERE Estado_impor = 0")
            result = connection.execute(select_query)
            count = result.scalar()

            if count is not None:
                print(
                    f"Registros actualizados en la tabla '{nombre_tabla}' con éxito. Total de registros actualizados: {count}")
            else:
                print(f"Error al obtener el total de registros actualizados en la tabla '{nombre_tabla}'.")
    except Exception as e:
        print(f"Error al actualizar registros: {e}")

# Establecer la conexión a la base de datos
engine = conectar_a_mysql(db_config)

# Verificar si la conexión fue exitosa antes de continuar
if engine is not None:
    try:
        # Iterar sobre las tablas y llamar a la función para actualizar registros
        for nombre_tabla in nombres_tablas:
            actualizar_registros(engine, nombre_tabla)

    finally:
        # Cierre de la conexión a la base de datos (opcional)
        engine.dispose()
else:
    print("No se pudo establecer la conexión a la base de datos.")
