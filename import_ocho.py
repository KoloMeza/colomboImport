import os
import pandas as pd
import os
import subprocess
from sqlalchemy import create_engine
from datetime import datetime

# Configuración de la base de datos
db_config = {
    'host': '34.174.155.124',
    'user': 'uqphvntlhtkz3',
    'password': '0315Columbus.86',
    'database': 'dbwbzbw8uywpo9',
}

# Carpeta donde se encuentran los archivos CSV
carpeta_csv = '/home/meza/Descargas/'

# Obtener la fecha actual en el formato "DDMM"
fecha_actual = datetime.now().strftime("%d%m")

# Definir el valor variable
valor_variable = "2301"

# Mapeo de nombres de archivos a nombres de tablas con valor_variable
mapeo_archivos_tablas = {
    f'{valor_variable} - ocho-Caja.csv': 'ochopsqcaja_Pruebas',
    f'{valor_variable} - ocho-Cuotas CxC.csv': 'ochopsqcuotascxc_Prueba',
    f'{valor_variable} - ocho-Movto ventas servicios.csv': 'ochopsqmovtoventasservicios_Prueba',
    f'{valor_variable} - ocho-Docto ventas servicios.csv': 'ochopsqdoctoventasservicios_Pruebas',
}

# Función para establecer la conexión a la base de datos
def conectar_a_mysql(db_config):
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Función para importar archivos CSV a la base de datos
def importar_csv_a_mysql(archivo_csv, nombre_tabla, engine):
    try:
        # Intentar leer el archivo CSV con codificación 'utf-8', si falla, intentar 'latin1'
        try:
            df = pd.read_csv(archivo_csv, encoding='utf-8', dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(archivo_csv, encoding='latin1', dtype=str)

        # Insertar los datos en la tabla de MySQL
        df.to_sql(name=nombre_tabla, con=engine, if_exists='append', index=False)
        print(f"Datos insertados en la tabla '{nombre_tabla}' con éxito.")
    except Exception as e:
        print(f"Error al importar el archivo CSV a MySQL: {e}")

# Establecer la conexión a la base de datos
engine = conectar_a_mysql(db_config)

# Verificar si la conexión fue exitosa antes de continuar
if engine is not None:
    try:
        # Bucle para buscar archivos CSV en la carpeta y realizar la importación
        for archivo, nombre_tabla in mapeo_archivos_tablas.items():
            ruta_completa = os.path.join(carpeta_csv, archivo)

            # Verificar si el archivo existe antes de intentar importarlo
            if os.path.isfile(ruta_completa):
                importar_csv_a_mysql(ruta_completa, nombre_tabla, engine)
            else:
                print(f"El archivo '{archivo}' no fue encontrado en la carpeta.")

    finally:
        # Cierre de la conexión a la base de datos (opcional)
        engine.dispose()
else:
    print("No se pudo establecer la conexión a la base de datos.")


# Después de la importación, ejecutar el script de actualización
script_actualizacion = 'update_ocho.py'
subprocess.run(['python3', script_actualizacion])