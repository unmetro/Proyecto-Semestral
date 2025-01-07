import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Directorios
refined_zone = os.path.expanduser("/Users/antonioaguilar/Documents/ETL_Project/refined-zone")

# Configuración de la conexión a PostgreSQL
db_config = {
    "dbname": "air_quality",
    "user": "postgres",
    "password": "4tacoss4",  # Reemplaza con tu contraseña
    "host": "localhost",
    "port": 5432
}

# Conectar a PostgreSQL
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Procesar cada archivo Parquet en refined-zone
for file in os.listdir(refined_zone):
    if file.endswith(".parquet"):
        file_path = os.path.join(refined_zone, file)
        print(f"Cargando archivo: {file}")

        # Leer archivo Parquet
        df = pd.read_parquet(file_path)

        # Generar el nombre de la tabla a partir del nombre del archivo
        table_name = os.path.splitext(file)[0]

        # Crear la tabla en PostgreSQL si no existe
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                {}
            );
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.SQL("{} {}").format(
                    sql.Identifier(col),
                    sql.SQL("TEXT") if df[col].dtype == "object" else sql.SQL("DOUBLE PRECISION")
                ) for col in df.columns
            )
        )
        cursor.execute(create_table_query)
        conn.commit()

        # Insertar los datos
        for i, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO {} VALUES ({})
            """).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Placeholder() for _ in row)
            )
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Datos cargados en la tabla: {table_name}")

# Cerrar conexión
cursor.close()
conn.close()
print("Carga a PostgreSQL completada.")