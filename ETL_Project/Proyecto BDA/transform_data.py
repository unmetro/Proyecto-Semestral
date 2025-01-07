import os
import pandas as pd

# Directorios
landing_zone = os.path.expanduser("~/Documents/ETL_Project/landing-zone")
raw_zone = os.path.expanduser("~/Documents/ETL_Project/raw-zone")

# Crear carpeta para almacenar los archivos Parquet
os.makedirs(raw_zone, exist_ok=True)

def clean_data(df):
    """
    Función para limpiar valores nulos en un DataFrame.
    Ajusta según tus necesidades.
    """
    # Rellenar valores nulos en columnas numéricas con la media
    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(df[num_cols].mean())

    # Rellenar valores nulos en columnas categóricas con "Desconocido"
    cat_cols = df.select_dtypes(include=['object']).columns
    df[cat_cols] = df[cat_cols].fillna("Desconocido")

    return df

# Procesar cada archivo CSV en la carpeta landing-zone
for file in os.listdir(landing_zone):
    if file.endswith(".csv"):
        file_path = os.path.join(landing_zone, file)
        print(f"Procesando: {file}")

        # Leer el archivo CSV
        df = pd.read_csv(file_path)

        # Limpiar los datos
        df = clean_data(df)

        # Guardar como Parquet
        output_file = os.path.join(raw_zone, f"{file}.parquet")
        df.to_parquet(output_file, index=False)
        print(f"Archivo Parquet generado: {output_file}")