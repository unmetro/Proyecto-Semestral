import os
import shutil

# Directorios
raw_zone = os.path.expanduser("/Users/antonioaguilar/Documents/ETL_Project/raw-zone")
refined_zone = os.path.expanduser("/Users/antonioaguilar/Documents/ETL_Project/refined-zone")

# Crear carpeta refined-zone si no existe
os.makedirs(refined_zone, exist_ok=True)

# Mover archivos Parquet de raw-zone a refined-zone
for file in os.listdir(raw_zone):
    if file.endswith(".parquet"):
        raw_file_path = os.path.join(raw_zone, file)
        refined_file_path = os.path.join(refined_zone, file)
        print(f"Moviendo archivo: {file}")

        # Copiar archivo al refined-zone
        shutil.copy(raw_file_path, refined_file_path)

print("Archivos movidos a refined-zone exitosamente.")