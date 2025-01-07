import os
import pandas as pd
from ydata_profiling import ProfileReport

# Directorios
landing_zone = os.path.expanduser("~/Documents/ETL_Project/landing-zone")
data_quality_reports = os.path.expanduser("~/Documents/ETL_Project/raw-zone/data_quality_reports")

# Crear carpeta para los reportes si no existe
os.makedirs(data_quality_reports, exist_ok=True)

# Procesar cada archivo CSV en la carpeta landing-zone
for file in os.listdir(landing_zone):
    if file.endswith(".csv"):
        file_path = os.path.join(landing_zone, file)
        print(f"Procesando: {file}")

        # Leer el archivo CSV
        df = pd.read_csv(file_path)

        # Crear el reporte de perfilado
        profile = ProfileReport(df, title=f"Reporte de Calidad - {file}", explorative=True)

        # Guardar el reporte en formato HTML
        output_file = os.path.join(data_quality_reports, f"{file}_report.html")
        profile.to_file(output_file)
        print(f"Reporte generado: {output_file}")