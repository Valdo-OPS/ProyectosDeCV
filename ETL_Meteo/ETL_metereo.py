from datetime import datetime
from meteostat import Daily, Point
import pandas as pd
import os
import sqlite3
import matplotlib.pyplot as plt


def etl_meteorologia_csv(nombre_archivo, latitud, longitud, fecha_inicio, fecha_fin, carpeta="salida"):
    # Crear punto geográfico
    punto = Point(latitud, longitud)

    # Descargar datos
    data = Daily(punto, fecha_inicio, fecha_fin).fetch()

    if data.empty:
        print("No se encontraron datos para el período y ubicación indicados.")
        return None

    # Filtrar columnas de interés
    data_filtrada = data[["tavg", "tmin", "tmax", "prcp", "wspd"]].copy()

    # Renombrar columnas
    data_filtrada.rename(columns={
        "tavg": "Temp_Promedio",
        "tmin": "Temp_Minima",
        "tmax": "Temp_Maxima",
        "prcp": "Precipitacion_mm",
        "wspd": "Viento_kmh"
    }, inplace=True)

    # Rellenar vacíos
    data_filtrada_limpia = data_filtrada.fillna("N/A")

    # Normalizar columnas
    df = data_filtrada_limpia.reset_index()
    df.rename(columns={"time": "Fecha"}, inplace=True)
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df.columns = [col.replace(" ", "_").replace("á", "a").replace("é", "e")
                  .replace("í", "i").replace("ó", "o").replace("ú", "u")
                  .replace("ñ", "n") for col in df.columns]

    for col in df.select_dtypes(include="number").columns:
        df[col] = df[col].round(3)

    # Guardar en CSV
    os.makedirs(carpeta, exist_ok=True)
    ruta = os.path.join(carpeta, nombre_archivo)
    df.to_csv(ruta, index=False)

    print(f"Archivo guardado: {ruta}")
    return df


def guardar_en_sqlite(df, nombre_tabla="meteorologia", archivo_db="meteorologia.db"):
    # Conectar o crear base de datos
    conn = sqlite3.connect(archivo_db)

    # Eliminar la tabla si ya existía (opcional)
    conn.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")

    # Cargar el DataFrame a la base
    df.to_sql(nombre_tabla, conn, index=False, if_exists="replace")

    # Cerrar conexión
    conn.close()

    print(f"Datos guardados en {archivo_db}, tabla '{nombre_tabla}'")


def visualizar_meteorologia(df):
    # Asegurar formato de fecha
    df["Fecha"] = pd.to_datetime(df["Fecha"])

    # --- Temperatura Promedio ---
    plt.figure(figsize=(10, 4))
    plt.plot(df["Fecha"], df["Temp_Promedio"], marker='o', label="Temp. Promedio")
    plt.title("Temperatura Promedio Diaria")
    plt.xlabel("Fecha")
    plt.ylabel("°C")
    plt.grid(True)
    plt.tight_layout()
    plt.legend()
    plt.show()

    # --- Temperaturas Mínima vs Máxima ---
    plt.figure(figsize=(10, 4))
    plt.plot(df["Fecha"], df["Temp_Minima"], linestyle='--', label="Mínima")
    plt.plot(df["Fecha"], df["Temp_Maxima"], linestyle='-', label="Máxima")
    plt.title("Temperaturas Extremas")
    plt.xlabel("Fecha")
    plt.ylabel("°C")
    plt.grid(True)
    plt.tight_layout()
    plt.legend()
    plt.show()

    # --- Precipitaciones ---
    plt.figure(figsize=(10, 4))
    df["Precipitacion_mm"] = pd.to_numeric(df["Precipitacion_mm"], errors="coerce")  # asegurar numérico
    plt.bar(df["Fecha"], df["Precipitacion_mm"], color="skyblue")
    plt.title("Precipitación Diaria (mm)")
    plt.xlabel("Fecha")
    plt.ylabel("mm")
    plt.grid(True, axis='y')
    plt.tight_layout()
    plt.show()

def ejecutar_pipeline_completa():
    # Parámetros de entrada
    nombre_archivo = "datos_meteorologicos.csv"
    latitud = 40.4168  # Madrid
    longitud = -3.7038
    fecha_inicio = datetime(2020, 1, 1)
    fecha_fin = datetime(2023, 12, 31)
    carpeta_salida = "meteorologia"

    # ETL y guardar en CSV
    df_meteorologia = etl_meteorologia_csv(nombre_archivo, latitud, longitud, fecha_inicio, fecha_fin, carpeta_salida)

    if df_meteorologia is not None:
        # Guardar en SQLite
        guardar_en_sqlite(df_meteorologia)

        # Visualizar datos
        visualizar_meteorologia(df_meteorologia)
        

if __name__ == "__main__":
    ejecutar_pipeline_completa()
