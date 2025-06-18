
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os

def pipeline_yfinance(lista_simbolos):
    libreria = {}

    for symbol in lista_simbolos:
        try:
            print(f"Procesando: {symbol}")

            df = yf.download(symbol, period="30d", interval="1d", auto_adjust=True)
            if df.empty:
                raise ValueError("Simbolo sin datos.")

            df = df.fillna("N/A")
            df.index = pd.to_datetime(df.index)
            df.index.name = "Fecha"

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(map(str, col)).strip() for col in df.columns]

            df.columns = [col.strip().replace(" ", "_").replace("-", "_") for col in df.columns]
            df = df.round(3)

            libreria[symbol] = df
            print(f"{symbol} procesado correctamente {df.shape} filas x columnas")

        except Exception:
            print(f"{symbol} Incorrecto; Insertar una moneda adecuada")

    return libreria

def guardar_csv(libreria, carpeta="data"):
    os.makedirs(carpeta, exist_ok=True)
    for simbolo, tabla in libreria.items():
        archivo = os.path.join(carpeta, f"{simbolo}.csv")
        tabla.to_csv(archivo)
        print(f"Guardado: {archivo}")

def guardar_sqlite(libreria, db_name="precios.db"):
    conn = sqlite3.connect(db_name)
    conn.execute("DROP TABLE IF EXISTS precios")

    tablas = []
    for simbolo, tabla in libreria.items():
        df = tabla.copy().reset_index()
        df["Name"] = simbolo
        tablas.append(df)

    df_total = pd.concat(tablas, ignore_index=True)
    df_total.to_sql("precios", conn, index=False, if_exists="replace")
    conn.close()
    print(f"Guardado completo en {db_name} ({len(df_total)} registros)")

def graficar_cierres(libreria):
    for simbolo, tabla in libreria.items():
        columna_close = next((col for col in tabla.columns if "Close" in col), None)
        if columna_close:
            plt.figure(figsize=(10, 4))
            plt.plot(tabla.index, tabla[columna_close], label=simbolo)
            plt.title(f"Precio de cierre – {simbolo}")
            plt.xlabel("Fecha")
            plt.ylabel("USD")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            plt.show()
        else:
            print(f"No se encontro columna 'Close' en {simbolo}. Columnas:")
            print(tabla.columns.tolist())

def ejecutar_pipeline_completa(lista_simbolos):
    libreria = pipeline_yfinance(lista_simbolos)
    guardar_csv(libreria)
    guardar_sqlite(libreria)
    graficar_cierres(libreria)

# Ejecutar ejemplo
if __name__ == "__main__":
    ejecutar_pipeline_completa(["AAPL", "TSLA", "MSFT"])
