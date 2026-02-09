import pandas as pd
from pathlib import Path
import json


# =====================================================
# CONFIGURACIÓN DE OUTPUT
# =====================================================

OUTPUT_PATH = Path("output")
OUTPUT_PATH.mkdir(exist_ok=True)


# =====================================================
# CUARENTENA
# =====================================================

def cuarentenar_registros(df_malos: pd.DataFrame, nombre_archivo: str):
    """
    Guarda registros inválidos sin detener el pipeline.
    """

    ruta_archivo = OUTPUT_PATH / nombre_archivo
    df_malos.to_csv(ruta_archivo, index=False)

    print(f"{len(df_malos)} registros enviados a cuarentena → {ruta_archivo}")


# =====================================================
# REPORTE DE CALIDAD
# =====================================================

def generar_reporte_calidad(metricas: dict):
    """
    Genera un reporte JSON con métricas de calidad.
    """

    ruta_reporte = OUTPUT_PATH / "data_quality_report.json"

    with open(ruta_reporte, "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=4)

    print(f"📊 Reporte de calidad generado → {ruta_reporte}")


# =====================================================
# VALIDACIONES
# =====================================================

def validar_esquema(df: pd.DataFrame, columnas_esperadas: list):

    faltantes = set(columnas_esperadas) - set(df.columns)

    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {faltantes}")


def validar_tipos(df: pd.DataFrame):

    errores = []

    if not pd.api.types.is_numeric_dtype(df["price"]):
        errores.append("price debe ser numérico")

    if not pd.api.types.is_numeric_dtype(df["latitude"]):
        errores.append("latitude debe ser numérico")

    if not pd.api.types.is_numeric_dtype(df["longitude"]):
        errores.append("longitude debe ser numérico")

    if errores:
        raise ValueError(f"Errores de tipo detectados: {errores}")


def validar_duplicados(df: pd.DataFrame, columna_clave: str, metricas: dict):

    duplicados = df[df.duplicated(columna_clave, keep=False)]

    metricas["duplicados"] = len(duplicados)

    if not duplicados.empty:
        cuarentenar_registros(duplicados, "registros_duplicados.csv")
        df = df.drop_duplicates(subset=columna_clave)

        print(f"{len(duplicados)} registros duplicados eliminados ⚠️")

    return df


def validar_no_nulos(df: pd.DataFrame, columnas_criticas: list, metricas: dict):

    registros_malos = df[df[columnas_criticas].isnull().any(axis=1)]

    metricas["nulos"] = len(registros_malos)

    if not registros_malos.empty:
        cuarentenar_registros(registros_malos, "registros_con_nulos.csv")
        df = df.drop(registros_malos.index)

        print(f"{len(registros_malos)} registros eliminados por nulos ⚠️")

    return df


def validar_precio(df: pd.DataFrame, metricas: dict):

    invalidos = df[df["price"] <= 0]

    metricas["precios_invalidos"] = len(invalidos)

    if not invalidos.empty:
        cuarentenar_registros(invalidos, "precios_invalidos.csv")
        df = df.drop(invalidos.index)

        print(f"{len(invalidos)} registros con precio inválido eliminados ⚠️")

    return df


def validar_coordenadas(df: pd.DataFrame, metricas: dict):

    invalidos = df[
        (df["latitude"] < -90) | (df["latitude"] > 90) |
        (df["longitude"] < -180) | (df["longitude"] > 180)
    ]

    metricas["coordenadas_invalidas"] = len(invalidos)

    if not invalidos.empty:
        cuarentenar_registros(invalidos, "coordenadas_invalidas.csv")
        df = df.drop(invalidos.index)

        print(f"{len(invalidos)} registros con coordenadas inválidas eliminados ⚠️")

    return df


# =====================================================
# PIPELINE PRINCIPAL
# =====================================================

def ejecutar_validaciones_listings(df: pd.DataFrame):

    metricas = {
        "registros_leidos": len(df),
        "duplicados": 0,
        "nulos": 0,
        "precios_invalidos": 0,
        "coordenadas_invalidas": 0
    }

    columnas_esperadas = [
        "id",
        "host_id",
        "price",
        "latitude",
        "longitude",
        "neighbourhood",
        "room_type"
    ]

    # VALIDACIONES DURAS
    validar_esquema(df, columnas_esperadas)
    validar_tipos(df)

    # VALIDACIONES SUAVES
    df = validar_duplicados(df, "id", metricas)
    df = validar_no_nulos(df, ["id", "host_id", "price"], metricas)
    df = validar_precio(df, metricas)
    df = validar_coordenadas(df, metricas)

    metricas["registros_finales"] = len(df)

    generar_reporte_calidad(metricas)

    print("\n✅ VALIDACIÓN COMPLETADA — Dataset listo para transformación")

    return df