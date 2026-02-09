import pandas as pd


# =====================================================
# LIMPIEZA CRÍTICA DE CLAVES DIMENSIONALES
# (ESTO ES LO MÁS IMPORTANTE DEL PIPELINE)
# =====================================================

def limpiar_claves_dimensionales(df: pd.DataFrame):
    """
    Nunca permitimos NULL en claves de dimensión.
    Regla básica de Data Warehousing.
    """

    columnas_location = ["neighbourhood_group", "neighbourhood"]

    for col in columnas_location:
        df[col] = (
            df[col]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .str.upper()
        )

    return df


# =====================================================
# LIMPIEZA Y NORMALIZACIÓN
# =====================================================

def normalizar_precio(df: pd.DataFrame):

    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df


def sanitizar_precio(df: pd.DataFrame):
    """
    Evita outliers absurdos.
    """

    df = df[df["price"] < 10000]

    return df


def estandarizar_textos(df: pd.DataFrame):

    columnas_texto = ["name", "room_type"]

    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def convertir_booleanos(df: pd.DataFrame):

    boolean_map = {"t": True, "f": False}

    for col in df.columns:
        if df[col].dtype == "object":
            valores_unicos = set(df[col].dropna().unique())

            if valores_unicos.issubset({"t", "f"}):
                df[col] = df[col].map(boolean_map)

    return df


def forzar_columnas_texto(df: pd.DataFrame):

    columnas_texto = [
        "host_name",
        "room_type",
        "license",
        "name"
    ]

    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].fillna("UNKNOWN").astype(str)

    return df


# =====================================================
# TIPOS SQL
# =====================================================

def forzar_tipos_sql(df: pd.DataFrame):

    df["host_id"] = df["host_id"].astype("int64")
    df["id"] = df["id"].astype("int64")

    df["calculated_host_listings_count"] = (
        df["calculated_host_listings_count"]
        .fillna(0)
        .clip(0, 10000)
        .astype("int32")
    )

    df["minimum_nights"] = (
        df["minimum_nights"]
        .fillna(1)
        .clip(1, 365)
        .astype("int32")
    )

    df["availability_365"] = (
        df["availability_365"]
        .clip(0, 365)
        .astype("int32")
    )

    df["number_of_reviews_ltm"] = (
        df["number_of_reviews_ltm"]
        .fillna(0)
        .clip(0, 10000)   # regla de negocio razonable
        .astype("int32")
    )
    df["reviews_per_month"] = (
        df["reviews_per_month"]
        .fillna(0)
        .clip(0, 50)   # nadie recibe 200 reviews/mes
    )

    return df


def convertir_numpy_a_python(df: pd.DataFrame):

    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype(object)

    return df


# =====================================================
# AMENITIES
# =====================================================

def parsear_amenities(df: pd.DataFrame):

    if "amenities" not in df.columns:
        return df

    df["amenities"] = (
        df["amenities"]
        .str.replace("{", "", regex=False)
        .str.replace("}", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.lower()
    )

    return df


def crear_flags_amenities(df: pd.DataFrame):

    if "amenities" not in df.columns:
        return df

    df["tiene_wifi"] = df["amenities"].str.contains("wifi", na=False)
    df["tiene_cocina"] = df["amenities"].str.contains("kitchen", na=False)
    df["tiene_estacionamiento"] = df["amenities"].str.contains("parking", na=False)

    return df


# =====================================================
# FEATURE ENGINEERING
# =====================================================

def calcular_ingreso_estimado(df: pd.DataFrame):

    df["ingreso_estimado"] = df["price"] * (365 - df["availability_365"])

    return df


def calcular_tasa_ocupacion(df: pd.DataFrame):

    df["tasa_ocupacion"] = 1 - (df["availability_365"] / 365)

    return df


def clasificar_precio(df: pd.DataFrame):

    df["categoria_precio"] = pd.cut(
        df["price"],
        bins=[0, 75, 150, 300, float("inf")],
        labels=["budget", "standard", "premium", "luxury"]
    ).astype(str)

    return df


# =====================================================
# SURROGATE KEYS
# =====================================================

def generar_surrogate_keys(df: pd.DataFrame):

    df = df.reset_index(drop=True)
    df["listing_sk_temp"] = df.index + 1

    return df


# =====================================================
# MASTER TRANSFORM
# =====================================================

def transformar_listings(df: pd.DataFrame):

    print("\n🔄 Iniciando transformaciones...")

    # 🔥 PRIMERO — limpieza crítica de claves
    df = limpiar_claves_dimensionales(df)

    df = normalizar_precio(df)
    df = sanitizar_precio(df)

    df = estandarizar_textos(df)
    df = forzar_columnas_texto(df)
    df = convertir_booleanos(df)

    df = parsear_amenities(df)
    df = crear_flags_amenities(df)

    df = calcular_ingreso_estimado(df)
    df = calcular_tasa_ocupacion(df)
    df = clasificar_precio(df)

    df = forzar_tipos_sql(df)
    df = convertir_numpy_a_python(df)

    df = generar_surrogate_keys(df)

    print("✅ Transformaciones completadas")

    return df