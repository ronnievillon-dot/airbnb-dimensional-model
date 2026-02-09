from src.utils.db_connector import get_connection
import pandas as pd


# =====================================================
# HELPERS
# =====================================================

def insertar_dataframe(cursor, df, tabla_temp):
    """
    Inserta dataframe en tabla temporal usando fast_executemany.
    """

    cols = ",".join(df.columns)
    placeholders = ",".join(["?"] * len(df.columns))

    query = f"INSERT INTO {tabla_temp} ({cols}) VALUES ({placeholders})"

    cursor.fast_executemany = True
    cursor.executemany(query, df.values.tolist())


# =====================================================
# DIM HOST — SCD TYPE 2
# =====================================================

def cargar_dim_host(cursor, df):

    cursor.execute("""
    IF OBJECT_ID('tempdb..#host_stage') IS NOT NULL
        DROP TABLE #host_stage;

    CREATE TABLE #host_stage(
        host_id BIGINT,
        host_name NVARCHAR(255),
        calculated_host_listings_count INT
    );
    """)

    host_df = df[[
        "host_id",
        "host_name",
        "calculated_host_listings_count"
    ]].drop_duplicates()

    insertar_dataframe(cursor, host_df, "#host_stage")

    cursor.execute("""

    UPDATE d
    SET end_date = GETDATE(),
        is_current = 0
    FROM dw.dim_host d
    JOIN #host_stage s
        ON d.host_id = s.host_id
    WHERE d.is_current = 1
      AND (
            d.host_name <> s.host_name
            OR d.calculated_host_listings_count <> s.calculated_host_listings_count
          );

    INSERT INTO dw.dim_host(
        host_id,
        host_name,
        calculated_host_listings_count,
        effective_date,
        is_current
    )
    SELECT
        s.host_id,
        s.host_name,
        s.calculated_host_listings_count,
        GETDATE(),
        1
    FROM #host_stage s
    LEFT JOIN dw.dim_host d
        ON s.host_id = d.host_id
        AND d.is_current = 1
    WHERE d.host_id IS NULL
       OR (
            d.host_name <> s.host_name
            OR d.calculated_host_listings_count <> s.calculated_host_listings_count
          );
    """)


# =====================================================
# DIM LOCATION (JOIN SEGURO)
# =====================================================

def cargar_dim_location(cursor, df):

    cursor.execute("""
    IF OBJECT_ID('tempdb..#location_stage') IS NOT NULL
        DROP TABLE #location_stage;

    CREATE TABLE #location_stage(
        neighbourhood_group NVARCHAR(100),
        neighbourhood NVARCHAR(150),
        latitude FLOAT,
        longitude FLOAT
    );
    """)

    loc_df = df[[
        "neighbourhood_group",
        "neighbourhood",
        "latitude",
        "longitude"
    ]].drop_duplicates()

    insertar_dataframe(cursor, loc_df, "#location_stage")

    cursor.execute("""

    INSERT INTO dw.dim_location(
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude
    )
    SELECT DISTINCT
        s.*
    FROM #location_stage s
    LEFT JOIN dw.dim_location d
        ON s.neighbourhood = d.neighbourhood
       AND s.neighbourhood_group = d.neighbourhood_group
    WHERE d.location_key IS NULL;

    """)


# =====================================================
# DIM PROPERTY
# =====================================================

def cargar_dim_property(cursor, df):

    cursor.execute("""
    IF OBJECT_ID('tempdb..#property_stage') IS NOT NULL
        DROP TABLE #property_stage;

    CREATE TABLE #property_stage(
        listing_id BIGINT,
        listing_name NVARCHAR(300),
        room_type NVARCHAR(50),
        minimum_nights INT,
        license NVARCHAR(100)
    );
    """)

    prop_df = df[[
        "id",
        "name",
        "room_type",
        "minimum_nights",
        "license"
    ]].drop_duplicates()

    prop_df.columns = [
        "listing_id",
        "listing_name",
        "room_type",
        "minimum_nights",
        "license"
    ]

    insertar_dataframe(cursor, prop_df, "#property_stage")

    cursor.execute("""

    MERGE dw.dim_property AS target
    USING #property_stage AS source
    ON target.listing_id = source.listing_id

    WHEN NOT MATCHED THEN
    INSERT (
        listing_id,
        listing_name,
        room_type,
        minimum_nights,
        license
    )
    VALUES (
        source.listing_id,
        source.listing_name,
        source.room_type,
        source.minimum_nights,
        source.license
    );

    """)


# =====================================================
# FACT TABLE (JOIN SEGURO)
# =====================================================

def cargar_fact(cursor, df):

    cursor.execute("""
    IF OBJECT_ID('tempdb..#fact_stage') IS NOT NULL
        DROP TABLE #fact_stage;

CREATE TABLE #fact_stage(
    listing_id BIGINT,
    host_id BIGINT,
    neighbourhood NVARCHAR(150),
    latitude FLOAT,
    longitude FLOAT,
    price FLOAT,
    availability_365 INT,
    number_of_reviews INT,
    number_of_reviews_ltm INT,
    reviews_per_month FLOAT
);

    """)

    fact_df = df.rename(columns={
    "id": "listing_id"
})[[
    "listing_id",
    "host_id",
    "neighbourhood",
    "latitude",
    "longitude",
    "price",
    "availability_365",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "reviews_per_month"
]]


    fact_df.columns = [
        "listing_id",
        "host_id",
        "neighbourhood",
        "latitude",
        "longitude",
        "price",
        "availability_365",
        "number_of_reviews",
        "number_of_reviews_ltm",
        "reviews_per_month"
    ]

    insertar_dataframe(cursor, fact_df, "#fact_stage")

    cursor.execute("""

    INSERT INTO dw.fact_listing_snapshot(
        snapshot_date,
        property_key,
        host_key,
        location_key,
        price,
        availability_365,
        number_of_reviews,
        number_of_reviews_ltm,
        reviews_per_month
    )
    SELECT DISTINCT 
        CAST(GETDATE() AS DATE),
        p.property_key,
        h.host_key,
        l.location_key,
        s.price,
        s.availability_365,
        s.number_of_reviews,
        s.number_of_reviews_ltm,
        s.reviews_per_month
    FROM #fact_stage s

    INNER JOIN dw.dim_property p
        ON s.listing_id = p.listing_id

    INNER JOIN dw.dim_host h
        ON s.host_id = h.host_id
        AND h.is_current = 1

    INNER JOIN dw.dim_location l
        ON s.latitude = l.latitude
        AND s.longitude = l.longitude


    WHERE NOT EXISTS (
        SELECT 1
        FROM dw.fact_listing_snapshot f
        WHERE f.property_key = p.property_key
         AND f.snapshot_date = CAST(GETDATE() AS DATE)
    );

    """)


# =====================================================
# MASTER LOAD — TRANSACCIONAL REAL
# =====================================================

def ejecutar_carga(df):

    conn = get_connection()
    cursor = conn.cursor()

    try:

        print("\n🚀 Iniciando carga al Data Warehouse...")

        cargar_dim_host(cursor, df)
        cargar_dim_location(cursor, df)
        cargar_dim_property(cursor, df)
        cargar_fact(cursor, df)

        conn.commit()

        print("✅ Carga completada exitosamente")

    except Exception as e:

        conn.rollback()

        print("\n🔥 ERROR REAL DEL LOAD:")
        print(e)

        raise

    finally:

        conn.close()