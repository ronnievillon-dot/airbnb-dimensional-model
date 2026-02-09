-- =====================================================
-- Crear esquema del Data Warehouse
-- =====================================================
CREATE SCHEMA dw;
GO

-- =====================================================
-- DIMENSION: HOST (SCD Type 2)
-- =====================================================
CREATE TABLE dw.dim_host (

    host_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
    host_id BIGINT NOT NULL,                -- business key

    host_name NVARCHAR(255),
    calculated_host_listings_count INT,

    effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    end_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1
);
GO

CREATE UNIQUE NONCLUSTERED INDEX idx_dim_host_business
ON dw.dim_host(host_id, is_current);
GO


-- =====================================================
-- DIMENSION: LOCATION
-- =====================================================
CREATE TABLE dw.dim_location (

    location_key INT IDENTITY(1,1) PRIMARY KEY,

    neighbourhood_group NVARCHAR(100),
    neighbourhood NVARCHAR(150),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);
GO

CREATE NONCLUSTERED INDEX idx_dim_location
ON dw.dim_location(neighbourhood_group, neighbourhood);
GO


-- =====================================================
-- DIMENSION: PROPERTY
-- =====================================================
CREATE TABLE dw.dim_property (

    property_key INT IDENTITY(1,1) PRIMARY KEY,
    listing_id BIGINT NOT NULL, -- business key

    listing_name NVARCHAR(300),
    room_type NVARCHAR(50),
    minimum_nights INT,
    license NVARCHAR(100)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX idx_dim_property_listing
ON dw.dim_property(listing_id);
GO


-- =====================================================
-- DIMENSION: DATE
-- =====================================================
CREATE TABLE dw.dim_date (

    date_key DATE PRIMARY KEY,
    year SMALLINT,
    quarter TINYINT,
    month TINYINT,
    day TINYINT,
    weekday TINYINT
);
GO


-- =====================================================
-- FACT TABLE: LISTING SNAPSHOT
-- Grain: One row per listing per snapshot date
-- =====================================================
CREATE TABLE dw.fact_listing_snapshot (

    snapshot_date DATE NOT NULL,

    property_key INT NOT NULL,
    host_key INT NOT NULL,
    location_key INT NOT NULL,

    price DECIMAL(10,2),
    availability_365 INT,
    number_of_reviews INT,
    number_of_reviews_ltm INT,
    reviews_per_month DECIMAL(6,2),

    CONSTRAINT PK_fact_listing
        PRIMARY KEY NONCLUSTERED (snapshot_date, property_key),

    --CONSTRAINT PK_fact_listing
      --  PRIMARY KEY (snapshot_date, property_key),

    CONSTRAINT FK_fact_property
        FOREIGN KEY (property_key)
        REFERENCES dw.dim_property(property_key),

    CONSTRAINT FK_fact_host
        FOREIGN KEY (host_key)
        REFERENCES dw.dim_host(host_key),

    CONSTRAINT FK_fact_location
        FOREIGN KEY (location_key)
        REFERENCES dw.dim_location(location_key)
);
GO


-- =====================================================
-- INDEXING STRATEGY (Analytics-focused)
-- =====================================================

CREATE NONCLUSTERED INDEX idx_fact_host
ON dw.fact_listing_snapshot(host_key);

CREATE NONCLUSTERED INDEX idx_fact_location
ON dw.fact_listing_snapshot(location_key);

CREATE NONCLUSTERED INDEX idx_fact_price
ON dw.fact_listing_snapshot(price);
GO


-- =====================================================
-- Columnstore index mejora queries analíticas
-- =====================================================

CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_listing
ON dw.fact_listing_snapshot;
GO
