/*====================================================
Pricing Intelligence
Identify over/underpriced listings compared to similar properties
====================================================*/

WITH listing_prices AS (

    SELECT
        dp.listing_id,
        dl.neighbourhood,
        dp.room_type,
        f.price AS current_price,

        -- Promedio de mercado para propiedades similares
        AVG(f.price) OVER (
            PARTITION BY dl.neighbourhood, dp.room_type
        ) AS market_average

    FROM dw.fact_listing_snapshot f
    JOIN dw.dim_property dp
        ON f.property_key = dp.property_key
    JOIN dw.dim_location dl
        ON f.location_key = dl.location_key

    WHERE f.price IS NOT NULL
),

price_analysis AS (

    SELECT *,
        -- Diferencia porcentual vs mercado
        CASE 
            WHEN market_average = 0 THEN 0
            ELSE ((current_price - market_average) / market_average) * 100
        END AS price_difference_pct
    FROM listing_prices
)

SELECT
    listing_id,
    current_price,
    market_average,
    price_difference_pct,

    CASE
        WHEN price_difference_pct <= -20 THEN 'underpriced'
        WHEN price_difference_pct >= 20 THEN 'overpriced'
        ELSE 'fair'
    END AS recommendation

FROM price_analysis
ORDER BY price_difference_pct DESC;
