/*====================================================
Market Opportunity Analysis
Identify neighborhoods with high demand and limited supply
====================================================*/

WITH neighborhood_metrics AS (
 
    SELECT
        dl.neighbourhood,

        COUNT(*) AS total_listings,

        AVG(ISNULL(f.reviews_per_month,0)) AS avg_reviews_per_month,
        SUM(f.number_of_reviews) AS total_reviews,

        AVG(f.availability_365) AS avg_availability

    FROM dw.fact_listing_snapshot f
    JOIN dw.dim_location dl
        ON f.location_key = dl.location_key

    GROUP BY dl.neighbourhood
),

scored_neighborhoods AS (

    SELECT *,

        -- Demand score
        (avg_reviews_per_month * 0.6 + total_reviews * 0.4) 
        AS demand_score,

        -- Supply score (m�s listings + m�s disponibilidad = mayor oferta)
        (total_listings * 0.5 + avg_availability * 0.5)
        AS supply_score

    FROM neighborhood_metrics
)

SELECT
    neighbourhood,
    demand_score,
    supply_score,

    (demand_score - supply_score) AS opportunity_score,

    CASE
        WHEN (demand_score - supply_score) > 50
            THEN 'High-priority expansion area'
        WHEN (demand_score - supply_score) > 0
            THEN 'Moderate opportunity'
        ELSE 'Low opportunity'
    END AS recommended_action

FROM scored_neighborhoods
ORDER BY opportunity_score DESC;
