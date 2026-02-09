/*====================================================
Host Performance Ranking
Rank hosts using a composite performance score
====================================================*/

WITH host_metrics AS (

    SELECT
        dh.host_id,
        dh.host_name,

        SUM(f.price * f.availability_365) AS revenue_potential,
        SUM(f.number_of_reviews) AS total_reviews,
        AVG(ISNULL(f.reviews_per_month,0)) AS avg_reviews_per_month,
        MAX(dh.calculated_host_listings_count) AS portfolio_size

    FROM dw.fact_listing_snapshot f
    JOIN dw.dim_host dh
        ON f.host_key = dh.host_key

    WHERE dh.is_current = 1

    GROUP BY
        dh.host_id,
        dh.host_name
),

scored_hosts AS (

    SELECT *,
    
        -- Score compuesto 
        (
            revenue_potential * 0.4 +
            total_reviews * 0.3 +
            avg_reviews_per_month * 0.2 +
            portfolio_size * 0.1
        ) AS performance_score

    FROM host_metrics
)

SELECT
    host_id,
    host_name,
    performance_score,

    RANK() OVER (
        ORDER BY performance_score DESC
    ) AS ranking,

    -- Breakdown para transparencia
    revenue_potential,
    total_reviews,
    avg_reviews_per_month,
    portfolio_size

FROM scored_hosts
ORDER BY ranking;
