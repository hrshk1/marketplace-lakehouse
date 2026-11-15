-- Business question: For each department, which city sells the most, and how
-- much of the department's GMV does that city account for?
-- Reads the pre-aggregated table, as a dashboard would.
WITH city_sales AS (
    SELECT category, city, sum(gmv) AS gmv
    FROM dw.agg_daily_sales
    GROUP BY category, city
),
ranked AS (
    SELECT city_sales.*,
           rank() OVER (PARTITION BY category ORDER BY gmv DESC) AS city_rank,
           sum(gmv) OVER (PARTITION BY category) AS category_gmv
    FROM city_sales
)
SELECT category,
       city,
       gmv,
       round(100 * gmv / nullif(category_gmv, 0), 1) AS share_of_category_pct
FROM ranked
WHERE city_rank = 1
ORDER BY category;
