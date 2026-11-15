-- Business question: How concentrated is revenue among sellers? Which sellers
-- make up the first 80 percent of revenue (the Pareto set)?
WITH seller_revenue AS (
    SELECT s.seller_name,
           sum(f.item_total) AS revenue
    FROM dw.fact_order_items AS f
    JOIN dw.dim_seller AS s ON s.seller_sk = f.seller_sk
    WHERE f.order_status <> 'cancelled'
    GROUP BY s.seller_name
),
shares AS (
    SELECT seller_name,
           revenue,
           revenue / sum(revenue) OVER () AS revenue_share,
           sum(revenue) OVER (ORDER BY revenue DESC, seller_name)
               / sum(revenue) OVER () AS cumulative_share
    FROM seller_revenue
)
SELECT seller_name,
       revenue,
       round(100 * revenue_share, 1) AS revenue_share_pct,
       round(100 * cumulative_share, 1) AS cumulative_share_pct,
       -- A seller belongs to the Pareto set if the sellers ranked above it
       -- have not yet reached 80 percent.
       cumulative_share - revenue_share < 0.8 AS in_top_80_pct
FROM shares
ORDER BY revenue DESC, seller_name;
