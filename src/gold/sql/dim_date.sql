-- Calendar dimension. One row per day plus the unknown member (date_key -1).
CREATE TABLE gold.dim_date USING iceberg AS
SELECT CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
       calendar_date AS full_date,
       weekday(calendar_date) + 1 AS day_of_week,
       date_format(calendar_date, 'EEEE') AS day_name,
       weekofyear(calendar_date) AS week_of_year,
       month(calendar_date) AS month,
       date_format(calendar_date, 'MMMM') AS month_name,
       quarter(calendar_date) AS quarter,
       year(calendar_date) AS year,
       weekday(calendar_date) >= 5 AS is_weekend,
       calendar_date = last_day(calendar_date) AS is_month_end
FROM (
    SELECT explode(sequence(DATE '2024-01-01', DATE '2029-12-31', INTERVAL 1 DAY)) AS calendar_date
)
UNION ALL
SELECT -1, CAST(NULL AS DATE), CAST(NULL AS INT), 'Unknown', CAST(NULL AS INT), CAST(NULL AS INT),
       'Unknown', CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS BOOLEAN), CAST(NULL AS BOOLEAN)
