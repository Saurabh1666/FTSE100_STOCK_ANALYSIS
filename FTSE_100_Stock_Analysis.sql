CREATE TABLE companies (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100)
);
SELECT * FROM companies LIMIT 5;

CREATE TABLE stock_prices (
    ticker VARCHAR(10),
    date DATE,
    price NUMERIC,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume BIGINT,
    change_percent NUMERIC
);
SELECT * FROM stock_prices LIMIT 5;
SELECT ticker, 
       ROUND(AVG(price), 2) AS avg_price
FROM stock_prices
GROUP BY ticker
ORDER BY avg_price DESC;
SELECT ticker, 
       MAX(price) AS max_price, 
       MIN(price) AS min_price
FROM stock_prices
GROUP BY ticker
ORDER BY max_price DESC;
SELECT ticker, 
       MAX(price) AS max_price, 
       MIN(price) AS min_price
FROM stock_prices
GROUP BY ticker
ORDER BY max_price DESC;
SELECT ticker, 
       MAX(price) AS max_price, 
       MIN(price) AS min_price
FROM stock_prices
GROUP BY ticker
ORDER BY max_price DESC;
SELECT ticker, 
       MAX(price) AS max_price, 
       MIN(price) AS min_price
FROM stock_prices
GROUP BY ticker
ORDER BY max_price DESC;
SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY volume DESC) AS rn
    FROM stock_prices
) sub
WHERE rn <= 5
ORDER BY ticker, volume DESC;
SELECT ticker, 
       DATE_TRUNC('month', date) AS month,
       ROUND(AVG(price), 2) AS avg_monthly_price
FROM stock_prices
GROUP BY ticker, month
ORDER BY ticker, month;
SELECT ticker, 
       SUM(volume) AS total_volume
FROM stock_prices
GROUP BY ticker
ORDER BY total_volume DESC;
SELECT ticker, 
       ROUND(STDDEV(change_percent), 2) AS volatility
FROM stock_prices
GROUP BY ticker
ORDER BY volatility DESC;
SELECT ticker, date, volume, change_percent
FROM stock_prices
WHERE volume > (
    SELECT PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY volume)
    FROM stock_prices
)
AND ABS(change_percent) > 2
ORDER BY volume DESC;
WITH monthly_avg AS (
  SELECT ticker,
         DATE_TRUNC('month', date) AS month,
         ROUND(AVG(price), 2) AS avg_month_price
  FROM stock_prices
  GROUP BY ticker, month
),
monthly_change AS (
  SELECT ticker, month,
         avg_month_price,
         LAG(avg_month_price) OVER (PARTITION BY ticker ORDER BY month) AS prev_month_price
  FROM monthly_avg
)
SELECT *, 
       ROUND(avg_month_price - prev_month_price, 2) AS price_change,
       CASE 
           WHEN avg_month_price > prev_month_price THEN 'Up'
           WHEN avg_month_price < prev_month_price THEN 'Down'
           ELSE 'No Change'
       END AS trend_direction
FROM monthly_change
WHERE prev_month_price IS NOT NULL
ORDER BY ticker, month;
SELECT ticker, 
       ROUND(AVG(price), 2) AS avg_price
FROM stock_prices
GROUP BY ticker
ORDER BY avg_price DESC;
SELECT ticker, 
       MAX(price) AS max_price, 
       MIN(price) AS min_price
FROM stock_prices
GROUP BY ticker
ORDER BY max_price DESC;
SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY volume DESC) AS rn
    FROM stock_prices
) sub
WHERE rn <= 5
ORDER BY ticker, volume DESC;
SELECT ticker, 
       DATE_TRUNC('month', date) AS month,
       ROUND(AVG(price), 2) AS avg_month_price
FROM stock_prices
GROUP BY ticker, month
ORDER BY ticker, month;
SELECT ticker, 
       ROUND(STDDEV(change_percent), 2) AS volatility
FROM stock_prices
GROUP BY ticker
ORDER BY volatility DESC;
SELECT ticker, date, volume, change_percent
FROM stock_prices
WHERE volume > (
    SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY volume)
    FROM stock_prices
)
AND ABS(change_percent) > 2
ORDER BY volume DESC;
WITH monthly_avg AS (
  SELECT ticker,
         DATE_TRUNC('month', date) AS month,
         ROUND(AVG(price), 2) AS avg_month_price
  FROM stock_prices
  GROUP BY ticker, month
),
monthly_change AS (
  SELECT ticker, month,
         avg_month_price,
         LAG(avg_month_price) OVER (PARTITION BY ticker ORDER BY month) AS prev_month_price
  FROM monthly_avg
)
SELECT *, 
       ROUND(avg_month_price - prev_month_price, 2) AS price_change,
       CASE 
           WHEN avg_month_price > prev_month_price THEN 'Up'
           WHEN avg_month_price < prev_month_price THEN 'Down'
           ELSE 'No Change'
       END AS trend_direction
FROM monthly_change
WHERE prev_month_price IS NOT NULL
ORDER BY ticker, month;
WITH monthly_avg AS (
  SELECT ticker,
         DATE_TRUNC('month', date) AS month,
         ROUND(AVG(price), 2) AS avg_month_price
  FROM stock_prices
  GROUP BY ticker, month
),
monthly_change AS (
  SELECT ticker, month,
         avg_month_price,
         LAG(avg_month_price) OVER (PARTITION BY ticker ORDER BY month) AS prev_month_price
  FROM monthly_avg
),
trend_direction AS (
  SELECT *,
         CASE 
             WHEN avg_month_price > prev_month_price THEN 'Up'
             WHEN avg_month_price < prev_month_price THEN 'Down'
             ELSE 'No Change'
         END AS trend_dir
  FROM monthly_change
),
reversal_flagged AS (
  SELECT *, 
         LAG(trend_dir) OVER (PARTITION BY ticker ORDER BY month) AS prev_trend
  FROM trend_direction
)
SELECT *
FROM reversal_flagged
WHERE trend_dir IS DISTINCT FROM prev_trend
  AND prev_trend IS NOT NULL
ORDER BY ticker, month;SELECT ticker, 
       ROUND(AVG(price), 2) AS avg_price
FROM stock_prices
GROUP BY ticker
ORDER BY avg_price DESC;
SELECT ticker, 
       DATE_TRUNC('month', date) AS month,
       ROUND(AVG(price), 2) AS avg_month_price
FROM stock_prices
GROUP BY ticker, month
ORDER BY ticker, month;
SELECT ticker, 
       SUM(volume) AS total_volume
FROM stock_prices
GROUP BY ticker
ORDER BY total_volume DESC;