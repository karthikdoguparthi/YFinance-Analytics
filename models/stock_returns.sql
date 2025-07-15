-- models/stock_returns.sql

WITH base AS (
    SELECT
        ticker,
        date,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close
    FROM {{ source('raw', 'stock_prices') }}
),
returns AS (
    SELECT
        ticker,
        date,
        (close - prev_close) / NULLIF(prev_close, 0) AS daily_return
    FROM base
    WHERE prev_close IS NOT NULL
)
SELECT
    r.*,
    s.sector
FROM returns r
LEFT JOIN {{ ref('sectors') }} s
    ON r.ticker = s.ticker