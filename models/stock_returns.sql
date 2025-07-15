<<<<<<< HEAD
with
    prices as (
        select
            ticker,
            date,
            adj_close,
            lag(adj_close) over (partition by ticker order by date) as prev_close
        from {{ source("raw", "stock_prices") }}
    ),

    returns as (
        select
            ticker,
            date,
            adj_close,
            prev_close,
            (adj_close - prev_close) / prev_close as daily_return
        from prices
        where prev_close is not null
    )

select *
from returns
=======
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
>>>>>>> 2b4efee17ecb41282497eb0dc8bce17229ef7134
