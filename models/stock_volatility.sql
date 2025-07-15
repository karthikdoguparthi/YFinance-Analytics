WITH returns AS (
    SELECT * FROM {{ ref('stock_returns') }}
),

rolling_vol AS (
    SELECT
        ticker,
        date,
        STDDEV_SAMP(daily_return) OVER (
            PARTITION BY ticker ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_volatility
    FROM returns
)

SELECT * FROM rolling_vol