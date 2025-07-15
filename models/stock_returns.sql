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
