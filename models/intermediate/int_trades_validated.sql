{{ config(
    materialized='view',
    tags=['trading_pipeline']
) }}

with staged_trades as (
    select * from {{ ref('stg_trade_events') }}
),

rule_evaluation as (
    select
        t.*,

        -- Construct an array of failed data quality rules per row (nulls are stripped)
        array_construct_compact(
            case when trade_id is null or trader_id is null
                 then 'ERR_MISSING_MANDATORY_KEY' end,
            case when ticker_symbol is null or not regexp_like(ticker_symbol, '^[A-Z]{1,5}$')
                 then 'ERR_INVALID_TICKER_FORMAT' end,
            case when quantity is null or quantity <= 0
                 then 'ERR_NON_POSITIVE_QUANTITY' end,
            case when execution_price is null or execution_price <= 0
                 then 'ERR_NON_POSITIVE_PRICE' end,
            case when executed_at is null or executed_at < '2020-01-01'::timestamp_ntz
                      -- Producer writes UTC; compare against UTC "now", never the
                      -- session-local clock (session TIMEZONE = America/Los_Angeles).
                      or executed_at > dateadd('minute', 5, convert_timezone('UTC', current_timestamp())::timestamp_ntz)
                 then 'ERR_TIMESTAMP_OUT_OF_BOUNDS' end
        ) as failed_rules

    from staged_trades t
),

scored as (
    select
        *,
        -- Row passes only if zero rules failed
        iff(array_size(failed_rules) = 0, true, false) as is_valid_trade,
        iff(array_size(failed_rules) = 0, 'PASS', 'FAIL') as dq_status
    from rule_evaluation
)

select * from scored
