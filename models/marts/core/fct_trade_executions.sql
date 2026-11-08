{{ config(
    materialized='incremental',
    unique_key='trade_execution_sk',
    table_format='iceberg',
    external_volume='ext_trading_s3',
    base_location_root='gold/core/fct_trade_executions',
    tags=['trading_pipeline']
) }}

with valid_trades as (
    select *
    from {{ ref('int_trades_validated') }}
    where is_valid_trade = true
    {% if is_incremental() %}
      -- 3-day lookback window to capture T+1/T+2 clearinghouse settlement updates
      and executed_at >= (select dateadd('day', -3, coalesce(max(executed_at), '1900-01-01'::timestamp_ntz)) from {{ this }})
    {% endif %}
),

traders as (
    select
        trader_sk,
        trader_natural_key,
        trader_tier,
        kyc_status
    from {{ ref('dim_traders') }}
),

enriched_facts as (
    select
        {{ dbt_utils.generate_surrogate_key(['t.trade_id']) }} as trade_execution_sk,
        t.trade_id,
        coalesce(d.trader_sk, md5('-1')) as trader_sk,
        coalesce(d.trader_tier, 'UNCLASSIFIED') as trader_tier,
        t.ticker_symbol,
        t.order_side,
        t.order_status,
        cast(t.executed_at as date) as trade_date,
        t.executed_at::timestamp_ntz(6) as executed_at,
        t.quantity,
        t.execution_price,
        t.commission_fee,

        -- Financial Fact Measures
        round(t.quantity * t.execution_price, 2) as gross_notional_value,
        round((t.quantity * t.execution_price) + t.commission_fee, 2) as net_settlement_amount,

        -- Business Logic Flag: Settled or Executed Market Fill
        case
            when t.order_status in ('EXECUTED', 'SETTLED')
                 and coalesce(d.kyc_status, 'PENDING') = 'VERIFIED'
                then true
            else false
        end as is_verified_settled_trade,

        t._kafka_ingested_at::timestamp_ntz(6) as _kafka_ingested_at,
        current_date() as ingested_date,
        current_timestamp()::timestamp_ntz(6) as dbt_updated_at

    from valid_trades t
    left join traders d
        on t.trader_id = d.trader_natural_key
)

select * from enriched_facts