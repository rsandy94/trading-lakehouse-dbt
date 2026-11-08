{{ config(
    materialized='incremental',
    unique_key='liquidity_metric_sk',
    table_format='iceberg',
    external_volume='ext_trading_s3',
    base_location_root='gold/agg/agg_daily_tier_liquidity',
    tags=['trading_pipeline']
) }}

with trades as (
    select *
    from {{ ref('fct_trade_executions') }}
    {% if is_incremental() %}
      -- Re-aggregate trailing 3 days to absorb T+1/T+2 clearinghouse settlements
      where trade_date >= (select dateadd('day', -3, coalesce(max(trade_date), '1900-01-01'::date)) from {{ this }})
    {% endif %}
),

traders as (
    select
        trader_sk,
        trader_tier,
        account_country
    from {{ ref('dim_traders') }}
),

aggregated as (
    select
        -- Aggregation Grain: 1 row per (trade_date + trader_tier + order_side)
        f.trade_date,
        coalesce(d.trader_tier, f.trader_tier) as trader_tier,
        f.order_side,

        -- Volume & Participation Counts
        count(distinct f.trade_id) as total_trades_submitted,
        count(distinct case when f.is_verified_settled_trade then f.trade_id end) as verified_settled_trades_count,
        count(distinct f.trader_sk) as active_traders_count,

        -- Liquidity & Revenue Aggregations
        sum(case when f.is_verified_settled_trade then f.quantity else 0 end) as total_shares_traded,
        sum(case when f.is_verified_settled_trade then f.gross_notional_value else 0 end) as total_notional_volume,
        sum(case when f.is_verified_settled_trade then f.commission_fee else 0 end) as total_commission_revenue,

        -- Derived Ratios (Safe division via Snowflake DIV0)
        round(
            div0(
                sum(case when f.is_verified_settled_trade then f.gross_notional_value else 0 end),
                count(distinct case when f.is_verified_settled_trade then f.trade_id end)
            ), 2
        ) as average_settled_trade_notional

    from trades f
    left join traders d
        on f.trader_sk = d.trader_sk
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['trade_date', 'trader_tier', 'order_side']) }} as liquidity_metric_sk,
        trade_date,
        trader_tier,
        order_side,
        total_trades_submitted,
        verified_settled_trades_count,
        active_traders_count,
        total_shares_traded,
        total_notional_volume,
        total_commission_revenue,
        average_settled_trade_notional,
        current_timestamp()::timestamp_ntz(6) as aggregated_at
    from aggregated
)

select * from final