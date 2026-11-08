{{ config(
    materialized='incremental',
    unique_key='trader_sk',
    table_format='iceberg',
    external_volume='ext_trading_s3',
    base_location_root='gold/core/dim_traders',
    tags=['trading_pipeline']
) }}

with incoming_traders as (
    select * from {{ ref('int_traders_hashed') }}
)

{% if is_incremental() %}
    ,current_dimension as (
        select
            trader_sk,
            row_hash_diff
        from {{ this }}
    ),

    delta_detection as (
        select
            i.trader_sk,
            i.trader_natural_key,
            i.first_name,
            i.last_name,
            i.email,
            i.kyc_status,
            i.trader_tier,
            i.account_country,
            i.row_hash_diff,
            i.source_updated_at::timestamp_ntz(6) as source_updated_at,
            current_timestamp()::timestamp_ntz(6) as dbt_updated_at
        from incoming_traders i
        left join current_dimension c
            on i.trader_sk = c.trader_sk
        where
            -- Condition 1: Brand new trader account
            c.trader_sk is null
            -- Condition 2: Existing trader whose mutable attributes changed
            or i.row_hash_diff != c.row_hash_diff
    )

    select * from delta_detection

{% else %}
    select
        trader_sk,
        trader_natural_key,
        first_name,
        last_name,
        email,
        kyc_status,
        trader_tier,
        account_country,
        row_hash_diff,
        source_updated_at::timestamp_ntz(6) as source_updated_at,
        current_timestamp()::timestamp_ntz(6) as dbt_updated_at
    from incoming_traders
{% endif %}