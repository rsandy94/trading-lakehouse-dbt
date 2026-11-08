{{ config(
    materialized='view',
    tags=['trading_pipeline']
) }}

with raw_traders as (
    select * from {{ ref('stg_traders') }}
),

hashed as (
    select
        -- 1. SURROGATE KEY: Deterministic MD5 hash of the natural business key
        {{ dbt_utils.generate_surrogate_key(['trader_id']) }} as trader_sk,

        trader_id as trader_natural_key,
        first_name,
        last_name,
        email,
        kyc_status,
        trader_tier,        -- 'RETAIL', 'PATTERN_DAY_TRADER', 'INSTITUTIONAL'
        account_country,

        -- 2. ROW HASH DIFF: Tracks changes across mutable attributes for incremental merge
        {{ dbt_utils.generate_surrogate_key([
            'first_name',
            'last_name',
            'email',
            'kyc_status',
            'trader_tier',
            'account_country'
        ]) }} as row_hash_diff,

        updated_at as source_updated_at
    from raw_traders
)

select * from hashed