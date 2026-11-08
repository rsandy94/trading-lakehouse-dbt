{{ config(
    materialized='view',
    tags=['trading_pipeline']
) }}

select
    trader_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    lower(trim(email)) as email,
    upper(trim(kyc_status)) as kyc_status,
    upper(trim(trader_tier)) as trader_tier,
    upper(trim(account_country)) as account_country,
    updated_at
from {{ source('kafka_trading', 'traders_cdc') }}
qualify row_number() over (
    partition by trader_id
    order by updated_at desc
) = 1