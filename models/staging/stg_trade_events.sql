{{ config(
    materialized='view',
    tags=['trading_pipeline']
) }}

with raw_events as (
    select
        trade_id,
        trader_id,
        upper(trim(ticker_symbol)) as ticker_symbol,
        upper(trim(order_side)) as order_side,
        upper(trim(order_status)) as order_status,
        cast(quantity as number(18, 2)) as quantity,
        cast(execution_price as number(18, 2)) as execution_price,
        coalesce(cast(commission_fee as number(18, 2)), 0.0) as commission_fee,
        executed_at as executed_at,
        cast(_kafka_partition as number(10, 0)) as _kafka_partition,
        cast(_kafka_offset as number(19, 0)) as _kafka_offset,
        _kafka_ingested_at as _kafka_ingested_at,
        _stream_consumed_at
    from  {{ ref('stg_trade_events_cdc') }}
),

deduplicated as (
    select *
    from raw_events
    -- Deduplicate Kafka at-least-once retries using the latest offset per trade_id
    qualify row_number() over (
        partition by trade_id
        order by _kafka_offset desc, _kafka_ingested_at desc
    ) = 1
)

select * from deduplicated