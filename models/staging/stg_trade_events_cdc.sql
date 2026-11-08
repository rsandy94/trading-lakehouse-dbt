{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['trading_pipeline']
) }}

-- Consumes the bronze stream. Committing this model advances the stream offset,
-- so has_data() flips to FALSE automatically after every successful build.
-- TRADE_EVENTS (unlike the landing table) has no RECORD_METADATA column; its
-- _kafka_partition/_kafka_offset were backfilled from landing's RECORD_METADATA
-- and are kept current by the transform task, so read them directly.
select
    trade_id, trader_id, ticker_symbol, order_side, order_status,
    quantity, execution_price, commission_fee, executed_at,
    _kafka_partition,
    _kafka_offset,
    _kafka_ingested_at,
    current_timestamp()::timestamp_ntz(6) as _stream_consumed_at
from BRONZE.TRADING.TRADE_EVENTS_STREAM
where metadata$action = 'INSERT'