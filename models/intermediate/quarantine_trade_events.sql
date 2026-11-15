{{ config(
    materialized='incremental',
    unique_key='trade_id',
    table_format='iceberg',
    external_volume='ext_trading_s3',
    base_location_root='silver/quarantine',
    tags=['trading_pipeline']
) }}

select
    trade_id,
    trader_id,
    ticker_symbol,
    order_side,
    order_status,
    quantity,
    execution_price,
    commission_fee,
    executed_at,
    to_varchar(failed_rules) as failed_rules_json,
    dq_status,
    _kafka_partition,
    _kafka_offset,
    _kafka_ingested_at,
    _stream_consumed_at,
    current_date() as quarantined_date,
    current_timestamp()::timestamp_ntz(6) as quarantined_at
from {{ ref('int_trades_validated') }}
where is_valid_trade = false

{% if is_incremental() %}
  -- _stream_consumed_at is stamped by Snowflake when stg_trade_events_cdc reads the stream,
  -- so producer clock skew or a Kafka replay cannot make this filter skip rows.
  and _stream_consumed_at > (select coalesce(max(_stream_consumed_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}