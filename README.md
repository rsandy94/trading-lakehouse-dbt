# trading-lakehouse-dbt

dbt project for an event-driven medallion lakehouse on Snowflake and Apache Iceberg. Equity trade executions stream in from Kafka, and dbt carries them from a Bronze stream through a Silver quarantine gate into a Gold star schema and a Platinum semantic view.

Dagster runs this project whenever the Bronze stream has new data. The orchestration code, Kafka producer and connector config are in [trading-lakehouse](https://github.com/rsandy94/trading-lakehouse).

## Layers

| Layer | Models | Materialization |
|---|---|---|
| Bronze | `BRONZE.TRADING.TRADE_EVENTS` and its append-only stream `TRADE_EVENTS_STREAM` (created outside dbt) | Iceberg table + stream |
| Silver | `stg_trade_events_cdc` consumes the stream, which advances its offset. `stg_trade_events` keeps the latest Kafka offset per `trade_id`. `int_traders_hashed` builds `trader_sk` and `row_hash_diff`. `int_trades_validated` scores every row against the quality rules. | Incremental table + views |
| Silver quarantine | `quarantine_trade_events`: rows that failed at least one rule, with the rules they broke | Incremental Iceberg table |
| Gold | `dim_traders` (hash-diff merge), `fct_trade_executions` (3-day lookback merge), `agg_daily_tier_liquidity` (daily rollup) | Incremental Iceberg tables |
| Platinum | `sem_trading_liquidity`: tables, relationship, dimensions, metrics and synonyms for BI tools and Cortex agents | Snowflake semantic view |

Every model is tagged `trading_pipeline`; Dagster selects on that tag.

## Setup

1. Install Python 3.12, then create and activate a virtual environment:
   ```bash
   python -m venv .dbtenv
   source .dbtenv/bin/activate      # Windows: .dbtenv\Scripts\activate
   ```
2. Install dependencies and dbt packages:
   ```bash
   pip install -r requirements.txt
   dbt deps
   ```
3. Add a `trading_lakehouse_dbt` profile to `~/.dbt/profiles.yml` for your Snowflake account. Credentials are not stored in this repo.
4. Create the external volume `ext_trading_s3`, the Bronze table and its stream. The Iceberg models write to `ext_trading_s3`.
5. Build:
   ```bash
   dbt build --select tag:trading_pipeline
   ```

## Semantic view

`models/semantic/sem_trading_liquidity.sql` uses the `semantic_view` materialization from [Snowflake-Labs/dbt_semantic_view](https://github.com/Snowflake-Labs/dbt_semantic_view). The model body is plain `CREATE SEMANTIC VIEW` syntax, with `ref()` for the Gold tables, so the view is rebuilt after its inputs and shows up in dbt lineage. It lands in `GOLD.SEMANTIC`.
