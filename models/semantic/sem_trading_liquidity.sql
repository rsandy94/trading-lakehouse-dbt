{{ config(
    materialized='semantic_view',
    copy_grants=true
) }}

-- Platinum layer: Snowflake semantic view over the Gold star schema.
-- Built by the Snowflake-Labs/dbt_semantic_view package, which wraps this body in
-- CREATE OR REPLACE SEMANTIC VIEW. ref() keeps it in the dbt (and Dagster) lineage.

TABLES (
    traders AS {{ ref('dim_traders') }}
        PRIMARY KEY (trader_sk)
        WITH SYNONYMS = ('clients', 'investors', 'accounts')
        COMMENT = 'Conformed trader dimension. One row per trader with descriptive attributes: tier, account country, and trader surrogate key used to join to trade executions.',

    trades AS {{ ref('fct_trade_executions') }}
        PRIMARY KEY (trade_execution_sk)
        WITH SYNONYMS = ('executions', 'orders', 'fills', 'stock trades')
        COMMENT = 'Grain: one trade execution event. Contains gross notional value, commission fee, verification/settlement flags, and the trader surrogate key for each executed order fill.'
)

RELATIONSHIPS (
    trades_to_traders AS trades (trader_sk) REFERENCES traders (trader_sk)
)

DIMENSIONS (
    traders.account_country AS account_country
        WITH SYNONYMS = ('country', 'geography')
        COMMENT = 'Country of registration of the trader''s account. Use for geographic liquidity analysis and regional compliance reporting.',

    traders.trader_tier AS trader_tier
        WITH SYNONYMS = ('client segment', 'margin tier', 'investor tier')
        COMMENT = 'Customer segment of the executing trader (e.g. retail, institutional tiers). Use to segment liquidity by client class.',

    trades.order_side AS order_side
        WITH SYNONYMS = ('buy or sell', 'direction')
        COMMENT = 'Direction of the order: BUY or SELL. Use to split volume and fill-rate metrics by side of the book.',

    trades.ticker_symbol AS ticker_symbol
        WITH SYNONYMS = ('stock', 'symbol', 'equity ticker')
        COMMENT = 'Equity instrument identifier (e.g. AAPL, MSFT) for the executed trade. Use to analyze liquidity per instrument.',

    trades.trade_date AS trade_date
        WITH SYNONYMS = ('execution date', 'settlement date')
        COMMENT = 'Date the trade was executed. Use to group liquidity and revenue metrics by day, week, or month.'
)

METRICS (
    trades.average_trade_size AS DIV0(
            SUM(CASE WHEN trades.is_verified_settled_trade THEN trades.gross_notional_value ELSE 0 END),
            COUNT_IF(trades.is_verified_settled_trade)
        )
        WITH SYNONYMS = ('average order value', 'mean trade notional')
        COMMENT = 'Mean gross notional value per verified, settled trade. Useful for detecting changes in typical order size by tier, ticker, or country.',

    trades.execution_fill_rate AS DIV0(
            COUNT_IF(trades.is_verified_settled_trade),
            COUNT(trades.trade_id)
        )
        WITH SYNONYMS = ('settlement rate', 'order fill percentage')
        COMMENT = 'Percentage of executed trades that reach verified-settled status: verified settled trades divided by all trades. Data-quality proxy for the Bronze-to-Gold split gate.',

    trades.net_commission_revenue AS SUM(CASE WHEN trades.is_verified_settled_trade THEN trades.commission_fee ELSE 0 END)
        WITH SYNONYMS = ('brokerage revenue', 'trading fees earned')
        COMMENT = 'Sum of commission fees earned on verified, settled trades. Represents broker revenue recognized from settled trading activity.',

    trades.settled_notional_volume AS SUM(CASE WHEN trades.is_verified_settled_trade THEN trades.gross_notional_value ELSE 0 END)
        WITH SYNONYMS = ('total trading volume', 'gross liquidity', 'settled volume')
        COMMENT = 'Total gross notional value of verified, settled trades in the selected scope. Primary measure of traded liquidity; excludes unverified or unsettled executions.'
)

COMMENT = 'Platinum Semantic Layer governing equity trade liquidity metrics for BI dashboards and Snowflake Cortex Agents. Key metric is settled_notional_volume (gross liquidity); execution_fill_rate doubles as the batch pass-rate health signal downstream of the Silver quarantine gate.'
