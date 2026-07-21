-- ============================================================================
-- Silver layer: cleaned, schema-validated, deduplicated tables.
-- One table per pipeline; created automatically on first successful run via
-- WriteToBigQuery(CREATE_IF_NEEDED) in engine/beam_pipeline.py, but you can
-- also pre-create explicitly with a stricter schema, e.g.:
-- ============================================================================
CREATE TABLE IF NOT EXISTS `silver.customer_orders`
(
  order_id        STRING NOT NULL,
  customer_id     STRING NOT NULL,
  order_amount    NUMERIC,
  order_status    STRING,
  order_ts        TIMESTAMP,
  -- lineage columns stamped by EnrichWithLineage in the Beam engine
  _ingested_at    TIMESTAMP,
  _pipeline_id    STRING,
  _run_id         STRING,
  _schema_version INT64
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY customer_id;

-- ============================================================================
-- Gold layer: business-curated, aggregated views/tables built on top of Silver.
-- These are typically materialized by a scheduled query or dbt/Dataform job,
-- triggered as the final step of the orchestration workflow (Ch.5.1.1).
-- ============================================================================
CREATE TABLE IF NOT EXISTS `gold.daily_order_summary`
(
  order_date      DATE,
  total_orders    INT64,
  total_revenue   NUMERIC,
  distinct_customers INT64,
  updated_at      TIMESTAMP
)
PARTITION BY order_date;

-- Example materialization query (schedule via Cloud Workflows / Composer):
-- MERGE `gold.daily_order_summary` T
-- USING (
--   SELECT DATE(order_ts) AS order_date,
--          COUNT(*) AS total_orders,
--          SUM(order_amount) AS total_revenue,
--          COUNT(DISTINCT customer_id) AS distinct_customers,
--          CURRENT_TIMESTAMP() AS updated_at
--   FROM `silver.customer_orders`
--   GROUP BY 1
-- ) S
-- ON T.order_date = S.order_date
-- WHEN MATCHED THEN UPDATE SET total_orders = S.total_orders,
--   total_revenue = S.total_revenue, distinct_customers = S.distinct_customers,
--   updated_at = S.updated_at
-- WHEN NOT MATCHED THEN INSERT ROW;
