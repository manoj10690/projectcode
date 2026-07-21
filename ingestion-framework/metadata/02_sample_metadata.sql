-- ============================================================================
-- Sample metadata to register one working pipeline end-to-end.
-- Run after sql/01_schema.sql. Replace the UUIDs consistently if you like,
-- or just let gen_random_uuid() defaults populate them and copy the printed
-- pipeline_id into your Dataflow launch command / UI.
-- ============================================================================

INSERT INTO pipelines (pipeline_id, name, description, source_type, source_config, sink_config, evolution_policy, created_by)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    'customer_orders_feed',
    'Nightly customer order extract dropped as JSON into the bronze bucket',
    'FILE',
    '{"path_glob": "gs://<PROJECT_ID>-dev-bronze/incoming/customer_orders/*.json"}',
    '{"project": "<PROJECT_ID>", "dataset": "silver", "table": "customer_orders"}',
    'ALLOW_ADDITIONS',
    'demo-admin'
);

INSERT INTO schema_ledger (schema_id, pipeline_id, version_number, schema_json, active_status)
VALUES (
    '22222222-2222-2222-2222-222222222222',
    '11111111-1111-1111-1111-111111111111',
    1,
    '{"order_id": "STRING", "customer_id": "STRING", "order_amount": "NUMERIC", "order_status": "STRING", "order_ts": "TIMESTAMP"}',
    TRUE
);

INSERT INTO dq_rules (schema_id, target_column, constraint_type, constraint_value, threshold_pct, severity) VALUES
    ('22222222-2222-2222-2222-222222222222', 'order_id', 'NOT_NULL', NULL, 0, 'FAIL'),
    ('22222222-2222-2222-2222-222222222222', 'customer_id', 'NOT_NULL', NULL, 0, 'FAIL'),
    ('22222222-2222-2222-2222-222222222222', 'order_amount', 'VALUE_MIN', '0', 0, 'FAIL'),
    ('22222222-2222-2222-2222-222222222222', 'order_status', 'ALLOWED_VALUES', 'PLACED,SHIPPED,CANCELLED,RETURNED', 1, 'WARN');
