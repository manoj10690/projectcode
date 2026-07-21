"""
Metadata-Driven Autonomous Data Ingestion Engine
=================================================
Maps to Chapter 5 "Implementation" -- the generic Beam/Dataflow pipeline that
reads its behaviour entirely from the Metadata Store (pipelines, schema_ledger,
dq_rules) instead of being hand-written per source.

Run locally (DirectRunner) against a sample file:
    python beam_pipeline.py \
        --pipeline_id <uuid> \
        --input gs://<bronze-bucket>/incoming/*.json \
        --runner DirectRunner \
        --db_host 127.0.0.1 --db_name ingest_metadata \
        --db_user ingest_app --db_password <pwd>

Run on Dataflow:
    python beam_pipeline.py \
        --pipeline_id <uuid> \
        --input gs://<bronze-bucket>/incoming/*.json \
        --runner DataflowRunner \
        --project <PROJECT_ID> --region us-central1 \
        --temp_location gs://<project>-dev-df-staging/tmp \
        --staging_location gs://<project>-dev-df-staging/staging \
        --service_account_email <dataflow-sa-email> \
        --db_host <CLOUD_SQL_PRIVATE_IP> --db_name ingest_metadata \
        --db_user ingest_app --db_password <pwd>
"""

import argparse
import json
import logging
import re
import uuid
from datetime import datetime, timezone

import apache_beam as beam
import psycopg2
import psycopg2.extras
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions


# ---------------------------------------------------------------------------
# Metadata access layer
# ---------------------------------------------------------------------------
class MetadataStore:
    """Thin synchronous wrapper around the Cloud SQL metadata tables.

    Beam workers each open their own connection (Beam DoFn.setup()), so this
    class is intentionally lightweight and stateless between calls.
    """

    def __init__(self, host, dbname, user, password, port=5432):
        self._conn_kwargs = dict(host=host, dbname=dbname, user=user, password=password, port=port)

    def _connect(self):
        return psycopg2.connect(**self._conn_kwargs)

    def get_active_schema(self, pipeline_id):
        with self._connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT schema_id, version_number, schema_json
                FROM schema_ledger
                WHERE pipeline_id = %s AND active_status = TRUE
                ORDER BY version_number DESC LIMIT 1
                """,
                (pipeline_id,),
            )
            return cur.fetchone()

    def get_rules(self, schema_id):
        with self._connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM dq_rules WHERE schema_id = %s", (schema_id,))
            return cur.fetchall()

    def get_pipeline(self, pipeline_id):
        with self._connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pipelines WHERE pipeline_id = %s", (pipeline_id,))
            return cur.fetchone()

    def start_run(self, pipeline_id, schema_id):
        run_id = str(uuid.uuid4())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pipeline_runs (run_id, pipeline_id, schema_id, status, started_at)
                   VALUES (%s, %s, %s, 'RUNNING', %s)""",
                (run_id, pipeline_id, schema_id, datetime.now(timezone.utc)),
            )
            conn.commit()
        return run_id

    def finish_run(self, run_id, status, rows_read, rows_written, rows_quarantined):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE pipeline_runs
                   SET status=%s, rows_read=%s, rows_written=%s, rows_quarantined=%s, ended_at=%s
                   WHERE run_id=%s""",
                (status, rows_read, rows_written, rows_quarantined, datetime.now(timezone.utc), run_id),
            )
            conn.commit()

    def raise_new_column_alert(self, run_id, pipeline_id, new_columns):
        """Records schema-drift detection so the self-service UI can surface it."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO dq_alerts (alert_id, run_id, alert_source, failed_row_count, sample_payload)
                   VALUES (%s, %s, 'DETERMINISTIC_GATE', 0, %s)""",
                (str(uuid.uuid4()), run_id, json.dumps({"new_columns": new_columns, "pipeline_id": pipeline_id})),
            )
            conn.commit()

    def raise_dq_alert(self, run_id, rule_id, failed_row_count, sample_payload):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO dq_alerts (alert_id, run_id, rule_id, alert_source, failed_row_count, sample_payload)
                   VALUES (%s, %s, %s, 'DETERMINISTIC_GATE', %s, %s)""",
                (str(uuid.uuid4()), run_id, rule_id, failed_row_count, json.dumps(sample_payload)),
            )
            conn.commit()


# ---------------------------------------------------------------------------
# Beam transforms
# ---------------------------------------------------------------------------
class ParseRecord(beam.DoFn):
    """Parses one raw JSON line into a dict. Malformed lines go to the bad-shape
    output tag so they can be routed to the DLQ without failing the pipeline."""

    OUTPUT_TAG_BAD = "bad_shape"

    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except json.JSONDecodeError as e:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_BAD, {"raw": element, "error": str(e)})


class SchemaDriftGate(beam.DoFn):
    """Compares each record's keys against the active schema_ledger definition.

    - Extra/unknown columns -> logged as a drift event (evolution_policy decides
      whether the pipeline auto-widens the sink table or just alerts).
    - Missing required (NOT_NULL-tagged) columns are left for the DQ gate below.
    """

    OUTPUT_TAG_DRIFT = "drift"

    def __init__(self, expected_columns):
        self.expected_columns = set(expected_columns)

    def process(self, record):
        record_columns = set(record.keys())
        new_columns = record_columns - self.expected_columns
        if new_columns:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_DRIFT, sorted(new_columns))
        yield record


class DeterministicDQGate(beam.DoFn):
    """Applies the dq_rules fetched from the metadata store to every record.
    Records that violate a FAIL-severity rule are routed to the quarantine tag;
    everything else (including WARN-only violations) passes through."""

    OUTPUT_TAG_QUARANTINE = "quarantine"

    def __init__(self, rules):
        self.rules = rules  # list of dq_rules rows

    def _violates(self, record, rule):
        value = record.get(rule["target_column"])
        ctype = rule["constraint_type"]
        if ctype == "NOT_NULL":
            return value is None or value == ""
        if ctype == "REGEX_MATCH":
            return value is not None and not re.match(rule["constraint_value"], str(value))
        if ctype == "VALUE_MIN":
            return value is not None and float(value) < float(rule["constraint_value"])
        if ctype == "VALUE_MAX":
            return value is not None and float(value) > float(rule["constraint_value"])
        if ctype == "ALLOWED_VALUES":
            allowed = [v.strip() for v in rule["constraint_value"].split(",")]
            return value is not None and str(value) not in allowed
        return False

    def process(self, record):
        violations = []
        for rule in self.rules:
            if self._violates(record, rule):
                violations.append(rule)

        fail_violations = [r for r in violations if r["severity"] == "FAIL"]
        if fail_violations:
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_TAG_QUARANTINE,
                {"record": record, "violated_rules": [r["rule_id"] for r in fail_violations]},
            )
        else:
            yield record


class EnrichWithLineage(beam.DoFn):
    """Stamps every good record with ingestion lineage metadata before it lands
    in the Bronze/Silver BigQuery table (Ch.4.3 lineage requirement)."""

    def __init__(self, pipeline_id, run_id, schema_version):
        self.pipeline_id = pipeline_id
        self.run_id = run_id
        self.schema_version = schema_version

    def process(self, record):
        record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        record["_pipeline_id"] = self.pipeline_id
        record["_run_id"] = self.run_id
        record["_schema_version"] = self.schema_version
        yield record


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_id", required=True, help="UUID row in the pipelines table")
    parser.add_argument("--input", required=True, help="gs:// glob of newly landed bronze files")
    parser.add_argument("--dlq_path", required=True, help="gs:// prefix to write quarantined/bad records to")
    parser.add_argument("--output_table", required=True, help="project:dataset.table for good records")
    parser.add_argument("--db_host", required=True)
    parser.add_argument("--db_name", required=True)
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_password", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    store = MetadataStore(known_args.db_host, known_args.db_name, known_args.db_user, known_args.db_password)
    schema_row = store.get_active_schema(known_args.pipeline_id)
    if not schema_row:
        raise RuntimeError(f"No active schema found for pipeline {known_args.pipeline_id}. "
                            f"Register one via the self-service UI first.")
    rules = store.get_rules(schema_row["schema_id"])
    run_id = store.start_run(known_args.pipeline_id, schema_row["schema_id"])
    expected_columns = list(schema_row["schema_json"].keys())

    logging.info("Starting run %s for pipeline %s (schema v%s, %d DQ rules)",
                 run_id, known_args.pipeline_id, schema_row["version_number"], len(rules))

    with beam.Pipeline(options=options) as p:
        raw = p | "ReadRaw" >> beam.io.ReadFromText(known_args.input)

        parsed, bad_shape = (
            raw
            | "ParseJSON" >> beam.ParDo(ParseRecord()).with_outputs(ParseRecord.OUTPUT_TAG_BAD, main="good")
        )

        drift_checked, drift_events = (
            parsed
            | "SchemaDriftCheck" >> beam.ParDo(SchemaDriftGate(expected_columns))
                .with_outputs(SchemaDriftGate.OUTPUT_TAG_DRIFT, main="checked")
        )

        clean, quarantined = (
            drift_checked
            | "DQGate" >> beam.ParDo(DeterministicDQGate(rules))
                .with_outputs(DeterministicDQGate.OUTPUT_TAG_QUARANTINE, main="clean")
        )

        enriched = clean | "EnrichLineage" >> beam.ParDo(
            EnrichWithLineage(known_args.pipeline_id, run_id, schema_row["version_number"])
        )

        # Good records -> BigQuery (Silver layer table registered per pipeline)
        enriched | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

        # Quarantined + malformed records -> DLQ bucket for triage
        (
            quarantined
            | "QuarantineToJSON" >> beam.Map(json.dumps)
            | "WriteQuarantine" >> beam.io.WriteToText(
                f"{known_args.dlq_path}/quarantine/run={run_id}/part", file_name_suffix=".json"
            )
        )
        (
            bad_shape
            | "BadShapeToJSON" >> beam.Map(json.dumps)
            | "WriteBadShape" >> beam.io.WriteToText(
                f"{known_args.dlq_path}/malformed/run={run_id}/part", file_name_suffix=".json"
            )
        )

        # Side-effect: log schema drift back to the metadata store per batch
        (
            drift_events
            | "CombineDriftColumns" >> beam.combiners.ToList()
            | "PersistDrift" >> beam.Map(
                lambda batches, pid=known_args.pipeline_id, rid=run_id: (
                    store.raise_new_column_alert(rid, pid, sorted({c for cols in batches for c in cols}))
                    if batches else None
                )
            )
        )

    store.finish_run(run_id, "SUCCEEDED", rows_read=0, rows_written=0, rows_quarantined=0)
    logging.info("Run %s complete.", run_id)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
