"""
Self-Service UI -- Cloud Run app.
Maps to Ch.3.3 ("UI: Google Cloud Run") -- lets a data owner register a new
pipeline, define its schema + DQ rules, and monitor runs/DQ alerts, without
ever touching Terraform, Beam code, or the orchestration layer directly.

Local run:
    export DB_HOST=127.0.0.1 DB_NAME=ingest_metadata DB_USER=ingest_app DB_PASSWORD=<pwd>
    pip install -r requirements.txt
    python app.py
"""

import json
import os
import uuid

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, redirect, render_template, request, url_for

app = Flask(__name__)


def get_conn():
    # Cloud Run + Cloud SQL: connect via the Cloud SQL Auth Proxy sidecar/unix
    # socket using DB_INSTANCE_CONNECTION_NAME, or via public IP for local dev.
    host = os.environ.get("DB_HOST", "127.0.0.1")
    return psycopg2.connect(
        host=host,
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


@app.route("/")
def dashboard():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT p.pipeline_id, p.name, p.source_type, p.is_active,
                   r.status AS last_run_status, r.started_at AS last_run_at,
                   r.rows_written, r.rows_quarantined
            FROM pipelines p
            LEFT JOIN LATERAL (
                SELECT * FROM pipeline_runs
                WHERE pipeline_id = p.pipeline_id
                ORDER BY started_at DESC LIMIT 1
            ) r ON TRUE
            ORDER BY p.created_at DESC
        """)
        pipelines = cur.fetchall()
    return render_template("index.html", pipelines=pipelines)


@app.route("/pipelines/new", methods=["GET", "POST"])
def new_pipeline():
    if request.method == "GET":
        return render_template("new_pipeline.html")

    form = request.form
    pipeline_id = str(uuid.uuid4())
    schema_id = str(uuid.uuid4())

    # schema fields submitted as parallel arrays: field_name[] / field_type[]
    field_names = request.form.getlist("field_name")
    field_types = request.form.getlist("field_type")
    schema_json = dict(zip(field_names, field_types))

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO pipelines (pipeline_id, name, description, source_type,
                                       source_config, sink_config, evolution_policy, created_by)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (pipeline_id, form["name"], form.get("description", ""), form["source_type"],
             json.dumps({"path_glob": form["source_glob"]}),
             json.dumps({"dataset": form["sink_dataset"], "table": form["sink_table"]}),
             form.get("evolution_policy", "ALLOW_ADDITIONS"), form.get("created_by", "ui-user")),
        )
        cur.execute(
            """INSERT INTO schema_ledger (schema_id, pipeline_id, version_number, schema_json, active_status)
               VALUES (%s,%s,1,%s,TRUE)""",
            (schema_id, pipeline_id, json.dumps(schema_json)),
        )

        # optional NOT_NULL rules for any field checked "required"
        required_fields = request.form.getlist("required_field")
        for field in required_fields:
            cur.execute(
                """INSERT INTO dq_rules (schema_id, target_column, constraint_type, severity)
                   VALUES (%s,%s,'NOT_NULL','FAIL')""",
                (schema_id, field),
            )
        conn.commit()

    return redirect(url_for("dashboard"))


@app.route("/pipelines/<pipeline_id>")
def pipeline_detail(pipeline_id):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM pipelines WHERE pipeline_id = %s", (pipeline_id,))
        pipeline = cur.fetchone()
        cur.execute(
            """SELECT * FROM schema_ledger WHERE pipeline_id = %s
               ORDER BY version_number DESC""", (pipeline_id,)
        )
        schemas = cur.fetchall()
        cur.execute(
            """SELECT r.* FROM dq_rules r
               JOIN schema_ledger s ON r.schema_id = s.schema_id
               WHERE s.pipeline_id = %s AND s.active_status = TRUE""", (pipeline_id,)
        )
        rules = cur.fetchall()
        cur.execute(
            """SELECT * FROM pipeline_runs WHERE pipeline_id = %s
               ORDER BY started_at DESC LIMIT 20""", (pipeline_id,)
        )
        runs = cur.fetchall()
        cur.execute(
            """SELECT a.* FROM dq_alerts a
               JOIN pipeline_runs r ON a.run_id = r.run_id
               WHERE r.pipeline_id = %s ORDER BY a.created_at DESC LIMIT 50""", (pipeline_id,)
        )
        alerts = cur.fetchall()

    return render_template("pipeline_detail.html", pipeline=pipeline, schemas=schemas,
                            rules=rules, runs=runs, alerts=alerts)


@app.route("/api/pipelines/<pipeline_id>/runs")
def api_runs(pipeline_id):
    """JSON endpoint the dashboard polls for live run status."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT run_id, status, rows_written, rows_quarantined, started_at, ended_at
               FROM pipeline_runs WHERE pipeline_id = %s
               ORDER BY started_at DESC LIMIT 10""", (pipeline_id,)
        )
        runs = cur.fetchall()
    return jsonify([dict(r) for r in runs])


@app.route("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
