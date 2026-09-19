import json
import os
import re
import time
import boto3

athena = boto3.client("athena")

DATABASE = os.environ.get("ATHENA_DATABASE", "alphafold_db")
TABLE = os.environ.get("ATHENA_TABLE", "curated")
OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT_LOCATION", "s3://alpha-lakehouse/athena-results/")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

MAX_LIMIT = 50
DEFAULT_LIMIT = 10
QUERY_TIMEOUT_SECONDS = 25


def sanitize_organism(value):
    # Same rule the ingest/Glue steps use for the organism partition value,
    # so a match here is guaranteed to match a real partition. It also
    # strips anything that could break out of the quoted string below
    # (no quotes/semicolons survive), so the query stays injection-safe
    # without needing Athena's prepared-statement machinery.
    value = value.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\-]", "_", value)


def run_query(sql):
    exec_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    deadline = time.time() + QUERY_TIMEOUT_SECONDS
    while time.time() < deadline:
        status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", f"query {state}"))
        time.sleep(0.5)
    else:
        raise TimeoutError("Athena query did not finish in time")

    rows, columns = [], None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=exec_id):
        result_rows = page["ResultSet"]["Rows"]
        if columns is None:
            columns = [c["Label"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
            result_rows = result_rows[1:]  # first page's first row is the header
        for row in result_rows:
            rows.append({
                col: field.get("VarCharValue")
                for col, field in zip(columns, row["Data"])
            })
    return rows


def respond(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    params = event.get("queryStringParameters") or {}

    organism_raw = params.get("organism")
    if not organism_raw:
        return respond(400, {"error": "missing required query param 'organism'"})
    organism = sanitize_organism(organism_raw)

    try:
        limit = int(params.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return respond(400, {"error": "'limit' must be an integer"})
    limit = max(1, min(limit, MAX_LIMIT))

    sql = f"""
        SELECT uniprot_accession, gene, global_metric_value, confidence_tier,
               tool_used, provider_id, model_created_date
        FROM {TABLE}
        WHERE organism = '{organism}'
          AND global_metric_value IS NOT NULL
        ORDER BY global_metric_value ASC
        LIMIT {limit}
    """

    try:
        rows = run_query(sql)
    except (RuntimeError, TimeoutError) as e:
        print(f"query failed: {e}")
        return respond(502, {"error": "query failed, check CloudWatch logs"})

    if not rows:
        return respond(404, {"error": f"no structures found for organism '{organism}'"})

    return respond(200, {"organism": organism, "count": len(rows), "results": rows})
