import boto3
import json
import re
import pandas as pd
from io import BytesIO

s3 = boto3.client("s3")
BUCKET = "alpha-lakehouse"

def list_raw_keys():
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="raw/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys

def confidence_tier(record):
    if record.get("fractionPlddtVeryHigh", 0) > 0.9:
        return "very_high"
    elif record.get("fractionPlddtConfident", 0) + record.get("fractionPlddtVeryHigh", 0) > 0.7:
        return "confident"
    else:
        return "low"

def sanitize(value):
    value = str(value).strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\-]", "_", value)

rows = []
seen_ids = set()

for key in list_raw_keys():
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = json.loads(obj["Body"].read())
    record = data[0]

    model_id = record.get("modelEntityId")
    if not model_id or model_id in seen_ids:
        continue
    seen_ids.add(model_id)

    rows.append({
        "model_entity_id": model_id,
        "uniprot_accession": key.split("/")[-1].replace(".json", ""),
        "organism": sanitize(record.get("organismScientificName", "unknown")),
        "tax_id": record.get("taxId"),
        "gene": record.get("gene"),
        "global_metric_value": record.get("globalMetricValue"),
        "confidence_tier": confidence_tier(record),
        "tool_used": record.get("toolUsed"),
        "provider_id": record.get("providerId"),
        "model_created_date": pd.to_datetime(record.get("modelCreatedDate")),
        "latest_version": record.get("latestVersion"),
        "is_uniprot_reviewed": record.get("isUniProtReviewed"),
        "sequence_start": record.get("sequenceStart"),
        "sequence_end": record.get("sequenceEnd"),
        "sequence_checksum": record.get("sequenceChecksum"),
    })

df = pd.DataFrame(rows)
print(f"Cleaned {len(df)} unique records from {len(seen_ids)} raw files")

for organism, group in df.groupby("organism"):
    buf = BytesIO()
    group.drop(columns=["organism"]).to_parquet(buf, index=False)  # drop before writing
    key = f"curated/organism={organism}/data.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"Wrote {len(group)} rows to {key}")