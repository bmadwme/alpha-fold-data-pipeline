import json
import os
import re
import urllib.request
import urllib.error
import boto3

s3 = boto3.client("s3")
BUCKET = os.environ.get("BUCKET_NAME", "alpha-lakehouse")

def fetch_prediction(uniprot_id):
    url = f"https://alphafold.ebi.ac.uk/api/prediction/{uniprot_id}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"{uniprot_id}: HTTP error {e.code}")
        return None
    except Exception as e:
        print(f"{uniprot_id}: failed ({e})")
        return None

def sanitize_partition_value(value):
    # Sanitize inputs for S3 partitioning safety 
    value = value.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_\-]", "_", value)

def get_partition_value(record):
    organism = record.get("organismScientificName")
    if organism:
        return sanitize_partition_value(organism)
    # Fallback
    tax_id = record.get("taxId")
    return f"taxid_{tax_id}" if tax_id else "unknown_organism"

def handler(event, context):
    uniprot_ids = event.get("uniprot_ids", [])
    succeeded, failed = [], []

    for uid in uniprot_ids:
        data = fetch_prediction(uid)
        if not data:
            failed.append(uid)
            continue

        record = data[0]
        organism = get_partition_value(record)
        key = f"raw/organism={organism}/{uid}.json"

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        succeeded.append(uid)

    result = {"succeeded": succeeded, "failed": failed}
    print(json.dumps(result))
    return result