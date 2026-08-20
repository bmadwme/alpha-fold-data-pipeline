import json
import time
import boto3

lambda_client = boto3.client("lambda")

with open("proteome_ids.json") as f:
    all_ids = json.load(f)

CHUNK_SIZE = 30
chunks = [all_ids[i:i + CHUNK_SIZE] for i in range(0, len(all_ids), CHUNK_SIZE)]

total_succeeded, total_failed = 0, 0

for i, chunk in enumerate(chunks):
    payload = {"uniprot_ids": chunk}
    resp = lambda_client.invoke(
        FunctionName="alphafold-ingest",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload)
    )
    result = json.loads(resp["Payload"].read())
    succeeded = len(result.get("succeeded", []))
    failed = len(result.get("failed", []))
    total_succeeded += succeeded
    total_failed += failed

    print(f"Batch {i+1}/{len(chunks)}: {succeeded} ok, {failed} failed "
          f"(running total: {total_succeeded} ok, {total_failed} failed)")

    time.sleep(0.5)  # small pause so we're not hammering AlphaFold's API

print(f"\nDONE. {total_succeeded} succeeded, {total_failed} failed out of {len(all_ids)}")