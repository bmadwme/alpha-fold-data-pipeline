import requests
import json

uniprot_ids = ["P00520", "P69905", "P68871", "Q9Y6K9", "P0DTD1"]

# Any key containing these substrings is worth flagging as a possible organism field
ORGANISM_HINTS = ["organism", "taxon", "tax_id", "taxId", "species"]

for uid in uniprot_ids:
    url = f"https://alphafold.ebi.ac.uk/api/prediction/{uid}"
    resp = requests.get(url)
    if not resp.ok:
        print(f"{uid}: FAILED ({resp.status_code})")
        continue

    data = resp.json()
    record = data[0]

    print(f"\n=== {uid} ===")
    print(json.dumps(record, indent=2))  # full output, no truncation

    found = [k for k in record.keys() if any(hint.lower() in k.lower() for hint in ORGANISM_HINTS)]
    print(f">>> organism-like keys found: {found if found else 'NONE'}")