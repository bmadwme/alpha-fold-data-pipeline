import urllib.request
import urllib.parse
import json

url = "https://rest.uniprot.org/uniprotkb/stream"
params = {
    "query": "proteome:UP000000625 AND reviewed:true",
    "fields": "accession",
    "format": "json"
}
full_url = f"{url}?{urllib.parse.urlencode(params)}"

req = urllib.request.Request(full_url, headers={"Accept-Encoding": "identity"})
with urllib.request.urlopen(req, timeout=60) as resp:
    data = json.loads(resp.read().decode())

ids = [entry["primaryAccession"] for entry in data["results"]]
print(f"Got {len(ids)} accessions")

with open("proteome_ids.json", "w") as f:
    json.dump(ids, f)