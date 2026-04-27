import json
import os
from urllib.parse import urlparse
from collections import Counter

FILES = [
    "crawl_data/pages.jsonl",
    "crawl_data/subdomains.jsonl",
]

ext_counts = Counter()
no_ext = 0

for file in FILES:
    if not os.path.exists(file):
        continue

    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line)
                url = entry.get("url", "")
            except:
                continue

            path = urlparse(url).path.lower()
            filename = path.rstrip("/").split("/")[-1]

            if "." in filename:
                ext = filename.rsplit(".", 1)[-1]
                ext_counts[ext] += 1
            else:
                no_ext += 1

print("Extensions found:")
for ext, count in ext_counts.most_common():
    print(f".{ext}: {count}")

print(f"\nNo extension: {no_ext}")