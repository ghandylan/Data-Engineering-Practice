import csv


def flatten_json(y, parent_key='', sep='.'):
    """
    Recursive function to flatten a JSON structure into a single-level dictionary.
    """
    items = []
    for k, v in y.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


# Example JSON data (nested)
json_data = {
    "name": "Alice",
    "location": {
        "city": "Wonderland",
        "coordinates": {
            "latitude": 50.123,
            "longitude": 8.456
        }
    },
    "contacts": [
        {"type": "email", "value": "alice@example.com"},
        {"type": "phone", "value": "123-456-7890"}
    ]
}

# Step 1: Flatten the JSON data
flattened_data = []
for entry in json_data.get("contacts", []):  # Flatten each item if it's a list of items
    flattened_entry = flatten_json(entry)  # Flatten each item individually
    flattened_data.append(flattened_entry)

# Step 2: Write the flattened data to a CSV file
csv_file = 'output.csv'

# Get headers from flattened data (unique keys across all entries)
headers = set()
for item in flattened_data:
    headers.update(item.keys())
headers = sorted(headers)  # Sort headers for consistent ordering

with open(csv_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(flattened_data)

print(f"Flattened JSON has been written to {csv_file}")
