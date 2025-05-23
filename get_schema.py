import json
import os
from typing import Any, Dict

import requests

# Define the API endpoints
ENDPOINTS = {
    "protocols": "https://api.llama.fi/protocols",
    "fees": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
    "revenue": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue",
}

# Directory to save schema files
OUTPUT_DIR = "schemas"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def infer_schema(data: Any) -> Dict[str, Any]:
    """
    Recursively infer the schema of a JSON object by determining the types and structure.
    """
    if isinstance(data, dict):
        schema = {"type": "object", "properties": {}}
        for key, value in data.items():
            schema["properties"][key] = infer_schema(value)
        return schema
    elif isinstance(data, list):
        schema = {"type": "array", "items": {}}
        # If the list is not empty, infer the schema of the first item
        if data:
            schema["items"] = infer_schema(data[0])
        return schema
    elif isinstance(data, str):
        return {"type": "string"}
    elif isinstance(data, int):
        return {"type": "integer"}
    elif isinstance(data, float):
        return {"type": "number"}
    elif isinstance(data, bool):
        return {"type": "boolean"}
    elif data is None:
        return {"type": "null"}
    else:
        return {"type": "unknown"}


def fetch_and_save_schema(endpoint_name: str, url: str) -> None:
    try:
        # Make the GET request
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Get the JSON data
        data = response.json()

        # Infer the schema
        schema = infer_schema(data)

        # Define the output file path
        output_file = os.path.join(OUTPUT_DIR, f"{endpoint_name}_schema.json")

        # Save the schema to a JSON file
        with open(output_file, "w") as f:
            json.dump(schema, f, indent=4)

        print(f"Schema for {endpoint_name} saved to {output_file}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {endpoint_name} schema: {e}")


def main():
    # Fetch and save schema for each endpoint
    for endpoint_name, url in ENDPOINTS.items():
        fetch_and_save_schema(endpoint_name, url)


if __name__ == "__main__":
    main()
