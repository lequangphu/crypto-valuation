import httpx
from pathlib import Path
import json
from typing import Any, Dict
from collections import defaultdict

def fetch_endpoint_data(url: str) -> dict:
    """Fetch data from an endpoint and return the response."""
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()

def analyze_schema(data: Any, path: str = "") -> Dict[str, set]:
    """Analyze the schema of the data and return a dictionary of field types."""
    schema = defaultdict(set)
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            schema[current_path].add(type(value).__name__)
            if isinstance(value, (dict, list)):
                nested_schema = analyze_schema(value, current_path)
                for k, v in nested_schema.items():
                    schema[k].update(v)
    elif isinstance(data, list) and data:
        # Analyze first item in list as representative
        nested_schema = analyze_schema(data[0], path)
        for k, v in nested_schema.items():
            schema[k].update(v)
    
    return schema

def analyze_endpoints(endpoints: Dict[str, str]) -> Dict[str, Dict[str, list]]:
    """Analyze schema for all provided endpoints."""
    all_schemas = {}
    
    for endpoint_name, url in endpoints.items():
        try:
            data = fetch_endpoint_data(url)
            schema = analyze_schema(data)
            
            # Convert sets to lists for JSON serialization
            schema_dict = {k: list(v) for k, v in schema.items()}
            all_schemas[endpoint_name] = schema_dict
            
            print(f"Successfully analyzed schema for {endpoint_name}")
            
        except Exception as e:
            print(f"Error analyzing {endpoint_name}: {str(e)}")
    
    return all_schemas

def save_schemas(schemas: Dict[str, Dict[str, list]], output_dir: str = "data") -> Path:
    """Save schema information to a JSON file."""
    data_dir = Path(output_dir)
    data_dir.mkdir(exist_ok=True)
    
    schema_file = data_dir / "schemas.json"
    with open(schema_file, "w") as f:
        json.dump(schemas, f, indent=2)
    
    return schema_file

def print_schema_summary(schemas: Dict[str, Dict[str, list]]):
    """Print a human-readable summary of the schemas."""
    print("\nSchema Summary:")
    for endpoint, schema in schemas.items():
        print(f"\n{endpoint}:")
        for field, types in schema.items():
            print(f"  {field}: {', '.join(types)}")

if __name__ == "__main__":
    # Example usage
    ENDPOINTS = {
        "protocols": "https://api.llama.fi/protocols",
        "fees": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
        "revenue": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue"
    }
    
    schemas = analyze_endpoints(ENDPOINTS)
    schema_file = save_schemas(schemas)
    print(f"\nSchema information saved to {schema_file}")
    print_schema_summary(schemas) 