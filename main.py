from schema_analyzer import analyze_endpoints, save_schemas, print_schema_summary

# Define the API endpoints
ENDPOINTS = {
    "protocols": "https://api.llama.fi/protocols",
    "fees": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
    "revenue": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue"
}

def main():
    # First, analyze and store the schema information
    schemas = analyze_endpoints(ENDPOINTS)
    schema_file = save_schemas(schemas)
    print(f"\nSchema information saved to {schema_file}")
    print_schema_summary(schemas)
    
    # TODO: Add your main analysis task here
    # This is where you'll implement the actual data analysis
    # using the schema information we've gathered

if __name__ == "__main__":
    main()

