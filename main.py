import httpx
from pathlib import Path
from typing import Dict, Any
from analysis.efficiency import analyze_efficiency, print_efficiency_results
from analysis.valuation import analyze_valuation, print_valuation_results

# Define the API endpoints
ENDPOINTS = {
    "protocols": "https://api.llama.fi/protocols",
    "fees": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
    "revenue": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue"
}

def fetch_endpoint_data(url: str) -> dict:
    """Fetch data from an endpoint and return the response."""
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()

def prepare_data_for_analysis():
    """Fetch and prepare data for analysis."""
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Fetch data from all endpoints
    data = {}
    for endpoint_name, url in ENDPOINTS.items():
        try:
            data[endpoint_name] = fetch_endpoint_data(url)
            print(f"Successfully fetched data for {endpoint_name}")
        except Exception as e:
            print(f"Error fetching {endpoint_name}: {str(e)}")
            return None
    
    return data

def main():
    # Fetch and prepare data
    data = prepare_data_for_analysis()
    if data:
        # Analyze operating efficiency
        efficiency_results = analyze_efficiency(data)
        print_efficiency_results(efficiency_results)
        
        # Analyze valuation metrics
        valuation_results = analyze_valuation(data)
        print_valuation_results(valuation_results)

if __name__ == "__main__":
    main()

