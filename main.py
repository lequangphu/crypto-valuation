import os
import requests
import duckdb
from dotenv import load_dotenv
from typing import List, Dict
import time

# Load environment variables
load_dotenv()
CMC_API_KEY = os.getenv("COINMARKETCAP_API_KEY")

# DeFiLlama API endpoints
DEFILLAMA_FEES_URL = "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees"
DEFILLAMA_REVENUE_URL = "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue"
DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
CMC_QUOTES_URL = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"

def fetch_defillama_data() -> List[Dict]:
    """Fetch fees and revenue data from DeFiLlama and merge them."""
    # Fetch fees
    try:
        fees_response = requests.get(DEFILLAMA_FEES_URL)
        fees_response.raise_for_status()
        fees_data = fees_response.json().get("protocols", [])
    except requests.RequestException as e:
        print(f"Error fetching fees data: {e}")
        fees_data = []

    # Fetch revenue
    try:
        revenue_response = requests.get(DEFILLAMA_REVENUE_URL)
        revenue_response.raise_for_status()
        revenue_data = revenue_response.json().get("protocols", [])
    except requests.RequestException as e:
        print(f"Error fetching revenue data: {e}")
        revenue_data = []

    # Create dictionaries for lookup
    fees_dict = {p["defillamaId"]: p for p in fees_data}
    revenue_dict = {p["defillamaId"]: p for p in revenue_data}

    # Merge fees and revenue data
    merged_data = []
    for defillama_id in set(fees_dict.keys()) & set(revenue_dict.keys()):
        fees = fees_dict[defillama_id]
        revenue = revenue_dict[defillama_id]
        merged_data.append({
            "defillamaId": defillama_id,
            "name": fees.get("name", revenue.get("name", "")),
            "total30d_fees": fees.get("total30d", 0),
            "total30d_revenue": revenue.get("total30d", 0),
            "total1y_fees": fees.get("total1y", 0),
            "total1y_revenue": revenue.get("total1y", 0),
        })

    return merged_data

def fetch_defillama_protocols() -> List[Dict]:
    """Fetch protocol data including cmcId from DeFiLlama."""
    try:
        response = requests.get(DEFILLAMA_PROTOCOLS_URL)
        response.raise_for_status()
        return [
            {
                "defillamaId": p["id"],
                "cmcId": p.get("cmcId"),
            }
            for p in response.json()
        ]
    except requests.RequestException as e:
        print(f"Error fetching protocols data: {e}")
        return []

def fetch_cmc_data(cmc_ids: List[str]) -> List[Dict]:
    """Fetch market cap and FDV from CoinMarketCap for given cmcIds."""
    cmc_data = []
    id_chunks = [cmc_ids[i:i + 100] for i in range(0, len(cmc_ids), 100)]  # CMC allows 100 IDs per request
    
    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY,
        "Accept": "application/json"
    }
    
    for chunk in id_chunks:
        params = {"id": ",".join(chunk)}
        try:
            response = requests.get(CMC_QUOTES_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json().get("data", {})
            
            for cmc_id, info in data.items():
                cmc_data.append({
                    "cmcId": cmc_id,
                    "market_cap": info.get("quote", {}).get("USD", {}).get("market_cap", 0),
                    "fdv": info.get("quote", {}).get("USD", {}).get("fully_diluted_market_cap", 0),
                })
        except requests.RequestException as e:
            print(f"Error fetching CMC data for chunk: {e}")
            continue
        
        time.sleep(1)  # Respect rate limits
    
    return cmc_data

def main():
    # Initialize DuckDB connection
    con = duckdb.connect(":memory:")
    
    # Fetch and store DeFiLlama data
    defillama_data = fetch_defillama_data()
    protocols_data = fetch_defillama_protocols()
    
    con.execute("""
        CREATE TABLE fees_revenue (
            defillamaId STRING, 
            name STRING, 
            total30d_fees DOUBLE, 
            total30d_revenue DOUBLE,
            total1y_fees DOUBLE,
            total1y_revenue DOUBLE
        )
    """)
    con.executemany(
        "INSERT INTO fees_revenue VALUES (?, ?, ?, ?, ?, ?)",
        [(p["defillamaId"], p["name"], p["total30d_fees"], p["total30d_revenue"], 
          p["total1y_fees"], p["total1y_revenue"]) for p in defillama_data]
    )
    
    con.execute("CREATE TABLE protocols (defillamaId STRING, cmcId STRING)")
    con.executemany(
        "INSERT INTO protocols VALUES (?, ?)",
        [(p["defillamaId"], p["cmcId"]) for p in protocols_data]
    )
    
    # Join fees/revenue and protocols to get cmcId
    con.execute("""
        CREATE TABLE defillama_data AS
        SELECT f.defillamaId, f.name, f.total30d_fees, f.total30d_revenue, 
               f.total1y_fees, f.total1y_revenue, p.cmcId
        FROM fees_revenue f
        LEFT JOIN protocols p ON f.defillamaId = p.defillamaId
    """)
    
    # Clean data: remove rows with missing or zero values in 30d/1y timeframe or cmcId
    con.execute("""
        CREATE TABLE cleaned_defillama AS
        SELECT * FROM defillama_data
        WHERE total30d_fees > 0
        AND total30d_revenue > 0
        AND total1y_fees > 0
        AND total1y_revenue > 0
        AND cmcId IS NOT NULL
        AND cmcId != ''
    """)
    
    # Get unique cmcIds for CMC API
    cmc_ids = con.execute("SELECT DISTINCT cmcId FROM cleaned_defillama").fetchall()
    cmc_ids = [row[0] for row in cmc_ids]
    
    # Fetch CoinMarketCap data
    cmc_data = fetch_cmc_data(cmc_ids)
    
    con.execute("CREATE TABLE cmc_data (cmcId STRING, market_cap DOUBLE, fdv DOUBLE)")
    con.executemany(
        "INSERT INTO cmc_data VALUES (?, ?, ?)",
        [(d["cmcId"], d["market_cap"], d["fdv"]) for d in cmc_data]
    )
    
    # Join datasets
    con.execute("""
        CREATE TABLE joined_data AS
        SELECT d.defillamaId, d.name, d.total30d_fees, d.total30d_revenue, 
               d.total1y_fees, d.total1y_revenue, d.cmcId,
               c.market_cap, c.fdv
        FROM cleaned_defillama d
        INNER JOIN cmc_data c ON d.cmcId = c.cmcId
        WHERE c.fdv > 0
    """)
    
    # Calculate valuation multiples
    con.execute("""
        CREATE TABLE final_data AS
        SELECT defillamaId, name, cmcId, 
               total30d_fees, total30d_revenue, 
               total1y_fees, total1y_revenue, 
               market_cap, fdv,
               fdv / total30d_fees AS fdv_fee_30d,
               fdv / total30d_revenue AS fdv_revenue_30d,
               fdv / (total30d_fees * 12) AS fdv_fee_annual,
               fdv / (total30d_revenue * 12) AS fdv_revenue_annual,
               fdv / total1y_fees AS fdv_fee_1y,
               fdv / total1y_revenue AS fdv_revenue_1y
        FROM joined_data
    """)
    
    # Export to CSV
    con.execute("COPY final_data TO 'valuation_multiples.csv' WITH (HEADER, DELIMITER ',')")
    
    # Print summary of top projects by FDV/fee (30d)
    summary = con.execute("""
        SELECT name, cmcId, fdv_fee_30d, fdv_revenue_30d, fdv_fee_1y, fdv_revenue_1y
        FROM final_data
        WHERE fdv_fee_30d IS NOT NULL
        ORDER BY fdv_fee_30d ASC
        LIMIT 5
    """).fetchdf()
    
    print("\nTop 5 Projects by Lowest FDV/Fee (30d):")
    print(summary.to_string(index=False))
    
    print("\nData processing complete. Results saved to 'valuation_multiples.csv'.")

if __name__ == "__main__":
    main()