import duckdb
from pathlib import Path
import json
from typing import Dict, Any

def analyze_valuation(data: Dict[str, Any], timeframes: list = ['24h', '7d', '30d', '1y']):
    """Analyze valuation metrics (fees/mcap and revenue/mcap) for different timeframes."""
    con = duckdb.connect(":memory:")
    
    # Save JSON data to temporary files
    data_dir = Path("data")
    protocols_file = data_dir / "protocols.json"
    fees_file = data_dir / "fees.json"
    revenue_file = data_dir / "revenue.json"
    
    with open(protocols_file, "w") as f:
        json.dump(data["protocols"], f)
    with open(fees_file, "w") as f:
        json.dump(data["fees"], f)
    with open(revenue_file, "w") as f:
        json.dump(data["revenue"], f)
    
    # Create views
    con.execute(f"""
        CREATE VIEW protocols AS 
        SELECT * FROM read_json_auto('{protocols_file}')
    """)
    
    con.execute(f"""
        CREATE VIEW fees AS 
        SELECT 
            unnest.id,
            unnest.name,
            unnest.total24h as fees_24h,
            unnest.total7d as fees_7d,
            unnest.total30d as fees_30d,
            unnest.total1y as fees_1y,
            unnest.totalAllTime as fees_all_time
        FROM read_json_auto('{fees_file}') as f,
        UNNEST(f.protocols) as unnest
    """)
    
    con.execute(f"""
        CREATE VIEW revenue AS 
        SELECT 
            unnest.id,
            unnest.name,
            unnest.total24h as revenue_24h,
            unnest.total7d as revenue_7d,
            unnest.total30d as revenue_30d,
            unnest.total1y as revenue_1y,
            unnest.totalAllTime as revenue_all_time
        FROM read_json_auto('{revenue_file}') as r,
        UNNEST(r.protocols) as unnest
    """)
    
    results = {
        'fees_mcap': {},
        'revenue_mcap': {}
    }
    
    for timeframe in timeframes:
        # Calculate fees/mcap metrics
        fees_query = f"""
            WITH valuation_data AS (
                SELECT 
                    f.name as protocol_name,
                    p.category,
                    p.chain,
                    p.mcap,
                    f.fees_{timeframe} as fees,
                    CASE 
                        WHEN p.mcap > 0 THEN f.fees_{timeframe} / p.mcap 
                        ELSE 0 
                    END as fees_mcap_ratio
                FROM fees f
                INNER JOIN protocols p ON f.id = p.id
                WHERE p.mcap > 0  -- Exclude protocols with no market cap
            )
            SELECT 
                protocol_name,
                category,
                chain,
                mcap,
                fees,
                fees_mcap_ratio
            FROM valuation_data
            ORDER BY fees_mcap_ratio DESC
            LIMIT 5
        """
        
        # Calculate revenue/mcap metrics
        revenue_query = f"""
            WITH valuation_data AS (
                SELECT 
                    r.name as protocol_name,
                    p.category,
                    p.chain,
                    p.mcap,
                    r.revenue_{timeframe} as revenue,
                    CASE 
                        WHEN p.mcap > 0 THEN r.revenue_{timeframe} / p.mcap 
                        ELSE 0 
                    END as revenue_mcap_ratio
                FROM revenue r
                INNER JOIN protocols p ON r.id = p.id
                WHERE p.mcap > 0  -- Exclude protocols with no market cap
            )
            SELECT 
                protocol_name,
                category,
                chain,
                mcap,
                revenue,
                revenue_mcap_ratio
            FROM valuation_data
            ORDER BY revenue_mcap_ratio DESC
            LIMIT 5
        """
        
        results['fees_mcap'][timeframe] = con.execute(fees_query).fetchdf()
        results['revenue_mcap'][timeframe] = con.execute(revenue_query).fetchdf()
    
    # Clean up
    protocols_file.unlink()
    fees_file.unlink()
    revenue_file.unlink()
    con.close()
    
    return results

def print_valuation_results(results: Dict[str, Any]):
    """Print valuation analysis results in a readable format."""
    print("\nTop 5 Undervalued Protocols by Fees/Market Cap")
    print("=" * 80)
    
    for timeframe, df in results['fees_mcap'].items():
        print(f"\nTimeframe: {timeframe}")
        print("-" * 80)
        print(df.to_string(index=False))
        print("-" * 80)
    
    print("\nTop 5 Undervalued Protocols by Revenue/Market Cap")
    print("=" * 80)
    
    for timeframe, df in results['revenue_mcap'].items():
        print(f"\nTimeframe: {timeframe}")
        print("-" * 80)
        print(df.to_string(index=False))
        print("-" * 80) 