import duckdb
from pathlib import Path
import json
from typing import Dict, Any

def analyze_efficiency(data: Dict[str, Any], timeframes: list = ['24h', '7d', '30d', '1y']):
    """Analyze operating efficiency (fees/tvl) for different timeframes."""
    con = duckdb.connect(":memory:")
    
    # Save JSON data to temporary files
    data_dir = Path("data")
    protocols_file = data_dir / "protocols.json"
    fees_file = data_dir / "fees.json"
    
    with open(protocols_file, "w") as f:
        json.dump(data["protocols"], f)
    with open(fees_file, "w") as f:
        json.dump(data["fees"], f)
    
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
    
    results = {}
    for timeframe in timeframes:
        # Calculate efficiency metrics
        query = f"""
            WITH efficiency_data AS (
                SELECT 
                    f.name as protocol_name,
                    p.category,
                    p.chain,
                    p.tvl,
                    f.fees_{timeframe} as fees,
                    CASE 
                        WHEN p.tvl > 0 THEN f.fees_{timeframe} / p.tvl 
                        ELSE 0 
                    END as efficiency_ratio
                FROM fees f
                INNER JOIN protocols p ON f.id = p.id
                WHERE p.tvl > 0  -- Exclude protocols with no TVL
            )
            SELECT 
                protocol_name,
                category,
                chain,
                tvl,
                fees,
                efficiency_ratio
            FROM efficiency_data
            ORDER BY efficiency_ratio DESC
            LIMIT 5
        """
        
        results[timeframe] = con.execute(query).fetchdf()
    
    # Clean up
    protocols_file.unlink()
    fees_file.unlink()
    con.close()
    
    return results

def print_efficiency_results(results: Dict[str, Any]):
    """Print efficiency analysis results in a readable format."""
    print("\nTop 5 Protocols by Operating Efficiency (Fees/TVL)")
    print("=" * 80)
    
    for timeframe, df in results.items():
        print(f"\nTimeframe: {timeframe}")
        print("-" * 80)
        print(df.to_string(index=False))
        print("-" * 80) 