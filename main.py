import json

import duckdb
import requests

# Define the API endpoints
ENDPOINTS = {
    "protocols": "https://api.llama.fi/protocols",
    "fees": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
    "revenue": "https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue",
}

# Output CSV file
OUTPUT_FILE = "joined_data_all.csv"


def fetch_data(endpoint_name: str, url: str) -> list:
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if endpoint_name in ["fees", "revenue"]:
            return data.get("protocols", [])
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {endpoint_name} data: {e}")
        return []


def main():
    # Step 1: Fetch data from all endpoints
    protocols_data = fetch_data("protocols", ENDPOINTS["protocols"])
    fees_data = fetch_data("fees", ENDPOINTS["fees"])
    revenue_data = fetch_data("revenue", ENDPOINTS["revenue"])

    # Step 2: Initialize DuckDB connection
    con = duckdb.connect()

    # Step 3: Load data into DuckDB tables
    con.execute("CREATE TABLE protocols (data JSON)")
    con.executemany(
        "INSERT INTO protocols VALUES (?)",
        [(json.dumps(row),) for row in protocols_data],
    )

    con.execute("CREATE TABLE fees (data JSON)")
    con.executemany(
        "INSERT INTO fees VALUES (?)", [(json.dumps(row),) for row in fees_data]
    )

    con.execute("CREATE TABLE revenue (data JSON)")
    con.executemany(
        "INSERT INTO revenue VALUES (?)", [(json.dumps(row),) for row in revenue_data]
    )

    # Step 4: Explode the chains array and extract all fields
    # For protocols
    con.execute("""
        CREATE TABLE protocols_expanded AS
        SELECT 
            JSON_EXTRACT(data, '$.name') AS name,
            JSON_EXTRACT(data, '$.category') AS category,
            UNNEST(CAST(JSON_EXTRACT(data, '$.chains') AS VARCHAR[])) AS chain,
            JSON_EXTRACT(data, '$.id') AS protocol_id,
            JSON_EXTRACT(data, '$.address') AS address,
            JSON_EXTRACT(data, '$.symbol') AS symbol,
            JSON_EXTRACT(data, '$.url') AS url,
            JSON_EXTRACT(data, '$.description') AS description,
            JSON_EXTRACT(data, '$.chain') AS main_chain,
            JSON_EXTRACT(data, '$.logo') AS logo,
            JSON_EXTRACT(data, '$.audits') AS audits,
            JSON_EXTRACT(data, '$.audit_note') AS audit_note,
            JSON_EXTRACT(data, '$.gecko_id') AS gecko_id,
            JSON_EXTRACT(data, '$.cmcId') AS cmc_id,
            JSON_EXTRACT(data, '$.module') AS module,
            JSON_EXTRACT(data, '$.twitter') AS twitter,
            JSON_EXTRACT_STRING(data, '$.forkedFrom') AS forked_from,
            JSON_EXTRACT_STRING(data, '$.oracles') AS oracles,
            JSON_EXTRACT(data, '$.listedAt') AS listed_at,
            JSON_EXTRACT(data, '$.methodology') AS methodology,
            JSON_EXTRACT(data, '$.slug') AS slug,
            COALESCE(JSON_EXTRACT(data, '$.tvl'), 0) AS tvl,
            COALESCE(JSON_EXTRACT(data, '$.change_1h'), 0) AS change_1h,
            COALESCE(JSON_EXTRACT(data, '$.change_1d'), 0) AS change_1d,
            COALESCE(JSON_EXTRACT(data, '$.change_7d'), 0) AS change_7d
        FROM protocols
        WHERE JSON_EXTRACT(data, '$.name') IS NOT NULL
          AND JSON_EXTRACT(data, '$.category') IS NOT NULL
          AND JSON_EXTRACT(data, '$.chains') IS NOT NULL
          AND json_type(JSON_EXTRACT(data, '$.chains')) = 'array'
    """)

    # For fees
    con.execute("""
        CREATE TABLE fees_expanded AS
        SELECT 
            JSON_EXTRACT(data, '$.name') AS name,
            JSON_EXTRACT(data, '$.category') AS category,
            UNNEST(CAST(JSON_EXTRACT(data, '$.chains') AS VARCHAR[])) AS chain,
            JSON_EXTRACT(data, '$.defillamaId') AS fees_defillama_id,
            JSON_EXTRACT(data, '$.displayName') AS fees_display_name,
            JSON_EXTRACT(data, '$.module') AS fees_module,
            JSON_EXTRACT(data, '$.logo') AS fees_logo,
            JSON_EXTRACT(data, '$.protocolType') AS fees_protocol_type,
            JSON_EXTRACT(data, '$.methodologyURL') AS fees_methodology_url,
            JSON_EXTRACT(data, '$.slug') AS fees_slug,
            JSON_EXTRACT(data, '$.id') AS fees_id,
            COALESCE(JSON_EXTRACT(data, '$.total24h'), 0) AS fees_24h,
            COALESCE(JSON_EXTRACT(data, '$.total48hto24h'), 0) AS fees_48h_to_24h,
            COALESCE(JSON_EXTRACT(data, '$.total7d'), 0) AS fees_7d,
            COALESCE(JSON_EXTRACT(data, '$.total14dto7d'), 0) AS fees_14d_to_7d,
            COALESCE(JSON_EXTRACT(data, '$.total60dto30d'), 0) AS fees_60d_to_30d,
            COALESCE(JSON_EXTRACT(data, '$.total30d'), 0) AS fees_30d,
            COALESCE(JSON_EXTRACT(data, '$.total1y'), 0) AS fees_1y,
            COALESCE(JSON_EXTRACT(data, '$.totalAllTime'), 0) AS fees_all_time,
            COALESCE(JSON_EXTRACT(data, '$.average1y'), 0) AS fees_average_1y,
            COALESCE(JSON_EXTRACT(data, '$.change_30dover30d'), 0) AS fees_change_30d_over_30d,
            JSON_EXTRACT_STRING(data, '$.breakdown24h') AS fees_breakdown_24h,
            JSON_EXTRACT_STRING(data, '$.breakdown30d') AS fees_breakdown_30d,
            COALESCE(JSON_EXTRACT(data, '$.total7DaysAgo'), 0) AS fees_7_days_ago,
            COALESCE(JSON_EXTRACT(data, '$.total30DaysAgo'), 0) AS fees_30_days_ago,
            COALESCE(JSON_EXTRACT(data, '$.latestFetchIsOk'), 0) AS fees_latest_fetch_is_ok
        FROM fees
        WHERE JSON_EXTRACT(data, '$.name') IS NOT NULL
          AND JSON_EXTRACT(data, '$.category') IS NOT NULL
          AND JSON_EXTRACT(data, '$.chains') IS NOT NULL
          AND json_type(JSON_EXTRACT(data, '$.chains')) = 'array'
    """)

    # For revenue
    con.execute("""
        CREATE TABLE revenue_expanded AS
        SELECT 
            JSON_EXTRACT(data, '$.name') AS name,
            JSON_EXTRACT(data, '$.category') AS category,
            UNNEST(CAST(JSON_EXTRACT(data, '$.chains') AS VARCHAR[])) AS chain,
            JSON_EXTRACT(data, '$.defillamaId') AS revenue_defillama_id,
            JSON_EXTRACT(data, '$.displayName') AS revenue_display_name,
            JSON_EXTRACT(data, '$.module') AS revenue_module,
            JSON_EXTRACT(data, '$.logo') AS revenue_logo,
            JSON_EXTRACT(data, '$.protocolType') AS revenue_protocol_type,
            JSON_EXTRACT(data, '$.methodologyURL') AS revenue_methodology_url,
            JSON_EXTRACT(data, '$.slug') AS revenue_slug,
            JSON_EXTRACT(data, '$.id') AS revenue_id,
            COALESCE(JSON_EXTRACT(data, '$.total24h'), 0) AS revenue_24h,
            COALESCE(JSON_EXTRACT(data, '$.total48hto24h'), 0) AS revenue_48h_to_24h,
            COALESCE(JSON_EXTRACT(data, '$.total7d'), 0) AS revenue_7d,
            COALESCE(JSON_EXTRACT(data, '$.total14dto7d'), 0) AS revenue_14d_to_7d,
            COALESCE(JSON_EXTRACT(data, '$.total60dto30d'), 0) AS revenue_60d_to_30d,
            COALESCE(JSON_EXTRACT(data, '$.total30d'), 0) AS revenue_30d,
            COALESCE(JSON_EXTRACT(data, '$.total1y'), 0) AS revenue_1y,
            COALESCE(JSON_EXTRACT(data, '$.totalAllTime'), 0) AS revenue_all_time,
            COALESCE(JSON_EXTRACT(data, '$.average1y'), 0) AS revenue_average_1y,
            COALESCE(JSON_EXTRACT(data, '$.change_30dover30d'), 0) AS revenue_change_30d_over_30d,
            JSON_EXTRACT_STRING(data, '$.breakdown24h') AS revenue_breakdown_24h,
            JSON_EXTRACT_STRING(data, '$.breakdown30d') AS revenue_breakdown_30d,
            COALESCE(JSON_EXTRACT(data, '$.total7DaysAgo'), 0) AS revenue_7_days_ago,
            COALESCE(JSON_EXTRACT(data, '$.total30DaysAgo'), 0) AS revenue_30_days_ago,
            COALESCE(JSON_EXTRACT(data, '$.latestFetchIsOk'), 0) AS revenue_latest_fetch_is_ok
        FROM revenue
        WHERE JSON_EXTRACT(data, '$.name') IS NOT NULL
          AND JSON_EXTRACT(data, '$.category') IS NOT NULL
          AND JSON_EXTRACT(data, '$.chains') IS NOT NULL
          AND json_type(JSON_EXTRACT(data, '$.chains')) = 'array'
    """)

    # Step 5: Perform inner join
    con.execute("""
        CREATE TABLE joined_data AS
        SELECT 
            p.name,
            p.category,
            p.chain,
            p.protocol_id,
            p.address,
            p.symbol,
            p.url,
            p.description,
            p.main_chain,
            p.logo,
            p.audits,
            p.audit_note,
            p.gecko_id,
            p.cmc_id,
            p.module,
            p.twitter,
            p.forked_from,
            p.oracles,
            p.listed_at,
            p.methodology,
            p.slug,
            p.tvl,
            p.change_1h,
            p.change_1d,
            p.change_7d,
            f.fees_defillama_id,
            f.fees_display_name,
            f.fees_module,
            f.fees_logo,
            f.fees_protocol_type,
            f.fees_methodology_url,
            f.fees_slug,
            f.fees_id,
            f.fees_24h,
            f.fees_48h_to_24h,
            f.fees_7d,
            f.fees_14d_to_7d,
            f.fees_60d_to_30d,
            f.fees_30d,
            f.fees_1y,
            f.fees_all_time,
            f.fees_average_1y,
            f.fees_change_30d_over_30d,
            f.fees_breakdown_24h,
            f.fees_breakdown_30d,
            f.fees_7_days_ago,
            f.fees_30_days_ago,
            f.fees_latest_fetch_is_ok,
            r.revenue_defillama_id,
            r.revenue_display_name,
            r.revenue_module,
            r.revenue_logo,
            r.revenue_protocol_type,
            r.revenue_methodology_url,
            r.revenue_slug,
            r.revenue_id,
            r.revenue_24h,
            r.revenue_48h_to_24h,
            r.revenue_7d,
            r.revenue_14d_to_7d,
            r.revenue_60d_to_30d,
            r.revenue_30d,
            r.revenue_1y,
            r.revenue_all_time,
            r.revenue_average_1y,
            r.revenue_change_30d_over_30d,
            r.revenue_breakdown_24h,
            r.revenue_breakdown_30d,
            r.revenue_7_days_ago,
            r.revenue_30_days_ago,
            r.revenue_latest_fetch_is_ok
        FROM protocols_expanded p
        INNER JOIN fees_expanded f
            ON p.name = f.name
            AND p.category = f.category
            AND p.chain = f.chain
        INNER JOIN revenue_expanded r
            ON p.name = r.name
            AND p.category = r.category
            AND p.chain = r.chain
    """)

    # Step 6: Export to CSV
    con.execute(f"COPY joined_data TO '{OUTPUT_FILE}' WITH (FORMAT CSV, HEADER)")
    print(f"Joined and cleaned data with all fields exported to {OUTPUT_FILE}")

    # Step 7: Clean up
    con.close()


if __name__ == "__main__":
    main()
