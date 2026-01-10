#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas>=2.0",
#     "requests>=2.31",
#     "xlrd>=2.0.1",
#     "openpyxl>=3.1.0",
# ]
# ///

"""
CFTC Hedge Fund Flows Data Pipeline
====================================

Downloads CFTC Disaggregated COT data, calculates weekly flows by sector,
and saves to /data directory for GitHub Actions auto-commit.

Usage: uv run update_flows.py
"""

import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime, timedelta

# CFTC Data Source
CFTC_BASE_URL = "https://www.cftc.gov/files/dea/history"
# Market codes from CFTC Market_and_Exchange_Names field
SECTOR_MAPPING = {
    "CORN": "Grains", "WHEAT": "Grains", "WHEAT-SRW": "Grains", "SOYBEANS": "Grains",
    "SOYBEAN OIL": "Oilseeds", "SOYBEAN MEAL": "Oilseeds",
    "LIVE CATTLE": "Meats", "LEAN HOGS": "Meats", "FEEDER CATTLE": "Meats",
    "COFFEE": "Softs", "SUGAR": "Softs", "COTTON": "Softs", "COCOA": "Softs",
}

def download_cftc_data(year: int) -> pd.DataFrame:
    """Download CFTC Disaggregated COT data for given year(s)."""
    import io
    import zipfile

    dfs = []
    # Load current year and previous year to ensure 20+ weeks of data
    years_to_load = [year - 1, year]

    for yr in years_to_load:
        zip_url = f"{CFTC_BASE_URL}/fut_disagg_xls_{yr}.zip"

        try:
            print(f"Downloading {yr} CFTC disaggregated data...")
            response = requests.get(zip_url, timeout=60)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                file_names = zf.namelist()
                cot_file = [f for f in file_names if f.endswith('.xls') or f.endswith('.xlsx')][0]
                df = pd.read_excel(zf.open(cot_file))
                dfs.append(df)
                print(f"Loaded {len(df):,} records from {yr}")

        except Exception as e:
            print(f"Warning: Could not load {yr} data: {e}")
            continue

    if not dfs:
        print("Download failed: No data loaded")
        sys.exit(1)

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Total: {len(combined):,} records")
    return combined

def process_cot_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process raw COT into weekly sector flows (thousand contracts)."""
    print("Processing hedge fund flows...")

    # Work with a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Disaggregated data has Money Manager positions as columns, not a category filter
    # Required columns: M_Money_Positions_Long_ALL, M_Money_Positions_Short_ALL
    long_col = 'M_Money_Positions_Long_ALL'
    short_col = 'M_Money_Positions_Short_ALL'

    if long_col not in df.columns or short_col not in df.columns:
        print(f"Available columns: {list(df.columns)[:10]}...")
        print(f"Money Manager columns not found")
        sys.exit(1)

    # Parse dates
    df['Date'] = pd.to_datetime(df['Report_Date_as_MM_DD_YYYY'])

    # Map commodities to sectors using Market_and_Exchange_Names
    def get_sector(name):
        name_upper = str(name).upper()
        for key, sector in SECTOR_MAPPING.items():
            if key in name_upper:
                return sector
        return None

    df['Sector'] = df['Market_and_Exchange_Names'].apply(get_sector)

    # Filter to mapped commodities only
    df = df.dropna(subset=['Sector']).copy()

    if df.empty:
        print("No matching commodities found")
        sys.exit(1)

    # Calculate net positions (Money Manager long - short)
    df['Net_Position'] = df[long_col] - df[short_col]

    # Aggregate by date and sector
    sector_positions = df.groupby(['Date', 'Sector'])['Net_Position'].sum().reset_index()
    pivoted = sector_positions.pivot(index='Date', columns='Sector', values='Net_Position')

    # Calculate weekly flows (change from previous week)
    flows = pivoted.diff().fillna(0)
    flows_k = (flows / 1000).round().astype(int)
    flows_k['Total'] = flows_k.sum(axis=1)

    flows_k = flows_k.reset_index().sort_values('Date')

    # Keep last 20 weeks
    cutoff = datetime.now() - timedelta(weeks=20)
    flows_k = flows_k[flows_k['Date'] >= cutoff]

    print(f"Processed {len(flows_k)} weeks")
    return flows_k

def save_data_files(df: pd.DataFrame, output_dir: str = "data"):
    """Save both CSV and JSON formats."""
    os.makedirs(output_dir, exist_ok=True)

    # Convert Date to string for JSON serialization
    df = df.copy()
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    # CSV: machine-readable tabular
    csv_path = os.path.join(output_dir, "flows_raw.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved CSV: {csv_path}")

    # JSON: API-friendly with metadata
    json_path = os.path.join(output_dir, "flows_raw.json")
    json_data = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "CFTC",
            "unit": "thousand_contracts"
        },
        "data": df.to_dict(orient="records")
    }
    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)
    print(f"Saved JSON: {json_path}")

def main():
    """Execute pipeline."""
    print("=" * 70)
    print("CFTC Hedge Fund Flows Pipeline")
    print(f"Started: {datetime.now()}")
    print("=" * 70)

    # Download and process
    current_year = datetime.now().year
    cot_raw = download_cftc_data(current_year)
    flows_data = process_cot_data(cot_raw)

    # Show preview
    print(f"\nLatest 3 weeks:")
    print(flows_data.tail(3).to_string(index=False))

    # Save files
    save_data_files(flows_data)

    # Verify files exist
    print(f"\nFiles in /data:")
    for f in os.listdir("data"):
        print(f"   - {f}")

    print("\nPipeline complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()