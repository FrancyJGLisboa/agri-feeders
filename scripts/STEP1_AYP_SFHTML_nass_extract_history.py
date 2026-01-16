#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "httpx",
#     "click",
#     "pandas",
#     "pyarrow",
# ]
# ///

"""
USDA NASS QuickStats Bulk Extractor - County Level for US Corn Belt
Generates CSV, Parquet, and JSON datasets for specific crops across Corn Belt states.
Includes Geolocation (Lat/Lon) and Area (Acres).

Features:
- Fetches county-level data (FIPS codes) from NASS QuickStats API
- Downloads Census Bureau Gazetteer for county land area AND coordinates
- Supports corn, soybeans, wheat, cotton
- Output formats: CSV, Parquet, JSON

Usage:
    export NASS_API_KEY="your_api_key"
    uv run STEP1_AYP_SFHTML_nass_extract_history.py --crop corn --start 2000 --end 2024
"""

import httpx
import click
import pandas as pd
import io
import sys
import time
import os
import zipfile
from pathlib import Path
from datetime import datetime

# --- Configuration ---

# Cache configuration
CACHE_DIR = Path.home() / '.nass_cache'
CACHE_FILE = CACHE_DIR / 'county_geo_ref.parquet' # Renamed to reflect it has geo data
CACHE_MAX_AGE_DAYS = 30

# US Corn Belt states (12 total)
CORN_BELT_STATES = ['IA', 'IL', 'IN', 'OH', 'NE', 'MN', 'WI', 'MO', 'KS', 'SD', 'ND', 'MI']

# State FIPS codes (for building full 5-digit FIPS)
STATE_FIPS = {
    'IA': '19', 'IL': '17', 'IN': '18', 'OH': '39', 'NE': '31', 'MN': '27',
    'WI': '55', 'MO': '29', 'KS': '20', 'SD': '46', 'ND': '38', 'MI': '26'
}

# Crop mapping (CLI name -> NASS commodity_desc)
CROPS = {
    'corn': 'CORN',
    'soybeans': 'SOYBEANS',
    'wheat': 'WHEAT',
    'cotton': 'COTTON',
}

# Variables to fetch from NASS
STAT_CATEGORIES = ['AREA PLANTED', 'YIELD', 'PRODUCTION']

NASS_BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"

API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json'
}


# --- County Geo Data: Census Bureau Gazetteer with Cache ---

def load_cached_geo_data():
    """
    Load county geo data from local cache if it exists, is fresh, 
    and contains the required lat/lon columns.
    Returns DataFrame or None.
    """
    if not CACHE_FILE.exists():
        return None

    # Check cache age
    cache_age = datetime.now().timestamp() - CACHE_FILE.stat().st_mtime
    cache_age_days = cache_age / (24 * 3600)

    if cache_age_days > CACHE_MAX_AGE_DAYS:
        print(f"📁 Cache exists but is {cache_age_days:.1f} days old (max {CACHE_MAX_AGE_DAYS})")
        return None

    print(f"📁 Loading county geo data from cache: {CACHE_FILE}")
    try:
        df = pd.read_parquet(CACHE_FILE)
        
        # Validation: Ensure new columns exist (in case user has old cache file)
        required_cols = {'fips', 'area_acres', 'latitude', 'longitude'}
        if not required_cols.issubset(df.columns):
            print("    ⚠️ Cache is outdated (missing coordinates). Re-fetching.")
            return None
            
        print(f"    ✅ Loaded {len(df)} counties from cache")
        return df
    except Exception as e:
        print(f"    ❌ Cache read error: {e}")
        return None


def save_geo_data_to_cache(df):
    """Save county geo reference DataFrame to local cache."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(CACHE_FILE, index=False)
        print(f"    💾 Cached to {CACHE_FILE}")
    except Exception as e:
        print(f"    ⚠️ Cache write error: {e}")


def fetch_geo_data_from_census():
    """
    Downloads Census Bureau Gazetteer file and builds reference DataFrame.
    Includes: FIPS, Area (Acres), Latitude, Longitude.
    URL: https://www2.census.gov/.../2023_Gaz_counties_national.zip
    """
    # Try cache first
    cached_df = load_cached_geo_data()
    if cached_df is not None:
        return cached_df

    url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.zip"
    print(f"🌍 Downloading County Geo Reference from Census Bureau...")
    print(f"    🔗 {url}")

    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.get(url)
            resp.raise_for_status()

        print("    📥 Parsing Gazetteer file...")

        # Unzip in memory
        zip_buffer = io.BytesIO(resp.content)
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                return pd.DataFrame()

            with zf.open(txt_files[0]) as f:
                # Gazetteer is tab-delimited. 
                # Columns usually: USPS, GEOID, ANSICODE, NAME, ALAND_SQMI, INTPTLAT, INTPTLONG...
                df = pd.read_csv(f, sep='\t', dtype=str)

        # Clean column names (Census files often have whitespace like "INTPTLONG   ")
        df.columns = [c.strip() for c in df.columns]

        required_cols = ['GEOID', 'ALAND_SQMI', 'INTPTLAT', 'INTPTLONG']
        
        # Check if columns exist
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"    ❌ Expected columns missing: {missing}. Found: {list(df.columns)}")
            return pd.DataFrame()

        # Select and Rename
        df = df[required_cols].copy()
        df.rename(columns={
            'GEOID': 'fips',
            'INTPTLAT': 'latitude',
            'INTPTLONG': 'longitude'
        }, inplace=True)

        # Convert Types
        df['ALAND_SQMI'] = pd.to_numeric(df['ALAND_SQMI'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # Convert sq miles to acres (1 sq mile = 640 acres)
        df['area_acres'] = df['ALAND_SQMI'] * 640.0
        
        # Drop temp column and cleanup
        df = df[['fips', 'area_acres', 'latitude', 'longitude']].dropna()
        
        print(f"    ✅ Successfully loaded {len(df)} counties with coordinates.")

        # Save to cache
        save_geo_data_to_cache(df)

        return df

    except Exception as e:
        print(f"    ❌ Error loading Census file: {e}")
        return pd.DataFrame()


# --- NASS API Query ---

def fetch_state_crop_data(client, api_key, state_alpha, commodity, year):
    """
    Query NASS API for county-level crop data for a single state and year.
    """
    params = {
        'key': api_key,
        'commodity_desc': commodity,
        'state_alpha': state_alpha,
        'agg_level_desc': 'COUNTY',
        'year': str(year),
        'format': 'json',
    }

    # Retry logic
    for attempt in range(3):
        try:
            response = client.get(NASS_BASE_URL, params=params, timeout=60.0)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            elif response.status_code == 413:
                print(f"    ⚠️ Too many records for {state_alpha}/{year}")
                return []
            elif response.status_code >= 500:
                time.sleep(1 * (attempt + 1))
                continue
            else:
                print(f"    ❌ HTTP {response.status_code}")
                return None
        except Exception as e:
            time.sleep(1)
            continue

    return None


def parse_nass_json_to_df(records, state_alpha):
    """
    Parse NASS JSON records into a DataFrame with wide format.
    """
    if not records:
        return pd.DataFrame()

    parsed = []
    state_fips = STATE_FIPS.get(state_alpha, '')

    for rec in records:
        county_ansi = rec.get('county_ansi', '')
        if not county_ansi or len(county_ansi) != 3:
            continue

        fips = state_fips + county_ansi

        value_str = rec.get('Value', '')
        if value_str in ['(D)', '(Z)', '(NA)', '(S)', '(H)', '(L)', ''] or value_str is None:
            val_float = 0.0
        else:
            try:
                val_float = float(value_str.replace(',', ''))
            except:
                val_float = 0.0

        parsed.append({
            'county_fips': fips,
            'county_name': rec.get('county_name', '').title(),
            'state_alpha': state_alpha,
            'year': rec.get('year', ''),
            'statistic': rec.get('statisticcat_desc', ''),
            'value': val_float
        })

    if not parsed:
        return pd.DataFrame()

    df_long = pd.DataFrame(parsed)

    df_wide = df_long.pivot_table(
        index=['year', 'county_fips', 'county_name', 'state_alpha'],
        columns='statistic',
        values='value',
        aggfunc='first'
    ).reset_index()

    return df_wide


@click.command()
@click.option('--crop', '-c', required=True, type=click.Choice(list(CROPS.keys())),
              help='Crop name (corn, soybeans, wheat, cotton)')
@click.option('--start', '-s', required=True, type=int, help='Start year')
@click.option('--end', '-e', required=True, type=int, help='End year')
def main(crop, start, end):
    """
    Extracts USDA NASS agricultural data.
    Merges with Census Bureau county land area and coordinates.
    """
    api_key = os.environ.get('NASS_API_KEY')
    if not api_key:
        print("❌ Error: NASS_API_KEY environment variable not set.")
        sys.exit(1)

    # 1. Fetch County Geo Reference (Area + Lat/Lon)
    geo_ref_df = fetch_geo_data_from_census()

    commodity = CROPS[crop]
    all_data = []

    print("-" * 60)
    print(f"🌽 Starting extraction for {crop.upper()} ({start}-{end})")
    
    with httpx.Client(headers=API_HEADERS) as client:
        for year in range(start, end + 1):
            print(f"📅 Processing Year: {year}")

            for state_alpha in CORN_BELT_STATES:
                print(f"  Downloading {state_alpha}...", end=" ", flush=True)
                time.sleep(0.5)

                records = fetch_state_crop_data(client, api_key, state_alpha, commodity, year)

                if records is not None:
                    df_state = parse_nass_json_to_df(records, state_alpha)
                    if not df_state.empty:
                        all_data.append(df_state)
                        print(f"✅ ({len(df_state)} counties)")
                    else:
                        print("⚠️ No data")
                else:
                    print("❌ API Error")

    print("-" * 60)

    if not all_data:
        print("❌ No data collected. Exiting.")
        sys.exit(1)

    final_df = pd.concat(all_data, ignore_index=True)

    # --- Post Processing ---

    for stat in ['AREA PLANTED', 'YIELD', 'PRODUCTION']:
        if stat not in final_df.columns:
            final_df[stat] = 0.0

    final_df.rename(columns={
        'AREA PLANTED': 'area_planted',
        'YIELD': 'yield_bu_acre',
        'PRODUCTION': 'production'
    }, inplace=True)

    final_df['production_1000bu'] = final_df['production'] / 1000.0
    final_df['area_planted_1000acres'] = final_df['area_planted'] / 1000.0

    # --- Merge with Geo Data (Lat/Lon/Acres) ---
    # We use a Left Join on FIPS
    if not geo_ref_df.empty:
        final_df = final_df.merge(geo_ref_df, left_on='county_fips', right_on='fips', how='left')
        
        # Fill missing geo data with 0 or NaN
        final_df['area_acres'] = final_df['area_acres'].fillna(0)
        # Note: We usually leave Lat/Lon as NaN if missing so they don't plot at 0,0 (Atlantic Ocean)
    else:
        final_df['area_acres'] = 0.0
        final_df['latitude'] = None
        final_df['longitude'] = None

    final_df.rename(columns={'area_acres': 'total_county_area_acres'}, inplace=True)

    # Final column selection
    cols_order = [
        'year',
        'county_name',
        'state_alpha',
        'county_fips',      # Kept fips for reference
        'latitude',         # Added
        'longitude',        # Added
        'yield_bu_acre',
        'production_1000bu',
        'area_planted_1000acres',
        'total_county_area_acres'
    ]
    
    # Filter columns that actually exist (in case API didn't return some stats)
    cols_order = [c for c in cols_order if c in final_df.columns]

    final_df = final_df[cols_order]
    final_df.sort_values(by=['year', 'state_alpha', 'county_name'], inplace=True)

    # --- Output ---
    output_base = f"dataset_us_{crop}_{start}_{end}"

    # 1. CSV
    csv_path = f"{output_base}.csv"
    final_df.to_csv(csv_path, index=False)
    print(f"💾 Saved CSV: {csv_path}")

    # 2. Parquet
    pq_path = f"{output_base}.parquet"
    final_df.to_parquet(pq_path, index=False)
    print(f"💾 Saved Parquet: {pq_path}")

    # 3. JSON (New Addition)
    json_path = f"{output_base}.json"
    final_df.to_json(json_path, orient='records', indent=4, double_precision=15)
    print(f"💾 Saved JSON: {json_path}")

    print(f"\nTotal Records: {len(final_df)}")
    filled_geo_count = final_df['latitude'].notna().sum()
    print(f"Counties with Geolocation: {filled_geo_count} / {len(final_df)}")


if __name__ == '__main__':
    main()