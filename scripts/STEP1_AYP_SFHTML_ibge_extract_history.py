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
IBGE SIDRA Bulk Extractor - Municipality Level
Generates CSV, Parquet, and JSON datasets for specific crops and states.
Includes Geolocation (Lat/Lon).

Usage:
    uv run STEP1_AYP_SFHTML_ibge_extract_history.py --crop soja --start 2020 --end 2023
    uv run STEP1_AYP_SFHTML_ibge_extract_history.py --crop milho --start 2022 --end 2022
"""

import httpx
import click
import pandas as pd
import sys
import time
import io
import os
from pathlib import Path
from datetime import datetime

# --- Configuration Maps ---

# Cache configuration
CACHE_DIR = Path.home() / '.ibge_cache'
CACHE_FILE = CACHE_DIR / 'municipios_geo.parquet'
CACHE_MAX_AGE_DAYS = 90  # Municipality coordinates rarely change

# Map State Abbreviations to IBGE IDs
STATE_MAP = {
    'RO': '11', 'AC': '12', 'AM': '13', 'RR': '14', 'PA': '15', 'AP': '16', 'TO': '17',
    'MA': '21', 'PI': '22', 'CE': '23', 'RN': '24', 'PB': '25', 'PE': '26', 'AL': '27',
    'SE': '28', 'BA': '29', 'MG': '31', 'ES': '32', 'RJ': '33', 'SP': '35', 'PR': '41',
    'SC': '42', 'RS': '43', 'MS': '50', 'MT': '51', 'GO': '52', 'DF': '53'
}

# Target States per requirement
# TARGET_STATES = ['GO', 'MS', 'MT', 'PR', 'RS', 'SC', 'MG', 'SP', 'PI', 'BA', 'TO', 'MA']
TARGET_STATES = ['MG', 'SP', 'BA', 'ES']


PRODUTOS_TEMPORARIOS = {
    'soja': '2713', 'milho': '2711', 'algodao': '2689', 'cana': '2696',
    'arroz': '2692', 'feijao': '2702', 'sorgo': '2714', 'trigo': '2719'
}

PRODUTOS_PERMANENTES = {
    'cafe': '2723', 'laranja': '2733', 'banana': '2720', 'uva': '2748',
    'cacau': '2722', 'manga': '2737'
}

PRODUTOS = {**PRODUTOS_TEMPORARIOS, **PRODUTOS_PERMANENTES}

# Variable Mapping
# key: internal_name, value: sidra_code
VARS_TEMP = {
    'production': '214',  # Quantidade produzida (Toneladas)
    'yield': '112',       # Rendimento médio (kg/hectare)
    'area_planted': '109' # Área plantada (Hectares)
}

VARS_PERM = {
    'production': '214',
    'yield': '112',
    'area_planted': '2313' # Note: Different code for permanent crops
}

# --- Geo Data Handling (Lat/Lon) ---

def load_cached_geo_data():
    """Load municipality geo data from local cache."""
    if not CACHE_FILE.exists():
        return None

    cache_age = datetime.now().timestamp() - CACHE_FILE.stat().st_mtime
    cache_age_days = cache_age / (24 * 3600)

    if cache_age_days > CACHE_MAX_AGE_DAYS:
        return None

    print(f"📁 Loading geo data from cache: {CACHE_FILE}")
    try:
        return pd.read_parquet(CACHE_FILE)
    except Exception:
        return None

def fetch_geo_data():
    """
    Downloads Brazilian Municipality Lat/Lon reference.
    Source: GitHub (kelvins/municipios-brasileiros) - widely used static reference.
    """
    cached = load_cached_geo_data()
    if cached is not None:
        return cached

    url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
    print(f"🌍 Downloading Munipality Coordinates...")
    print(f"    🔗 {url}")

    try:
        # Standard httpx client
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
        
        # Parse CSV
        # Columns: codigo_ibge, nome, latitude, longitude, capital, codigo_uf
        df = pd.read_csv(io.BytesIO(response.content))
        
        # Select relevant columns
        df = df[['codigo_ibge', 'latitude', 'longitude']].copy()
        
        # Ensure ID is string (SIDRA uses strings)
        df['codigo_ibge'] = df['codigo_ibge'].astype(str)
        
        # Save to cache
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(CACHE_FILE, index=False)
        
        return df
    except Exception as e:
        print(f"    ⚠️ Warning: Could not fetch geo data ({e}). Output will lack coordinates.")
        return pd.DataFrame()

# --- IBGE API Logic ---

def get_query_params(crop_name):
    """Returns table_id, classification_id, product_code, and variable map."""
    crop = crop_name.lower()
    if crop in PRODUTOS_TEMPORARIOS:
        return '1612', '81', PRODUTOS[crop], VARS_TEMP
    elif crop in PRODUTOS_PERMANENTES:
        return '1613', '82', PRODUTOS[crop], VARS_PERM
    else:
        raise ValueError(f"Crop '{crop_name}' not supported. Available: {list(PRODUTOS.keys())}")

def fetch_state_data(table_id, period, var_codes_str, class_id, prod_code, state_code):
    """
    Fetches data for ALL municipalities in a specific state.
    URL Syntax: localities=N6[N3[state_code]] -> All Munis (N6) inside State (N3)
    """
    base_url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{table_id}"
    
    # URL Construction
    url = (
        f"{base_url}/periodos/{period}/variaveis/{var_codes_str}"
        f"?localidades=N6[N3[{state_code}]]"
        f"&classificacao={class_id}[{prod_code}]"
    )

    try:
        response = httpx.get(url, timeout=60.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        print(f"    Error fetching state {state_code}: {e}")
        return None

def parse_sidra_json_to_df(json_data, state_abbr, var_map):
    """
    Parses complex SIDRA nested JSON into a flat Pandas DataFrame.
    """
    if not json_data:
        return pd.DataFrame()

    records = []
    code_to_name = {v: k for k, v in var_map.items()}

    for var_block in json_data:
        variable_id = var_block.get('id')
        if variable_id not in code_to_name:
            continue
            
        var_name = code_to_name[variable_id]
        results = var_block.get('resultados', [])
        
        if not results:
            continue

        series_list = results[0].get('series', [])
        
        for item in series_list:
            muni_code = item['localidade']['id']
            muni_name = item['localidade']['nome']
            serie_data = item.get('serie', {})
            
            for year, value in serie_data.items():
                if value in ['...', '-', 'X', '..'] or value is None:
                    val_float = 0.0
                else:
                    try:
                        val_float = float(value)
                    except:
                        val_float = 0.0

                records.append({
                    'municipio_cod': muni_code,
                    'region_name': muni_name,
                    'state_name': state_abbr,
                    'year': year,
                    'variable': var_name,
                    'value': val_float
                })

    if not records:
        return pd.DataFrame()

    df_long = pd.DataFrame(records)
    
    # Pivot to Wide Format
    df_wide = df_long.pivot_table(
        index=['year', 'municipio_cod', 'region_name', 'state_name'],
        columns='variable',
        values='value',
        fill_value=0
    ).reset_index()

    return df_wide

@click.command()
@click.option('--crop', '-c', required=True, help='Crop name (e.g., soja, milho)')
@click.option('--start', '-s', required=True, type=int, help='Start year')
@click.option('--end', '-e', required=True, type=int, help='End year')
def main(crop, start, end):
    """
    Extracts IBGE agricultural data for specified states and generates CSV/Parquet/JSON.
    """
    try:
        table_id, class_id, prod_code, var_map = get_query_params(crop)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # 1. Fetch Geo Data (Lat/Lon)
    geo_df = fetch_geo_data()

    var_codes_str = ",".join(var_map.values())
    all_data = []
    
    print(f"🚜 Starting extraction for {crop.upper()} ({start}-{end})")
    print(f"📍 Target States: {', '.join(TARGET_STATES)}")
    print("-" * 60)

    # Loop Logic: Year -> State
    for year in range(start, end + 1):
        print(f"📅 Processing Year: {year}")
        
        for state_abbr in TARGET_STATES:
            state_code = STATE_MAP.get(state_abbr)
            if not state_code:
                continue

            print(f"  Downloading {state_abbr}...", end=" ", flush=True)
            
            raw_json = fetch_state_data(
                table_id, str(year), var_codes_str, class_id, prod_code, state_code
            )

            if raw_json:
                df_state = parse_sidra_json_to_df(raw_json, state_abbr, var_map)
                if not df_state.empty:
                    all_data.append(df_state)
                    print(f"✅ ({len(df_state)} munis)")
                else:
                    print("⚠️ No data")
            else:
                print("❌ API Error")
            
            time.sleep(0.5)

    print("-" * 60)
    
    if not all_data:
        print("❌ No data collected. Exiting.")
        sys.exit(1)

    # Combine all chunks
    final_df = pd.concat(all_data, ignore_index=True)

    # --- Post Processing ---
    
    required_vars = ['production', 'yield', 'area_planted']
    for v in required_vars:
        if v not in final_df.columns:
            final_df[v] = 0.0

    # Conversions
    final_df.rename(columns={'yield': 'yield_kg_ha'}, inplace=True)
    final_df['production_1000t'] = final_df['production'] / 1000.0
    final_df['area_planted_1000ha'] = final_df['area_planted'] / 1000.0

    # --- Merge with Geo Data ---
    if not geo_df.empty:
        # Ensure matching types (IBGE uses 7 digits generally)
        final_df['municipio_cod'] = final_df['municipio_cod'].astype(str)
        # Left Join
        final_df = final_df.merge(geo_df, left_on='municipio_cod', right_on='codigo_ibge', how='left')
    else:
        final_df['latitude'] = None
        final_df['longitude'] = None

    # Final selection
    cols_order = [
        'year', 
        'region_name', 
        'state_name',
        'municipio_cod', # Kept for reference
        'latitude',      # Added
        'longitude',     # Added
        'yield_kg_ha', 
        'production_1000t', 
        'area_planted_1000ha'
    ]
    
    # Filter columns that exist (in case geo failed)
    cols_order = [c for c in cols_order if c in final_df.columns]
    
    final_df = final_df[cols_order]
    final_df.sort_values(by=['year', 'state_name', 'region_name'], inplace=True)

    # --- Output ---
    output_base = f"dataset_{crop}_{start}_{end}"
    
    # 1. CSV
    csv_path = f"{output_base}.csv"
    final_df.to_csv(csv_path, index=False)
    print(f"💾 Saved CSV: {csv_path}")

    # 2. Parquet
    pq_path = f"{output_base}.parquet"
    final_df.to_parquet(pq_path, index=False)
    print(f"💾 Saved Parquet: {pq_path}")

    # 3. JSON
    json_path = f"{output_base}.json"
    final_df.to_json(json_path, orient='records', indent=4, double_precision=15)
    print(f"💾 Saved JSON: {json_path}")

    print(f"\nTotal Records: {len(final_df)}")
    if 'latitude' in final_df.columns:
        print(f"Munis with Geolocation: {final_df['latitude'].notna().sum()} / {len(final_df)}")

if __name__ == '__main__':
    main()