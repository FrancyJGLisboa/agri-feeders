#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "click",
# ]
# ///

"""
NASS JSON Converter for HTML App
Usage:
    uv run STEP2_convert_to_required_nass_json_format.py input.json --crop corn --out app_ready_corn_usa.json
    uv run STEP2_convert_to_required_nass_json_format.py input.json --crop soybeans --out app_ready_soybeans_usa.json
    uv run STEP2_convert_to_required_nass_json_format.py input.json --crop wheat --out app_ready_wheat_usa.json

"""

import json
import re
import click
from pathlib import Path

# --- Constants ---
# Keeping imperial units - no conversion needed
# ACRES_TO_HA = 0.404686 (NOT USED - keeping acres)
# BUSHEL_TO_TONNES conversion factors (NOT USED - keeping bushels)

def make_slug(name, state):
    """Generates 'adair-ia' from 'Adair' and 'IA'."""
    if not name or not state: return "unknown"
    clean_name = re.sub(r'[^a-zA-Z0-9]', '-', name.lower())
    clean_name = re.sub(r'-+', '-', clean_name).strip('-')
    return f"{clean_name}-{state.lower().strip()}"

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--crop', '-c', required=True, type=click.Choice(['corn', 'soybeans', 'wheat']), help='Crop name')
@click.option('--out', '-o', default='app_ready_nass.json', help='Output JSON file')
def main(input_file, crop, out):
    """
    Converts flat NASS JSON to App Hierarchical JSON (keeping imperial units: acres/bushels).
    """
    print(f"📖 Reading {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # App Structure
    final_json = {
        "municipios": {},
        "area": { crop: {} },
        "producao": { crop: {} }
    }

    # No conversion factor needed - keeping imperial units
    count = 0

    for row in raw_data:
        # Skip if no coordinates
        if row.get('latitude') is None or row.get('longitude') is None:
            continue

        year = str(row['year'])
        county = row['county_name']
        state = row['state_alpha']
        key = make_slug(county, state)

        # 1. Metadata (Geographic)
        if key not in final_json['municipios']:
            final_json['municipios'][key] = {
                "lat": row['latitude'],
                "lon": row['longitude'],
                "label": f"{county} ({state})",
                "uf": state
            }

        # 2. Area: Keeping in 1000 Acres (imperial units)
        # The input is usually "area_planted_1000acres"
        val_area_1k = row.get('area_planted_1000acres')
        if val_area_1k is not None and val_area_1k > 0:
            # Keep in 1000 acres (no conversion)
            area_acres = val_area_1k

            if year not in final_json['area'][crop]:
                final_json['area'][crop][year] = {}
            final_json['area'][crop][year][key] = round(area_acres, 2)

        # 3. Production: Keeping in 1000 Bushels (imperial units)
        # The input is usually "production_1000bu"
        val_prod_1k = row.get('production_1000bu')
        if val_prod_1k is not None and val_prod_1k > 0:
            # Keep in 1000 bushels (no conversion)
            prod_bu = val_prod_1k

            if year not in final_json['producao'][crop]:
                final_json['producao'][crop][year] = {}
            final_json['producao'][crop][year][key] = round(prod_bu, 2)
        
        count += 1

    # Save
    with open(out, 'w', encoding='utf-8') as f:
        # Use separators for compact JSON
        json.dump(final_json, f, separators=(',', ':'))

    print(f"✅ Success! Saved to {out}")
    print(f"   Processed {count} records.")
    print(f"   Unique Counties: {len(final_json['municipios'])}")
    print(f"   Years: {sorted(final_json['area'][crop].keys())}")

if __name__ == '__main__':
    main()