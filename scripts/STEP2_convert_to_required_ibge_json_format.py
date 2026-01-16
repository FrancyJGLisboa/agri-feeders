#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "click",
#     "unidecode",
# ]
# ///

"""
IBGE JSON Converter for HTML App
Usage:
    uv run STEP2_convert_to_required_ibge_json_format.py input.json --crop soja --out app_ready_soja_br.json
    uv run STEP2_convert_to_required_ibge_json_format.py input.json --crop milho --out app_ready_milho_br.json
"""

import json
import re
import click
import unicodedata
from unidecode import unidecode

def normalize_slug(text):
    """
    Transforms 'Abaré - BA' into 'abare-ba'.
    Removes accents, spaces, special chars.
    """
    if not text: return "unknown"
    # Remove accents using unidecode
    text = unidecode(text).lower()
    # Replace non-alphanumeric with hyphen
    text = re.sub(r'[^a-z0-9]', '-', text)
    # Remove duplicate hyphens and strip
    text = re.sub(r'-+', '-', text).strip('-')
    return text

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--crop', '-c', required=True, help='Crop name (soja, milho)')
@click.option('--out', '-o', default='app_ready_ibge.json', help='Output JSON file')
def main(input_file, crop, out):
    """
    Converts flat IBGE JSON (1000 ha/t) to App Hierarchical JSON (ha/tonnes).
    """
    print(f"📖 Reading {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    final_json = {
        "municipios": {},
        "area": { crop: {} },
        "producao": { crop: {} }
    }

    count = 0
    for row in raw_data:
        # Check Geo availability
        if row.get('latitude') is None or row.get('longitude') is None:
            continue

        year = str(row['year'])
        region_name = row['region_name'] # e.g. "Sorriso - MT"
        state = row['state_name']
        
        # Create Slug
        key = normalize_slug(region_name)

        # 1. Metadata
        if key not in final_json['municipios']:
            final_json['municipios'][key] = {
                "lat": row['latitude'],
                "lon": row['longitude'],
                "label": region_name,
                "uf": state,
                "ibge_code": row.get('municipio_cod')
            }

        # 2. Area Conversion: 1000 ha -> ha
        # Input key: "area_planted_1000ha"
        val_area_1k = row.get('area_planted_1000ha')
        if val_area_1k is not None and val_area_1k > 0:
            area_ha = val_area_1k * 1000
            
            if year not in final_json['area'][crop]:
                final_json['area'][crop][year] = {}
            final_json['area'][crop][year][key] = round(area_ha, 2)

        # 3. Production Conversion: 1000 t -> t
        # Input key: "production_1000t"
        val_prod_1k = row.get('production_1000t')
        if val_prod_1k is not None and val_prod_1k > 0:
            prod_ton = val_prod_1k * 1000
            
            if year not in final_json['producao'][crop]:
                final_json['producao'][crop][year] = {}
            final_json['producao'][crop][year][key] = round(prod_ton, 2)
        
        count += 1

    # Save
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(final_json, f, separators=(',', ':'), ensure_ascii=False)

    print(f"✅ Success! Saved to {out}")
    print(f"   Processed {count} records.")
    print(f"   Unique Munis: {len(final_json['municipios'])}")
    print(f"   Years: {sorted(final_json['area'][crop].keys())}")

if __name__ == '__main__':
    main()