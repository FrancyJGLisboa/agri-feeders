#!/usr/bin/env python3
"""
Extract historical crop calendar for US Corn and US Soybean
"""

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "openpyxl>=3.1.0",
# ]
# ///

import pandas as pd
import json
import sys
from datetime import datetime

def convert_to_serializable(obj):
    """Convert pandas objects to JSON serializable format"""
    if isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, (int, float, str, bool)):
        return obj
    else:
        return str(obj)

def extract_crop_calendar(file_path):
    """Extract crop calendar data for Corn and Soybean"""

    xl_file = pd.ExcelFile(file_path)
    crop_calendar = {
        "metadata": {
            "source": "USA_50pctPlantedDate_CornSoy.xlsx",
            "description": "Historical crop calendar for US Corn and Soybean - 50% planted dates",
            "extraction_date": "2024-01-16",
            "note": "Dates represent the day of year when 50% of corn/soybean was planted in each state"
        },
        "corn": {},
        "soybean": {}
    }

    for sheet_name in xl_file.sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"\nProcessing sheet: {sheet_name}")

        # Check if sheet contains corn or soybean data
        sheet_lower = sheet_name.lower()
        if 'corn' in sheet_lower:
            print(f"Found Corn data in sheet: {sheet_name}")
            print(f"Shape: {df.shape}")
            crop_calendar["corn"][sheet_name] = convert_to_serializable(df.to_dict('records'))
        elif 'soy' in sheet_lower:
            print(f"Found Soybean data in sheet: {sheet_name}")
            print(f"Shape: {df.shape}")
            crop_calendar["soybean"][sheet_name] = convert_to_serializable(df.to_dict('records'))

    return crop_calendar

def print_summary(crop_calendar):
    """Print a summary of the extracted data"""
    print("\n" + "="*80)
    print("EXTRACTION SUMMARY")
    print("="*80)

    for crop in ['corn', 'soybean']:
        print(f"\n{crop.upper()} Data:")
        for sheet_name, data in crop_calendar[crop].items():
            print(f"  - Sheet '{sheet_name}': {len(data)} years of data")

            # Show date range if data exists
            if data:
                years = []
                for row in data:
                    if 'Row Labels' in row:
                        years.append(row['Row Labels'])

                if years:
                    print(f"    Years: {min(years)} - {max(years)}")
                    print(f"    States covered: {len([k for k in data[0].keys() if k != 'Row Labels'])}")

def main():
    file_path = '/Users/francy/Downloads/USA_50pctPlantedDate_CornSoy.xlsx'
    output_file = '/Users/francy/agri-feeders/crop_calendar_us_corn_soybean.json'

    print("="*80)
    print("CROP CALENDAR EXTRACTION - US CORN AND SOYBEAN")
    print("="*80)

    try:
        # Extract crop calendar data
        crop_calendar = extract_crop_calendar(file_path)

        # Save to JSON file
        print(f"\nSaving to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(crop_calendar, f, indent=2, ensure_ascii=False)

        print(f"\nCrop calendar saved successfully!")
        print(f"File: {output_file}")

        # Print summary
        print_summary(crop_calendar)

        # Show a sample of the data
        print("\n" + "="*80)
        print("SAMPLE DATA (First 3 years)")
        print("="*80)

        for crop in ['corn', 'soybean']:
            print(f"\n{crop.upper()}:")
            if crop_calendar[crop]:
                for sheet_name, data in crop_calendar[crop].items():
                    print(f"\n  Sheet: {sheet_name}")
                    for i, row in enumerate(data[:3]):
                        print(f"    Year {row.get('Row Labels', 'N/A')}: {list(row.keys())[1:5]} ...")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()