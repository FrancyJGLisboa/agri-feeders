#!/usr/bin/env python3
"""
Reformat crop calendar to match required JSON structure
"""

# /// script
# dependencies = [
#   "pandas>=2.0.0",
# ]
# ///

import json
from datetime import datetime

def get_state_abbreviation(state_name):
    """Convert full state name to 2-letter abbreviation"""
    state_map = {
        'ARKANSAS': 'AR',
        'ILLINOIS': 'IL',
        'INDIANA': 'IN',
        'IOWA': 'IA',
        'KANSAS': 'KS',
        'KENTUCKY': 'KY',
        'LOUISIANA': 'LA',
        'MICHIGAN': 'MI',
        'MINNESOTA': 'MN',
        'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO',
        'NEBRASKA': 'NE',
        'NORTH DAKOTA': 'ND',
        'OHIO': 'OH',
        'SOUTH DAKOTA': 'SD',
        'TENNESSEE': 'TN',
        'WISCONSIN': 'WI',
        'US TOTAL': 'US'
    }
    return state_map.get(state_name.upper(), state_name)

def convert_date_format(date_str):
    """Convert YYYY-MM-DD to MM-DD format"""
    if not date_str or date_str == 'null':
        return None
    try:
        # Parse YYYY-MM-DD format
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        # Return MM-DD format
        return dt.strftime('%m-%d')
    except:
        return None

def reformat_crop_calendar():
    """Reformat the crop calendar to match required structure"""

    # Read the existing JSON
    with open('/Users/francy/agri-feeders/crop_calendar_us_corn_soybean.json', 'r') as f:
        data = json.load(f)

    # Create reformatted structure
    corn_calendar = {"by_planted_year": {}}
    soybean_calendar = {"by_planted_year": {}}

    # Process Corn data
    corn_data = data['corn']['CORN']
    for row in corn_data:
        year = str(row['Row Labels'])
        corn_calendar["by_planted_year"][year] = {}

        for state_name, date_str in row.items():
            if state_name == 'Row Labels':
                continue

            state_code = get_state_abbreviation(state_name)
            mm_dd = convert_date_format(date_str)

            if mm_dd:
                corn_calendar["by_planted_year"][year][state_code] = mm_dd

    # Process Soybean data
    soy_data = data['soybean']['SOY']
    for row in soy_data:
        year = str(row['Row Labels'])
        soybean_calendar["by_planted_year"][year] = {}

        for state_name, date_str in row.items():
            if state_name == 'Row Labels':
                continue

            state_code = get_state_abbreviation(state_name)
            mm_dd = convert_date_format(date_str)

            if mm_dd:
                soybean_calendar["by_planted_year"][year][state_code] = mm_dd

    return corn_calendar, soybean_calendar

def main():
    print("="*80)
    print("REFORMATTING CROP CALENDAR")
    print("="*80)

    # Reformat the data
    corn_calendar, soybean_calendar = reformat_crop_calendar()

    # Save corn calendar
    corn_file = '/Users/francy/agri-feeders/crop_calendar_us_corn.json'
    with open(corn_file, 'w') as f:
        json.dump(corn_calendar, f, indent=2)
    print(f"\nCorn calendar saved: {corn_file}")

    # Save soybean calendar
    soybean_file = '/Users/francy/agri-feeders/crop_calendar_us_soybean.json'
    with open(soybean_file, 'w') as f:
        json.dump(soybean_calendar, f, indent=2)
    print(f"Soybean calendar saved: {soybean_file}")

    # Create combined file
    combined = {
        "corn": corn_calendar,
        "soybean": soybean_calendar
    }
    combined_file = '/Users/francy/agri-feeders/crop_calendar_us_corn_soybean_formatted.json'
    with open(combined_file, 'w') as f:
        json.dump(combined, f, indent=2)
    print(f"Combined calendar saved: {combined_file}")

    # Print sample
    print("\n" + "="*80)
    print("SAMPLE OUTPUT - Corn Calendar")
    print("="*80)
    print(json.dumps(corn_calendar, indent=2)[:500] + "...")

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Corn: {len(corn_calendar['by_planted_year'])} years")
    print(f"  Sample year (2002): {list(corn_calendar['by_planted_year']['2002'].keys())[:5]}...")
    print(f"Soybean: {len(soybean_calendar['by_planted_year'])} years")
    print(f"  Sample year (2002): {list(soybean_calendar['by_planted_year']['2002'].keys())[:5]}...")

if __name__ == '__main__':
    main()