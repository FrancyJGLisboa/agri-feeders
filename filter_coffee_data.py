#!/usr/bin/env python3
"""
Filter coffee dataset to remove rows with zero yield_kg_ha
"""

# /// script
# dependencies = [
#   "pandas>=2.0.0",
# ]
# ///

import pandas as pd
import sys

def main():
    input_file = '/Users/francy/agri-feeders/data/dataset_cafe_2000_2024.csv'
    output_file = '/Users/francy/agri-feeders/data/dataset_cafe_2000_2024_filtered.csv'

    print(f"Reading dataset from {input_file}...")
    df = pd.read_csv(input_file)

    print(f"Total rows before filtering: {len(df)}")

    # Filter out rows where yield_kg_ha is zero
    filtered_df = df[df['yield_kg_ha'] != 0.0]

    print(f"Rows with yield_kg_ha=0 removed: {len(df) - len(filtered_df)}")
    print(f"Total rows after filtering: {len(filtered_df)}")

    # Save filtered dataset
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered dataset saved to {output_file}")

    # Also update the original file
    df[df['yield_kg_ha'] != 0.0].to_csv(input_file, index=False)
    print(f"Original file updated: {input_file}")

if __name__ == '__main__':
    main()