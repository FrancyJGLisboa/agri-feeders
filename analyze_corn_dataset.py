#!/usr/bin/env python3
"""
Analyze US Corn Dataset - Comprehensive Description
"""

# /// script
# dependencies = [
#   "pandas>=2.0.0",
# ]
# ///

import pandas as pd
import numpy as np

def analyze_dataset():
    file_path = '/Users/francy/agri-feeders/data/dataset_us_corn_2000_2024.csv'

    print("=" * 80)
    print("US CORN DATASET ANALYSIS")
    print("=" * 80)

    # Read dataset
    df = pd.read_csv(file_path)

    # Basic info
    print(f"\n1. DATASET OVERVIEW")
    print(f"{'='*40}")
    print(f"Total rows: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Date range: {df['year'].min()} - {df['year'].max()}")
    print(f"Missing values: {df.isnull().sum().sum()}")

    # Column information
    print(f"\n2. COLUMN INFORMATION")
    print(f"{'='*40}")
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].count()
        print(f"\n{col}:")
        print(f"  - Type: {dtype}")
        print(f"  - Non-null count: {non_null:,} / {len(df):,}")
        if dtype in ['int64', 'float64']:
            print(f"  - Min: {df[col].min():.2f}")
            print(f"  - Max: {df[col].max():.2f}")
            print(f"  - Mean: {df[col].mean():.2f}")
            print(f"  - Median: {df[col].median():.2f}")

    # Year coverage
    print(f"\n3. YEAR COVERAGE")
    print(f"{'='*40}")
    year_counts = df['year'].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"{year}: {count:,} records")

    # State coverage
    print(f"\n4. STATE COVERAGE")
    print(f"{'='*40}")
    state_counts = df['state_alpha'].value_counts().sort_values(ascending=False)
    for state, count in state_counts.items():
        print(f"{state}: {count:,} records")

    # County coverage
    print(f"\n5. COUNTY COVERAGE")
    print(f"{'='*40}")
    print(f"Total unique counties: {df['county_name'].nunique():,}")
    print(f"Total unique county-state combinations: {df[['county_name', 'state_alpha']].drop_duplicates().shape[0]:,}")

    # Geographic coverage
    print(f"\n6. GEOGRAPHIC COVERAGE")
    print(f"{'='*40}")
    print(f"Latitude range: {df['latitude'].min():.4f} to {df['latitude'].max():.4f}")
    print(f"Longitude range: {df['longitude'].min():.4f} to {df['longitude'].max():.4f}")

    # Production statistics
    print(f"\n7. PRODUCTION STATISTICS")
    print(f"{'='*40}")
    print(f"Total production (all years): {df['production_1000bu'].sum():,.0f} thousand bushels")
    print(f"Average yield: {df['yield_bu_acre'].mean():.2f} bushels/acre")
    print(f"Total planted area: {df['area_planted_1000acres'].sum():,.0f} thousand acres")

    print("\n" + "=" * 80)

if __name__ == '__main__':
    analyze_dataset()