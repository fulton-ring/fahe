#!/usr/bin/env python3
"""
Data Finalization Script for FAHE Project
Renames columns and selects only the 5 specified columns from Appalachian data,
then writes CSV with:
- headers WITHOUT quotes
- ONLY county_fips quoted ("1007")
- 502_investment_dollars as integer (303030)
"""

import os
import logging
from pathlib import Path
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def finalize_dataframe(df, filename):
    """
    Rename columns, select only the 5 specified columns, coerce dtypes.
    """
    logger.info(f"Finalizing dataframe for {filename}")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Original columns: {list(df.columns)}")

    final_df = df.copy()

    # Rename columns
    column_mapping = {
        'fiscal_year': 'year',
        'investment_dollars': '502_investment_dollars',
        'number_of_investments': 'number_of_502_investment'
    }
    final_df = final_df.rename(columns=column_mapping)
    logger.info(f"Columns after renaming: {list(final_df.columns)}")

    # Required columns
    required_columns = [
        'year',
        'county',
        'state_name',
        'county_fips',
        '502_investment_dollars',
        'number_of_502_investment'
    ]
    missing_columns = [c for c in required_columns if c not in final_df.columns]
    if missing_columns:
        logger.error(f"Missing required columns in {filename}: {missing_columns}")
        return pd.DataFrame()

    # Keep only required columns
    final_df = final_df[required_columns].copy()

    # ===== DTYPE COERCION =====
    # county_fips -> string digits (if you want 5-digit FIPS, add .str.zfill(5))
    final_df['county_fips'] = (
        final_df['county_fips'].astype(str)
        .str.extract(r'(\d+)')[0]
        .fillna('')
        # .str.zfill(5)  # uncomment for "01007"
    )

    # 502_investment_dollars -> int (remove punctuation first)
    final_df['502_investment_dollars'] = (
        final_df['502_investment_dollars'].astype(str)
        .str.replace(',', '', regex=False)
        .str.replace('$', '', regex=False)
    )
    final_df['502_investment_dollars'] = pd.to_numeric(
        final_df['502_investment_dollars'], errors='coerce'
    ).fillna(0).astype('int64')

    # number_of_502_investment -> int
    final_df['number_of_502_investment'] = pd.to_numeric(
        final_df['number_of_502_investment'], errors='coerce'
    ).fillna(0).astype('int64')

    logger.info(f"dtypes after coercion:\n{final_df.dtypes}")
    return final_df

def write_csv_only_county_fips_quoted(df: pd.DataFrame, output_path: Path):
    """
    Write CSV with:
      - header unquoted
      - only county_fips quoted
      - numeric ints for dollars and counts
    """
    cols = ['year','county','state_name','county_fips','502_investment_dollars','number_of_502_investment']
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        # header (no quotes)
        f.write(','.join(cols) + '\n')
        # rows
        for row in df[cols].itertuples(index=False, name=None):
            year, county, state_name, county_fips, dollars, n_loans = row
            # ensure safe strings (no commas expected in county/state_name)
            county = '' if pd.isna(county) else str(county)
            state_name = '' if pd.isna(state_name) else str(state_name)
            county_fips = '' if pd.isna(county_fips) else str(county_fips)

            # dollars and n_loans are already ints from coercion
            line = f'{year},{county},{state_name},"{county_fips}",{int(dollars)},{int(n_loans)}\n'
            f.write(line)

def process_csv_file(input_path, output_path):
    """Process a single CSV file."""
    try:
        logger.info(f"Processing {input_path}")

        # Read as strings for full control over types
        df = pd.read_csv(input_path, encoding='utf-8', dtype=str)
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {input_path}")

        finalized_df = finalize_dataframe(df, os.path.basename(input_path))
        if finalized_df.empty:
            logger.error(f"Failed to finalize data for {input_path}")
            return False, 0

        # Custom writer to satisfy quoting rules
        write_csv_only_county_fips_quoted(finalized_df, output_path)

        logger.info(f"Successfully saved finalized data to {output_path}")
        logger.info(f"Finalized records: {len(finalized_df)}")
        return True, len(finalized_df)

    except Exception as e:
        logger.error(f"Error processing {input_path}: {str(e)}")
        return False, 0

def main():
    appalachian_data_dir = Path("appalachian_data")
    final_data_dir = Path("final_data")

    final_data_dir.mkdir(exist_ok=True)
    logger.info(f"Created/verified directory: {final_data_dir}")

    csv_files = list(appalachian_data_dir.glob("*.csv"))
    if not csv_files:
        logger.error("No CSV files found in appalachian_data directory")
        return

    logger.info(f"Found {len(csv_files)} CSV files to finalize")

    successful_files = 0
    failed_files = 0
    total_finalized_records = 0

    for csv_file in csv_files:
        output_file = final_data_dir / csv_file.name
        success, record_count = process_csv_file(csv_file, output_file)
        if success:
            successful_files += 1
            total_finalized_records += record_count
        else:
            failed_files += 1

    logger.info("=" * 70)
    logger.info("DATA FINALIZATION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total files processed: {len(csv_files)}")
    logger.info(f"Successfully processed: {successful_files}")
    logger.info(f"Failed to process: {failed_files}")
    logger.info(f"Total finalized records: {total_finalized_records}")
    logger.info(f"Finalized files saved to: {final_data_dir}")

if __name__ == "__main__":
    main()

