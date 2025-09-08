#!/usr/bin/env python3
"""
Data Filtering Script for FAHE Project
Filters cleaned CSV files to extract records with funding_code containing "502"
and selects specific columns for analysis.
"""

import pandas as pd
import os
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def filter_dataframe(df, filename):
    """
    Filter dataframe to extract records with funding_code containing "502"
    and select only specified columns.
    
    Args:
        df: pandas DataFrame
        filename: Name of the file being processed
        
    Returns:
        Filtered pandas DataFrame
    """
    logger.info(f"Filtering dataframe for {filename}")
    
    # Original shape
    original_shape = df.shape
    logger.info(f"Original shape: {original_shape}")
    
    # Filter for funding_code containing "502"
    # Check if funding_code column exists
    if 'funding_code' not in df.columns:
        logger.error(f"funding_code column not found in {filename}")
        return pd.DataFrame()
    
    # Filter for records where funding_code contains "502"
    filtered_df = df[df['funding_code'].str.contains('502', case=False, na=False)]
    
    logger.info(f"Records with funding_code containing '502': {len(filtered_df)} out of {len(df)}")
    
    # Select only the specified columns
    required_columns = [
        'fiscal_year',
        'state_name', 
        'county',
        'zip_code',
        'county_fips',
        'funding_code',
        'program_area',
        'investment_dollars',
        'number_of_investments'
    ]
    
    # Check which columns are available
    available_columns = [col for col in required_columns if col in filtered_df.columns]
    missing_columns = [col for col in required_columns if col not in filtered_df.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns in {filename}: {missing_columns}")
    
    if not available_columns:
        logger.error(f"No required columns found in {filename}")
        return pd.DataFrame()
    
    # Select only the available required columns
    final_df = filtered_df[available_columns].copy()
    
    # Log the filtering results
    logger.info(f"Final filtered shape: {final_df.shape}")
    logger.info(f"Columns selected: {list(final_df.columns)}")
    
    # Show some statistics
    if len(final_df) > 0:
        logger.info(f"Sample funding codes found: {final_df['funding_code'].unique()[:5]}")
        logger.info(f"Program areas: {final_df['program_area'].unique()}")
        logger.info(f"Investment dollars range: ${final_df['investment_dollars'].min()} - ${final_df['investment_dollars'].max()}")
    
    return final_df

def process_csv_file(input_path, output_path):
    """
    Process a single CSV file for filtering.
    
    Args:
        input_path: Path to input cleaned CSV file
        output_path: Path to output filtered CSV file
    """
    try:
        logger.info(f"Processing {input_path}")
        
        # Read the cleaned CSV file
        df = pd.read_csv(input_path, encoding='utf-8')
        
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {input_path}")
        
        # Filter the dataframe
        filtered_df = filter_dataframe(df, os.path.basename(input_path))
        
        if len(filtered_df) == 0:
            logger.warning(f"No data matching criteria found in {input_path}")
            # Create empty file with headers
            filtered_df = pd.DataFrame(columns=[
                'fiscal_year', 'state_name', 'county', 'zip_code', 
                'county_fips', 'funding_code', 'program_area', 
                'investment_dollars', 'number_of_investments'
            ])
        
        # Save the filtered dataframe
        filtered_df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Successfully saved filtered data to {output_path}")
        logger.info(f"Filtered records: {len(filtered_df)}")
        
        return True, len(filtered_df)
        
    except Exception as e:
        logger.error(f"Error processing {input_path}: {str(e)}")
        return False, 0

def main():
    """
    Main function to process all cleaned CSV files and apply filters.
    """
    # Define paths
    cleaned_data_dir = Path("cleaned_data")
    filtered_data_dir = Path("filtered_data")
    
    # Create filtered_data directory if it doesn't exist
    filtered_data_dir.mkdir(exist_ok=True)
    logger.info(f"Created/verified directory: {filtered_data_dir}")
    
    # Get all CSV files in cleaned_data directory
    csv_files = list(cleaned_data_dir.glob("*.csv"))
    
    if not csv_files:
        logger.error("No CSV files found in cleaned_data directory")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to filter")
    
    # Process each CSV file
    successful_files = 0
    failed_files = 0
    total_filtered_records = 0
    
    for csv_file in csv_files:
        output_file = filtered_data_dir / csv_file.name
        
        success, record_count = process_csv_file(csv_file, output_file)
        
        if success:
            successful_files += 1
            total_filtered_records += record_count
        else:
            failed_files += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("DATA FILTERING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files processed: {len(csv_files)}")
    logger.info(f"Successfully processed: {successful_files}")
    logger.info(f"Failed to process: {failed_files}")
    logger.info(f"Total filtered records: {total_filtered_records}")
    logger.info(f"Filtered files saved to: {filtered_data_dir}")
    
    if successful_files > 0:
        logger.info("\nFiltered files:")
        for file in filtered_data_dir.glob("*.csv"):
            # Get record count for each file
            try:
                df = pd.read_csv(file)
                record_count = len(df)
                logger.info(f"  - {file.name}: {record_count} records")
            except:
                logger.info(f"  - {file.name}: Error reading file")
    
    # Show sample of filtered data
    if total_filtered_records > 0:
        logger.info("\nSample of filtered data:")
        sample_files = list(filtered_data_dir.glob("*.csv"))[:3]  # Show first 3 files
        for file in sample_files:
            try:
                df = pd.read_csv(file)
                if len(df) > 0:
                    logger.info(f"\n{file.name} - First few records:")
                    logger.info(f"Columns: {list(df.columns)}")
                    logger.info(f"Sample funding codes: {df['funding_code'].unique()[:3]}")
                    break
            except:
                continue

if __name__ == "__main__":
    main()

