import pandas as pd
import glob
import re
import warnings

# Suppress DtypeWarning from pandas
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# Path to your folder
path = r"C:\Users\athar\OneDrive\Desktop\fulton_ring\fahe\education\georgia"

# Get all CSV files in the folder
all_files = glob.glob(path + "/*.csv")

# Create an empty list to store DataFrames
dfs = []

# Loop through each file and read
for file in all_files:
    # Read CSV
    df = pd.read_csv(file)

    # Extract 4-digit year (e.g. 2016, 2020, 2023) or fallback to 2-digit
    match = re.search(r"(20\d{2})", file)   # looks for full year like 2016, 2020
    if match:
        year = int(match.group(1))
    else:
        # fallback: last 2 digits before ".csv"
        match2 = re.search(r"(\d{2})(?=\.csv$)", file)
        year = 2000 + int(match2.group(1)) if match2 else None

    # Add year column
    df["year"] = year

    dfs.append(df)

# Concatenate all DataFrames
df_all = pd.concat(dfs, ignore_index=True)

# Delete row at index 0
df_all = df_all.drop(0)
# Reset index after deletion
df_all = df_all.reset_index(drop=True)

# Split NAME column into County and State
df_all[['County', 'State']] = df_all['NAME'].str.split(',', expand=True)

# Clean up extra spaces
df_all['County'] = df_all['County'].str.strip()
df_all['State'] = df_all['State'].str.strip()

# Remove the word "County" from County column
df_all['County'] = df_all['County'].str.replace('County', '', regex=False).str.strip()

# Reorder columns: GEO_ID, year, County, State, then the rest
cols = df_all.columns.tolist()

# Ensure order
new_order = ['GEO_ID', 'year', 'County', 'State'] + [col for col in cols if col not in ['GEO_ID', 'year', 'County', 'State']]

df_all = df_all[new_order]

# List of columns to keep
keep = [
    "GEO_ID", "NAME", "State", "County", "year",
    "S1501_C01_003E",   
    "S1501_C02_003E",   
    "S1501_C01_005E",   
    "S1501_C02_005EE",   
    "S1501_C01_059E",   # Median earnings (headline for education table)
]

# Filter to the columns that exist, then rename
existing = [c for c in keep if c in df_all.columns]

# Dictionary for renaming columns
renamed = {
    "S1501_C01_003E": "Adults(18-24) with High School",
    "S1501_C02_003E": "Percent Adults(18-24) with High School",
    "S1501_C01_005E": "Adults(18-24) with College Degree",  # Bachelor's or higher
    "S1501_C02_005E": "Percent Adults(18-24) with College Degree",
    "S1501_C01_059E": "Total Median Earnings",
}

# Create the output DataFrame with existing columns and renamed
df_out = df_all[existing].rename(columns=renamed)

# Load the list of Appalachian counties
keep_county = pd.read_csv(r"C:\Users\athar\OneDrive\Desktop\fulton_ring\fahe\appalachian_counties.csv")

# Filter for Alabama counties from the list
keep_georgia = keep_county[keep_county["state"] == "Georgia"]

# Get a set of Alabama county names for efficient lookup
georgia_counties = set(keep_georgia["county"])

# Keep only rows in df_out where 'County' is in that list
df_out2 = df_out[df_out["County"].isin(georgia_counties)]

# Save the final DataFrame to a CSV file
df_out2.to_csv("df_2.csv", index=False)