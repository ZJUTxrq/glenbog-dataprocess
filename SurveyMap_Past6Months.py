import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent
input_file = DIR / "Glenbog.csv"
output_file = DIR / "SurveyMap_Past6Months.csv"

# =========================================================
# Read the original Glenbog dataset
# =========================================================
df = pd.read_csv(input_file)

# =========================================================
# Define the columns required for the Survey Map dataset
# These columns are selected to match the target structure
# =========================================================
survey_map_columns = [
    "scientificName",
    "vernacularName",
    "class",
    "order",
    "family",
    "eventDate",
    "year",
    "month",
    "day",
    "dataResourceName",
    "recordedBy",
    "samplingProtocol",
    "decimalLatitude",
    "decimalLongitude",
    "dataGeneralizations",
    "locality",
    "verbatimLocality"
]

for col in survey_map_columns:
    if col not in df.columns:
        df[col] = ""

# =========================================================
# Convert eventDate to datetime format
# errors='coerce' will turn invalid dates into NaT
# utc=True ensures a consistent timezone-aware format
# =========================================================
df["eventDate_dt"] = pd.to_datetime(df["eventDate"], errors="coerce", utc=True)

# Remove rows where eventDate could not be parsed
df = df.dropna(subset=["eventDate_dt"]).copy()

# =========================================================
# Filter records from the past 6 months relative to today
# normalize() removes the time portion and keeps only the date
# =========================================================
today = pd.Timestamp.today(tz="UTC").normalize()
cutoff_date = today - pd.DateOffset(months=6)

filtered = df[df["eventDate_dt"] >= cutoff_date].copy()

# =========================================================
# Keep only the columns needed for the Survey Map output
# =========================================================
survey_df = filtered[survey_map_columns].copy()

# =========================================================
# Standardise eventDate format to YYYY-MM-DD
# =========================================================
survey_df["eventDate"] = pd.to_datetime(
    survey_df["eventDate"], errors="coerce", utc=True
).dt.strftime("%Y-%m-%d")

# =========================================================
# Convert year, month, and day to numeric format
# Using float keeps the output style similar to the existing survey map file
# Example: 2025.0 instead of 2025
# =========================================================
for col in ["year", "month", "day"]:
    survey_df[col] = pd.to_numeric(survey_df[col], errors="coerce").astype(float)

# =========================================================
# Convert latitude and longitude to numeric values
# Invalid values will become NaN
# =========================================================
survey_df["decimalLatitude"] = pd.to_numeric(survey_df["decimalLatitude"], errors="coerce")
survey_df["decimalLongitude"] = pd.to_numeric(survey_df["decimalLongitude"], errors="coerce")

# =========================================================
# Remove rows with missing spatial coordinates
# These records cannot be mapped in the survey map
# =========================================================
survey_df = survey_df.dropna(subset=["decimalLatitude", "decimalLongitude"])

# =========================================================
# Remove duplicate rows to improve data quality
# =========================================================
survey_df = survey_df.drop_duplicates()

# =========================================================
# Sort the final dataset for consistency
# =========================================================
survey_df = survey_df.sort_values(
    by=["eventDate", "scientificName", "decimalLatitude", "decimalLongitude"],
    ascending=[True, True, True, True]
).reset_index(drop=True)

# =========================================================
# Export the cleaned dataset to CSV
# =========================================================
survey_df.to_csv(output_file, index=False, encoding="utf-8")

# =========================================================
# Print a short summary
# =========================================================
print("=== Cleaning Summary ===")
print(f"Today's date: {today.date()}")
print(f"Cutoff date (past 6 months): {cutoff_date.date()}")
print(f"Output file: {output_file}")
print(f"Number of records in cleaned dataset: {len(survey_df)}")
print(f"Date range kept: {survey_df['eventDate'].min()} to {survey_df['eventDate'].max()}")

print("\nPreview of cleaned data:")
print(survey_df.head())