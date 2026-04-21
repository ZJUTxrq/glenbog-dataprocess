"""
Data Cleaning Script – Time Dot Graph (Long-Term Native Species)
Source file : records-2026-04-19.csv  (ALA / Glenbog State Forest)

Filters applied:
  1. Species-level records only  (taxonRank == 'species')
  2. Native species only         (excludes establishmentMeans == 'introduced')
  3. From 1970 onwards           (year >= 1970)
  4. Valid eventDate required
  5. Only species with 2+ records retained

Output columns: scientificName, vernacularName, class, order, family, eventDate
"""

from pathlib import Path
import pandas as pd

DIR         = Path(__file__).parent
INPUT_FILE  = DIR / 'Glenbog.csv'
OUTPUT_FILE = DIR / 'TimeDotGraph_Data.csv'
YEAR_FROM   = 1970

# Load
df = pd.read_csv(INPUT_FILE, encoding='latin1', low_memory=False)
print(f"[1] Loaded          {len(df):>5,} rows")

# Step 1: species-level only
before = len(df)
df = df[df['taxonRank'] == 'species']
print(f"[2] taxonRank=species  →  {len(df):>5,} rows  (removed {before - len(df):,})")

# Step 2: native species only
INTRODUCED_GENERA = ['Bos', 'Sus', 'Felis', 'Oryctolagus', 'Vulpes']
before = len(df)
if 'establishmentMeans' in df.columns:
    df = df[df['establishmentMeans'] != 'introduced']
else:
    df = df[~df['genus'].isin(INTRODUCED_GENERA)]
print(f"[3] Native only       →  {len(df):>5,} rows  (removed {before - len(df):,})")

# Step 3: 1970 onwards
before = len(df)
df = df[df['year'] >= YEAR_FROM]
print(f"[4] year >= {YEAR_FROM}    →  {len(df):>5,} rows  (removed {before - len(df):,})")

# Step 4: valid date
before = len(df)
df['eventDate'] = pd.to_datetime(df['eventDate'], errors='coerce', utc=True)
df = df.dropna(subset=['eventDate', 'scientificName'])
print(f"[5] Valid date/name   →  {len(df):>5,} rows  (removed {before - len(df):,})")

# Step 5: species with 2+ records
before = len(df)
counts = df.groupby('scientificName')['scientificName'].transform('count')
df = df[counts >= 2]
print(f"[6] 2+ records/spp    →  {len(df):>5,} rows  (removed {before - len(df):,})")
print(f"    → {df['scientificName'].nunique()} unique species remain")

# Select output columns only
df['eventDate'] = df['eventDate'].dt.strftime('%Y-%m-%d')
df = df[['scientificName', 'vernacularName', 'class', 'order', 'family', 'eventDate']]
df = df.sort_values(['class', 'order', 'scientificName', 'eventDate']).reset_index(drop=True)

df.to_csv(OUTPUT_FILE, index=False)
print(f"\n[7] Saved → {OUTPUT_FILE}")
print(f"    {len(df):,} rows  |  {df.shape[1]} columns  |  {df['scientificName'].nunique()} species")
