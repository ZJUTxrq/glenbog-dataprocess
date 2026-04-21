import re
import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent
INPUT_FILE = DIR / "Glenbog.csv"

KEEP = [
    "scientificName", "vernacularName",
    "kingdom", "phylum", "class", "order", "family", "genus",
    "eventDate", "year", "month", "day",
    "decimalLatitude", "decimalLongitude",
    "dataResourceName", "dataGeneralizations",
    "habitat", "occurrenceRemarks",
    "basisOfRecord", "individualCount",
]

df = pd.read_csv(INPUT_FILE, encoding="latin-1", low_memory=False)
df = df[[c for c in KEEP if c in df.columns]].copy()

df["scientificName_clean"] = df["scientificName"].str.replace(
    r"\s*\([^)]+\)\s*", " ", regex=True
).str.strip()

df["eventDate"] = pd.to_datetime(df["eventDate"], errors="coerce", utc=True)
df["eventDate"] = df["eventDate"].dt.tz_localize(None)

mask_missing = df["eventDate"].isna()
if mask_missing.any():
    df.loc[mask_missing, "month"] = df.loc[mask_missing, "month"].fillna(6)
    df.loc[mask_missing, "day"]   = df.loc[mask_missing, "day"].fillna(15)
    filled = pd.to_datetime(
        df.loc[mask_missing, ["year", "month", "day"]],
        errors="coerce"
    )
    df.loc[mask_missing, "eventDate"] = filled

def class_display(row):
    if row["class"] == "Mammalia":
        o = str(row.get("order", "")).strip()
        if o == "Chiroptera":
            return "Mammalia – Chiroptera"
        elif o == "Monotremata":
            return "Mammalia – Monotremata"
        else:
            return "Mammalia – other"
    return row["class"]

df["class_display"] = df.apply(class_display, axis=1)

species_summary = (
    df.groupby(["class_display", "order", "scientificName_clean", "vernacularName"])
    .agg(
        num_observations=("scientificName", "count"),
        most_recent_date=("eventDate",      "max"),
    )
    .reset_index()
    .sort_values(["class_display", "vernacularName"])
)
species_summary["most_recent_date"] = (
    species_summary["most_recent_date"].dt.strftime("%Y-%m-%d")
)
species_summary.to_csv(DIR / "Species_Summary.csv", index=False)
print(f"Saved  Species_Summary.csv   ({len(species_summary):,} species)")
