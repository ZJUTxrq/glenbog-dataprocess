"""
Data cleaning script for Glenbog ALA (Atlas of Living Australia) occurrence data.
Follows analysis requirements from "Saving Glenbog State Forest.pptx":
  - Slide 6: All Species Summary (class breakdown, Mammalia split by order)
  - Slide 7: Key Species (per-species obs count + most recent date)
  - Slide 8: At-Risk Species (sensitive/threatened species with status)
  - Slide 9: Location data for maps
"""

import csv
import pandas as pd
from datetime import datetime

INPUT_FILE = "Glenbog.csv"

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
df = pd.read_csv(INPUT_FILE, encoding="latin-1", low_memory=False)
print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

# ---------------------------------------------------------------------------
# Select relevant columns
# ---------------------------------------------------------------------------
KEEP_COLS = [
    # Identification
    "scientificName", "vernacularName",
    # Taxonomy
    "kingdom", "phylum", "class", "order", "family", "genus",
    # Observation
    "eventDate", "year", "month", "day",
    "decimalLatitude", "decimalLongitude",
    # Data source
    "dataResourceName",
    # Sensitivity / at-risk flag
    "dataGeneralizations",
    # Habitat notes
    "habitat", "occurrenceRemarks",
    # Basis
    "basisOfRecord",
    "individualCount",
]
df = df[[c for c in KEEP_COLS if c in df.columns]].copy()

# ---------------------------------------------------------------------------
# Parse dates
# ---------------------------------------------------------------------------
df["eventDate"] = pd.to_datetime(df["eventDate"], errors="coerce", utc=True)
df["eventDate"] = df["eventDate"].dt.tz_localize(None)   # drop tz for simplicity

# ---------------------------------------------------------------------------
# Derive: class_display — split Mammalia into sub-groups per PPT slide 6
# ---------------------------------------------------------------------------
CLASS_DESCRIPTIONS = {
    "Actinopterygii":   "fish",
    "Amphibia":         "frogs",
    "Arachnida":        "spiders",
    "Aves":             "birds",
    "Equisetopsida":    "plants",
    "Insecta":          "insects",
    "Lecanoromycetes":  "fungi",
    "Reptilia":         "lizards/reptiles",
    # Mammalia sub-groups assigned below
    "Mammalia – Chiroptera":  "bats",
    "Mammalia – Monotremata": "echidnas",
    "Mammalia – other":       "other mammals",
}

def class_display(row):
    if row["class"] == "Mammalia":
        order = str(row.get("order", "")).strip()
        if order == "Chiroptera":
            return "Mammalia – Chiroptera"
        elif order == "Monotremata":
            return "Mammalia – Monotremata"
        else:
            return "Mammalia – other"
    return row["class"]

df["class_display"] = df.apply(class_display, axis=1)
df["class_description"] = df["class_display"].map(CLASS_DESCRIPTIONS).fillna("")

# ---------------------------------------------------------------------------
# Derive: at_risk_status  (Slide 8)
# "sensitive" = record has dataGeneralizations (generalised GPS by NSW OEH)
# ---------------------------------------------------------------------------
def at_risk_status(row):
    dg = str(row.get("dataGeneralizations", ""))
    if "generalised" in dg.lower() or "generalis" in dg.lower():
        return "Sensitive (generalised GPS)"
    return ""

df["at_risk_status"] = df.apply(at_risk_status, axis=1)

# ---------------------------------------------------------------------------
# Derive: habitat_type  (Slide 8 — burrow / tree hollow)
# ---------------------------------------------------------------------------
def habitat_type(row):
    combined = " ".join([
        str(row.get("habitat", "")),
        str(row.get("occurrenceRemarks", "")),
    ]).lower()
    tags = []
    if "hollow" in combined:
        tags.append("tree hollow")
    if "burrow" in combined:
        tags.append("burrow")
    return "; ".join(tags)

df["habitat_type"] = df.apply(habitat_type, axis=1)

# ---------------------------------------------------------------------------
# Output 1: cleaned_observations.csv  — full, relevant columns
# ---------------------------------------------------------------------------
df.to_csv("cleaned_observations.csv", index=False)
print(f"Saved cleaned_observations.csv  ({len(df):,} rows)")

# ---------------------------------------------------------------------------
# Output 2: species_summary.csv  (Slide 7 – Key Species)
# Scientific name, common name, class, order, # observations, most recent date
# Sorted by class_display then vernacularName
# ---------------------------------------------------------------------------
species_summary = (
    df.groupby(["class_display", "order", "scientificName", "vernacularName"])
    .agg(
        num_observations=("scientificName", "count"),
        most_recent_date=("eventDate", "max"),
    )
    .reset_index()
    .sort_values(["class_display", "vernacularName"])
)
species_summary["most_recent_date"] = species_summary["most_recent_date"].dt.strftime("%Y-%m-%d")
species_summary.to_csv("species_summary.csv", index=False)
print(f"Saved species_summary.csv  ({len(species_summary):,} species)")

# ---------------------------------------------------------------------------
# Output 3: class_summary.csv  (Slide 6 – All Species Summary)
# Total species & observations per class_display; sorted by class_display
# ---------------------------------------------------------------------------
class_summary = (
    df.groupby(["class_display"])
    .agg(
        num_observations=("scientificName", "count"),
        num_species=("scientificName", "nunique"),
    )
    .reset_index()
    .sort_values("class_display")
)
class_summary["class_description"] = class_summary["class_display"].map(CLASS_DESCRIPTIONS).fillna("")

# Totals row
totals = pd.DataFrame([{
    "class_display": "Grand Total",
    "num_observations": class_summary["num_observations"].sum(),
    "num_species": class_summary["num_species"].sum(),
    "class_description": "",
}])
class_summary = pd.concat([class_summary, totals], ignore_index=True)

class_summary.to_csv("class_summary.csv", index=False)
print(f"Saved class_summary.csv")
print(class_summary.to_string(index=False))

# ---------------------------------------------------------------------------
# Output 4: at_risk_species.csv  (Slide 8 – At-Risk Species)
# Only species flagged sensitive; show name, obs, most recent date, status, habitat
# Sorted by class_display, order, vernacularName
# ---------------------------------------------------------------------------
at_risk_df = df[df["at_risk_status"] != ""].copy()

at_risk_summary = (
    at_risk_df.groupby([
        "class_display", "order", "scientificName", "vernacularName", "at_risk_status"
    ])
    .agg(
        num_observations=("scientificName", "count"),
        most_recent_date=("eventDate", "max"),
        habitat_types=("habitat_type", lambda x: "; ".join(sorted(set(v for v in x if v)))),
    )
    .reset_index()
    .sort_values(["class_display", "order", "vernacularName"])
)
at_risk_summary["most_recent_date"] = at_risk_summary["most_recent_date"].dt.strftime("%Y-%m-%d")
at_risk_summary.to_csv("at_risk_species.csv", index=False)
print(f"\nSaved at_risk_species.csv  ({len(at_risk_summary):,} at-risk species)")
print(at_risk_summary[["scientificName", "vernacularName", "at_risk_status", "num_observations", "most_recent_date"]].to_string(index=False))

# ---------------------------------------------------------------------------
# Output 5: location_data.csv  (Slide 9 – Maps)
# Records with coordinates, flagging at-risk species
# ---------------------------------------------------------------------------
loc_df = df[df["decimalLatitude"].notna() & df["decimalLongitude"].notna()].copy()
loc_df["is_at_risk"] = loc_df["at_risk_status"] != ""
loc_df[[
    "scientificName", "vernacularName", "class_display", "order",
    "decimalLatitude", "decimalLongitude",
    "eventDate", "dataResourceName",
    "at_risk_status", "is_at_risk",
]].to_csv("location_data.csv", index=False)
print(f"\nSaved location_data.csv  ({len(loc_df):,} georeferenced records)")
