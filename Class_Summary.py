"""Slide 6 — class breakdown (Mammalia split by order) → class_summary.csv"""

import re
from pathlib import Path
import pandas as pd

_DIR = Path(__file__).parent
INPUT_FILE = _DIR / "Glenbog.csv"

CLASS_DESCRIPTIONS = {
    "Actinopterygii":         "fish",
    "Amphibia":               "frogs",
    "Arachnida":              "spiders",
    "Aves":                   "birds",
    "Equisetopsida":          "plants",
    "Insecta":                "insects",
    "Lecanoromycetes":        "fungi",
    "Reptilia":               "lizards / reptiles",
    "Mammalia – Chiroptera":  "bats",
    "Mammalia – Monotremata": "echidnas",
    "Mammalia – other":       "other mammals",
}

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

class_summary = (
    df.groupby("class_display")
    .agg(
        num_observations=("scientificName", "count"),
        num_species     =("scientificName", "nunique"),
    )
    .reset_index()
    .sort_values("class_display")
)
class_summary["class_description"] = (
    class_summary["class_display"].map(CLASS_DESCRIPTIONS).fillna("")
)
totals = pd.DataFrame([{
    "class_display":     "Grand Total",
    "num_observations":  class_summary["num_observations"].sum(),
    "num_species":       class_summary["num_species"].sum(),
    "class_description": "",
}])
class_summary = pd.concat([class_summary, totals], ignore_index=True)
class_summary.to_csv(_DIR / "class_summary.csv", index=False)

print(f"Saved  Class_Summary.csv")
print(class_summary.to_string(index=False))
