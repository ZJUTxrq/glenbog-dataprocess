import re
import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent
INPUT_FILE = DIR / "Glenbog.csv"

# Key species of interest for Glenbog State Forest
KEY_SPECIES = {
    "Eopsaltria australis",
    "Petroica phoenicea",
    "Callocephalon fimbriatum",
    "Ninox strenua",
    "Climacteris erythrops",
    "Tyto tenebricosa",
    "Cormobates leucophaea",
    "Vombatus ursinus",
    "Petauroides volans",
    "Petaurus breviceps",
    "Petaurus australis",
}

df = pd.read_csv(INPUT_FILE, encoding="latin-1", low_memory=False)

# Clean scientific name: remove parenthetical qualifiers
df["scientificName_clean"] = (
    df["scientificName"]
    .astype(str)
    .apply(lambda n: re.sub(r"\s*\([^)]*\)", "", n).strip())
)

# Filter to key species only
df = df[df["scientificName_clean"].isin(KEY_SPECIES)].copy()

# Parse eventDate
df["eventDate"] = pd.to_datetime(df["eventDate"], errors="coerce", utc=True)
df["eventDate"] = df["eventDate"].dt.tz_localize(None)

# Build class display label (split Mammalia by order)
def class_display(row):
    if row.get("class") == "Mammalia":
        o = str(row.get("order", "")).strip()
        if o == "Chiroptera":
            return "Mammalia – Chiroptera"
        elif o == "Monotremata":
            return "Mammalia – Monotremata"
        else:
            return "Mammalia – other"
    return row.get("class", "")

df["class_display"] = df.apply(class_display, axis=1)

# Aggregate: observation count and most recent date per species
summary = (
    df.groupby(["class_display", "vernacularName", "scientificName_clean"])
    .agg(
        num_observations=("scientificName_clean", "count"),
        most_recent_date=("eventDate", "max"),
    )
    .reset_index()
    .sort_values(["class_display", "vernacularName"])
)

summary["most_recent_date"] = summary["most_recent_date"].dt.strftime("%Y-%m-%d")

summary = summary.rename(columns={
    "class_display":       "class",
    "vernacularName":      "common_name",
    "scientificName_clean": "scientific_name",
})

summary = summary[["class", "common_name", "scientific_name",
                   "num_observations", "most_recent_date"]]

summary.to_csv(DIR / "Key_Species.csv", index=False)
print(f"Saved Key_Species.csv  ({len(summary)} species)")
print(summary.to_string(index=False))
