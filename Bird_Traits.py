"""Merge Glenbog bird observations with BIRDBASE traits → Bird_Traits.csv"""

import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent

# ── Glenbog observation stats ────────────────────────────────────────────────
df = pd.read_csv(DIR / 'Glenbog.csv', low_memory=False)
birds = df[df['class'] == 'Aves'].copy()
birds['eventDate'] = pd.to_datetime(birds['eventDate'], errors='coerce')
birds = birds[birds['eventDate'] > '1901-01-01']

def most_common(s):
    s = s.dropna()
    return s.mode()[0] if not s.empty else None

obs = birds.groupby('species').apply(lambda g: pd.Series({
    'common_name':      most_common(g['vernacularName']),
    'most_recent_date': g['eventDate'].max().strftime('%Y-%m-%d') if not g['eventDate'].isna().all() else None,
})).reset_index().rename(columns={'species': 'scientific_name'})

# ── BIRDBASE traits ──────────────────────────────────────────────────────────
bb = pd.read_excel(DIR / 'BIRDBASE v2025.1 Sekercioglu et al. Final.xlsx', header=1)
bb = bb.rename(columns={
    'Latin (BirdLife > IOC > Clements>AviList)': 'scientific_name',
    '2024 IUCN Red List category':               'iucn_status',
    'Average Mass':                               'average_mass_g',
    'Primary Habitat':                            'primary_habitat',
    'Primary Diet':                               'primary_diet',
    'Mig':                                        'migratory',
})
bb = bb[['scientific_name', 'iucn_status', 'average_mass_g', 'primary_habitat', 'primary_diet', 'migratory']]

# ── Merge ────────────────────────────────────────────────────────────────────
traits = obs.merge(bb, on='scientific_name', how='left')
traits.to_csv(DIR / 'Bird_Traits.csv', index=False)

print(f'Saved Bird_Traits.csv — {len(traits)} species')
print(traits[['scientific_name', 'common_name', 'iucn_status', 'primary_habitat', 'primary_diet', 'average_mass_g']].to_string(index=False))
