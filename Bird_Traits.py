"""Merge Glenbog bird observations with BIRDBASE traits → Bird_Traits.csv"""

import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent

NEST_TYPE_LABELS = {
    'NestType_BU': 'Burrow',
    'NestType_CP': 'Cup / bowl',
    'NestType_CR': 'Crevice',
    'NestType_CV': 'Cavity (tree)',
    'NestType_DM': 'Dome / oven',
    'NestType_HC': 'Half cup / shallow',
    'NestType_NO': 'No nest',
    'NestType_O': "Other bird's nest",
    'NestType_PL': 'Platform',
    'NestType_PN': 'Pendant / bag / purse',
    'NestType_SA': 'Saucer',
    'NestType_SC': 'Scrape',
    'NestType_SP': 'Sphere / globular',
    'NestType_M': 'Mound',
}

NEST_SITE_LABELS = {
    'NestSBS_A': 'Bamboo',
    'NestSBS_B': 'Building',
    'NestSBS_C': 'Stump',
    'NestSBS_G': 'Ground',
    'NestSBS_K': 'Cactus',
    'NestSBS_N': 'Nest',
    'NestSBS_P': 'Pole',
    'NestSBS_R': 'Rock',
    'NestSBS_S': 'Shrub / bush / vine',
    'NestSBS_T': 'Tree',
    'NestSBS_W': 'Water',
    'NestSBS_Z': 'Grass',
}


def join_active_labels(row, labels):
    active = []
    for column, label in labels.items():
        value = row.get(column)
        if pd.notna(value) and value == 1:
            active.append(label)
    return ', '.join(active) if active else None

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

nest = pd.read_excel(DIR / 'BIRDBASE v2025.1 Sekercioglu et al. Final.xlsx', sheet_name='Nest Details')
nest['nest_type'] = nest.apply(lambda row: join_active_labels(row, NEST_TYPE_LABELS), axis=1)
nest['nest_site'] = nest.apply(lambda row: join_active_labels(row, NEST_SITE_LABELS), axis=1)
nest = nest.rename(columns={'Latin.Name': 'scientific_name'})
nest = nest[['scientific_name', 'nest_type', 'nest_site']]

# ── Merge ────────────────────────────────────────────────────────────────────
traits = (
    obs.merge(bb, on='scientific_name', how='left')
    .merge(nest, on='scientific_name', how='left')
)
traits.to_csv(DIR / 'Bird_Traits.csv', index=False)

print(f'Saved Bird_Traits.csv — {len(traits)} species')
print(traits[['scientific_name', 'common_name', 'iucn_status', 'primary_habitat', 'primary_diet', 'average_mass_g', 'nest_type', 'nest_site']].to_string(index=False))
