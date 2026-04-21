import pandas as pd
import re
from pathlib import Path

DIR = Path(__file__).parent
df = pd.read_csv(DIR / 'Glenbog.csv', encoding='cp1252')

def normalize_scientific_name(name):
    if pd.isna(name):
        return None
    name = str(name).strip()
    name = re.sub(r'\s*\([^)]*\)', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

df['scientificName_clean'] = df['scientificName'].apply(normalize_scientific_name)

# sey threatened species
status_data = {
    'Callocephalon fimbriatum': {
        'common_name': 'Gang-gang Cockatoo',
        'aus_status': 'Endangered',
        'nsw_status': 'Endangered'
    },
    'Petauroides volans': {
        'common_name': 'Southern Greater Glider',
        'aus_status': 'Endangered',
        'nsw_status': 'Endangered'
    },
    'Petaurus australis': {
        'common_name': 'Yellow-bellied Glider',
        'aus_status': 'Vulnerable',
        'nsw_status': 'Vulnerable'
    },
    'Ninox strenua': {
        'common_name': 'Powerful Owl',
        'aus_status': 'Not listed',
        'nsw_status': 'Vulnerable'
    },
    'Scoteanax rueppellii': {
        'common_name': 'Greater Broad-nosed Bat',
        'aus_status': 'Not listed',
        'nsw_status': 'Vulnerable'
    },
    'Tyto tenebricosa': {
        'common_name': 'Sooty Owl',
        'aus_status': 'Not listed',
        'nsw_status': 'Vulnerable'
    }
}

df['aus_status'] = ''
df['nsw_status'] = ''

for sp, info in status_data.items():
    mask = df['scientificName_clean'] == sp
    df.loc[mask, 'aus_status'] = info['aus_status']
    df.loc[mask, 'nsw_status'] = info['nsw_status']
#  Marked as sensitive (generalized GPS)
#  If dataGeneralizations is not empty, it is assumed that generalized GPS exists.

if 'dataGeneralizations' in df.columns:
    df['generalised_flag'] = df['dataGeneralizations'].notna() & (
        df['dataGeneralizations'].astype(str).str.strip() != ''
    )
else:
    df['generalised_flag'] = False

#  Mark habitat
#  Try to identify burrow / tree hollow from the habitat field.

def detect_habitat_flag(text):
    if pd.isna(text):
        return ''
    t = str(text).lower().strip()

    has_burrow = 'burrow' in t
    has_tree_hollow = ('tree hollow' in t) or ('treehollow' in t) or ('hollow' in t)

    if has_burrow and has_tree_hollow:
        return 'burrow; tree hollow'
    elif has_burrow:
        return 'burrow'
    elif has_tree_hollow:
        return 'tree hollow'
    else:
        return ''

if 'habitat' in df.columns:
    df['habitat_flag_record'] = df['habitat'].apply(detect_habitat_flag)
else:
    df['habitat_flag_record'] = ''

df['eventDate_parsed'] = pd.to_datetime(df['eventDate'], errors='coerce')

#  Generate species-level summary
#  One species per line

species_list = (
    df.groupby('scientificName_clean', dropna=False)
      .agg(
          class_name=('class', lambda x: x.dropna().astype(str).iloc[0] if x.dropna().shape[0] > 0 else ''),
          order_name=('order', lambda x: x.dropna().astype(str).iloc[0] if x.dropna().shape[0] > 0 else ''),
          scientificName=('scientificName_clean', lambda x: x.dropna().astype(str).iloc[0] if x.dropna().shape[0] > 0 else ''),
          vernacularName=('vernacularName', lambda x: x.dropna().astype(str).iloc[0] if x.dropna().shape[0] > 0 else ''),
          number_of_observations=('scientificName_clean', 'size'),
          most_recent_observation=('eventDate_parsed', 'max'),
          aus_status=('aus_status', lambda x: x[x != ''].iloc[0] if (x != '').any() else ''),
          nsw_status=('nsw_status', lambda x: x[x != ''].iloc[0] if (x != '').any() else ''),
          sensitive_generalised_gps=('generalised_flag', 'max'),
          habitat_flag=('habitat_flag_record', lambda x: '; '.join(sorted(set([i for i in x if str(i).strip() != ''])))),
          decimalLatitude=('decimalLatitude', 'mean'),
          decimalLongitude=('decimalLongitude', 'mean'),
      )
      .reset_index(drop=True)
)

def combine_risk_status(row):
    status_parts = []

    if row['aus_status'] in ['Endangered', 'Vulnerable']:
        status_parts.append(f'Aus: {row["aus_status"]}')
    if row['nsw_status'] in ['Endangered', 'Vulnerable']:
        status_parts.append(f'NSW: {row["nsw_status"]}')
    if row['sensitive_generalised_gps']:
        status_parts.append('Sensitive (generalised GPS)')
    if row['habitat_flag']:
        status_parts.append(f'Habitat: {row["habitat_flag"]}')

    return '; '.join(status_parts)

species_list['at_risk_status'] = species_list.apply(combine_risk_status, axis=1)
species_list['most_recent_observation'] = species_list['most_recent_observation'].dt.strftime('%Y-%m-%d')
species_list['sensitive_generalised_gps'] = species_list['sensitive_generalised_gps'].map({True: 'Yes', False: 'No'})

#  Sort by class, order & common name
species_list = species_list.sort_values(
    by=['class_name', 'order_name', 'vernacularName'],
    ascending=[True, True, True]
)

species_list = species_list[species_list['at_risk_status'].str.strip() != '']

final_species_list = species_list[
    [
        'class_name',
        'order_name',
        'scientificName',
        'vernacularName',
        'number_of_observations',
        'most_recent_observation',
        'aus_status',
        'nsw_status',
        'sensitive_generalised_gps',
        'habitat_flag',
        'at_risk_status',
        'decimalLatitude',
        'decimalLongitude',
    ]
].rename(columns={
    'class_name': 'class',
    'order_name': 'order'
})

print(final_species_list.head(20))
print('\nTotal species:', len(final_species_list))

final_species_list.to_csv(
    DIR / 'At_Risk_Species.csv',
    index=False,
    encoding='utf-8'
)

print('\nExported：At_Risk_Species.csv')