import pandas as pd
from pathlib import Path

DIR = Path(__file__).parent
df = pd.read_csv(DIR / 'Glenbog.csv')

order_summary = df.groupby(['order', 'class']).agg(
    total_species=('scientificName', 'nunique'),
    total_observations=('scientificName', 'count') 
).reset_index()


total_obs_sum = order_summary['total_observations'].sum()
order_summary['proportion'] = (order_summary['total_observations'] / total_obs_sum * 100).round(1)
def get_common_name(group):
    names = group['vernacularName'].dropna()
    return names.mode()[0] if not names.empty else "Other Species"


desc_mapping = df.groupby('order').apply(get_common_name).to_dict()
order_summary['order_description'] = order_summary['order'].map(desc_mapping)

order_summary = order_summary.sort_values(by='total_observations', ascending=False)

order_summary.to_csv(DIR / 'Order_Summary.csv', index=False)

