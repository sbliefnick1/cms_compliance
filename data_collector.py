import pandas as pd
import requests
import sqlalchemy as sa
from requests_html import HTMLSession

from config import connection_string

resource_url = 'https://openpaymentsdata.cms.gov/developers'
headers = {'X-App-Token': 'Nta0Tts7rzXfqkRjpeiwOMwz6'}

# read in crosswalk of NPI ID and physician profile ID
crosswalk = pd.read_csv('phys_id_crosswalk.csv', header=0, usecols=['npi', 'physician_master_profile_id'])
crosswalk.columns = ['npi', 'physician_profile_id']
crosswalk = crosswalk.drop_duplicates()

# get resource url page
session = HTMLSession()
r = session.get(resource_url, headers=headers)
r.html.render()

# finds all table rows
rows = r.html.find('tr')

# zeroth tr contains nothing, so start with first
# td 0: dataset name
# td 2: API endpoint
resource_urls = [row.find('td')[2].full_text for row in rows[1:] if
                 row.find('td')[0].full_text.startswith('General Payment Data')]

# loop over resources (years) and physicians and collect all their data
df = pd.DataFrame()
for resource in resource_urls:
    for phys in crosswalk.physician_profile_id:
        url = f'{resource}?physician_profile_id={phys}'
        response = requests.get(url, headers=headers).json()
        if response:
            temp = pd.DataFrame.from_dict(response)
            df = pd.concat([df, temp], ignore_index=True, sort=False)

# merge with npi id so we can join on it
df['physician_profile_id'] = df['physician_profile_id'].apply(pd.to_numeric)
df = pd.merge(df, crosswalk, how='left', on='physician_profile_id')

# clean up data
str_cols = ['ndc_of_associated_covered_drug_or_biological1',
            'ndc_of_associated_covered_drug_or_biological2',
            'ndc_of_associated_covered_drug_or_biological3',
            'ndc_of_associated_covered_drug_or_biological4',
            'ndc_of_associated_covered_drug_or_biological5',
            'associated_drug_or_biological_ndc_1',
            'associated_drug_or_biological_ndc_2',
            'associated_drug_or_biological_ndc_3',
            'associated_drug_or_biological_ndc_4',
            'associated_drug_or_biological_ndc_5'
            ]

num_cols = ['physician_profile_id',
            'applicable_manufacturer_or_applicable_gpo_making_payment_id',
            'number_of_payments_included_in_total_amount',
            'total_amount_of_payment_usdollars',
            'record_id',
            'program_year',
            'npi'
            ] + str_cols

date_cols = ['date_of_payment',
             'payment_publication_date'
             ]

for col in str_cols:
    df[col] = df[col].str.replace('-', '')

df[num_cols] = df[num_cols].apply(pd.to_numeric)
df[date_cols] = df[date_cols].apply(pd.to_datetime)

# insert to db
df.set_index('record_id', inplace=True)

engine = sa.create_engine(connection_string)
df.to_sql('EBI_CMS_Compliance', engine, if_exists='replace')
