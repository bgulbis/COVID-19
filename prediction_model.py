# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import datetime

# %%
df_confirmed = pd.read_csv('csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

df_confirmed = df_confirmed.melt(
    id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
    var_name='Date', 
    value_name='Confirmed'
)

df_confirmed['Date'] = pd.to_datetime(df_confirmed['Date'], format='%m/%d/%y')

# %%
df_confirmed_us = pd.read_csv('csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv')

df_confirmed_us = df_confirmed_us.drop(['UID', 'iso2', 'iso3', 'code3', 'Combined_Key'], axis=1)

df_confirmed_us = df_confirmed_us.melt(
    id_vars=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_'], 
    var_name='Date', 
    value_name='Confirmed'
)

df_confirmed_us['Date'] = pd.to_datetime(df_confirmed_us['Date'], format='%m/%d/%y')

# %%
df_deaths = pd.read_csv('csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')

df_deaths = df_deaths.melt(
    id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
    var_name='Date', 
    value_name='Deaths'
)

df_deaths['Date'] = pd.to_datetime(df_deaths['Date'], format='%m/%d/%y')

# %%
df_deaths_us = pd.read_csv('csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv')

df_deaths_us = df_deaths_us.drop(['UID', 'iso2', 'iso3', 'code3', 'Combined_Key'], axis=1)

df_deaths_us = df_deaths_us.melt(
    id_vars=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Population'], 
    var_name='Date', 
    value_name='Deaths'
)

df_deaths_us['Date'] = pd.to_datetime(df_deaths_us['Date'], format='%m/%d/%y')

# %%
df_merge_global = df_confirmed.merge(
    df_deaths, 
    on=['Province/State', 'Country/Region', 'Lat', 'Long', 'Date']
).sort_values(['Country/Region', 'Province/State', 'Date'])

df_merge_us = df_confirmed_us.merge(
    df_deaths_us, 
    on=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Date']
).sort_values(['Country_Region', 'Province_State', 'Admin2', 'Date'])

df_merge_global['Place'] = (
    df_merge_global['Country/Region'] + \
    df_merge_global['Province/State'].fillna("")
)

df_merge_us['Place'] = (
    df_merge_us['Country_Region'] + \
    df_merge_us['Province_State'].fillna("") + \
    df_merge_us['Admin2'].fillna("")
)

# %%
nm = 'Confirmed'
x = df_merge_global[nm]
place = df_merge_global['Place']

df_merge_global[nm] = np.where(
    (x.shift(1) > x) &
        (x.shift(1) > 0) &
        (x.shift(-1) > 0) &
        (place == place.shift(1)) &
        (place == place.shift(-1)) &
        ~place.shift(-1).isnull(),
    np.sqrt(x.shift(1) * x.shift(-1)),
    x
)

for i in [0, -1]:
    df_merge_global[nm] = np.where(
        (x.shift(2 + i) > x) &
            (x.shift(2 + i) > 0) &
            (x.shift(-1 + i) > 0) &
            (place == place.shift(2 + i)) &
            (place == place.shift(-1 + i)) &
            ~place.shift(-1 + i).isnull(),
        np.sqrt(x.shift(2 + i) * x.shift(-1 + i)),
        x
    )

# %%
nm = 'Deaths'
x = df_merge_global[nm]

df_merge_global[nm] = np.where(
    (x.shift(1) > x) &
        (x.shift(1) > 0) &
        (x.shift(-1) > 0) &
        (place == place.shift(1)) &
        (place == place.shift(-1)) &
        ~place.shift(-1).isnull(),
    np.sqrt(x.shift(1) * x.shift(-1)),
    x
)

# %%
dataset = df_merge_global.copy()

# %%
def rollDates(df, i, preserve=False):
    df = df.copy()
    if preserve:
        df['Date_i'] = df.Date
    df.Date = df.Date + datetime.timedelta(i)
    return df

# %%
WINDOWS = [1, 2, 4, 7, 12, 20, 30]

for window in WINDOWS:
    csuffix = '_{}d_prior_value'.format(window)
    
    base = rollDates(dataset, window)
    dataset = dataset.merge(
        base[['Date', 'Place', 'Confirmed', 'Deaths']], 
        on=['Date', 'Place'],
        suffixes=('', csuffix), 
        how='left')

    for c in ['Confirmed', 'Deaths']:
        dataset[c + csuffix].fillna(0, inplace=True)
        dataset[c + csuffix] = np.log(dataset[c + csuffix] + 1)
        dataset[c + '_{}d_prior_slope'.format(window)] = \
                    (np.log(dataset[c] + 1) \
                         - dataset[c+ csuffix]) / window
        dataset[c + '_{}d_ago_zero'.format(window)] = 1.0*(dataset[c + csuffix] == 0)     

# %%
# https://www.bsg.ox.ac.uk/research/research-projects/coronavirus-government-response-tracker
df_oxcgrt = pd.read_csv('extra_data/OxCGRT_Download_260420_182343_Full.csv')

# %%
df_oxcgrt_select = df_oxcgrt[[
    c for c in df_oxcgrt.columns if not any(
        z in c for z in [
            '_Notes',
            'Unnamed',
            'Confirmed',
            'CountryCode',
            'S8',
            'S9',
            'S10',
            'S11',
            'StringencyIndexForDisplay'
        ]
    )
]]

df_oxcgrt_select = df_oxcgrt_select.rename(columns={'CountryName': 'Country'})

df_oxcgrt_select['Date'] = df_oxcgrt_select['Date'].astype(str).apply(datetime.datetime.strptime, args=('%Y%m%d', ))
# %%
cds = []
for country in df_oxcgrt_select['Country'].unique():
    cd = df_oxcgrt_select[df_oxcgrt_select['Country'] == country]
    cd = cd.fillna(method = 'ffill').fillna(0)
    cd['StringencyIndex'] = cd['StringencyIndex'].cummax()  # for now
    col_count = cd.shape[1]
    
    # now do a diff columns
    # and ewms of it
    for col in [c for c in df_oxcgrt_select.columns if 'S' in c]:
        col_diff = cd[col].diff()
        cd[col+"_chg_5d_ewm"] = col_diff.ewm(span = 5).mean()
        cd[col+"_chg_20_ewm"] = col_diff.ewm(span = 20).mean()
        
    # stringency
    cd['StringencyIndex_5d_ewm'] = cd['StringencyIndex'].ewm(span = 5).mean()
    cd['StringencyIndex_20d_ewm'] = cd['StringencyIndex'].ewm(span = 20).mean()
    
    cd['S_data_days'] = (cd['Date'] - cd['Date'].min()).dt.days
    for s in [1, 10, 20, 30, 50, ]:
        cd['days_since_Stringency_{}'.format(s)] = np.clip((cd['Date'] - cd[(cd['StringencyIndex'] > s)].Date.min()).dt.days, 0, None)
        
    cds.append(cd.fillna(0)[['Country', 'Date'] + cd.columns.to_list()[col_count:]])

df_oxcgrt_select = pd.concat(cds)

# %%
df_oxcgrt_select['Country'] = df_oxcgrt_select['Country'].replace(
    {
        'United States': "US",
        'South Korea': "Korea, South",
        'Taiwan': "Taiwan*",
        'Myanmar': "Burma", 
        'Slovak Republic': "Slovakia",
        'Czech Republic': 'Czechia',
    }
)

# %%
sup_data = pd.read_excel('outside_data/Data Join - Copy1.xlsx')

# %%
sup_data.columns = [c.replace(' ', '_') for c in sup_data.columns.to_list()]
sup_data.drop(columns = [c for c in sup_data.columns.to_list() if 'Unnamed:' in c], inplace=True)
sup_data.drop(columns = [ 'Date', 'ConfirmedCases',
       'Fatalities', 'log-cases', 'log-fatalities', 'continent'], inplace=True)
sup_data['Migrants_in'] = np.clip(sup_data.Migrants, 0, None)
sup_data['Migrants_out'] = -np.clip(sup_data.Migrants, None, 0)
sup_data.drop(columns = 'Migrants', inplace=True)
sup_data.rename(columns={'Country_Region': 'Country'}, inplace=True)
sup_data['Place'] = sup_data.Country +  sup_data.Province_State.fillna("")
sup_data = sup_data[sup_data.columns.to_list()[2:]]
sup_data = sup_data.replace('N.A.', np.nan).fillna(-0.5)

for c in sup_data.columns[:-1]:
    m = sup_data[c].max() #- sup_data 
    
    if m > 300 and c!='TRUE_POPULATION':
        print(c)
        sup_data[c] = np.log(sup_data[c] + 1)
        assert sup_data[c].min() > -1

for c in sup_data.columns[:-1]:
    m = sup_data[c].max() #- sup_data 
    
    if m > 300:
        print(c)

# %%
split_date = df_merge_global.Date.max() - datetime.timedelta(7)

# %%
df_train_global = df_merge_global[df_merge_global['Date'] < split_date]
df_test_global = df_merge_global[df_merge_global['Date'] >= split_date]

df_train_us = df_merge_us[df_merge_us['Date'] < split_date]
df_test_us = df_merge_us[df_merge_us['Date'] >= split_date]

# %%
