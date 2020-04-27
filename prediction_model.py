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
df_merge_global = df_confirmed.merge(df_deaths, on=['Province/State', 'Country/Region', 'Lat', 'Long', 'Date'])
df_merge_us = df_confirmed_us.merge(df_deaths_us, on=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Date'])

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
split_date = df_merge_global.Date.max() - datetime.timedelta(7)

# %%
df_train_global = df_merge_global[df_merge_global['Date'] < split_date]
df_test_global = df_merge_global[df_merge_global['Date'] >= split_date]

df_train_us = df_merge_us[df_merge_us['Date'] < split_date]
df_test_us = df_merge_us[df_merge_us['Date'] >= split_date]

# %%
