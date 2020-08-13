# %%
import pandas as pd
import os

# %%
ts_path = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
# %%
print('Getting latest data')

df_confirmed = pd.read_csv(ts_path + 'time_series_covid19_confirmed_global.csv')

df_confirmed = df_confirmed.melt(
    id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
    var_name='Date', 
    value_name='Confirmed'
)

df_confirmed['Date'] = pd.to_datetime(df_confirmed['Date'], format='%m/%d/%y')

# %%
df_confirmed_us = pd.read_csv(ts_path + 'time_series_covid19_confirmed_US.csv')

df_confirmed_us = df_confirmed_us.drop(['UID', 'iso2', 'iso3', 'code3', 'Combined_Key'], axis=1)

df_confirmed_us = df_confirmed_us.melt(
    id_vars=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_'], 
    var_name='Date', 
    value_name='Confirmed'
)

df_confirmed_us = df_confirmed_us[~df_confirmed_us['Date'].str.contains('.1', regex=False)]
df_confirmed_us['Date'] = pd.to_datetime(df_confirmed_us['Date'], format='%m/%d/%y')

# %%
df_deaths = pd.read_csv(ts_path + 'time_series_covid19_deaths_global.csv')

df_deaths = df_deaths.melt(
    id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
    var_name='Date', 
    value_name='Deaths'
)

df_deaths['Date'] = pd.to_datetime(df_deaths['Date'], format='%m/%d/%y')

# %%
df_deaths_us = pd.read_csv(ts_path + 'time_series_covid19_deaths_US.csv')

df_deaths_us = df_deaths_us.drop(['UID', 'iso2', 'iso3', 'code3', 'Combined_Key'], axis=1)

df_deaths_us = df_deaths_us.melt(
    id_vars=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Population'], 
    var_name='Date', 
    value_name='Deaths'
)

df_deaths_us = df_deaths_us[~df_deaths_us['Date'].str.contains('.1', regex=False)]
df_deaths_us['Date'] = pd.to_datetime(df_deaths_us['Date'], format='%m/%d/%y')

# %%
# df_recovered = pd.read_csv(ts_path + 'time_series_covid19_recovered_global.csv')

# df_recovered = df_recovered.melt(
#     id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
#     var_name='Date', 
#     value_name='Recovered'
# )

# df_recovered['Date'] = pd.to_datetime(df_recovered['Date'], format='%m/%d/%y')

# %%
df_merge_global = df_confirmed.merge(
    df_deaths, 
    on=['Province/State', 'Country/Region', 'Lat', 'Long', 'Date']
).sort_values(['Country/Region', 'Province/State', 'Date'])
# ).merge(
#     df_recovered,
#     on=['Province/State', 'Country/Region', 'Lat', 'Long', 'Date']

# df_merge_global['Active'] = df_merge_global['Confirmed'] - df_merge_global['Deaths'] - df_merge_global['Recovered']

df_merge_global = df_merge_global.rename(columns={"Province/State": "State", "Country/Region": "Country"})

df_merge_global = df_merge_global[df_merge_global['Country'] != 'US']

df_merge_us = df_confirmed_us.merge(
    df_deaths_us, 
    on=['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Lat', 'Long_', 'Date']
).sort_values(['Country_Region', 'Province_State', 'Admin2', 'Date'])

# df_merge_us['FIPS'] = df_merge_us['FIPS'].astype('Int64')
df_merge_us = df_merge_us.drop(columns='FIPS')

df_merge_us = df_merge_us.rename(
    columns={"Admin2": "County", "Province_State": "State", "Country_Region": "Country", "Long_": "Long"}
)

df_merge_global['Place'] = df_merge_global['Country'].str.cat(
    df_merge_global['State'], 
    sep='_', 
    na_rep=''
)

df_merge_us['Place'] = df_merge_us['Country'].str.cat(
    [df_merge_us['State'], df_merge_us['County']], 
    sep='_', 
    na_rep=''
)

# %%
df_pop = pd.read_csv('d:/Projects/data_projects/COVID-19/extra_data/population-sizes-worldwide/population_sizes.csv')
df_pop = df_pop.rename(columns={"Country_Region": "Country", "Province_State": "State"})
df_pop = df_pop[df_pop['Country'] != 'US']
df_pop = df_pop.drop(columns='Source')
df_merge_global = df_merge_global.merge(df_pop, on=['Country', 'State'])

# %%
df_world = pd.concat([df_merge_global, df_merge_us])
df_world = df_world.sort_values(['Country', 'State', 'County', 'Date'])

df_info = df_world.copy()
df_info = df_info.drop(columns=['Date', 'Confirmed', 'Deaths'])
df_info = df_info.drop_duplicates()

df_counts = df_world.copy()
df_counts = df_counts.drop(
    columns=['Country', 'State', 'County', 'Lat', 'Long', 'Population']
)

df_counts_long = df_counts.melt(
    id_vars=['Date', 'Place'], 
    var_name='Type', 
    value_name='Counts'
)

# %%
print('Getting latest US tracking data')

df_track = pd.read_csv('https://covidtracking.com/api/states/daily.csv')

df_track['date'] = pd.to_datetime(df_track['date'], format='%Y%m%d')
df_track = df_track.sort_values(['state', 'date'])
df_track = df_track.drop(columns=[
    'dataQualityGrade', 
    'lastUpdateEt', 
    'dateModified', 
    'checkTimeEt', 
    'dateChecked',
    'hash', 
    'commercialScore', 
    'negativeRegularScore',
    'negativeScore', 
    'positiveScore', 
    'score', 
    'grade'
])
# df_track['pos_test_rate'] = df_track['positiveIncrease'] / df_track['totalTestResultsIncrease']
# df_track = df_track.groupby(['state', 'date']).sum()

# %%
print('Save to csv')

# df_info.to_csv('d:/Projects/data_projects/COVID-19/data/place_info.csv', index=False)
df_counts_long.to_csv('d:/Projects/data_projects/COVID-19/data/daily_counts_long.csv', index=False)
df_track.to_csv('d:/Projects/data_projects/COVID-19/data/daily_states_tracking.csv', index=False)

# %%
# append new data to Excel

# m = 'w'
# if os.path.exists('data/daily_counts.xlsx'):
#     print('Reading current data from Excel')

#     curr_counts = pd.read_excel('data/daily_counts.xlsx', sheet_name='counts')
#     new_counts = df_counts_long.merge(curr_counts, indicator=True, how='outer')
#     new_counts = new_counts[new_counts['_merge'] != 'both']

#     curr_state = pd.read_excel('data/daily_counts.xlsx', sheet_name='states')
#     new_state = df_track.merge(curr_state, indicator=True, how='outer')
#     new_state = new_state[new_state['_merge'] != 'both']

#     m = 'a'
# else:
#     new_counts = df_counts_long
#     new_state = df_track

# if len(new_counts) > 0:
#     print('Saving to Excel')

#     with pd.ExcelWriter('data/daily_counts.xlsx', mode=m) as writer:
#         new_counts.to_excel(writer, sheet_name='counts', index=False)
#         new_state.to_excel(writer, sheet_name='states', index=False)

#         if m == 'w': 
#             df_info.to_excel(writer, sheet_name='places', index=False)

# df_counts_long.to_excel('data/daily_counts_long.xlsx', index=False)