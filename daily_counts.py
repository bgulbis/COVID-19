# %%
import pandas as pd

# %%
ts_path = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
# %%
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
df_pop = pd.read_csv('extra_data/population-sizes-worldwide/population_sizes.csv')
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
df_info.to_csv('data/place_info.csv', index=False)
df_counts_long.to_csv('data/daily_counts_long.csv', index=False)

# %%
# save data to Excel
# with pd.ExcelWriter('data/daily_counts.xlsx') as writer:
#     df_merge_global.to_excel(writer, sheet_name='global', index=False)
#     df_merge_us.to_excel(writer, sheet_name='us', index=False)

