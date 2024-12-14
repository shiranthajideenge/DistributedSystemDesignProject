import pandas as pd
data = pd.read_csv('/home/dsd/shared/asset_cropland-fires_emissions.csv')

# what are the emission factors values were considered between the period of 2015 and 2018?
start_date = '2015-01-01'
end_date = '2018-12-30'

mask = (data['start_time'] > start_date) & (data['start_time'] <= end_date)
fd = data.loc[mask]
emission_factor = fd['emissions_factor'].unique()

print(f'Emission factors are {emission_factor}')

# country with the highest average emissions quantity
emissions_avg = data.groupby(['asset_name', 'emissions_quantity'], as_index=False).size()
emissions_avg.groupby(['asset_name']).mean()
highest_country = emissions_avg.loc[emissions_avg['emissions_quantity'] == emissions_avg['emissions_quantity'].max()]
country = highest_country['asset_name'].values[0]
quantity = highest_country['emissions_quantity'].values[0]

print(f'highest country that produces emissions : {country} with quantity of {quantity}')

# What are the types of gases that USA emits?
gas_types = data[data['iso3_country'] == 'USA']
types = gas_types['gas'].unique()

print(f'types of gas in usa : {types}')

# how many times was the data modified?
times_data_modified = data['created_date'].unique().size
print(f'data modified {times_data_modified} times')