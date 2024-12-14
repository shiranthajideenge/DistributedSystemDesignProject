import time
from collections import OrderedDict

import numpy
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def main():
    slave_workers = size - 1
    rows = GetNumberOfRows()
    chunk_size = int(rows / slave_workers)
    chunk_distribution = distribute_rows(n_rows=chunk_size, n_processes=slave_workers)
    print('using MPI....')

    start_time = time.time()

    distributeTasks(chunk_distribution, size)

    results = GetResults()

    print('All results:')

    for result in GetTotalResults(results):
          print(f'{result}')

    end_time = time.time()
    print("Total time of handling MPI : " + str(end_time - start_time))


def GetNumberOfRows():
    return int(sum(1 for line in open('/home/dsd/shared/asset_cropland-fires_emissions.csv', encoding="utf8")))


def distribute_rows(n_rows: int, n_processes):
    reading_info = []
    skip_rows = 0
    reading_info.append([n_rows - skip_rows, skip_rows])
    skip_rows = n_rows

    for _ in range(1, n_processes - 1):
        reading_info.append([n_rows, skip_rows])
        skip_rows = skip_rows + n_rows

    reading_info.append([None, skip_rows])
    return reading_info


def distributeTasks(distribution, size):
    for worker in range(1, size):
        chunk = worker - 1
        comm.send(distribution[chunk], dest=worker)


def GetResults():
    results = []
    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        results.append(result)
        print(f'received from Worker slave {worker}')
    return results


def GetTotalResults(results):

    emission_factors = []
    country_with_highest_average_df = []
    usa_emission_types = []
    times_date_modified = []

    for res in results:
        emission_factors.append(res[0])
        country_with_highest_average_df.append(res[1])
        usa_emission_types.append(res[2])
        times_date_modified.append(res[3])

    #emission factors
    unique_emission_factors = aggregate_emission_factors(emission_factors)
    #highest average
    highest_country = aggregate_highest_country(country_with_highest_average_df)
    # usa emissions
    usa_emission_types_result = aggregate_usa_emission(usa_emission_types)
    #times date modified
    times_modified = aggregate_total_time_modified(times_date_modified)

    return unique_emission_factors, highest_country, usa_emission_types_result, times_modified


def aggregate_emission_factors(emission_factors):
    emission_factors_combined = numpy.concatenate(emission_factors, axis=0)
    return f'Emission factors: {list(OrderedDict.fromkeys(emission_factors_combined))}'


def aggregate_highest_country(country_with_highest):
    dict = {}
    for country in country_with_highest:
        dict.update(country)

    max_value = max(dict.values())
    max_key = max(dict, key=dict.get)
    return f'highest country is {max_key} with {max_value}'


def aggregate_usa_emission(usa_emission):
    usa_emission_types_combined = numpy.concatenate(usa_emission, axis=0)
    return f'usa emission types: {list(OrderedDict.fromkeys(usa_emission_types_combined))}'


def aggregate_total_time_modified(times_modified):
    total_times_modified = 0
    for time in times_modified:
        total_times_modified += time

    return f'times data modified is {total_times_modified}'


def get_emissions_distributed(reading_info: list):
    start_time = time.time()
    df = pd.read_csv('/home/dsd/shared/asset_cropland-fires_emissions.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    print('processing one chunk...')

    emission_factors = emission_factors_considered(df)
    country_with_highest_average_df = country_with_highest_average(df)
    usa_emission_types = types_of_gas_usa_emits(df)
    times_date_modified =how_many_times_data_modified(df)

    end_time = time.time()
    print("Chunk time taken  : " + str(end_time - start_time))

    return emission_factors, country_with_highest_average_df, usa_emission_types, times_date_modified


def emission_factors_considered(df):
    # what are the emission factors values were considered between the period of 2015 and 2018?
    start_date = '2015-01-01'
    end_date = '2018-12-30'

    mask = (df['start_time'] > start_date) & (df['start_time'] <= end_date)
    fd = df.loc[mask]

    emission_factor = fd['emissions_factor'].unique()
    return emission_factor


def types_of_gas_usa_emits(df):
    gas_types = df[df['iso3_country'] == 'USA']
    types = gas_types['gas'].unique()
    return types


def country_with_highest_average(df):
    emissions_avg = df.groupby(['asset_name', 'emissions_quantity'], as_index=False).size()
    emissions_avg.groupby(['asset_name']).mean()
    highest_country = emissions_avg.loc[emissions_avg['emissions_quantity'] == emissions_avg['emissions_quantity'].max()]
    country = highest_country['asset_name'].values[0]
    quantity = highest_country['emissions_quantity'].values[0]

    return {country: quantity}


def how_many_times_data_modified(df):
    times_data_modified = df['created_date'].unique().size
    return times_data_modified


if __name__ == "__main__":

    if rank == 0:
        main()
    elif rank > 0:
        chunk = comm.recv()
        print(f'Worker {rank} is assigned chunk info {chunk}')
        result = get_emissions_distributed(chunk)
        print(f'Worker slave {rank} is done. Sending back to master')
        comm.send(result, dest=0)