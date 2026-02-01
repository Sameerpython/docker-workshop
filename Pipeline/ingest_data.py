#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


year=2021
moth=1

urls=f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    target_table='yellow_taxi_data'

    pg_user= 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port =5432
    pg_db = 'ny_taxi'
    engine = create_engine('postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{ny_taxi}')

    df.head(0).to_sql(name={target_table}, con=engine, if_exists='replace')

    df_iter = pd.read_csv(
        urls,

        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )
    first=True
    for chuncks in tqdm(df_iter):
        if first:
            df.head(0).to_sql(name={target_table}, con=engine, if_exists='replace')
            first=False

        chuncks.to_sql(name={target_table}, con=engine, if_exists='append')


if __name





