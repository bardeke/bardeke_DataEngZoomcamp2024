

from time import time
from sqlalchemy import create_engine
import pandas as pd
import argparse
import os



def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    password = params.password
    db = params.db
    password = params.password
    tblname1 = params.tblname1
    tblname2 = params.tblname2
    url = params.url

    # download csv
    csv_name = 'output_csv'

    os.system(f"wget {url} -O {csv_name}")

    # connect to db
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #connection = engine.connect()

    # read taxi data
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    # create or overwrite table
    df.head(n=0).to_sql(name='tblname1', con=engine, if_exists='replace')

    # write data
    df.to_sql(name='tblname1', con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name='tblname1', con=engine, if_exists='append')
            t_end = time()
            print('inserting another chunk - took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('No more data to insert. Exiting the loop.')
            break

    # read and upload look up
    df = pd.read_csv('taxi+_zone_lookup.csv')

    t_start = time()
    df.to_sql(name='tbl_zone_lookup', con=engine, if_exists='append')
    t_end = time()
    print('inserting - took %.3f second' % (t_end - t_start))



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('user', help='username for postgres')
    parser.add_argument('password', help='pw for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='dbname for postgres')
    parser.add_argument('tblname1', help='tblname for postgres')
    parser.add_argument('tblname2', help='tblname lookup for postgres')
    parser.add_argument('url', help='url taxi data for postgres')


    args = parser.parse_args()
    main(args)
    #print(args.accumulate(args.integers))






