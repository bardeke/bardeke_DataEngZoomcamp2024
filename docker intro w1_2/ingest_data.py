

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
    tblname1 = params.table_name1
    tblname2 = params.table_name2
    url1 = params.url1
    url2 = params.url2

    # download csv
    csv_name1 = 'output1_csv'
    os.system(f"wget {url1} -O {csv_name1}")

    # connect to db
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #connection = engine.connect()

    # read taxi data
    df_iter = pd.read_csv(csv_name1, iterator=True, chunksize=100000, compression='gzip')

    df = next(df_iter)
    df.columns = [col.lower() for col in df.columns]
    print('testing update')
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    # create or overwrite table
    df.head(n=0).to_sql(name=tblname1, con=engine, if_exists='replace')

    # write data
  
    df.to_sql(name=tblname1, con=engine, if_exists='append')
    
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.columns = [col.lower() for col in df.columns]
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=tblname1, con=engine, if_exists='append')
            t_end = time()
            print('inserting another chunk - took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('No more data to insert. Exiting the loop.')
            break
        
    csv_name2 = 'output2_csv'
    os.system(f"wget {url2} -O {csv_name2}")

    # read and upload look up
    df = pd.read_csv(csv_name2)
    df.columns = [col.lower() for col in df.columns]
    t_start = time()
    df.to_sql(name=tblname2, con=engine, if_exists='replace')
    t_end = time()
    print('inserting lookup table - took %.3f second' % (t_end - t_start))



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='pw for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='dbname for postgres')
    parser.add_argument('--table_name1', help='tblname green taxi for postgres')
    parser.add_argument('--table_name2', help='tblname look up for postgres')
    parser.add_argument('--url1', help='url taxi data for postgres')
    parser.add_argument('--url2', help='url lookup for postgres')

    args = parser.parse_args()
    main(args)
    #print(args.accumulate(args.integers))






