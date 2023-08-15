import pandas as pd
from sqlalchemy import create_engine
import db_credentials as db
from time import time

# creating sql engine
engine = create_engine(f'postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')

engine.connect()

# reading csv file
#file = r"C:\Users\ASUS\Desktop\Cohort_5\archive\yellow_tripdata_2016-02.csv"

#data = pd.read_csv(csv_file, chunksize=100000)

#df_data = next(data)

# looking at the file to get information
#print(df_data.head())

#print(df_data.info())

# creating function for data extraction, transformation and loading
def etl_nyc_taxi(file, chunk_size, table_name, connection):
	try:
		df_data = pd.read_csv(file, chunksize=chunk_size)
		count = 1
		overall_start_time = time()
		for chunk in df_data:
			if count <= 10:
				t_start = time()
				chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'])
				chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])
				chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
				t_end = time()
				print(f'{count}) loaded data chunk in {t_end - t_start:.3f} seconds')
				count +=1
				
			else:
				overall_end_time = time() - overall_start_time
				print(f'Finished loading a total of {chunk_size*(count-1):,} records in {overall_end_time/60:.3f} minutes')
				break;

	except Exception as e:
		print('There is an error encounted in the pipeline')
	
	finally:
		connection.dispose()

file = r"C:\Users\ASUS\Desktop\Cohort_5\archive\yellow_tripdata_2016-02.csv"
chunk_size = 100000
table_name = 'nyc_taxi'
connection = engine

etl_nyc_taxi(file, chunk_size, table_name, connection)
		

