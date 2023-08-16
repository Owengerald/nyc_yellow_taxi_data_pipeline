import pandas as pd 
from sqlalchemy import create_engine
import db_credentials as db

# creating sql engine
engine = create_engine(f'postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')

engine.connect()

# viewing the table
data_overview_query = '''SELECT * FROM nyc_taxi LIMIT 4'''

data_overview = pd.read_sql(data_overview_query, engine)

#print(data_overview)


# operations_and_performance report

# 1) How many trips were recorded in the dataset?
total_trip_count_query = '''
			SELECT COUNT(*) AS total_trip_count
			FROM nyc_taxi;
			'''
total_trip_count = pd.read_sql(total_trip_count_query, engine)
#print(total_trip_count)


# 2) What is the average trip distance for all trips?
avg_trip_distance_query = '''
			SELECT AVG(trip_distance) AS avg_trip_distance 
			FROM nyc_taxi;
			'''
avg_trip_distance = pd.read_sql(avg_trip_distance_query, engine)
#print(avg_trip_distance)


# 3) Which Vendor has the highest number of trips?
vendor_with_highest_num_trips_query ='''
			SELECT "VendorID" AS vendor_with_highest_num_trips
			FROM nyc_taxi
			GROUP BY "VendorID"
			ORDER BY COUNT(*) DESC
			LIMIT 1;
			'''
vendor_with_highest_num_trips = pd.read_sql(vendor_with_highest_num_trips_query, engine)
#print(vendor_with_highest_num_trips)


# 4) Which Vendor has the lowest number of trips?
vendor_with_lowest_num_trips_query ='''
			SELECT "VendorID" AS vendor_with_lowest_num_trips
			FROM nyc_taxi
			GROUP BY "VendorID"
			ORDER BY COUNT(*) ASC
			LIMIT 1;
			'''
vendor_with_lowest_num_trips = pd.read_sql(vendor_with_lowest_num_trips_query, engine)
#print(vendor_with_lowest_num_trips)


# 5) What is the average passenger count per trip?
avg_passgnr_count_ptrip_query = '''
			SELECT AVG(passenger_count) AS avg_passenger_count
			FROM nyc_taxi;
			'''
avg_passgnr_count_ptrip = pd.read_sql(avg_trip_distance_query, engine)
#print(avg_passgnr_count_ptrip)







# creating report table for operations and performance


report = '''
		SELECT CURRENT_DATE AS ingestion_date, * FROM

		(SELECT COUNT(*) AS total_trip_count
				FROM nyc_taxi) AS a,

		(SELECT AVG(trip_distance) AS avg_trip_distance 
				FROM nyc_taxi) AS b, 

		(SELECT "VendorID" AS vendor_with_highest_num_trips
					FROM nyc_taxi
					GROUP BY "VendorID"
					ORDER BY COUNT(*) DESC
					LIMIT 1) AS c, 

		(SELECT "VendorID" AS vendor_with_lowest_num_trips
					FROM nyc_taxi
					GROUP BY "VendorID"
					ORDER BY COUNT(*) ASC
					LIMIT 1) AS d, 

		(SELECT AVG(passenger_count) AS avg_passenger_count
					FROM nyc_taxi) AS e 
	'''


def transform_data(query,connection):
    try:
        report_query = pd.read_sql(query,connection)
        print('successfully transformed the data')
        return report_query      
    except Exception as e:
        print(f'Encountered error:{e}, while transforming')


def load_report_to_warehouse(dataframe,table,connection):
    try:
        dataframe.to_sql(table, con=connection,if_exists='append')
        print(f'successfully updated {table} table')
    except:
        print(f'Could not update {table}')


if __name__ == "__main__":
	df_report = transform_data(report, engine)
	load_report_to_warehouse(df_report, 'operations_and_performance_report', engine)
	print(df_report)


engine.dispose()














