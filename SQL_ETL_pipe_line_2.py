import pandas as pd 
from sqlalchemy import create_engine
import db_credentials as db
import sql_etl_table_1 as et

# creating sql engine
engine = create_engine(f'postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')

engine.connect()


#customer_demographics_and_preferences

# 1) What is the average trip amount given by passengers?
avg_trip_amount_query = '''
			SELECT AVG(total_amount) AS average_trip_amount
			FROM nyc_taxi;
			'''
avg_trip_amount = pd.read_sql(avg_trip_amount_query, engine)
#print(avg_trip_amount)


# 2) What is the average trip distance by passengers?
avg_trip_dist_by_passngr_query = '''
				SELECT AVG(Trip_distance) AS average_trip_distance
				FROM nyc_taxi;
				'''
avg_trip_dist_by_passngr = pd.read_sql(avg_trip_dist_by_passngr_query, engine)
#print(avg_trip_dist_by_passngr)


# 3) How many trips were flagged as 'store and forward'?
store_and_fwd_flag_trips_query = '''
				SELECT COUNT(*) AS num_of_store_and_fwd_trips
				FROM nyc_taxi
				WHERE store_and_fwd_flag = 'Y';
				'''
num_store_and_fwd_trips = pd.read_sql(store_and_fwd_flag_trips_query, engine)
#print(num_store_and_fwd_trips)


# 4) How many trips were shared rides (passenger count > 1)?
num_of_shared_rides_query = '''
			SELECT COUNT(*) AS num_of_shared_rides
			FROM nyc_taxi
			WHERE passenger_count > 1;
			'''
num_of_shared_rides = pd.read_sql(num_of_shared_rides_query, engine)
#print(num_of_shared_rides)



# creating report table for customer demographics and preferences

report = '''
		SELECT CURRENT_DATE AS ingestion_date, * FROM

		(SELECT AVG(total_amount) AS average_trip_amount
		FROM nyc_taxi) as a,

		(SELECT AVG(Trip_distance) AS average_trip_distance
		FROM nyc_taxi) as b,

		(SELECT COUNT(*) AS num_of_store_and_fwd_trips
		FROM nyc_taxi
		WHERE store_and_fwd_flag = 'Y') AS c,

		(SELECT COUNT(*) AS num_of_shared_rides
		FROM nyc_taxi
		WHERE passenger_count > 1) AS d 
		'''

# Using the function from the sql_etl_table_1 module

df_report = et.transform_data(report, engine)
#print(df_report)

et.load_report_to_warehouse(df_report, 'customer_demographics_and_preferences', engine)

engine.dispose()
