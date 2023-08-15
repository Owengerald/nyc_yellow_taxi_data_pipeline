import pandas as pd 
from sqlalchemy import create_engine
import db_credentials as db
import sql_etl_table_1 as et

# creating sql engine
engine = create_engine(f'postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')

engine.connect()

# financial_performance
# 1) What is the average fare amount per trip?
avg_fare_amount_query = '''
			SELECT AVG(fare_amount) AS avg_fare_amount
			FROM nyc_taxi;
			'''
avg_fare_amount = pd.read_sql(avg_fare_amount_query, engine)
#print(avg_fare_amount)


# 2) How much revenue was generated from tolls and surcharges combined?
total_tolls_and_surcharges_revenue_query = '''
			SELECT SUM( tolls_amount + improvement_surcharge) AS total_tolls_and_surcharges_revenue
			FROM nyc_taxi;
			'''
total_tolls_and_surcharges_revenue = pd.read_sql(total_tolls_and_surcharges_revenue_query, engine)
#print(total_tolls_and_surcharges_revenue)


# 3) What is the average total amount paid by passengers?
avg_total_amount_query = '''
			SELECT AVG(total_amount) AS avg_total_amount
			FROM nyc_taxi;
			'''
avg_total_amount = pd.read_sql(avg_total_amount_query, engine)
#print(avg_total_amount)



# creating report table for financial performance

report = '''
		SELECT CURRENT_DATE AS ingestion_date, * FROM

		(SELECT AVG(fare_amount) AS avg_fare_amount
		FROM nyc_taxi) AS a,

		(SELECT SUM( tolls_amount + improvement_surcharge) AS total_tolls_and_surcharges_revenue
		FROM nyc_taxi) AS b,

		(SELECT AVG(total_amount) AS avg_total_amount
		FROM nyc_taxi) AS c 
		'''

# Using the function from the sql_etl_table_1 module

df_report = et.transform_data(report, engine)
#print(df_report)

et.load_report_to_warehouse(df_report, 'financial_performance', engine)

engine.dispose()
