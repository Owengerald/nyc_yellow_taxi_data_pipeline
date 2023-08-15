# Ingestion and SQL ETL Data Pipelines

This repository contains ETL (Extract, Transform, Load) data pipelines implemented in Python and SQL.
The pipelines are designed to process datasets downloaded from a specific source and transform the data for analysis and reporting.

## Dataset Source
The dataset used in these ETL data pipelines was downloaded from "https://data.cityofnewyork.us/Transportation/2016-Yellow-Taxi-Trip-Data/k67s-dv2t".
The dataset includes "VendorID" A code indicating the TPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.,
tpep_pickup_datetime and tpep_dropoff_datetime: The date and time when the meter was engaged and disengaged respectively, trip_distance, etc.
Full Dictionary of Dataset is to be found in link above.  

## Goal of the ETL Data Pipelines
The primary goal of these ETL data pipelines is to clean, transform, and load the dataset into a relational database, making it ready for analysis and reporting.
The pipelines involve data extraction, cleaning, and loading into appropriate database tables.

## Credentials Management
Database credentials are managed securely using a separate Python module named "db_credentials.py".
This module stores sensitive information such as usernames and passwords, separate from the main codebase.

## SQL ETL Pipeline Modules
The SQL ETL pipelines are organized into separate modules. Notably, the module named "SQL_ETL_pipeline_1.py" plays a central role
as other SQL ETL pipeline modules depend on it. This module contains two essential functions:
- transform_data(): This function performs data transformation on the extracted dataset before loading it into the database.
- load_report_to_warehouse(): This function loads the transformed data into the database.

## Special Observation
In the first SQL ETL pipeline module, "SQL_ETL_pipeline_1", a data issue was observed with the column name "VendorID."
Due to inconsistent capitalization, this column couldn't be used directly in SQL queries.
To resolve this, quotation marks were added around the column name in SQL queries, ensuring consistent access and proper functionality.
An alternative is fix this is to consider rendering all your Pandas columns to lower case.


## Installation and Usage
1. Clone the repository:
   ```bash
   git clone https://github.com/Owengerald/nyc_yellow_taxi_data_pipeline.git
   ```

2. Navigate to the repository directory:
   ```bash
   cd nyc_yellow_taxi_ETL_data_pipeline

3. Update database credentials in "db_credentials.py".

4. Run the ETL pipelines:
   ```bash
   python main_etl.py
   ```

## Contributing
Contributions are welcome! If you find issues or improvements, please feel free to submit pull requests or report issues in the repository.

## License
This project is **All Rights Reserved**. You may not use, distribute, or reproduce any part of the code or content without explicit permission from the project owner.
(https://github.com/Owengerald/nyc_yellow_taxi_data_pipeline/assets/134776284/58a514bc-ca11-44f5-8688-0495a635fcc3)
