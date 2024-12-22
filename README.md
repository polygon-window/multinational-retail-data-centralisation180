# Multinational Retail Data Centralisation
---
## Introduction

The aim of this project is to create a program that will firstly extract data from a variety of sources, then clean that data before uploading them as tables to a PGadmin relational database. This is a simplified overview of the program but the rest of this README document will look at the process in further detail.

## Installation Instructions

This project has several dependancies to ensure the program runs as intended. Please ensure you have these librarys available on your working enviroment before running the program. 

### These libraries are:

* pyYAML
* sqlalchemy
* Pandas
* Tabula-py
* openjdk
* Jpype1
* Requests
* Boto3
* RE
* Json


## Usage Instructions

Once you have downloaded the program and the neccesary dependancies the next step will be to create a database within pgadmin4, this database will be the endpoint for the extracted and cleaned data to be uploaded to. Once you have created this database, the credentials need to be stored in a file named pgadmin_creds.yaml and should be formatted in a dictionary as below:

host: "your_host_name" \
database: "your_database_name" \
user: "your_username" \
password: "your_password" \
port: "your_port" 

Once this file has been created another yaml file must be created for extracting the user data from an AWS RDS database. this file must be called db_creds.yaml and should present the credentials in the format below.

RDS_HOST: "host_address" \
RDS_PASSWORD: "password" \
RDS_USER: "username" \
RDS_DATABASE: "postgres" \
RDS_PORT: "portnumber" 

The next stage is to initialise the objects by calling the classes: DatabaseConnector(), DataExtractor() and DataCleaning() in main.py. 

From here you can call the various methods from the classes to extract and clean the data as you wish before using the upload_to_db() method from the the DatabaseConnector() class located in database_utils.py to upload the data to your pgadmin4 database.

## File Structure

### data_extraction.py

This file defines the `DataExtractor` class, which provides various methods to extract data from different sources. These include databases, APIs, PDF files, and Amazon S3 buckets. The key functionalities are:

1. **Read RDS Table**  
   Extracts data from a specified table in an RDS database and returns it as a pandas DataFrame.

2. **Retrieve PDF Data**  
   Extracts tabular data from a PDF file and combines it into a single DataFrame.

3. **List Number of Stores**  
   Queries an API to retrieve the total number of stores from the provided endpoint.

4. **Retrieve Store Data**  
   Iterates through an API to fetch detailed data for all stores and consolidates it into a DataFrame.

5. **Extract from S3 (CSV)**  
   Downloads a CSV file from an S3 bucket and loads its content into a pandas DataFrame.

6. **Extract JSON from S3**  
   Downloads a JSON file from an S3 bucket, parses it, and converts it into a pandas DataFrame for further processing.

---

### database_utils.py

This file contains the `DatabaseConnector` class, which simplifies database interactions by managing connections and performing common operations. The key functionalities provided are:

1. **Read Database Credentials**  
   Reads database connection details securely from a YAML file.

2. **Initialize Database Engine**  
   Creates an SQLAlchemy engine using the provided credentials, enabling interaction with the database.

3. **List Database Tables**  
   Retrieves and displays a list of all table names in the connected database.

4. **Upload Data to Database**  
   Uploads a pandas DataFrame to a specified database table.

---

### data_cleaning.py

This file contains the `DataCleaning` class, which provides a set of methods to clean and preprocess various datasets, ensuring data consistency and quality. Below is an outline of its key functionalities:

1. **User Data Cleaning**  
   Cleans user data by removing null values, handling invalid dates, and removing duplicates.

2. **Card Data Cleaning**  
   Cleans card data by standardizing date formats, removing duplicates, and dropping invalid rows.

3. **Store Data Cleaning**  
   Preprocesses store data by handling missing values within specific index ranges, standardizing date formats, and cleaning staff_numbers.

4. **Product Weight Conversion**  
   Converts product weight data into a uniform metric (kilograms) using a flexible regex-based approach to handle diverse formats.

5. **Product Data Cleaning**  
   Cleans product data by removing null values and preparing it for analysis.

6. **Order Data Cleaning**  
   Cleans orders data by removing unnecessary columns.

7. **Event Data Cleaning**  
   Processes event data by converting date-related columns into numeric formats and removing invalid rows.

These methods ensure that raw datasets are transformed into a consistent and usable format for analysis or database storage.

---

### main.py

This script orchestrates the extraction, cleaning, and loading of data from various sources into a database, ensuring a complete ETL (Extract, Transform, Load) pipeline.

---

### sql_queries.sql

This file contains SQL scripts for structuring and formatting tables in the final PostgreSQL database. It also includes queries for tasks 1 through 9, designed to manage, manipulate, and analyze the data effectively.



