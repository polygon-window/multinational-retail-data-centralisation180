# Multinational Retail Data Centralisation

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
RDS_DATABASE: postgres \
RDS_PORT: "portnumber" 

The next stage is to initialise the objects by calling the classes: DatabaseConnector(), DataExtractor() and DataCleaning() in main.py. 

From here you can call the various methods from the classes to extract and clean the data as you wish before using the upload_to_db() method from the the DatabaseConnector() class located in database_utils.py to upload the data to your pgadmin4 database.

## File Structure