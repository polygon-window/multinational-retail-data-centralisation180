from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor, products_df
import pandas as pd


# Initialize objects
db_connector = DatabaseConnector()
data_cleaning = DataCleaning()
data_extractor = DataExtractor()

# Database credentials file
file_path = "db_creds.yaml"

# Initialize the database engine
db_creds = db_connector.read_db_creds(file_path)
engine = db_connector.init_db_engine(db_creds)

# user data extraction - reads a RDS and returns a dataframe
table_name = "legacy_users" 
raw_user_data = data_extractor.read_rds_table(db_connector, table_name)

# order data extraction - reads a RDS and returns a dataframe
raw_order_data = data_extractor.read_rds_table(db_connector, "orders_table")

# Events data extraction - reads a JSON file from an aws s3 bucket
bucket_name = "data-handling-public"
key = "date_details.json"
raw_events_data = data_extractor.extract_json_from_s3(bucket_name, key)

# Card details extraction - reads a pdf file and returns a dataframe
pdf_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
# Check if a DataFrame was returned
if pdf_df is not None:
    print(pdf_df.head())  # Print the first few rows
else:
    print("No tables were found in the PDF.")

# Stores data extraction - extracts data from an API and creates a dataframe
# API key dictionary
api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
headers = {"x-api-key": api_key}
# Number of stores API end point - to return the number of stores
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
stores_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
number_of_stores = data_extractor.list_numbers_of_stores(number_of_stores_endpoint, headers)
stores_df = data_extractor.retrieve_stores_data(stores_data_endpoint, headers, number_of_stores)
for column in stores_df.columns:
    print(column)

# Products extraction - reads data from a csv file and returns a dataframe
s3_address = "s3://data-handling-public/products.csv"
products_df = data_extractor.extract_from_s3(s3_address)

# Clean data
cleaned_user_data = data_cleaning.clean_user_data(raw_user_data)
cleaned_card_data = data_cleaning.clean_card_data(pdf_df)
cleaned_store_data = data_cleaning.clean_store_data(stores_df)
uniform_weight_column = data_cleaning.convert_product_weights(products_df)
cleaned_product_data = data_cleaning.clean_products_data(uniform_weight_column)
cleaned_order_data = data_cleaning.clean_order_data(raw_order_data)
cleaned_events_data = data_cleaning.clean_events_data(raw_events_data)

# Upload cleaned data to a new table
db_connector.upload_to_db(cleaned_user_data, table_name="dim_users", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_card_data, table_name="dim_card_details", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_store_data, table_name="dim_store_details", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_product_data, table_name="dim_products", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_order_data, table_name="orders_table", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_events_data, table_name="dim_date_times", file_path="pgadmin_creds.yaml")

# Confirm table listing
print(f"Available tables: {db_connector.list_db_tables(engine)}")
