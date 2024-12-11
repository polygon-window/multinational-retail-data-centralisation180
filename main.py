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

# Step 1: Initialize the database engine
db_creds = db_connector.read_db_creds(file_path)
engine = db_connector.init_db_engine(db_creds)

# Step 2: Extract data
"""table_name = "legacy_users" 
raw_user_data = data_extractor.read_rds_table(db_connector, table_name)"""
#raw_order_data = data_extractor.read_rds_table(db_connector, "orders_table")
#print(raw_order_data)
#extract json
# S3 bucket and key details
bucket_name = "data-handling-public"
key = "date_details.json"

raw_events_data = data_extractor.extract_json_from_s3(bucket_name, key)
 # Fetch data from the PDF
#pdf_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

    
# Check if a DataFrame was returned
"""if pdf_df is not None:
    print(pdf_df.head())  # Print the first few rows
else:
    print("No tables were found in the PDF.")"""

# Step 3: Clean data
#cleaned_user_data = data_cleaning.clean_user_data(raw_user_data)
#cleaned_card_data = data_cleaning.clean_card_data(pdf_df)
#print(cleaned_card_data.shape[0])
#cleaned_store_data = data_cleaning.clean_store_data(stores_df)
#uniform_weight_column = data_cleaning.convert_product_weights(products_df)
#cleaned_product_data = data_cleaning.clean_products_data(uniform_weight_column)
#pd.set_option("display.max_rows", None)  # Display all rows
#pd.set_option("display.max_columns", None)  # Display all columns
#print(cleaned_product_data.shape[0])
#cleaned_order_data = data_cleaning.clean_order_data(raw_order_data)
cleaned_events_data = data_cleaning.clean_events_data(raw_events_data)
print(cleaned_events_data.shape[0])

# Step 4: Upload cleaned data to a new table
#db_connector.upload_to_db(cleaned_user_data, table_name="dim_users", file_path="pgadmin_creds.yaml")
#db_connector.upload_to_db(cleaned_card_data, table_name="dim_card_details", file_path="pgadmin_creds.yaml")
#db_connector.upload_to_db(cleaned_store_data, table_name="dim_store_details", file_path="pgadmin_creds.yaml")
#db_connector.upload_to_db(cleaned_product_data, table_name="dim_products", file_path="pgadmin_creds.yaml")
#db_connector.upload_to_db(cleaned_order_data, table_name="orders_table", file_path="pgadmin_creds.yaml")
db_connector.upload_to_db(cleaned_events_data, table_name="dim_date_times", file_path="pgadmin_creds.yaml")

# Step 5: Confirm table listing
#print(f"Available tables: {db_connector.list_db_tables(engine)}")
