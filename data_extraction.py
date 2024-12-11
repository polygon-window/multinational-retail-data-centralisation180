import pandas as pd
import tabula
import requests
import boto3
import json
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


class DataExtractor:
    """
    A class to extract data from a database.
    """

    def read_rds_table(self, db_connector, table_name):
        """
        Reads a table from an RDS database and returns it as a DataFrame.

        :param db_connector: An instance of DatabaseConnector to manage the connection.
        :param table_name: The name of the table to read.
        :return: DataFrame containing the table data.
        """
        creds = db_connector.read_db_creds("db_creds.yaml")
        engine = db_connector.init_db_engine(creds)

        # Execute the query
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, engine)
        return data
    
    def retrieve_pdf_data(self, link):
        # Read remote PDF into a list of DataFrames
        df_list = tabula.read_pdf(link, pages="all", multiple_tables=True)
        
        # Handle the list of DataFrames
        if df_list:  # Check if the list is not empty
            combined_df = pd.concat(df_list, ignore_index=True)  # Combine all DataFrames
            return combined_df
        else:
            return None

    def list_numbers_of_stores(self, number_of_stores_endpoint, headers): #returns 451 stores
        try:
            response = requests.get(number_of_stores_endpoint, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx).
            print(f"Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            data = response.json()
            return data.get("number_stores")  # Adjust the key based on the API's response structure.
        except requests.exceptions.RequestException as exception:
            print(f"Error while retrieving the number of stores: {exception}")
            return None
        
    def retrieve_stores_data(self, stores_data_endpoint, headers, number_of_stores):
        """
        Retrieve all store data from the API and save it in a pandas DataFrame.

        Args:
            stores_data_endpoint (str): The API endpoint to retrieve a store's details.
            headers (dict): The header dictionary containing API key.
            number_of_stores (int): The total number of stores to retrieve.

        Returns:
            pd.DataFrame: A DataFrame containing all stores' data.
        """
        stores_data = []

        for store_number in range(0, number_of_stores):
            try:
                response = requests.get(stores_data_endpoint.format(store_number=store_number), headers=headers)
                #print(f"Store {store_number} - Status code: {response.status_code}") #FOR DEBUGGING
                response.raise_for_status()
                store_data = response.json()
                stores_data.append(store_data)
            except requests.exceptions.RequestException as exception:
                print(f"Error retrieving data for store {store_number}: {exception}")
                # Continue to next store even if this one fails

        # Create a DataFrame
        return pd.DataFrame(stores_data)
    
    def extract_from_s3(self, s3_address):
        """
        Downloads a CSV file from an S3 bucket and returns it as a pandas DataFrame.

        Args:
            s3_address (str): The S3 address of the CSV file (e.g., 's3://bucket-name/file.csv').

        Returns:
            pd.DataFrame: DataFrame containing the data from the CSV file.
        """
        # Parse the S3 address
        if not s3_address.startswith("s3://"):
            raise ValueError("Invalid S3 address. It should start with 's3://'")

        s3_parts = s3_address[5:].split("/", 1)
        if len(s3_parts) != 2:
            raise ValueError("S3 address should be in the format 's3://bucket-name/key'")

        bucket_name, key = s3_parts

        # Initialize S3 client
        s3_client = boto3.client("s3")

        try:
            # Download the file object
            response = s3_client.get_object(Bucket=bucket_name, Key=key)

            # Save the content to a temporary file and read it with pandas
            with open("/tmp/tempfile.csv", "wb") as temp_file:
                temp_file.write(response["Body"].read())

            # Load the CSV file into a pandas DataFrame
            df = pd.read_csv("/tmp/tempfile.csv")
            return df

        except Exception as exception:
            raise RuntimeError(f"Failed to extract data from S3: {exception}")
        
    def extract_json_from_s3(self, bucket_name, key):
        # Initialize the S3 client
        s3 = boto3.client("s3")
    
        # Download the file object from S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
    
        # Read the content of the file
        data = response["Body"].read().decode("utf-8")
    
        # Parse JSON content
        json_data = json.loads(data)
    
        # Convert to a Pandas DataFrame for easier handling
        df = pd.DataFrame(json_data)
        return df


db_extractor = DataExtractor()
s3_address = "s3://data-handling-public/products.csv"
products_df = db_extractor.extract_from_s3(s3_address)
#print(products_df["weight"])


"""# API key dictionary
api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
headers = {"x-api-key": api_key}
# Number of stores API end point
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
stores_data_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
number_of_stores = db_extractor.list_numbers_of_stores(number_of_stores_endpoint, headers)
stores_df = db_extractor.retrieve_stores_data(stores_data_endpoint, headers, number_of_stores)"""
"""for column in stores_df.columns:
    print(column)"""


if __name__ == "__main__":
    # Initialize required objects
    db_connector = DatabaseConnector()
    db_extractor = DataExtractor()
    data_cleaning = DataCleaning()




    """# Fetch data from the PDF
    pdf_df = db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    # Check if a DataFrame was returned
    if pdf_df is not None:
        print(pdf_df.head())  # Print the first few rows
    else:
        print("No tables were found in the PDF.")"""

"""# Specify the table name
    table_name = "legacy_users"

    # Extract the data from the database
    user_data = db_extractor.read_rds_table(db_connector, table_name)

    # Clean the extracted data
    cleaned_user_data = data_cleaning.clean_user_data(user_data)

    # Print the cleaned data
    print(cleaned_user_data)"""


