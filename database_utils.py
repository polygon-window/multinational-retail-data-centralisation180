import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    """
    A class to manage database connections and operations.
    """

    def read_db_creds(self, file_path):
        """
        Reads the database credentials from a YAML file.

        :param file_path: Path to the credentials YAML file.
        :return: A dictionary with the credentials.
        """
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        return data

    def init_db_engine(self, creds):
        """
        Creates an SQLAlchemy engine using the provided credentials.

        :param creds: A dictionary containing database credentials.
        :return: An SQLAlchemy engine object.
        """
        host = creds["RDS_HOST"]
        password = creds["RDS_PASSWORD"]
        user = creds["RDS_USER"]
        database = creds["RDS_DATABASE"]
        port = creds["RDS_PORT"]

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self, engine):
        """
        Lists all the tables in the database.

        :param engine: An SQLAlchemy engine object.
        :return: A list of table names.
        """
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name, file_path="pgadmin_creds.yaml", if_exists="replace"):
        """
        Uploads a DataFrame to a database table.

        :param df: The DataFrame to upload.
        :param table_name: The name of the table to upload to.
        :param file_path: Path to the YAML file with database credentials.
        :param if_exists: Specifies what to do if the table already exists ("replace", "append", "fail").
        """
        creds = self.read_db_creds(file_path)

        db_url = f"postgresql://{creds["user"]}:{creds["password"]}@{creds["host"]}:{creds["port"]}/{creds["database"]}"
        engine = create_engine(db_url)

        try:
            df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as exception:
            print(f"Failed to upload data: {exception}")


if __name__ == "__main__":
    # Initialize the DatabaseConnector
    connection = DatabaseConnector()

    # Load the credentials
    file_path = "db_creds.yaml"
    db_creds = connection.read_db_creds(file_path)

    # Initialize the database engine
    engine = connection.init_db_engine(db_creds)

    # List the tables in the database
    tables = connection.list_db_tables(engine)
    print(f"Available tables in the database: {tables}")

    # Test the connection
    try:
        with engine.connect() as conn:
            print("Connection to the database was successful!")
    except Exception as exception:
        print(f"Failed to connect to the database: {exception}")

    # Example usage: Uploading cleaned data (ensure the cleaned DataFrame is passed from another script)
    # from data_extraction import get_cleaned_user_data
    # cleaned_user_data = get_cleaned_user_data()  # Ensure this function provides the DataFrame
    # connection.upload_to_db(cleaned_user_data, table_name='dim_users', file_path='pgadmin_creds.yaml')
