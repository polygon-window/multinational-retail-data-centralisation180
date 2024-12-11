import pandas as pd
import numpy as np
import re


class DataCleaning:

    def clean_user_data(self, df):
        # Change "NULL" string values to None 
        df.replace("NULL", None, inplace=True)
        # Remove rows with Null values
        df.dropna(inplace=True)
        # Convert join_date column to datetime and turn invalid dates to NaT
        df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce", format="mixed") 
        df["join_date"] = df["join_date"].dt.strftime("%Y-%m-%d")
        # Remove NaT columns
        df.dropna(subset=["join_date"], inplace=True)
        # Remove duplicate rows
        df.drop_duplicates(inplace=True) 
        return df
        
    def clean_card_data(self, df):
        # Change "NULL" string values to None 
        df.replace("NULL", None, inplace=True)        
        # Remove rows with Null values
        df.dropna(inplace=True)
        # Remove duplicate card numbers
        if "card_number" in df.columns:
            df.drop_duplicates(subset="card_number", inplace=True)
        # Convert "date_payment_confirmed" column to datetime and turn invalid dates to NaT
        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], errors="coerce", format="mixed") 
        df["date_payment_confirmed"] = df["date_payment_confirmed"].dt.strftime("%Y-%m-%d")
        # Drop rows with invalid or missing dates
        df.dropna(subset=["date_payment_confirmed"], inplace=True)
        # Reset index after cleaning
        df.reset_index(drop=True, inplace=True)
        return df

        
    def clean_store_data(self, df):
        # Range for the dropna section to operate within to exclude the web portal
        start_index = 1
        end_index = 450

        # Step 1: Replace "NULL" with None
        df.replace("NULL", None, inplace=True)
        # print(f"Row count after replacing 'NULL': {len(df)}")

        # Step 2: Convert 'opening_date' to datetime and handle invalid dates
        df["opening_date"] = pd.to_datetime(df["opening_date"], errors="coerce", format="mixed")
        df["opening_date"] = df["opening_date"].dt.strftime("%Y-%m-%d")
        # print(f"Row count after opening_date cleaning: {len(df)}")

        ''' # Step 3: Identify rows with NaN before dropna
        print("Rows with missing values before dropna:")
        print(df[df.isnull().any(axis=1)])'''

        # Step 4: Apply dropna only within the specified index range
        df_in_range = df.loc[start_index:end_index]
        df_out_of_range = df.drop(df_in_range.index)

        # Drop rows with missing values in range
        df_in_range = df_in_range.dropna(subset=[col for col in df.columns if col != "lat"])

        # Combine cleaned data
        df = pd.concat([df_in_range, df_out_of_range]).sort_index()
        # print(f"Row count after dropna: {len(df)}")

        # Step 5: Clean "staff_numbers"
        df["staff_numbers"] = df["staff_numbers"].apply(lambda x: "".join(filter(str.isdigit, str(x))))
        print(f"Row count after cleaning staff_numbers: {len(df)}")
        return df

    @staticmethod
    def convert_product_weights(products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans and converts the weight column in the products DataFrame to a float representing weight in kg.

        Args:
            products_df (pd.DataFrame): The products DataFrame with a weight column.

        Returns:
            pd.DataFrame: The updated DataFrame with cleaned and converted weights.
        """
        
        def convert_to_kg(weight):
            if pd.isnull(weight):
                return None

            # Handle formats like "40 x 100g"
            multiplication_match = re.match(r"(\d+)\s*[xX\*]\s*(\d+)\s*(\w*)", str(weight).lower())
            if multiplication_match:
                count, value, unit = multiplication_match.groups()
                try:
                    count = int(count)
                    value = float(value)
                except ValueError:
                    return None

                # Convert individual value to kg and multiply
                if unit in ["g", "gram", "grams"]:
                    return count * (value / 1000)
                elif unit in ["kg", "kilogram", "kilograms"]:
                    return count * value
                elif unit in ["mg", "milligram", "milligrams"]:
                    return count * (value / 1_000_000)
                elif unit in ["lb", "pound", "pounds"]:
                    return count * (value * 0.453592)
                elif unit in ["oz", "ounce", "ounces"]:
                    return count * (value * 0.0283495)
                else:
                    return None

            # Extract numeric value and unit using regex for other formats
            match = re.match(r"([0-9.]+)\s*(\w*)", str(weight).lower())
            if not match:
                return None

            value, unit = match.groups()
            try:
                value = float(value)
            except ValueError:
                return None

            # Convert to kilograms based on unit
            if unit in ["kg", "kilogram", "kilograms"]:
                return value
            elif unit in ["g", "gram", "grams"]:
                return value / 1000
            elif unit in ["mg", "milligram", "milligrams"]:
                return value / 1_000_000
            elif unit in ["lb", "pound", "pounds"]:
                return value * 0.453592
            elif unit in ["oz", "ounce", "ounces"]:
                return value * 0.0283495
            elif unit in ["l", "liter", "liters", "ml", "milliliter", "milliliters"]:
                return value / 1000  # Assuming 1:1 density (ml to g)
            else:
                return None

        # Apply conversion to the weight column
        if "weight" in products_df.columns:
            products_df["weight"] = products_df["weight"].apply(convert_to_kg)
        return products_df

    
    def clean_products_data(self, df):
        # Step 1: Replace "NULL" with None 
        df = df.replace("NULL", None)

        # Step 2: Remove rows with Null values
        df.dropna(inplace=True)
        return df

    def clean_order_data(self, df):
        df = df.drop(columns=["first_name", "last_name", "1"])
        return df
    
    def clean_events_data(self, df):
        # Step 1: Replace "NULL" with None
        df = df.replace("NULL", None)
        
        # Step 2: Convert "day", "month", and "year" columns to numeric values
        #    Invalid parsing will result in NaN
        for col in ["day", "month", "year"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Step 3: Remove rows with Null values
        df.dropna(inplace=True)
        return df