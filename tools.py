import os
import pandas as pd
import duckdb
import sqlite3

# DuckDB file path
DUCKDB_PATH = r"C:\Astra_AI_Agent\db\analytics.duckdb"

# SQLite file path
SQLITE_PATH = r"C:\Astra_AI_Agent\db\analytics_sqlite.db"


# Load file
def load_file(file_path):
    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == ".csv":
        dataframe = pd.read_csv(file_path)
        return dataframe

    if file_extension == ".xlsx":
        dataframe = pd.read_excel(file_path)
        return dataframe

    if file_extension == ".xls":
        dataframe = pd.read_excel(file_path)
        return dataframe

    raise ValueError("File type not supported")


# Review data
def profile_data(dataframe):
    profile = {
        "rows": int(dataframe.shape[0]),
        "columns": int(dataframe.shape[1]),
        "column_names": list(dataframe.columns),
        "missing_values": dataframe.isna().sum().to_dict(),
        "duplicates": int(dataframe.duplicated().sum()),
        "dtypes": {column: str(dtype) for column, dtype in dataframe.dtypes.to_dict().items()},
    }

    return profile

# Remove extra spaces from text
def strip_text(value):
    if isinstance(value, str):
        return value.strip()
    return value


# Remove illegal Excel characters
def clean_text(value):
    if isinstance(value, str):
        new_text = ""

        for character in value:
            if ord(character) >= 32:
                new_text = new_text + character

        return new_text

    return value


# Turn empty text into missing value
def empty_text_to_missing(value):
    if isinstance(value, str):
        if value.strip() == "":
            return pd.NA
    return value


# Try to convert text columns to datetime if they look like dates
def try_convert_dates(dataframe):
    updated_dataframe = dataframe.copy()

    for column in updated_dataframe.columns:
        if updated_dataframe[column].dtype == "object":
            if "date" in column:
                updated_dataframe[column] = pd.to_datetime(
                    updated_dataframe[column],
                    errors="ignore"
                )

    return updated_dataframe


# Try to convert number-like text columns into numbers
def try_convert_numbers(dataframe):
    updated_dataframe = dataframe.copy()

    for column in updated_dataframe.columns:
        if updated_dataframe[column].dtype == "object":
            updated_dataframe[column] = pd.to_numeric(
                updated_dataframe[column],
                errors="ignore"
            )

    return updated_dataframe


# Main universal cleaning
def clean_data(dataframe):
    cleaned_dataframe = dataframe.copy()

    cleaned_dataframe.columns = [
        column.strip().lower().replace(" ", "_")
        for column in cleaned_dataframe.columns
    ]

    cleaned_dataframe = cleaned_dataframe.drop_duplicates()

    for column in cleaned_dataframe.columns:
        if cleaned_dataframe[column].dtype == "object":
            cleaned_dataframe[column] = cleaned_dataframe[column].apply(strip_text)
            cleaned_dataframe[column] = cleaned_dataframe[column].apply(clean_text)
            cleaned_dataframe[column] = cleaned_dataframe[column].apply(empty_text_to_missing)

    cleaned_dataframe = cleaned_dataframe.dropna(how="all")

    cleaned_dataframe = try_convert_dates(cleaned_dataframe)
    
    cleaned_dataframe = try_convert_numbers(cleaned_dataframe)

    return cleaned_dataframe

# Save processed CSV
def save_csv(dataframe):
    output_path = r"C:\Astra_AI_Agent\data\processed\cleaned_dataset.csv"
    dataframe.to_csv(output_path, index=False)
    return "Saved CSV file to: " + output_path


# Save processed Excel
def save_excel(dataframe):
    output_path = r"C:\Astra_AI_Agent\data\processed\cleaned_dataset.xlsx"

    excel_dataframe = dataframe.copy()

    for column in excel_dataframe.columns:
        excel_dataframe[column] = excel_dataframe[column].apply(clean_text)

    excel_dataframe.to_excel(output_path, index=False)

    return "Saved Excel file to: " + output_path


# Save to DuckDB
def save_to_duckdb(dataframe):
    os.makedirs(r"C:\Astra_AI_Agent\db", exist_ok=True)

    connection = duckdb.connect(DUCKDB_PATH)
    connection.register("temp_table", dataframe)
    connection.execute("CREATE OR REPLACE TABLE dataset AS SELECT * FROM temp_table")
    connection.close()

    return "Saved DuckDB database to: " + DUCKDB_PATH


# Save to SQLite
def save_to_sqlite(dataframe):
    os.makedirs(r"C:\Astra_AI_Agent\db", exist_ok=True)

    connection = sqlite3.connect(SQLITE_PATH)
    dataframe.to_sql("dataset", connection, if_exists="replace", index=False)
    connection.close()

    return "Saved SQLite database to: " + SQLITE_PATH


# Run SQL on DuckDB
def run_sql(query):
    connection = duckdb.connect(DUCKDB_PATH)
    result_dataframe = connection.execute(query).df()
    connection.close()

    return result_dataframe