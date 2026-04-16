from tools import (
    load_file,
    profile_data,
    clean_data,
    save_csv,
    save_excel,
    save_to_duckdb,
    save_to_sqlite,
    run_sql,
)


def analyze_file(file_path):
    dataframe = load_file(file_path)

    profile = profile_data(dataframe)

    cleaned_dataframe = clean_data(dataframe)

    csv_message = save_csv(cleaned_dataframe)
    excel_message = save_excel(cleaned_dataframe)
    duckdb_message = save_to_duckdb(cleaned_dataframe)
    sqlite_message = save_to_sqlite(cleaned_dataframe)

    result = {
        "profile": profile,
        "csv_message": csv_message,
        "excel_message": excel_message,
        "duckdb_message": duckdb_message,
        "sqlite_message": sqlite_message,
    }

    return result


def ask_sql(query):
    return run_sql(query)