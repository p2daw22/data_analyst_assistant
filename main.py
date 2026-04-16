from agent import analyze_file, ask_sql


def main():
    while True:
        print("Astra AI Agent")
        print("1. Analyze file")
        print("2. Run SQL on DuckDB")
        print("3. Exit")

        choice = input("Choose 1, 2, or 3: ").strip()
    
        if choice == "1":
            file_path = input("Enter file path: ").strip()

            result = analyze_file(file_path)

            print("\nData Profile:")
            print("Rows:", result["profile"]["rows"])
            print("Columns:", result["profile"]["columns"])
            print("Column Names:", result["profile"]["column_names"])
            print("Missing Values:", result["profile"]["missing_values"])
            print("Duplicates:", result["profile"]["duplicates"])
            print("Data Types:", result["profile"]["dtypes"])

            print(result["csv_message"])
            print(result["excel_message"])
            print(result["duckdb_message"])
            print(result["sqlite_message"])

        elif choice == "2":
            query = input("Enter SQL query: ").strip()
            query_result = ask_sql(query)
            print(query_result)
   
        elif choice == "3":
            print("Bye")
            break 

        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()