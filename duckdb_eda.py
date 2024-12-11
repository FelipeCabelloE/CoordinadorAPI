import duckdb
from rich import print


def main():
    db = duckdb.connect("./data.duckdb")
    result = db.execute("SELECT * FROM api_data").fetch_df()
    description = result.describe()
    print(description)


if __name__ == "__main__":
    main()
