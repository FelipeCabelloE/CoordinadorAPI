import asyncio
from datetime import date, datetime, timedelta
from typing import List, Optional

import dlt
import duckdb
import httpx
import pandas as pd
from pydantic import BaseModel, ValidationError
from rich import print
from tenacity import retry, retry_if_exception_type, stop_after_attempt

# Example REST API endpoint and headers
API_URL = "https://sipub.api.coordinador.cl:443/costo-marginal-real/v4/findByDate"

# Request configuration

today = datetime.today()
START_DATE = (today - timedelta(days=123)).strftime("%Y-%m-%d")
END_DATE = (today - timedelta(days=120)).strftime("%Y-%m-%d")
PAGE_LIMIT = 1000  # Number of records per page for testing purposes
USER_KEY = dlt.secrets["sources.rest_api.coordinador_sip_token"]
# Define Pydantic models for data validation


class DataItem(BaseModel):
    id_info: Optional[int]
    barra_info: str
    barra_transf: str
    fecha: date
    hra: Optional[int]
    min: Optional[int]
    cmg_clp_kwh_: Optional[float]
    cmg_usd_mwh_: Optional[float]
    version: str
    fecha_hora: datetime
    fecha_minuto: datetime


class APIResponse(BaseModel):
    data: List[DataItem]
    type: str
    totalPages: int
    page: int
    limit: int


semaphore = asyncio.Semaphore(10)


# Function to fetch a single page of data


@retry(retry=retry_if_exception_type(httpx.HTTPError), stop=stop_after_attempt(4))
async def fetch_data(
    client: httpx.AsyncClient, start_date, end_date, page, limit, api_key
):
    async with semaphore:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "limit": limit,
            "user_key": api_key,
        }
        # try:
        print(f"Fetching page {page}")
        response = await client.get(API_URL, params=params)
        response.raise_for_status()
        print(f"success for page {page}")
        return response.json()
        # except httpx.TimeoutException as e:
        #     print(f"Error fetching data for page {page}: {e}")
        #     return e


# Function to fetch all pages dynamically
async def fetch_all_data(start_date, end_date, limit, user_key):
    async with httpx.AsyncClient() as client:
        # Fetch the first page to determine total pages
        first_page_response = await fetch_data(
            client, start_date, end_date, 1, limit, user_key
        )
        if not first_page_response:
            print("Failed to fetch the first page.")
            return []

        # Validate the first page response
        try:
            validated_response = APIResponse(**first_page_response)
        except ValidationError as e:
            print("Validation error for the first page response:", e)
            return []

        # Extract total pages
        total_pages = validated_response.totalPages
        print(f"Total pages to fetch: {total_pages}")

        # Fetch all pages (modify range for testing if total_pages is too large)
        tasks = [
            fetch_data(client, start_date, end_date, page, limit, user_key)
            for page in range(1, total_pages + 1)
        ]
        raw_responses = await asyncio.gather(*tasks)

        # Validate all responses
        valid_responses = []
        for i, response in enumerate(raw_responses, start=1):
            if response:
                try:
                    valid_responses.append(APIResponse(**response))
                except ValidationError as e:
                    print(f"Validation error for page {i}:", e)
        return valid_responses


# Process data into DataFrame
def process_data(data):
    # Combine all "data" arrays from validated responses
    records = [item.dict() for response in data for item in response.data]
    # Convert to DataFrame
    return pd.DataFrame(records)


# Load data into DuckDB
def load_into_duckdb(df, db_file="data.duckdb", table_name="api_data"):
    conn = duckdb.connect(db_file)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    conn.close()


# Main function
async def main():
    # Fetch data for the given date range and limit
    raw_data = await fetch_all_data(START_DATE, END_DATE, PAGE_LIMIT, USER_KEY)
    if raw_data:
        df = process_data(raw_data)
        # Ensure column data types match schema
        df = df.astype(
            {
                "id_info": "Int64",  # Supports nullable int
                "barra_info": str,
                "barra_transf": str,
                "fecha": "datetime64[ns]",  # Pandas datetime format
                "hra": "Int64",  # Supports nullable int
                "min": "Int64",  # Supports nullable int
                "cmg_clp_kwh_": float,
                "cmg_usd_mwh_": float,
                "version": str,
                "fecha_hora": "datetime64[ns]",
                "fecha_minuto": "datetime64[ns]",
            }
        )
        load_into_duckdb(df)
        print("Data successfully loaded into DuckDB.")


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
