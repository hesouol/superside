from abc import ABC
import logging
import json
import requests
import os
import pandas as pd
from datetime import date, timedelta

from base_etl import BaseETL

LIMIT = 50
RETRIES = 5


class SupersideETL(BaseETL, ABC):
    def __init__(self, processing_date):
        self.user_key = os.getenv("CB_API_KEY")
        self.total_records = None
        self.limit = LIMIT
        self.retries = RETRIES

        if processing_date is None:
            self.updated_since = date.min
            self.partition_date = date.today()
        else:
            self.updated_since = processing_date - timedelta(days=1)
            self.partition_date = processing_date

        if self.user_key is None:
            raise ValueError(
                """
                   Evironment variable: CB_API_KEY must be set.
                   Please refer to https://data.crunchbase.com/docs/crunchbase-basic-using-api
                   and check how you can get your key.
                """
            )

    # Function to extract specific location type values
    @staticmethod
    def extract_location_value(location_identifiers, location_type):
        if hasattr(location_identifiers, "__iter__"):
            for location in location_identifiers:
                if location["location_type"] == location_type:
                    return location["value"]
        else:
            logging.warning(
                f"location_identifiers {location_identifiers} is not iterable"
            )
        return None

    def extract(self, last_id: str) -> pd.DataFrame:
        url = "https://api.crunchbase.com/api/v4/searches/organizations"

        body = {
            "field_ids": [
                "uuid",
                "linkedin",
                "location_identifiers",
                "website_url",
                "created_at",
                "updated_at",
                "permalink",
            ],
            "order": [{"field_id": "rank_org", "sort": "asc"}],
            "query": [
                {
                    "type": "predicate",
                    "field_id": "updated_at",
                    "operator_id": "gte",  # FIXME: Operator gt is invalid even though it's present in the docs
                    "values": [f"{self.updated_since}"],
                }
            ],
            "limit": self.limit,
        }

        if last_id is not None:
            body["after_id"] = last_id

        payload = json.dumps(body)

        headers = {"X-cb-user-key": self.user_key, "Content-Type": "application/json"}

        error = None
        session = requests.session()

        for retry in range(1, self.retries + 1):
            try:
                response = session.request("POST", url, headers=headers, data=payload)
                response.raise_for_status()
                break
            except Exception as e:  # FIXME: too many different errors from the API
                error = e
                logging.warning(
                    f"Failed to connect to {url}. Try #{retry}/{self.retries}. Exception: {e}")
                session = requests.session()  # refresh session
        else:
            logging.error(f"Failed to connect to {url} after {self.retries} retries. Error: {error}")
            raise error

        data = response.json()
        self.total_records = data["count"]
        df = pd.json_normalize(data["entities"])
        return df

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.rename(
            columns={
                "properties.permalink": "permalink",
                "properties.website_url": "website_url",
                "properties.updated_at": "updated_at",
                "properties.created_at": "created_at",
                "properties.linkedin.value": "linkedin",
            }
        ).drop(
            columns=[
                "properties.identifier.permalink",
                "properties.identifier.image_id",
                "properties.identifier.uuid",
                "properties.identifier.entity_def_id",
                "properties.identifier.value",
                "properties.uuid",
            ]
        )
        # Apply the function to create new columns
        df["city"] = df["properties.location_identifiers"].apply(
            lambda x: SupersideETL.extract_location_value(x, "city")
        )
        df["region"] = df["properties.location_identifiers"].apply(
            lambda x: SupersideETL.extract_location_value(x, "region")
        )
        df["country"] = df["properties.location_identifiers"].apply(
            lambda x: SupersideETL.extract_location_value(x, "country")
        )

        df = df.drop(columns=["properties.location_identifiers"])
        return df

    def run(
        self,
    ):
        loaded_records = 0
        last_id = None
        while self.total_records is None or loaded_records < self.total_records:
            df = self.extract(last_id=last_id)
            df = self.transform(df)
            self.load(
                data=df,
                context="org",
                extraction_date=self.partition_date,
                file_name=f"org_{loaded_records}.parquet",
            )
            last_id = df.iloc[-1]["uuid"]
            loaded_records += LIMIT
            logging.info(f"Collected {loaded_records}/{self.total_records}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Superside ETL")
    parser.add_argument(
        "--processing-date",
        required=False,
        type=date.fromisoformat,
        help="Date used to filter new records (format:YYYY-mm-dd), if not provided all records will be extracted.",
    )
    args = parser.parse_args()
    etl = SupersideETL(processing_date=args.processing_date)
    etl.run()
