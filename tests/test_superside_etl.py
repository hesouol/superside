import unittest
from datetime import date
from unittest.mock import patch, Mock
import pandas as pd
from requests import HTTPError

# Enabling test modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import superside_etl


class TestSupersideETL(unittest.TestCase):
    def setUp(self):
        os.environ["CB_API_KEY"] = "fake_key"

    def tearDown(self):
        del os.environ["CB_API_KEY"]

    @patch("requests.Session.request")
    def test_extract(self, mock_request):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "count": 1,
            "entities": [{"name": "fake_entity"}],
        }
        mock_request.return_value = mock_response

        subject = superside_etl.SupersideETL(processing_date=date(2024, 1, 1))
        df, _ = subject.extract(last_id=None)
        _, kwargs = mock_request.call_args

        used_headers = kwargs["headers"]
        used_body = kwargs["data"]

        self.assertEqual(used_headers["X-cb-user-key"], "fake_key")
        self.assertIn(
            '"values": ["2023-12-31"]', used_body
        )  # Check query filter by date
        self.assertNotIn("last_id", used_body)  # First extract must not have last_id
        self.assertEqual(len(df), 1)
        self.assertEqual(subject.total_records, 1)

        subject.extract(last_id="fake_last_id")

        _, kwargs = mock_request.call_args
        used_body = kwargs["data"]
        self.assertIn("last_id", used_body)
        self.assertIn("fake_last_id", used_body)

    @patch("requests.Session.request")
    def test_extract_retries(self, mock_request_2):
        mock_response_2 = Mock()
        mock_response_2.status_code = 500
        mock_response_2.json.return_value = {"error": "fake_error"}
        mock_response_2.raise_for_status = Mock(side_effect=HTTPError("fake_500_error"))
        mock_request_2.return_value = mock_response_2

        subject = superside_etl.SupersideETL(processing_date=None)
        self.assertRaises(HTTPError, subject.extract, last_id=None)
        self.assertEqual(mock_request_2.call_count, subject.retries)

    def test_transform(self):
        subject = superside_etl.SupersideETL(processing_date=None)
        records = [
            {
                "uuid": "fake_uuid",
                "properties": {
                    "permalink": "fake_permalink",
                    "identifier": {
                        "permalink": "fake_permalink",
                        "image_id": None,
                        "uuid": None,
                        "entity_def_id": None,
                        "value": None,
                    },
                    "linkedin": {"value": "fake_linkedin"},
                    "uuid": "fake_uuid",
                    "created_at": "fake_created_at",
                    "location_identifiers": [
                        {
                            "permalink": None,
                            "uuid": None,
                            "location_type": "city",
                            "entity_def_id": None,
                            "value": "fake_city",
                        },
                        {
                            "permalink": None,
                            "uuid": None,
                            "location_type": "region",
                            "entity_def_id": None,
                            "value": "fake_region",
                        },
                        {
                            "permalink": None,
                            "uuid": None,
                            "location_type": "country",
                            "entity_def_id": None,
                            "value": "fake_country",
                        },
                        {
                            "permalink": None,
                            "uuid": None,
                            "location_type": "continent",
                            "entity_def_id": None,
                            "value": "fake_continent",
                        },
                    ],
                    "website_url": "fake_website_url",
                    "updated_at": "fake_updated_at",
                },
            }
        ]
        df = pd.json_normalize(records)
        result_df = subject.transform(df)
        result_record = result_df.iloc[0]

        self.assertEqual(result_record["permalink"], "fake_permalink")
        self.assertEqual(result_record["uuid"], "fake_uuid")
        self.assertEqual(result_record["created_at"], "fake_created_at")
        self.assertEqual(result_record["website_url"], "fake_website_url")
        self.assertEqual(result_record["updated_at"], "fake_updated_at")
        self.assertEqual(result_record["linkedin"], "fake_linkedin")
        self.assertEqual(result_record["country"], "fake_country")
        self.assertEqual(result_record["region"], "fake_region")
        self.assertEqual(result_record["city"], "fake_city")
        self.assertEqual(len(result_df.columns), 9)

    def test_run(self):
        subject = superside_etl.SupersideETL(processing_date=None)
        subject.extract = lambda last_id: (pd.json_normalize([{"uuid": "fake_uuid"}]), 1)  # total_records=1
        subject.transform = lambda df: df
        subject.load = lambda data, context, extraction_date, file_name: self.assertEqual(data.iloc[0]["uuid"], "fake_uuid")
        subject.run()


if __name__ == "__main__":
    unittest.main()
